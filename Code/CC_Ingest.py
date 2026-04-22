import datetime
from pathlib import Path

import icepython as ice
import pandas as pd

OUT = Path(
    r"C:\Users\virat.arya\ETG\SoftsDatabase - Documents\Database\Hardmine\ICEBREAKER\Certs&Grading\Database"
)
OUT.mkdir(parents=True, exist_ok=True)

TODAY = datetime.date.today().isoformat()
START = "2015-01-01"

FIELD = "Bags Close"


def get_cc_symbols():
    try:
        result = ice.get_search("CC #CER-IFND", rows=500)
        if not result:
            return []
        syms = []
        for row in result:
            sym = str(row[0] if isinstance(row, (list, tuple)) else row).strip()
            if sym.startswith("CC.") and "#CER-IFND" in sym:
                syms.append(sym)
        return syms
    except Exception as exc:
        print(f"  warn search: {exc}")
        return []


def parse_cc_sym(sym):
    inner = sym.replace("CC.", "").replace(" #CER-IFND", "").strip()
    if inner == "TOTAL":
        return ("TOTAL", None, "TOT")
    if inner.startswith("TOTAL") and len(inner) > 5:
        return ("TOTAL", None, inner[5:])
    if len(inner) == 6:
        return (inner[:3], inner[3], inner[4:])
    return None


def fetch_timeseries(symbols):
    if not symbols:
        return pd.DataFrame()
    try:
        result = ice.get_timeseries(
            symbols, [FIELD], granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.DataFrame()
        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df["Time"] = pd.to_datetime(df["Time"])
        df = df.rename(columns={"Time": "Date"})
        df.columns = ["Date"] + [c.replace(f".{FIELD}", "") for c in df.columns[1:]]
        return df
    except Exception as exc:
        print(f"  warn: {exc}")
        return pd.DataFrame()


def fetch_price():
    try:
        result = ice.get_timeseries(
            "%CC 1!", ["Settle"], granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.Series(dtype=float, name="CC_Price")
        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df["Time"] = pd.to_datetime(df["Time"])
        return df.set_index("Time").iloc[:, 0].rename("CC_Price")
    except Exception as exc:
        print(f"  warn CC price: {exc}")
        return pd.Series(dtype=float, name="CC_Price")


def build_cc():
    global START
    out_file = OUT / "cert_cc.parquet"

    # ── Upsert: read existing, fetch only new rows ───────────────────────────
    existing = None
    if out_file.exists():
        try:
            existing = pd.read_parquet(out_file)
            if not existing.empty and "Date" in existing.columns:
                last_date = pd.to_datetime(existing["Date"]).max()
                fetch_from = (last_date + pd.Timedelta(days=1)).date().isoformat()
                print(f"[CC] Existing: {len(existing)} rows, last={last_date.date()}")
                print(f"[CC] Incremental fetch from {fetch_from}")
                START = fetch_from
        except Exception as exc:
            print(f"[CC] warn reading existing parquet: {exc}")

    # ── Discover + fetch symbols ─────────────────────────────────────────────
    print("[CC] Discovering symbols ...")
    all_syms = get_cc_symbols()
    print(f"[CC] Found {len(all_syms)} symbols")

    if not all_syms:
        if existing is not None:
            print("[CC] No symbols found — returning existing.")
            return existing
        print("[CC] No symbols found. Aborting.")
        return

    sym_to_col = {}
    for sym in all_syms:
        parsed = parse_cc_sym(sym)
        if parsed is None:
            continue
        origin, group, port = parsed
        sym_to_col[sym] = f"CC-TOT-{port}" if group is None else f"CC-{origin}-{group}-{port}"

    sym_to_col.pop("CC.TOTAL #CER-IFND", None)
    valid_syms = list(sym_to_col.keys())
    print(f"[CC] Fetching {len(valid_syms)} symbols ...")

    parts = []
    n_batches = -(-len(valid_syms) // 50)
    for i in range(0, len(valid_syms), 50):
        batch = valid_syms[i : i + 50]
        print(f"  batch {i // 50 + 1}/{n_batches}: {batch[0]} ... {batch[-1]}")
        batch_df = fetch_timeseries(batch)
        if not batch_df.empty:
            parts.append(batch_df)

    if not parts:
        if existing is not None:
            print("[CC] No new data — already up to date.")
            return existing
        print("[CC] No data returned.")
        return

    df = parts[0]
    for p in parts[1:]:
        df = df.merge(p, on="Date", how="outer")
    df = df.sort_values("Date").reset_index(drop=True)
    df = df.rename(columns=sym_to_col)

    # ── Origin totals ────────────────────────────────────────────────────────
    origin_groups = set()
    for col in df.columns:
        p = col.split("-")
        if len(p) == 4 and p[0] == "CC" and p[1] != "TOT":
            origin_groups.add((p[1], p[2]))
    for (origin, group) in origin_groups:
        port_cols = [c for c in df.columns if c.startswith(f"CC-{origin}-{group}-") and not c.endswith("-TOT")]
        if port_cols:
            df[f"CC-{origin}-{group}-TOT"] = df[port_cols].sum(axis=1, min_count=1)

    # ── Grand total ──────────────────────────────────────────────────────────
    grand = fetch_timeseries(["CC.TOTAL #CER-IFND"])
    if not grand.empty:
        grand = grand.rename(columns={"CC.TOTAL #CER-IFND": "CC-TOT-TOT"})
        df = df.merge(grand, on="Date", how="left")
    else:
        tot_cols = [c for c in df.columns if c.endswith("-TOT") and c != "CC-TOT-TOT"]
        if tot_cols:
            df["CC-TOT-TOT"] = df[tot_cols].sum(axis=1, min_count=1)

    # ── CC price ─────────────────────────────────────────────────────────────
    print("[CC] Fetching CC price ...")
    cc_price = fetch_price()
    if not cc_price.empty:
        price_df = cc_price.reset_index().rename(columns={"Time": "Date"})
        df = df.merge(price_df, on="Date", how="left")
        df["CC_Price"] = df["CC_Price"].ffill()

    if "CC-TOT-TOT" in df.columns:
        df = df.dropna(subset=["CC-TOT-TOT"])

    # ── Upsert merge ─────────────────────────────────────────────────────────
    if existing is not None and not df.empty:
        n_new = len(df)
        df = pd.concat([existing, df], ignore_index=True)
        df = df.drop_duplicates(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        print(f"[CC] Upserted: {len(df)} rows total ({n_new} new)")
    elif existing is not None:
        print("[CC] No new rows — already up to date.")
        return existing

    df.to_parquet(out_file, index=False)
    print(f"\n[CC] Saved: {out_file}")
    print(f"     Rows: {len(df)} | Cols: {len(df.columns)}")
    if not df.empty:
        print(f"     Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
    return df


if __name__ == "__main__":
    build_cc()

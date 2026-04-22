import datetime
from pathlib import Path

import icepython as ice
import pandas as pd

OUT = Path(
    r"C:\Users\virat.arya\ETG\SoftsDatabase - Documents\Database\Hardmine\ICEBREAKER\Certs&Grading\Database"
)
OUT.mkdir(parents=True, exist_ok=True)

TODAY = datetime.date.today().isoformat()
START = "2020-01-01"
LOOKBACK_DAYS = 90  # re-fetch last N days to catch panel updates

FIELDS = [
    "Panel Date", "Panel Time", "Panel Type", "Port ID", "Origin",
    "Group No Close", "Allowance Close", "Tenderable", "Regrade",
    "Previous Tenderable", "SDU Close", "LDU Close", "BDU Close", "Lots Close",
]

DEDUP_KEYS = ["PublishedDate", "PortId", "Origin", "GroupNumber"]


def get_lcc_grd_symbols():
    try:
        result = ice.get_search("C #GRD-IFND", rows=500)
        syms = []
        for row in (result or []):
            sym = str(row[0] if isinstance(row, (list, tuple)) else row).strip()
            if sym.startswith("C.") and "#GRD-IFND" in sym:
                syms.append(sym)
        return syms
    except Exception as exc:
        print(f"  warn search: {exc}")
        return []


def fetch_grading_panel(sym):
    try:
        result = ice.get_timeseries(
            sym, FIELDS, granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.DataFrame()

        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df = df.rename(columns={"Time": "Date"})
        df.columns = ["Date"] + [c.replace(f"{sym}.", "") for c in df.columns[1:]]
        df = df.dropna(subset=["Lots Close"])
        if df.empty:
            return pd.DataFrame()

        df["Panel Date"] = pd.to_datetime(df["Panel Date"], errors="coerce")
        panel_rows = (
            df.sort_values("Date")
            .groupby(["Panel Date", "Port ID", "Origin", "Group No Close"], dropna=False)
            .last()
            .reset_index()
        )
        return panel_rows[[
            "Panel Date", "Panel Time", "Panel Type", "Port ID", "Origin",
            "Group No Close", "Allowance Close", "Tenderable", "Regrade",
            "Previous Tenderable", "SDU Close", "LDU Close", "BDU Close", "Lots Close",
        ]]
    except Exception as exc:
        print(f"  warn {sym}: {exc}")
        return pd.DataFrame()


def build_lcc_grading():
    global START
    out_file = OUT / "grading_lcc.parquet"

    # ── Upsert: read existing, fetch only recent window to catch updates ─────
    existing = None
    if out_file.exists():
        try:
            existing = pd.read_parquet(out_file)
            if not existing.empty and "PublishedDate" in existing.columns:
                last_date = pd.to_datetime(existing["PublishedDate"]).max()
                lookback_start = (last_date - pd.Timedelta(days=LOOKBACK_DAYS)).date().isoformat()
                fetch_from = max(lookback_start, START)
                print(f"[LCC GRD] Existing: {len(existing)} rows, last={last_date.date()}")
                print(f"[LCC GRD] Fetching from {fetch_from} (last {LOOKBACK_DAYS}d + any new panels)")
                START = fetch_from
        except Exception as exc:
            print(f"[LCC GRD] warn reading existing: {exc}")

    print("[LCC GRD] Discovering symbols ...")
    syms = get_lcc_grd_symbols()
    print(f"[LCC GRD] Found {len(syms)} symbols")

    all_panels = []
    for sym in syms:
        print(f"  Fetching {sym} ...")
        panel_df = fetch_grading_panel(sym)
        if not panel_df.empty:
            print(f"    {len(panel_df)} panels")
            all_panels.append(panel_df)
        else:
            print(f"    no data")

    if not all_panels:
        if existing is not None:
            print("[LCC GRD] No new data — returning existing.")
            return existing
        print("[LCC GRD] No grading data returned.")
        return

    df = pd.concat(all_panels, ignore_index=True)
    df = (
        df.sort_values("Panel Date")
        .drop_duplicates(subset=["Panel Date", "Port ID", "Origin", "Group No Close"], keep="last")
        .reset_index(drop=True)
    )

    df = df.rename(columns={
        "Panel Date": "PublishedDate", "Panel Time": "PanelTime",
        "Panel Type": "PanelType", "Port ID": "PortId",
        "Group No Close": "GroupNumber", "Allowance Close": "TotalAllowance",
        "Previous Tenderable": "PreviousTenderable",
        "SDU Close": "SDU", "LDU Close": "LDU", "BDU Close": "BDU", "Lots Close": "Lots",
    })
    df.insert(0, "Commodity", "LCC")
    df["PublishedDate"] = pd.to_datetime(df["PublishedDate"])
    for col in ["GroupNumber", "TotalAllowance", "Tenderable", "Regrade",
                "PreviousTenderable", "SDU", "LDU", "BDU", "Lots"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.sort_values("PublishedDate").reset_index(drop=True)

    # ── Upsert merge ─────────────────────────────────────────────────────────
    if existing is not None and not df.empty:
        combined = pd.concat([existing, df], ignore_index=True)
        combined = (
            combined.sort_values("PublishedDate")
            .drop_duplicates(subset=DEDUP_KEYS, keep="last")
            .reset_index(drop=True)
        )
        n_new = len(combined) - len(existing)
        print(f"[LCC GRD] Upserted: {len(combined)} rows total ({n_new} new/updated)")
        df = combined

    df.to_parquet(out_file, index=False)
    print(f"\n[LCC GRD] Saved: {out_file}")
    print(f"          Rows: {len(df)} | Date range: {df['PublishedDate'].min().date()} to {df['PublishedDate'].max().date()}")
    return df


if __name__ == "__main__":
    build_lcc_grading()

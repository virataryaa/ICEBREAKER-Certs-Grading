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

PORT_MAP = {
    "AMS": "C.AMS #STO-IFND", "ANT": "C.ANT #STO-IFND", "BAR": "C.BAR #STO-IFND",
    "BRE": "C.BRE #STO-IFND", "GEN": "C.GEN #STO-IFND", "HAM": "C.HAM #STO-IFND",
    "LEH": "C.LEH #STO-IFND", "LIV": "C.LIV #STO-IFND", "LON": "C.LON #STO-IFND",
    "MAR": "C.MAR #STO-IFND", "NOR": "C.NOR #STO-IFND", "ROT": "C.ROT #STO-IFND",
    "TRI": "C.TRI #STO-IFND", "TOT": "C.TOTAL #STO-IFND",
}

FIELD_SUFFIX = {
    "Total Valid Close":                    "TOTAL",
    "SDU With Perpetual Valid Cert Close":  "SDU_PERP",
    "SDU With Initial Valid Cert Close":    "SDU_INIT",
    "SDU With Exp Cert Close":              "SDU_EXP",
    "SDU Non Tendered Close":               "SDU_NONTEND",
    "SDU Suspended Close":                  "SDU_SUSP",
    "LDU With Perpetual Valid Cert Close":  "LDU_PERP",
    "LDU With Initial Valid Cert Close":    "LDU_INIT",
    "LDU With Exp Cert Close":              "LDU_EXP",
    "LDU Non Tendered Close":               "LDU_NONTEND",
    "LDU Suspended Close":                  "LDU_SUSP",
    "BDU With Perpetual Valid Cert Close":  "BDU_PERP",
    "BDU With Initial Valid Cert Close":    "BDU_INIT",
    "BDU With Exp Cert Close":              "BDU_EXP",
    "BDU Non Tendered Close":               "BDU_NONTEND",
    "BDU Suspended Close":                  "BDU_SUSP",
}

ALL_FIELDS = list(FIELD_SUFFIX.keys())


def fetch_timeseries(symbols):
    if not symbols:
        return pd.DataFrame()
    try:
        result = ice.get_timeseries(
            symbols, ALL_FIELDS, granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.DataFrame()
        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df["Time"] = pd.to_datetime(df["Time"])
        df = df.rename(columns={"Time": "Date"})
        return df
    except Exception as exc:
        print(f"  warn: {exc}")
        return pd.DataFrame()


def fetch_price():
    try:
        result = ice.get_timeseries(
            "%LCC 1!-ICE", ["Settle"], granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.Series(dtype=float, name="LCC_Price")
        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df["Time"] = pd.to_datetime(df["Time"])
        return df.set_index("Time").iloc[:, 0].rename("LCC_Price")
    except Exception as exc:
        print(f"  warn LCC price: {exc}")
        return pd.Series(dtype=float, name="LCC_Price")


def build_lcc():
    global START
    out_file = OUT / "cert_lcc.parquet"

    # ── Upsert: read existing, fetch only new rows ───────────────────────────
    existing = None
    if out_file.exists():
        try:
            existing = pd.read_parquet(out_file)
            if not existing.empty and "Date" in existing.columns:
                last_date = pd.to_datetime(existing["Date"]).max()
                fetch_from = (last_date + pd.Timedelta(days=1)).date().isoformat()
                print(f"[LCC] Existing: {len(existing)} rows, last={last_date.date()}")
                print(f"[LCC] Incremental fetch from {fetch_from}")
                START = fetch_from
        except Exception as exc:
            print(f"[LCC] warn reading existing parquet: {exc}")

    all_syms = list(PORT_MAP.values())
    print(f"[LCC] Fetching {len(all_syms)} port symbols ...")

    parts = []
    for i in range(0, len(all_syms), 10):
        batch = all_syms[i : i + 10]
        print(f"  batch {i // 10 + 1}: {batch[0]} ... {batch[-1]}")
        batch_df = fetch_timeseries(batch)
        if not batch_df.empty:
            parts.append(batch_df)

    if not parts:
        if existing is not None:
            print("[LCC] No new data — already up to date.")
            return existing
        print("[LCC] No data returned.")
        return

    df = parts[0]
    for p in parts[1:]:
        df = df.merge(p, on="Date", how="outer")
    df = df.sort_values("Date").reset_index(drop=True)

    rename_map = {}
    for port, sym in PORT_MAP.items():
        for field, suffix in FIELD_SUFFIX.items():
            rename_map[f"{sym}.{field}"] = f"LCC-{port}-{suffix}"
    df = df.rename(columns=rename_map)

    tot_col = "LCC-TOT-TOTAL"
    if tot_col in df.columns:
        df = df.dropna(subset=[tot_col])

    print("[LCC] Fetching LCC price ...")
    lcc_price = fetch_price()
    if not lcc_price.empty:
        price_df = lcc_price.reset_index().rename(columns={"Time": "Date"})
        df = df.merge(price_df, on="Date", how="left")
        df["LCC_Price"] = df["LCC_Price"].ffill()

    # ── Upsert merge ─────────────────────────────────────────────────────────
    if existing is not None and not df.empty:
        n_new = len(df)
        df = pd.concat([existing, df], ignore_index=True)
        df = df.drop_duplicates(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        print(f"[LCC] Upserted: {len(df)} rows total ({n_new} new)")
    elif existing is not None:
        print("[LCC] No new rows — already up to date.")
        return existing

    df.to_parquet(out_file, index=False)
    print(f"\n[LCC] Saved: {out_file}")
    print(f"      Rows: {len(df)} | Cols: {len(df.columns)}")
    if not df.empty:
        print(f"      Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
    return df


if __name__ == "__main__":
    build_lcc()

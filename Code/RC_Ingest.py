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

# ---------------------------------------------------------------------------
# Symbol map: output column -> ICE symbol
# Column names match existing cert_lrc.parquet so app.py reads without change
# ICE adds LEH (Leherhafen) and MAR (Marseille) vs LSEG
# ---------------------------------------------------------------------------

PORT_MAP = {
    "LRC-AMS-VG": "RC.AMS #STO-IFND",
    "LRC-ANT-VG": "RC.ANT #STO-IFND",
    "LRC-BAR-VG": "RC.BAR #STO-IFND",
    "LRC-BRE-VG": "RC.BRE #STO-IFND",
    "LRC-FEL-VG": "RC.FEL #STO-IFND",
    "LRC-GEN-VG": "RC.GEN #STO-IFND",
    "LRC-HAM-VG": "RC.HAM #STO-IFND",
    "LRC-LIV-VG": "RC.LIV #STO-IFND",
    "LRC-LON-VG": "RC.LON #STO-IFND",
    "LRC-NOR-VG": "RC.NOR #STO-IFND",
    "LRC-ROT-VG": "RC.ROT #STO-IFND",
    "LRC-TRI-VG": "RC.TRI #STO-IFND",
    # New vs LSEG
    "LRC-LEH-VG": "RC.LEH #STO-IFND",
    "LRC-MAR-VG": "RC.MAR #STO-IFND",
}

TOTAL_COL = "LRC-TOT-VG"
TOTAL_SYM = "RC.TOTAL #STO-IFND"
FIELD     = "Lots With Val Cert Close"


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

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
            "%RC 1!-ICE", ["Settle"], granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.Series(dtype=float, name="RC_Price")
        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df["Time"] = pd.to_datetime(df["Time"])
        return df.set_index("Time").iloc[:, 0].rename("RC_Price")
    except Exception as exc:
        print(f"  warn RC price: {exc}")
        return pd.Series(dtype=float, name="RC_Price")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_rc():
    # --- 1. Fetch all port symbols + total ----------------------------------
    all_syms = list(PORT_MAP.values()) + [TOTAL_SYM]
    print(f"[RC] Fetching {len(all_syms)} port symbols ...")

    parts = []
    for i in range(0, len(all_syms), 50):
        batch = all_syms[i : i + 50]
        print(f"  batch {i // 50 + 1}: {batch[0]} ... {batch[-1]}")
        batch_df = fetch_timeseries(batch)
        if not batch_df.empty:
            parts.append(batch_df)

    if not parts:
        print("[RC] No data returned.")
        return

    df = parts[0]
    for p in parts[1:]:
        df = df.merge(p, on="Date", how="outer")
    df = df.sort_values("Date").reset_index(drop=True)

    # Rename ICE symbols -> output column names
    reverse_map = {v: k for k, v in PORT_MAP.items()}
    reverse_map[TOTAL_SYM] = TOTAL_COL
    df = df.rename(columns=reverse_map)

    # --- 2. RC price --------------------------------------------------------
    print("[RC] Fetching RC price ...")
    rc_price = fetch_price()
    if not rc_price.empty:
        price_df = rc_price.reset_index().rename(columns={"Time": "Date"})
        df = df.merge(price_df, on="Date", how="left")
        df["RC_Price"] = df["RC_Price"].ffill()

    # --- 3. Drop rows where total is missing --------------------------------
    if TOTAL_COL in df.columns:
        df = df.dropna(subset=[TOTAL_COL])

    # --- 4. Save ------------------------------------------------------------
    out_file = OUT / "cert_lrc.parquet"
    df.to_parquet(out_file, index=False)
    print(f"[RC] Saved: {out_file}")
    print(f"     Rows: {len(df)} | Cols: {len(df.columns)}")
    print(f"     Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
    return df


if __name__ == "__main__":
    build_rc()

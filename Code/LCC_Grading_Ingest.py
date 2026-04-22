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

FIELDS = [
    "Panel Date",
    "Panel Time",
    "Panel Type",
    "Port ID",
    "Origin",
    "Group No Close",
    "Allowance Close",
    "Tenderable",
    "Regrade",
    "Previous Tenderable",
    "SDU Close",
    "LDU Close",
    "BDU Close",
    "Lots Close",
]


def get_lcc_grd_symbols():
    """Return all C.N #GRD-IFND symbols (London Cocoa only, exclude RC)."""
    try:
        result = ice.get_search("C #GRD-IFND", rows=500)
        syms = []
        for row in (result or []):
            sym = str(row[0] if isinstance(row, (list, tuple)) else row).strip()
            # Must start with "C." but NOT "CC." or "RC."
            if sym.startswith("C.") and "#GRD-IFND" in sym:
                syms.append(sym)
        return syms
    except Exception as exc:
        print(f"  warn search: {exc}")
        return []


def fetch_grading_panel(sym):
    """Pull full timeseries for one GRD symbol, return one row per unique panel."""
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
        df = df.sort_values("Date")

        # One row per unique panel — keep most recent observation
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
        print("[LCC GRD] No grading data returned.")
        return

    df = pd.concat(all_panels, ignore_index=True)

    # Deduplicate: same panel in multiple symbols — keep latest lot count
    df = (
        df.sort_values("Panel Date")
        .drop_duplicates(
            subset=["Panel Date", "Port ID", "Origin", "Group No Close"], keep="last"
        )
        .reset_index(drop=True)
    )

    # Rename columns to match grading screenshot format
    df = df.rename(columns={
        "Panel Date":       "PublishedDate",
        "Panel Time":       "PanelTime",
        "Panel Type":       "PanelType",
        "Port ID":          "PortId",
        "Group No Close":   "GroupNumber",
        "Allowance Close":  "TotalAllowance",
        "Previous Tenderable": "PreviousTenderable",
        "SDU Close":        "SDU",
        "LDU Close":        "LDU",
        "BDU Close":        "BDU",
        "Lots Close":       "Lots",
    })

    df.insert(0, "Commodity", "LCC")

    df["PublishedDate"] = pd.to_datetime(df["PublishedDate"])
    for col in ["GroupNumber", "TotalAllowance", "Tenderable", "Regrade",
                "PreviousTenderable", "SDU", "LDU", "BDU", "Lots"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("PublishedDate").reset_index(drop=True)

    out_file = OUT / "grading_lcc.parquet"
    df.to_parquet(out_file, index=False)
    print(f"\n[LCC GRD] Saved: {out_file}")
    print(f"          Rows: {len(df)} | Date range: {df['PublishedDate'].min().date()} to {df['PublishedDate'].max().date()}")
    return df


if __name__ == "__main__":
    df = build_lcc_grading()

    if df is not None:
        print("\n--- Latest Grading Panels ---")
        latest = df.sort_values("PublishedDate", ascending=False)
        print(latest[["PublishedDate", "PortId", "Origin", "GroupNumber",
                       "Tenderable", "SDU", "LDU", "BDU"]].head(20).to_string(index=False))

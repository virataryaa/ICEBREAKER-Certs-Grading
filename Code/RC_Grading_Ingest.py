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
    "Lots Close",
    "Panel Date",
    "Panel Time",
    "Port ID",
    "Origin",
    "Class",
    "Tenderable",
    "Allowance Close",
    "UKContUS",
]


def fetch_grading_panel(sym):
    """Pull full timeseries for one GRD symbol, return tidy long-format rows."""
    try:
        result = ice.get_timeseries(
            sym, FIELDS, granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.DataFrame()

        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df = df.rename(columns={"Time": "Date"})

        # Strip symbol prefix from column names
        df.columns = ["Date"] + [c.replace(f"{sym}.", "") for c in df.columns[1:]]

        # Drop rows with no lot data
        df = df.dropna(subset=["Lots Close"])
        if df.empty:
            return pd.DataFrame()

        # Each symbol has ONE panel date (fixed metadata) but daily lot counts.
        # Take the last known value for each unique panel date → one row per panel.
        df["Panel Date"] = pd.to_datetime(df["Panel Date"], errors="coerce")
        df = df.sort_values("Date")

        # Collapse: for each unique (Panel Date, Port ID, Origin, Class),
        # keep the most recent Lots Close observation.
        panel_rows = (
            df.sort_values("Date")
            .groupby(["Panel Date", "Port ID", "Origin", "Class"], dropna=False)
            .last()
            .reset_index()
        )

        return panel_rows[["Panel Date", "Panel Time", "Port ID", "Origin",
                            "Class", "Tenderable", "Allowance Close", "UKContUS", "Lots Close"]]

    except Exception as exc:
        print(f"  warn {sym}: {exc}")
        return pd.DataFrame()


def build_rc_grading():
    all_panels = []

    for i in range(1, 13):
        sym = f"RC.{i} #GRD-IFND"
        print(f"[GRD] Fetching {sym} ...")
        panel_df = fetch_grading_panel(sym)
        if not panel_df.empty:
            print(f"       {len(panel_df)} panels found")
            all_panels.append(panel_df)
        else:
            print(f"       no data")

    if not all_panels:
        print("[GRD] No grading data returned.")
        return

    df = pd.concat(all_panels, ignore_index=True)

    # Deduplicate: if same panel appears in multiple symbols, keep latest lot count
    df = (
        df.sort_values("Panel Date")
        .drop_duplicates(subset=["Panel Date", "Port ID", "Origin", "Class"], keep="last")
        .reset_index(drop=True)
    )

    # Rename to match existing RC_Grading.xlsx / app.py column names exactly
    df = df.rename(columns={
        "Panel Date":     "PanelDate",
        "Panel Time":     "PanelTime",
        "Port ID":        "PortId",
        "Allowance Close":"Allowance",
        "Lots Close":     "NoLots",
    })

    # Convert Tenderable: non-zero -> "Y", zero -> "N" (matches Excel format)
    df["Tenderable"] = df["Tenderable"].apply(
        lambda v: "Y" if pd.notna(v) and float(v) != 0 else "N"
    )

    # Add Commodity column (matches Excel)
    df.insert(0, "Commodity", "RC")

    # Final column order matching RC_Grading.xlsx
    df = df[["Commodity", "UKContUS", "PanelDate", "PanelTime",
             "Origin", "PortId", "Class", "Tenderable", "Allowance", "NoLots"]]

    df["PanelDate"] = pd.to_datetime(df["PanelDate"])
    df["NoLots"]    = pd.to_numeric(df["NoLots"], errors="coerce")
    df["Allowance"] = pd.to_numeric(df["Allowance"], errors="coerce")
    df = df.sort_values("PanelDate").reset_index(drop=True)

    out_file = OUT / "grading_rc.parquet"
    df.to_parquet(out_file, index=False)
    print(f"\n[GRD] Saved: {out_file}")
    print(f"      Rows: {len(df)} | Date range: {df['PanelDate'].min().date()} to {df['PanelDate'].max().date()}")
    return df


if __name__ == "__main__":
    df = build_rc_grading()

    if df is not None:
        print("\n--- Latest Grading Panels ---")
        latest = df.sort_values("PanelDate", ascending=False)
        print(latest[["PanelDate", "PortId", "Origin", "Class", "Tenderable", "NoLots"]]
              .head(20).to_string(index=False))

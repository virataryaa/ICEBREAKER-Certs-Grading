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
    "Lots Close", "Panel Date", "Panel Time", "Port ID",
    "Origin", "Class", "Tenderable", "Allowance Close", "UKContUS",
]

DEDUP_KEYS = ["PanelDate", "PortId", "Origin", "Class"]


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
    global START
    out_file = OUT / "grading_rc.parquet"

    # ── Upsert: read existing, fetch only recent window to catch updates ─────
    existing = None
    if out_file.exists():
        try:
            existing = pd.read_parquet(out_file)
            if not existing.empty and "PanelDate" in existing.columns:
                last_date = pd.to_datetime(existing["PanelDate"]).max()
                lookback_start = (last_date - pd.Timedelta(days=LOOKBACK_DAYS)).date().isoformat()
                fetch_from = max(lookback_start, START)
                print(f"[RC GRD] Existing: {len(existing)} rows, last={last_date.date()}")
                print(f"[RC GRD] Fetching from {fetch_from} (last {LOOKBACK_DAYS}d + any new panels)")
                START = fetch_from
        except Exception as exc:
            print(f"[RC GRD] warn reading existing: {exc}")

    all_panels = []
    for i in range(1, 13):
        sym = f"RC.{i} #GRD-IFND"
        print(f"[RC GRD] Fetching {sym} ...")
        panel_df = fetch_grading_panel(sym)
        if not panel_df.empty:
            print(f"         {len(panel_df)} panels")
            all_panels.append(panel_df)
        else:
            print(f"         no data")

    if not all_panels:
        if existing is not None:
            print("[RC GRD] No new data — returning existing.")
            return existing
        print("[RC GRD] No grading data returned.")
        return

    df = pd.concat(all_panels, ignore_index=True)
    df = (
        df.sort_values("Panel Date")
        .drop_duplicates(subset=["Panel Date", "Port ID", "Origin", "Class"], keep="last")
        .reset_index(drop=True)
    )

    df = df.rename(columns={
        "Panel Date": "PanelDate", "Panel Time": "PanelTime",
        "Port ID": "PortId", "Allowance Close": "Allowance", "Lots Close": "NoLots",
    })
    df["Tenderable"] = df["Tenderable"].apply(
        lambda v: "Y" if pd.notna(v) and float(v) != 0 else "N"
    )
    df.insert(0, "Commodity", "RC")
    df = df[["Commodity", "UKContUS", "PanelDate", "PanelTime",
             "Origin", "PortId", "Class", "Tenderable", "Allowance", "NoLots"]]
    df["PanelDate"] = pd.to_datetime(df["PanelDate"])
    df["NoLots"]    = pd.to_numeric(df["NoLots"], errors="coerce")
    df["Allowance"] = pd.to_numeric(df["Allowance"], errors="coerce")
    df = df.sort_values("PanelDate").reset_index(drop=True)

    # ── Upsert merge ─────────────────────────────────────────────────────────
    if existing is not None and not df.empty:
        # new data takes precedence (keep="last" after concat [existing, new])
        combined = pd.concat([existing, df], ignore_index=True)
        combined = (
            combined.sort_values("PanelDate")
            .drop_duplicates(subset=DEDUP_KEYS, keep="last")
            .reset_index(drop=True)
        )
        n_new = len(combined) - len(existing)
        print(f"[RC GRD] Upserted: {len(combined)} rows total ({n_new} new/updated)")
        df = combined

    df.to_parquet(out_file, index=False)
    print(f"\n[RC GRD] Saved: {out_file}")
    print(f"         Rows: {len(df)} | Date range: {df['PanelDate'].min().date()} to {df['PanelDate'].max().date()}")
    return df


if __name__ == "__main__":
    build_rc_grading()

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

# ---------------------------------------------------------------------------
# Symbol maps
# ---------------------------------------------------------------------------

# Short code (matches existing parquet/app.py) -> ICE 3-letter country prefix
ORIGIN_ICE = {
    "BRZ": "BRA",
    "BUR": "BDI",
    "COL": "COL",
    "COS": "CRI",
    "ELS": "SLV",
    "HON": "HND",
    "IND": "IND",
    "MEX": "MEX",
    "NIC": "NIC",
    "PAN": "PAN",
    "PER": "PER",
    "RWA": "RWA",
    "TAN": "TZA",
    "UGA": "UGA",
    "VEN": "VEN",
    "GUA": "GTM",
    # New origins ICE has that LSEG didn't
    "DOM": "DOM",
    "ECU": "ECU",
    "KEN": "KEN",
    "PNG": "PNG",
}

# Port short code -> ICE port suffix
PORT_ICE = {
    "AN": "BEANR",
    "BA": "ESBCN",
    "BR": "DEBRE",   # new vs LSEG
    "HA": "DEHAM",
    "HO": "USHOU",
    "MI": "USMIA",
    "NO": "USMSY",
    "NY": "USNYC",
    "VA": "USHNV",   # new vs LSEG
}

ALL_ORIGINS = list(ORIGIN_ICE.keys())
ALL_PORTS   = list(PORT_ICE.keys())


def ice_sym(origin_short, port_short):
    """Build ICE symbol for a given origin/port short code."""
    return f"KC.{ORIGIN_ICE[origin_short]}{PORT_ICE[port_short]} #CER-IFND"


def ice_total_sym(port_short):
    """ICE TOTAL-origin symbol for a port (gives port-level total)."""
    return f"KC.TOTAL{PORT_ICE[port_short]} #CER-IFND"


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_timeseries(symbols, field="Bags Close"):
    """Pull daily timeseries for a list of ICE symbols, return wide DataFrame."""
    if not symbols:
        return pd.DataFrame()
    try:
        result = ice.get_timeseries(
            symbols, [field], granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.DataFrame()
        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df["Time"] = pd.to_datetime(df["Time"])
        df = df.rename(columns={"Time": "Date"})
        # Strip the field suffix from column names: "KC.BRABEANR #CER-IFND.Bags Close" -> "KC.BRABEANR #CER-IFND"
        df.columns = ["Date"] + [c.replace(f".{field}", "") for c in df.columns[1:]]
        return df
    except Exception as exc:
        print(f"  warn: {exc}")
        return pd.DataFrame()


def fetch_price():
    """KC front-month continuous settle price."""
    try:
        result = ice.get_timeseries(
            "%KC 1!", ["Settle"], granularity="D", start_date=START, end_date=TODAY
        )
        if not result or len(result) < 2:
            return pd.Series(dtype=float, name="KC_Price")
        df = pd.DataFrame(list(result[1:]), columns=result[0])
        df["Time"] = pd.to_datetime(df["Time"])
        return df.set_index("Time").iloc[:, 0].rename("KC_Price")
    except Exception as exc:
        print(f"  warn KC price: {exc}")
        return pd.Series(dtype=float, name="KC_Price")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_kc():
    # --- 1. Fetch all origin × port symbols in batches of 50 ---------------
    print("[KC] Fetching certified stocks by origin × port ...")
    sym_to_col = {}   # ICE symbol -> output column name
    for origin in ALL_ORIGINS:
        for port in ALL_PORTS:
            sym = ice_sym(origin, port)
            col = f"KC-{origin}-{port}"
            sym_to_col[sym] = col

    all_syms = list(sym_to_col.keys())
    parts = []
    for i in range(0, len(all_syms), 50):
        batch = all_syms[i : i + 50]
        print(f"  batch {i // 50 + 1}/{-(-len(all_syms) // 50)}: {batch[0]} ... {batch[-1]}")
        batch_df = fetch_timeseries(batch)
        if not batch_df.empty:
            parts.append(batch_df)

    if not parts:
        print("[KC] No cert data returned.")
        return

    certs = parts[0]
    for p in parts[1:]:
        certs = certs.merge(p, on="Date", how="outer")
    certs = certs.sort_values("Date").reset_index(drop=True)

    # Rename ICE symbols -> readable column names
    certs = certs.rename(columns=sym_to_col)

    # --- 2. Origin totals (sum across all ports) ----------------------------
    print("[KC] Computing origin totals ...")
    for origin in ALL_ORIGINS:
        port_cols = [f"KC-{origin}-{p}" for p in ALL_PORTS if f"KC-{origin}-{p}" in certs.columns]
        certs[f"KC-{origin}-TOT"] = certs[port_cols].sum(axis=1, min_count=1)

    # --- 3. Port totals from ICE TOTAL-origin symbols -----------------------
    print("[KC] Fetching port totals (TOTAL origin) ...")
    tot_syms = {ice_total_sym(p): f"KC-TOT-{p}" for p in ALL_PORTS}
    tot_df = fetch_timeseries(list(tot_syms.keys()))
    if not tot_df.empty:
        tot_df = tot_df.rename(columns=tot_syms)
        certs = certs.merge(tot_df, on="Date", how="left")

    # Grand total
    print("[KC] Fetching grand total ...")
    grand = fetch_timeseries(["KC.TOTAL #CER-IFND"])
    if not grand.empty:
        grand = grand.rename(columns={"KC.TOTAL #CER-IFND": "KC-TOT-TOT"})
        certs = certs.merge(grand, on="Date", how="left")

    # --- 4. Grading columns (stub — pending ICE confirmation) ---------------
    grade_ports = ["AN", "HA", "HO", "MI", "NO", "NY"]
    for p in grade_ports:
        certs[f"KC-{p}-PASSGRAD"] = pd.NA
        certs[f"KC-{p}-FAILGRAD"] = pd.NA
    certs["KC-TOT-PASSGRAD"] = pd.NA
    certs["KC-TOT-FAILGRAD"] = pd.NA
    certs["KC-TOT-TOTGRADE"] = pd.NA
    certs["KC-TOT-PENDING"]  = pd.NA

    # --- 5. KC price --------------------------------------------------------
    print("[KC] Fetching KC price ...")
    kc_price = fetch_price()
    if not kc_price.empty:
        certs["KC_Price"] = kc_price.reindex(certs["Date"].values, method="ffill").values

    # --- 6. Drop rows where grand total is missing --------------------------
    if "KC-TOT-TOT" in certs.columns:
        certs = certs.dropna(subset=["KC-TOT-TOT"])

    # --- 7. Save ------------------------------------------------------------
    out_file = OUT / "cert_kc.parquet"
    certs.to_parquet(out_file, index=False)
    print(f"[KC] Saved: {out_file}")
    print(f"     Rows: {len(certs)} | Cols: {len(certs.columns)}")
    print(f"     Date range: {certs['Date'].min().date()} to {certs['Date'].max().date()}")
    return certs


if __name__ == "__main__":
    build_kc()

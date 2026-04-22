import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(layout="wide")

BASE = Path(__file__).resolve().parent.parent
DB = BASE / "Database"

for f_name in ["cert_lrc.parquet", "cert_kc.parquet", "cert_cc.parquet", "cert_lcc.parquet", "grading_rc.parquet", "grading_lcc.parquet"]:
    if not (DB / f_name).exists():
        st.error(f"Missing: {DB / f_name}")
        st.stop()

# ── Load data ──────────────────────────────────────────────────────────────────
cert_lrc = pd.read_parquet(DB / "cert_lrc.parquet")
cert_lrc["Date"] = pd.to_datetime(cert_lrc["Date"])
cert_lrc = cert_lrc.sort_values("Date")

cert_kc = pd.read_parquet(DB / "cert_kc.parquet")
cert_kc["Date"] = pd.to_datetime(cert_kc["Date"])
cert_kc = cert_kc.sort_values("Date")

cert_cc = pd.read_parquet(DB / "cert_cc.parquet")
cert_cc["Date"] = pd.to_datetime(cert_cc["Date"])
cert_cc = cert_cc.sort_values("Date")

cert_lcc = pd.read_parquet(DB / "cert_lcc.parquet")
cert_lcc["Date"] = pd.to_datetime(cert_lcc["Date"])
cert_lcc = cert_lcc.sort_values("Date")

grading_rc = pd.read_parquet(DB / "grading_rc.parquet")
grading_rc["PanelDate"] = pd.to_datetime(grading_rc["PanelDate"])
grading_rc["NoLots"] = pd.to_numeric(grading_rc["NoLots"], errors="coerce").fillna(0)
grading_rc["PortId"] = grading_rc["PortId"].astype(str).str.strip()
grading_rc["Origin"] = grading_rc["Origin"].astype(str).str.strip()
grading_rc["Class"] = grading_rc["Class"].astype(str).str.strip()
grading_rc = grading_rc.dropna(subset=["PanelDate"]).sort_values("PanelDate")

grading_lcc = pd.read_parquet(DB / "grading_lcc.parquet")
grading_lcc["PublishedDate"] = pd.to_datetime(grading_lcc["PublishedDate"])
grading_lcc["PortId"] = grading_lcc["PortId"].astype(str).str.strip()
grading_lcc["Origin"] = grading_lcc["Origin"].astype(str).str.strip()
grading_lcc["TotalLots"] = grading_lcc[["SDU", "LDU", "BDU"]].sum(axis=1)
grading_lcc = grading_lcc.dropna(subset=["PublishedDate"]).sort_values("PublishedDate")

# ── LRC mappings ───────────────────────────────────────────────────────────────
LRC_TABLE_PORT_MAP = {
    "LRC-AMS-VG": "AMS", "LRC-ANT-VG": "ANT", "LRC-BAR-VG": "BAR",
    "LRC-BRE-VG": "BRE", "LRC-FEL-VG": "FEL", "LRC-GEN-VG": "GEN",
    "LRC-HAM-VG": "HAM", "LRC-LIV-VG": "LIV", "LRC-LON-VG": "LON",
    "LRC-NOR-VG": "NOR", "LRC-ROT-VG": "ROT", "LRC-TRI-VG": "TRI",
    "LRC-LEH-VG": "LEH", "LRC-MAR-VG": "MAR",
}
LRC_CHART_PORT_MAP = {
    "LRC-ANT-VG": "ANT", "LRC-LON-VG": "LON",
    "LRC-FEL-VG": "FEL", "LRC-BAR-VG": "BAR",
}
LRC_TOTAL_COL = "LRC-TOT-VG"
LRC_TABLE_COLS = [LRC_TOTAL_COL, *LRC_TABLE_PORT_MAP.keys()]

# ── KC mappings ────────────────────────────────────────────────────────────────
KC_ORIGIN_NAMES = {
    "BRZ": "Brazil", "BUR": "Burundi", "COL": "Colombia", "COS": "Costa Rica",
    "DOM": "Dominican Rep.", "ECU": "Ecuador", "ELS": "El Salvador",
    "GUA": "Guatemala", "HON": "Honduras", "IND": "India", "KEN": "Kenya",
    "MEX": "Mexico", "NIC": "Nicaragua", "PAN": "Panama", "PER": "Peru",
    "PNG": "Papua New Guinea", "RWA": "Rwanda", "TAN": "Tanzania",
    "UGA": "Uganda", "VEN": "Venezuela", "TOT": "Total",
}
KC_PORT_NAMES = {
    "AN": "Antwerp", "BA": "Barcelona", "BR": "Bremen", "HA": "Hamburg",
    "HO": "Houston", "MI": "Miami", "NO": "New Orleans", "NY": "New York",
    "VA": "Virginia", "TOT": "Total",
}
KC_GRADE_PORTS = ["AN", "HA", "HO", "MI", "NO", "NY"]
KC_ORIGINS_NO_TOT = [o for o in KC_ORIGIN_NAMES if o != "TOT"]
KC_PORTS_NO_TOT = [p for p in KC_PORT_NAMES if p != "TOT"]

# ── CC mappings ────────────────────────────────────────────────────────────────
CC_TOTAL_COL = "CC-TOT-TOT"
CC_PORT_MAP = {
    "CC-TOT-AL": "Albany", "CC-TOT-BA": "Baltimore", "CC-TOT-DR": "Del. River",
    "CC-TOT-HR": "Hampton Rds", "CC-TOT-NY": "New York",
}
CC_CHART_PORTS = list(CC_PORT_MAP.keys())
CC_ORIGIN_NAMES = {
    "ARR": "Arriba", "BOL": "Bolivia", "BRA": "Brazil", "CIV": "Côte d'Ivoire",
    "CMR": "Cameroon", "COD": "DR Congo", "COG": "Congo", "COL": "Colombia",
    "CRI": "Costa Rica", "ECU": "Ecuador", "GHA": "Ghana", "GNQ": "Eq. Guinea",
    "GRD": "Grenada", "GTM": "Guatemala", "HND": "Honduras", "HTI": "Haiti",
    "IDN": "Indonesia", "JAM": "Jamaica", "LBR": "Liberia", "LKA": "Sri Lanka",
    "MDG": "Madagascar", "MEX": "Mexico", "MYS": "Malaysia", "NGA": "Nigeria",
    "NIC": "Nicaragua", "PAN": "Panama", "PER": "Peru", "PHL": "Philippines",
    "PNG": "Papua New Guinea", "SLB": "Solomon Isl.", "SLE": "Sierra Leone",
    "SLV": "El Salvador", "SNZ": "St. Niña", "STP": "São Tomé",
    "SUR": "Suriname", "TGO": "Togo", "TTO": "Trinidad & Tobago",
    "TZA": "Tanzania", "UGA": "Uganda", "VEN": "Venezuela", "VNM": "Vietnam",
    "VUT": "Vanuatu", "WSM": "Samoa", "ZHS": "S. America",
}

# ── LCC mappings ───────────────────────────────────────────────────────────────
LCC_TOTAL_COL = "LCC-TOT-TOTAL"
LCC_PORTS = ["AMS", "ANT", "BAR", "BRE", "GEN", "HAM", "LEH", "LIV", "LON", "MAR", "NOR", "ROT", "TRI"]
LCC_PORT_NAMES = {
    "AMS": "Amsterdam", "ANT": "Antwerp", "BAR": "Barcelona", "BRE": "Bremen",
    "GEN": "Genoa", "HAM": "Hamburg", "LEH": "Le Havre", "LIV": "Liverpool",
    "LON": "London", "MAR": "Marseille", "NOR": "Noorderham", "ROT": "Rotterdam",
    "TRI": "Trieste",
}
LCC_PORT_TOTAL_MAP = {f"LCC-{p}-TOTAL": p for p in LCC_PORTS}
LCC_TABLE_COLS = [LCC_TOTAL_COL, *LCC_PORT_TOTAL_MAP.keys()]

# ── Shared helper functions ────────────────────────────────────────────────────
def fmt_change(value):
    if pd.isna(value):
        return ""
    return f"{int(value):,}"


def safe_num(value, default=0):
    return default if pd.isna(value) else value


def safe_int(value, default=0):
    return int(safe_num(value, default))


def style_change_value(value):
    if pd.isna(value):
        return ""
    if value > 0:
        return "background-color: #e8f5e9; color: #1b5e20;"
    if value < 0:
        return "background-color: #ffebee; color: #b71c1c;"
    return "color: #5f6b7a;"


def style_total_col(series):
    if series.name not in {"TOT", "Total", "TOTAL"}:
        return ["" for _ in series]
    styled = []
    for value in series:
        base = "font-weight: 700; background-color: #fff3cd; color: #3b2f00;"
        if pd.isna(value):
            styled.append(base)
        elif value > 0:
            styled.append(base + " border-left: 3px solid #2e7d32;")
        elif value < 0:
            styled.append(base + " border-left: 3px solid #c62828;")
        else:
            styled.append(base + " border-left: 3px solid #b08900;")
    return styled


def plain_total_col_style(series):
    if series.name not in {"TOT", "Total", "TOTAL"}:
        return ["" for _ in series]
    return ["font-weight: 700; background-color: #fff3cd; color: #3b2f00;" for _ in series]


def certs_table_section(df, total_col, port_map, key_prefix):
    """Render a certs table + change table side by side."""
    table_df = (
        df[["Date", total_col, *port_map.keys()]]
        .sort_values("Date", ascending=False)
        .rename(columns={"Date": "Date", total_col: "TOTAL", **port_map})
        .copy()
    )
    table_df["Date"] = table_df["Date"].dt.strftime("%d-%b-%y")

    change_df = df[["Date", total_col, *port_map.keys()]].sort_values("Date", ascending=False).copy()
    _num_cols = [total_col, *port_map.keys()]
    change_df[_num_cols] = change_df[_num_cols].apply(pd.to_numeric, errors="coerce").diff(-1)
    change_df = change_df.rename(columns={"Date": "Date", total_col: "TOTAL", **port_map})
    change_df["Date"] = change_df["Date"].dt.strftime("%d-%b-%y")
    chg_value_cols = [c for c in change_df.columns if c != "Date"]

    left, right = st.columns(2)
    with left:
        st.markdown("### Certified Stocks by Port")
        st.dataframe(table_df, use_container_width=True, hide_index=True, height=420)
    with right:
        st.markdown("### Certs Change per Port")
        styled = (
            change_df.style
            .format({c: fmt_change for c in chg_value_cols}, na_rep="")
            .map(style_change_value, subset=chg_value_cols)
            .apply(style_total_col, subset=["TOTAL"])
        )
        st.dataframe(styled, use_container_width=True, hide_index=True, height=420)


def grading_pivot(frame, index_col, label_name):
    pivot = frame.pivot_table(index=index_col, columns="Origin", values="NoLots", aggfunc="sum", fill_value=0)
    pivot["Total"] = pivot.sum(axis=1)
    ordered = [c for c in pivot.columns if c != "Total"] + ["Total"]
    return pivot[ordered].reset_index().rename(columns={index_col: label_name})


def style_grading_pivot(df_in, label_col):
    value_cols = [c for c in df_in.columns if c != label_col]
    heatmap_cols = [c for c in value_cols if c != "Total"]
    return (
        df_in.style
        .format({c: "{:,.0f}" for c in value_cols})
        .background_gradient(cmap="Blues", subset=heatmap_cols, axis=None)
        .set_properties(subset=["Total"], **{"font-weight": "700", "background-color": "#fff3cd", "color": "#3b2f00"})
    )


def grading_col_config(df_in, label_col):
    config = {
        label_col: st.column_config.TextColumn(label_col, width="small"),
        "Total": st.column_config.NumberColumn("Total", format="%d", width="small"),
    }
    for col in df_in.columns:
        if col not in config:
            config[col] = st.column_config.NumberColumn(col, format="%d", width="small")
    return config


def style_monthly_usage(df_in):
    return (
        df_in.style
        .format({
            "Passed Gradings": "{:,.0f}", "Certs Change": "{:,.0f}",
            "Monthly Usage": "{:,.0f}", "Avg Certs": "{:,.0f}", "Usage / Certs": "{:.0%}",
        })
        .bar(subset=["Passed Gradings"], color="#7fd39b", vmin=0)
        .bar(subset=["Certs Change"], color=["#ffb3b3", "#a8ddb5"], align="mid",
             vmin=df_in["Certs Change"].min(), vmax=df_in["Certs Change"].max())
        .background_gradient(cmap="Greens", subset=["Monthly Usage"])
        .background_gradient(cmap="Greens", subset=["Usage / Certs"])
    )


def style_kc_pending_table(df_in):
    passed_max = float(df_in["Passed"].max(skipna=True) or 0)
    failed_max = float(df_in["Failed"].max(skipna=True) or 0)
    fresh_max = float(df_in["Fresh Pending"].max(skipna=True) or 0)
    imp_max = float(df_in["Imp Decerts"].max(skipna=True) or 0)

    def soft_fill(value, max_value, rgb):
        if pd.isna(value) or max_value <= 0:
            return ""
        alpha = min(float(value) / max_value, 1.0) * 0.55
        return f"background-color: rgba({rgb}, {alpha:.3f});"

    return (
        df_in.style
        .format({"Passed": "{:,.1f}", "Failed": "{:,.1f}", "%": "{:.0%}",
                 "Pending": "{:,.1f}", "Certs": "{:,.1f}", "Certs Change": "{:,.1f}",
                 "Fresh Pending": "{:,.1f}", "Imp Decerts": "{:,.1f}"}, na_rep="")
        .map(lambda v: soft_fill(v, passed_max, "52, 168, 83"), subset=["Passed"])
        .map(lambda v: soft_fill(v, failed_max, "231, 76, 60"), subset=["Failed"])
        .map(lambda v: soft_fill(v, fresh_max, "82, 183, 136"), subset=["Fresh Pending"])
        .map(lambda v: soft_fill(v, imp_max, "255, 122, 89"), subset=["Imp Decerts"])
        .bar(subset=["Certs Change"], color=["#ffb3b3", "#a8ddb5"], align="mid",
             vmin=float(df_in["Certs Change"].min()), vmax=float(df_in["Certs Change"].max()))
    )


# ── Pre-compute RC grading aggregates (used outside tab for charts) ─────────────
rc_grading_origin = grading_rc.groupby(["PanelDate", "Origin"], as_index=False)["NoLots"].sum()
rc_grading_port = grading_rc.groupby(["PanelDate", "PortId"], as_index=False)["NoLots"].sum()
rc_grading_class = grading_rc.groupby(["PanelDate", "Class"], as_index=False)["NoLots"].sum()

origin_colors = {
    "Brazilian Conillon": "#2f5b89", "Vietnam": "#c62828", "Indonesia": "#9e9e9e",
    "Uganda": "#ffbf00", "Cote d'Ivoire": "#4f7ac8", "Tanzania": "#6aa84f",
    "Angola": "#f6d7c3", "Cameroon": "#d8ead3", "Republic of Madagascar": "#ececec",
    "India": "#8e7cc3",
}
port_colors_rc = {
    "ANT": "#3e4c61", "BAR": "#b7a59c", "BRE": "#f4cccc", "FEL": "#b6d7a8",
    "GEN": "#f9cb9c", "HAM": "#8d99ae", "LON": "#c00000", "NOR": "#fff200",
    "ROT": "#1f4e79", "TRI": "#ead1dc",
}
class_colors = {"P": "#212d40", "4": "#ff0000", "3": "#d9d9d9", "2": "#b4c7e7", "1": "#a9d18e"}


def add_grouped_bar(fig, frame, group_col, color_map, y_col="NoLots", x_col="PanelDate"):
    for name in sorted(frame[group_col].dropna().unique()):
        subset = frame[frame[group_col] == name]
        fig.add_trace(go.Bar(x=subset[x_col], y=subset[y_col], name=str(name),
                             marker_color=color_map.get(name, "#6c757d")))


# ── CC origin totals helper ────────────────────────────────────────────────────
def compute_cc_origin_totals(df):
    """Returns DataFrame with one column per origin = sum across grades of {origin}-{grade}-TOT."""
    origin_map = {}
    for col in df.columns:
        parts = col.split("-")
        if len(parts) == 4 and parts[0] == "CC" and parts[3] == "TOT" and parts[1] != "TOT":
            origin = parts[1]
            origin_map.setdefault(origin, []).append(col)
    result = {}
    for origin, cols in origin_map.items():
        result[origin] = df[cols].fillna(0).sum(axis=1)
    return pd.DataFrame(result, index=df.index)


# ── Tabs ───────────────────────────────────────────────────────────────────────
st.title("Certs & Grading")
robusta_tab, arabica_tab, cc_tab, lcc_tab = st.tabs(["Robusta (LRC)", "Arabica (KC)", "Cocoa US (CC)", "Cocoa UK (LCC)"])


# ══════════════════════════════════════════════════════════════════════════════
# ROBUSTA TAB
# ══════════════════════════════════════════════════════════════════════════════
with robusta_tab:

    with st.expander("1) Certs Tables", expanded=False):
        lrc_table_range = st.slider(
            "Tables Date Range",
            min_value=cert_lrc["Date"].min().to_pydatetime(),
            max_value=cert_lrc["Date"].max().to_pydatetime(),
            value=(cert_lrc["Date"].min().to_pydatetime(), cert_lrc["Date"].max().to_pydatetime()),
            key="lrc_tables_range",
        )
        lrc_table_df = cert_lrc[
            (cert_lrc["Date"] >= pd.to_datetime(lrc_table_range[0]))
            & (cert_lrc["Date"] <= pd.to_datetime(lrc_table_range[1]))
        ].copy()
        if lrc_table_df.empty:
            st.warning("No data for selected range.")
        else:
            certs_table_section(lrc_table_df, LRC_TOTAL_COL, LRC_TABLE_PORT_MAP, "lrc")

    with st.expander("2) Certs Analysis", expanded=True):
        lrc_analysis_range = st.slider(
            "Analysis Date Range",
            min_value=cert_lrc["Date"].min().to_pydatetime(),
            max_value=cert_lrc["Date"].max().to_pydatetime(),
            value=(cert_lrc["Date"].min().to_pydatetime(), cert_lrc["Date"].max().to_pydatetime()),
            key="lrc_analysis_range",
        )
        lrc_df = cert_lrc[
            (cert_lrc["Date"] >= pd.to_datetime(lrc_analysis_range[0]))
            & (cert_lrc["Date"] <= pd.to_datetime(lrc_analysis_range[1]))
        ].copy()

        if lrc_df.empty:
            st.warning("No data for selected range.")
        else:
            latest = lrc_df.iloc[-1]
            prev = lrc_df.iloc[-2] if len(lrc_df) > 1 else latest

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Latest Date", latest["Date"].strftime("%d-%b-%Y"))
            k2.metric("TOT Change", fmt_change(latest[LRC_TOTAL_COL] - prev[LRC_TOTAL_COL]))
            k3.metric("ANT Change", fmt_change(latest["LRC-ANT-VG"] - prev["LRC-ANT-VG"]))
            k4.metric("FEL Change", fmt_change(latest["LRC-FEL-VG"] - prev["LRC-FEL-VG"]))
            k5.metric("LON Change", fmt_change(latest["LRC-LON-VG"] - prev["LRC-LON-VG"]))

            top_left, top_right = st.columns(2)
            with top_left:
                st.markdown("### PORT STOCKS OVER TIME")
                fig1 = go.Figure()
                for col, name in LRC_CHART_PORT_MAP.items():
                    fig1.add_trace(go.Scatter(x=lrc_df["Date"], y=lrc_df[col], name=name))
                st.plotly_chart(fig1, use_container_width=True)

            with top_right:
                st.markdown("### ROLLING CHANGE")
                rolling_preset = st.radio(
                    "Rolling Window", options=[5, 20, 60, 90], index=1, horizontal=True,
                    format_func=lambda x: f"{x}D", key="lrc_rolling_window",
                )
                lrc_roll = lrc_df.copy()
                lrc_roll["Total_roll"] = lrc_roll[LRC_TOTAL_COL].diff(rolling_preset)
                lrc_roll["ANT_roll"] = lrc_roll["LRC-ANT-VG"].diff(rolling_preset)
                lrc_roll["UK_roll"] = (lrc_roll["LRC-LON-VG"] + lrc_roll["LRC-FEL-VG"]).diff(rolling_preset)
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=lrc_roll["Date"], y=lrc_roll["Total_roll"], name=f"{rolling_preset}d Total"))
                fig2.add_trace(go.Scatter(x=lrc_roll["Date"], y=lrc_roll["ANT_roll"], name=f"{rolling_preset}d Antwerp"))
                fig2.add_trace(go.Scatter(x=lrc_roll["Date"], y=lrc_roll["UK_roll"], name=f"{rolling_preset}d UK"))
                st.plotly_chart(fig2, use_container_width=True)

            col_left, col_right = st.columns([2, 1])
            with col_left:
                st.markdown("### TOTAL CERTIFIED STOCKS")
                fig3 = go.Figure()
                fig3.add_trace(go.Scatter(x=lrc_df["Date"], y=lrc_df[LRC_TOTAL_COL], fill="tozeroy", name="Total"))
                st.plotly_chart(fig3, use_container_width=True)

            with col_right:
                st.markdown("### PORT SHARE (%)")
                ports_latest = {
                    "ANT": latest["LRC-ANT-VG"], "LON": latest["LRC-LON-VG"],
                    "FEL": latest["LRC-FEL-VG"], "BAR": latest["LRC-BAR-VG"],
                }
                fig4 = go.Figure(data=[go.Pie(labels=list(ports_latest.keys()),
                                              values=list(ports_latest.values()), hole=0.6)])
                st.plotly_chart(fig4, use_container_width=True)

    with st.expander("3) Grading Analysis", expanded=False):
        g_f1, g_f2, g_f3, g_f4 = st.columns(4)
        grading_date_range = g_f1.slider(
            "Grading Date Range",
            min_value=grading_rc["PanelDate"].min().to_pydatetime(),
            max_value=grading_rc["PanelDate"].max().to_pydatetime(),
            value=(grading_rc["PanelDate"].min().to_pydatetime(), grading_rc["PanelDate"].max().to_pydatetime()),
            key="rc_grading_date_range",
        )
        selected_ports = g_f2.multiselect("Ports", options=sorted(grading_rc["PortId"].dropna().unique()),
                                           default=sorted(grading_rc["PortId"].dropna().unique()))
        selected_origins = g_f3.multiselect("Origins", options=sorted(grading_rc["Origin"].dropna().unique()),
                                             default=sorted(grading_rc["Origin"].dropna().unique()))
        selected_classes = g_f4.multiselect("Classes", options=sorted(grading_rc["Class"].dropna().unique()),
                                             default=sorted(grading_rc["Class"].dropna().unique()))

        tenderable_opts = sorted(grading_rc["Tenderable"].dropna().unique())
        selected_tenderable = st.multiselect("Tenderable", options=tenderable_opts, default=tenderable_opts)

        grc_filtered = grading_rc[
            (grading_rc["PanelDate"] >= pd.to_datetime(grading_date_range[0]))
            & (grading_rc["PanelDate"] <= pd.to_datetime(grading_date_range[1]))
            & (grading_rc["PortId"].isin(selected_ports))
            & (grading_rc["Origin"].isin(selected_origins))
            & (grading_rc["Class"].isin(selected_classes))
            & (grading_rc["Tenderable"].isin(selected_tenderable))
        ].copy()

        if grc_filtered.empty:
            st.warning("No grading data for selected filters.")
        else:
            grc_origin = grc_filtered.groupby(["PanelDate", "Origin"], as_index=False)["NoLots"].sum()
            grc_port = grc_filtered.groupby(["PanelDate", "PortId"], as_index=False)["NoLots"].sum()
            grc_class = grc_filtered.groupby(["PanelDate", "Class"], as_index=False)["NoLots"].sum()

            fig_org = go.Figure()
            add_grouped_bar(fig_org, grc_origin, "Origin", origin_colors)
            fig_org.update_layout(barmode="group", title="Robusta Grading Per Origin", height=720,
                                  legend_title_text="", margin=dict(l=20, r=20, t=70, b=20))
            fig_prt = go.Figure()
            add_grouped_bar(fig_prt, grc_port, "PortId", port_colors_rc)
            fig_prt.update_layout(barmode="group", title="Robusta Grading Per Port", height=340,
                                  legend_title_text="", margin=dict(l=20, r=20, t=60, b=20))
            fig_cls = go.Figure()
            add_grouped_bar(fig_cls, grc_class, "Class", class_colors)
            fig_cls.update_layout(barmode="group", title="Robusta Grading Per Class", height=340,
                                  legend_title_text="", margin=dict(l=20, r=20, t=60, b=20))

            gl, gr = st.columns([1.35, 1])
            with gl:
                st.plotly_chart(fig_org, use_container_width=True)
            with gr:
                st.plotly_chart(fig_prt, use_container_width=True)
                st.plotly_chart(fig_cls, use_container_width=True)

            with st.expander("Grading Table by Month", expanded=False):
                grc_tbl = grc_filtered.copy()
                grc_tbl["Year"] = grc_tbl["PanelDate"].dt.year
                grc_tbl["MonthNum"] = grc_tbl["PanelDate"].dt.month
                grc_tbl["MonthName"] = grc_tbl["PanelDate"].dt.strftime("%B")
                grc_tbl["Day"] = grc_tbl["PanelDate"].dt.day
                years = sorted(grc_tbl["Year"].dropna().unique(), reverse=True)

                year_summary = grading_pivot(grc_tbl, "Year", "Year")
                st.markdown("### Year Summary")
                st.dataframe(style_grading_pivot(year_summary, "Year"), use_container_width=True,
                             hide_index=True, column_config=grading_col_config(year_summary, "Year"),
                             height=min(320, 45 + len(year_summary) * 35))

                sel_year = st.selectbox("Select year for month/day drilldown", options=years, index=0, key="rc_pivot_year")
                yr_df = grc_tbl[grc_tbl["Year"] == sel_year].copy()
                month_order = yr_df[["MonthName", "MonthNum"]].drop_duplicates().sort_values("MonthNum")
                ordered_months = month_order["MonthName"].tolist()

                month_summary = grading_pivot(yr_df, "MonthName", "Month")
                month_summary["Month"] = pd.Categorical(month_summary["Month"], categories=ordered_months, ordered=True)
                month_summary = month_summary.sort_values("Month")
                month_summary["Month"] = month_summary["Month"].astype(str)
                st.markdown(f"### {sel_year} Month Summary")
                st.dataframe(style_grading_pivot(month_summary, "Month"), use_container_width=True,
                             hide_index=True, column_config=grading_col_config(month_summary, "Month"),
                             height=min(420, 45 + len(month_summary) * 35))

                st.markdown(f"### {sel_year} Daily Detail")
                for month_name in ordered_months:
                    mdf = yr_df[yr_df["MonthName"] == month_name].copy()
                    mdaily = grading_pivot(mdf, "Day", "Day")
                    mtotal = int(mdaily["Total"].sum()) if not mdaily.empty else 0
                    with st.expander(f"{month_name} | Total {mtotal:,.0f}", expanded=False):
                        st.dataframe(style_grading_pivot(mdaily, "Day"), use_container_width=True,
                                     hide_index=True, column_config=grading_col_config(mdaily, "Day"),
                                     height=min(420, 45 + len(mdaily) * 35))

    with st.expander("4) Monthly Usage", expanded=False):
        passed_monthly = (
            grading_rc[grading_rc["Tenderable"] == "Y"]
            .assign(Month=lambda x: x["PanelDate"].dt.to_period("M").dt.to_timestamp())
            .groupby("Month", as_index=False)["NoLots"].sum()
            .rename(columns={"NoLots": "Passed Gradings"})
        )
        certs_monthly = (
            cert_lrc.assign(Month=lambda x: x["Date"].dt.to_period("M").dt.to_timestamp())
            .groupby("Month").agg(Month_End_Certs=(LRC_TOTAL_COL, "last"), Avg_Certs=(LRC_TOTAL_COL, "mean"))
            .reset_index()
        )
        certs_monthly["Certs Change"] = (certs_monthly["Month_End_Certs"] - certs_monthly["Month_End_Certs"].shift(1)).fillna(0)
        certs_monthly = certs_monthly.rename(columns={"Avg_Certs": "Avg Certs"})

        mu = passed_monthly.merge(certs_monthly, on="Month", how="inner")
        mu["Monthly Usage"] = mu["Passed Gradings"] - mu["Certs Change"]
        mu["Usage / Certs"] = (mu["Monthly Usage"] / mu["Avg Certs"]).fillna(0)
        mu["Month Label"] = mu["Month"].dt.strftime("%b'%y")
        mu = mu.sort_values("Month").reset_index(drop=True)

        mu_bounds = st.slider("Months to display", 0, len(mu) - 1, (0, len(mu) - 1), key="lrc_mu_bounds")
        mu = mu.iloc[mu_bounds[0]: mu_bounds[1] + 1].copy()
        mu_display = mu[["Month", "Month Label", "Passed Gradings", "Certs Change", "Monthly Usage", "Avg Certs", "Usage / Certs"]].rename(columns={"Month Label": "Month", "Month": "Month Date"})
        mu_display2 = mu_display.drop(columns=["Month Date"])

        st.markdown("### Robusta Monthly Usage")
        fig_mu = go.Figure()
        fig_mu.add_trace(go.Scatter(x=mu["Month"], y=mu["Monthly Usage"], name="Monthly Usage",
                                    mode="lines+markers", line=dict(color="#1b7f3b", width=3), marker=dict(size=6)))
        fig_mu.update_layout(title="Monthly Usage", height=240, margin=dict(l=20, r=20, t=40, b=20), showlegend=False)

        fig_cc_bar = go.Figure()
        fig_cc_bar.add_trace(go.Bar(x=mu["Month"], y=mu["Certs Change"], marker_color="rgba(173,216,230,0.85)"))
        fig_cc_bar.update_layout(title="Certs Change", height=240, margin=dict(l=20, r=20, t=40, b=20), showlegend=False)

        fig_pg = go.Figure()
        fig_pg.add_trace(go.Bar(x=mu["Month"], y=mu["Passed Gradings"], marker_color="rgba(34,139,34,0.85)"))
        fig_pg.update_layout(title="Passed Gradings", height=240, margin=dict(l=20, r=20, t=40, b=20), showlegend=False)

        fig_ur = go.Figure()
        fig_ur.add_trace(go.Scatter(x=mu["Month"], y=mu["Usage / Certs"] * 100, mode="lines+markers",
                                    line=dict(color="#4c956c", width=3), marker=dict(size=6)))
        fig_ur.update_layout(title="Usage / Certs", height=240, margin=dict(l=20, r=20, t=40, b=20),
                             yaxis=dict(ticksuffix="%"), showlegend=False)

        ul, ur = st.columns([1.35, 1])
        with ul:
            st.dataframe(
                style_monthly_usage(mu_display2), use_container_width=True, hide_index=True,
                column_config={
                    "Month": st.column_config.TextColumn("Month", width="small"),
                    "Passed Gradings": st.column_config.NumberColumn("Passed Gradings", format="%d", width="small"),
                    "Certs Change": st.column_config.NumberColumn("Certs Change", format="%d", width="small"),
                    "Monthly Usage": st.column_config.NumberColumn("Monthly Usage", format="%d", width="small"),
                    "Avg Certs": st.column_config.NumberColumn("Avg Certs", format="%d", width="small"),
                    "Usage / Certs": st.column_config.TextColumn("Usage / Certs", width="small"),
                },
                height=min(700, 45 + len(mu_display2) * 35),
            )
        with ur:
            tl, tr = st.columns(2)
            bl, br = st.columns(2)
            with tl: st.plotly_chart(fig_mu, use_container_width=True)
            with tr: st.plotly_chart(fig_cc_bar, use_container_width=True)
            with bl: st.plotly_chart(fig_pg, use_container_width=True)
            with br: st.plotly_chart(fig_ur, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# ARABICA TAB
# ══════════════════════════════════════════════════════════════════════════════
with arabica_tab:
    st.subheader("Arabica Certs Analysis")

    kc_min_date = cert_kc["Date"].min().date()
    kc_max_date = cert_kc["Date"].max().date()
    kc_latest_full = cert_kc.dropna(subset=["KC-TOT-TOT"]).iloc[-1]
    kc_prev_frame = cert_kc.dropna(subset=["KC-TOT-TOT"])
    kc_prev_full = kc_prev_frame.iloc[-2] if len(kc_prev_frame) > 1 else kc_latest_full

    kc_total_change = safe_int(kc_latest_full.get("KC-TOT-TOT", 0)) - safe_int(kc_prev_full.get("KC-TOT-TOT", 0))
    kc_pending_latest = safe_int(kc_latest_full.get("KC-TOT-PENDING", 0))
    kc_pending_prev = safe_int(kc_prev_full.get("KC-TOT-PENDING", 0))
    kc_pending_change = kc_pending_latest - kc_pending_prev
    kc_pass_cols = [f"KC-{p}-PASSGRAD" for p in KC_GRADE_PORTS]
    kc_fail_cols = [f"KC-{p}-FAILGRAD" for p in KC_GRADE_PORTS]
    kc_pass_latest = sum(safe_int(kc_latest_full.get(c, 0)) for c in kc_pass_cols)
    kc_fail_latest = sum(safe_int(kc_latest_full.get(c, 0)) for c in kc_fail_cols)
    kc_pass_pct = (kc_pass_latest / (kc_pass_latest + kc_fail_latest) * 100) if (kc_pass_latest + kc_fail_latest) else 0.0
    kc_fresh_pending = max(0, kc_pending_latest - kc_pending_prev + kc_pass_latest + kc_fail_latest)
    kc_implied_decerts = kc_pass_latest - kc_total_change

    st.markdown(
        """<style>
        .mini-kpi-wrap{display:flex;gap:10px;margin:6px 0 14px 0;flex-wrap:wrap}
        .mini-kpi{background:#f7f8fb;border:1px solid #e7eaf0;border-radius:10px;padding:8px 12px;min-width:180px}
        .mini-kpi-label{font-size:0.68rem;text-transform:uppercase;letter-spacing:0.08em;color:#7a8594;margin-bottom:4px}
        .mini-kpi-value{font-size:1.15rem;font-weight:700;color:#1f2a44;line-height:1.1}
        .mini-kpi-sub{font-size:0.78rem;margin-top:4px;color:#5d6b7c}
        .mini-kpi-pos{color:#1f7a3d}.mini-kpi-neg{color:#c0392b}
        </style>""", unsafe_allow_html=True,
    )
    st.markdown(
        f"""<div class="mini-kpi-wrap">
            <div class="mini-kpi"><div class="mini-kpi-label">Date</div>
                <div class="mini-kpi-value">{kc_latest_full["Date"].strftime("%d-%b-%Y")}</div></div>
            <div class="mini-kpi"><div class="mini-kpi-label">Total Certs</div>
                <div class="mini-kpi-value">{safe_int(kc_latest_full.get("KC-TOT-TOT",0)):,}</div>
                <div class="mini-kpi-sub {'mini-kpi-pos' if kc_total_change>=0 else 'mini-kpi-neg'}">{kc_total_change:+,} vs prior day</div></div>
            <div class="mini-kpi"><div class="mini-kpi-label">Pending</div>
                <div class="mini-kpi-value">{kc_pending_latest:,}</div>
                <div class="mini-kpi-sub {'mini-kpi-pos' if kc_pending_change>=0 else 'mini-kpi-neg'}">{kc_pending_change:+,} vs prior day</div></div>
            <div class="mini-kpi"><div class="mini-kpi-label">Implied Decerts</div>
                <div class="mini-kpi-value">{kc_implied_decerts:,}</div></div>
            <div class="mini-kpi"><div class="mini-kpi-label">Fresh Pending</div>
                <div class="mini-kpi-value">{kc_fresh_pending:,}</div></div>
            <div class="mini-kpi"><div class="mini-kpi-label">Passed Bags</div>
                <div class="mini-kpi-value">{kc_pass_latest:,}</div>
                <div class="mini-kpi-sub mini-kpi-pos">{kc_pass_pct:.0f}% pass rate</div></div>
        </div>""", unsafe_allow_html=True,
    )

    kc_c1, kc_c2 = st.columns(2)
    with kc_c1:
        older_date = st.date_input("Older Date", value=kc_max_date - pd.Timedelta(days=7),
                                   min_value=kc_min_date, max_value=kc_max_date, key="kc_older_date")
    with kc_c2:
        latest_date = st.date_input("Latest Date", value=kc_max_date,
                                    min_value=kc_min_date, max_value=kc_max_date, key="kc_latest_date")

    older_sub = cert_kc[cert_kc["Date"] <= pd.Timestamp(older_date)]
    latest_sub = cert_kc[cert_kc["Date"] <= pd.Timestamp(latest_date)]
    latest_kc = cert_kc.dropna(subset=["KC-TOT-TOT"]).iloc[-1]
    grand_total = float(safe_num(latest_kc.get("KC-TOT-TOT", 0)))

    mat_left, mat_right = st.columns(2)
    with mat_left:
        st.markdown("### Two-Date Change Matrix")
        if older_sub.empty or latest_sub.empty:
            st.warning("No KC data for selected comparison dates.")
        else:
            older_row = older_sub.iloc[-1]
            latest_row = latest_sub.iloc[-1]
            st.caption(f"Comparing {older_row['Date'].strftime('%d-%b-%Y')} to {latest_row['Date'].strftime('%d-%b-%Y')}")
            change_rows = []
            for origin in KC_ORIGINS_NO_TOT:
                row = {"Origin": KC_ORIGIN_NAMES[origin]}
                for port in KC_PORTS_NO_TOT + ["TOT"]:
                    col = f"KC-{origin}-{port}"
                    v1, v2 = older_row.get(col, pd.NA), latest_row.get(col, pd.NA)
                    row[KC_PORT_NAMES[port]] = int(v2 - v1) if pd.notna(v1) and pd.notna(v2) else 0
                change_rows.append(row)
            total_row = {"Origin": "TOTAL"}
            for port in KC_PORTS_NO_TOT + ["TOT"]:
                col = f"KC-TOT-{port}"
                v1, v2 = older_row.get(col, pd.NA), latest_row.get(col, pd.NA)
                total_row[KC_PORT_NAMES[port]] = int(v2 - v1) if pd.notna(v1) and pd.notna(v2) else 0
            change_rows.append(total_row)

            kc_change_df = pd.DataFrame(change_rows).set_index("Origin")
            kc_chg_val_cols = [c for c in kc_change_df.columns if c != "Total"]
            st.dataframe(
                kc_change_df.style
                .map(style_change_value, subset=kc_chg_val_cols)
                .apply(plain_total_col_style, subset=["Total"])
                .apply(lambda row: ["font-weight: 700; background-color: #fff3cd; color: #3b2f00;" if row.name == "TOTAL" and col == "Total"
                                    else "font-weight: 700;" if row.name == "TOTAL" else "" for col in kc_change_df.columns], axis=1)
                .format(lambda v: f"{v:+,}" if v != 0 else ""),
                use_container_width=False, width=700, height=min(900, 42 + len(kc_change_df) * 35),
            )

    with mat_right:
        st.markdown("### Latest Certified Stocks Matrix")
        abs_rows = []
        for origin in KC_ORIGINS_NO_TOT:
            row = {"Origin": KC_ORIGIN_NAMES[origin]}
            for port in KC_PORTS_NO_TOT:
                row[KC_PORT_NAMES[port]] = safe_int(latest_kc.get(f"KC-{origin}-{port}", 0))
            origin_total = safe_int(latest_kc.get(f"KC-{origin}-TOT", 0))
            row["Total"] = origin_total
            row["Share %"] = origin_total / grand_total * 100 if grand_total else 0
            abs_rows.append(row)
        total_row = {"Origin": "TOTAL BY ORIGIN"}
        for port in KC_PORTS_NO_TOT:
            total_row[KC_PORT_NAMES[port]] = safe_int(latest_kc.get(f"KC-TOT-{port}", 0))
        total_row["Total"] = int(grand_total)
        total_row["Share %"] = 100.0 if grand_total else 0.0
        abs_rows.append(total_row)
        port_share_row = {"Origin": "PORT SHARE %"}
        for port in KC_PORTS_NO_TOT:
            pt = safe_int(latest_kc.get(f"KC-TOT-{port}", 0))
            port_share_row[KC_PORT_NAMES[port]] = pt / grand_total * 100 if grand_total else 0.0
        port_share_row["Total"] = 100.0 if grand_total else 0.0
        port_share_row["Share %"] = 0.0
        abs_rows.append(port_share_row)

        kc_abs_df = pd.DataFrame(abs_rows).set_index("Origin")
        hmap_df = kc_abs_df.drop(index="PORT SHARE %", errors="ignore")
        hmap_cols = [c for c in hmap_df.columns if c not in {"Total", "Share %"}]
        st.table(
            kc_abs_df.style
            .background_gradient(cmap="Greens", subset=pd.IndexSlice[hmap_df.index, hmap_cols], axis=None)
            .format("{:,.0f}", subset=pd.IndexSlice[kc_abs_df.index.difference(["PORT SHARE %"]), hmap_cols + ["Total"]])
            .format("{:,.1f}%", subset=pd.IndexSlice[["PORT SHARE %"], hmap_cols + ["Total", "Share %"]])
            .format("{:.1f}%", subset=pd.IndexSlice[kc_abs_df.index.difference(["PORT SHARE %"]), ["Share %"]])
            .set_properties(subset=["Total"], **{"font-weight": "700", "background-color": "#fff3cd", "color": "#3b2f00"})
            .apply(lambda row: ["font-weight: 700;" if row.name in {"TOTAL BY ORIGIN", "PORT SHARE %"} else "" for _ in row], axis=1)
        )

    with st.expander("3) Certs Visuals", expanded=True):
        kc_range = st.slider(
            "Arabica Date Range",
            min_value=cert_kc["Date"].min().to_pydatetime(),
            max_value=cert_kc["Date"].max().to_pydatetime(),
            value=(cert_kc["Date"].min().to_pydatetime(), cert_kc["Date"].max().to_pydatetime()),
            key="kc_analysis_range",
        )
        kc_filtered = cert_kc[
            (cert_kc["Date"] >= pd.to_datetime(kc_range[0]))
            & (cert_kc["Date"] <= pd.to_datetime(kc_range[1]))
        ].copy()

        if kc_filtered.empty:
            st.warning("No KC data for selected range.")
        else:
            kc_latest = kc_filtered.dropna(subset=["KC-TOT-TOT"]).iloc[-1]
            al, ar = st.columns([2, 1])
            with al:
                fig_kc_tot = go.Figure()
                fig_kc_tot.add_trace(go.Scatter(x=kc_filtered["Date"], y=kc_filtered["KC-TOT-TOT"], fill="tozeroy", name="KC Total"))
                fig_kc_tot.update_layout(title="KC Total Certified Stocks", height=260, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_kc_tot, use_container_width=True)
            with ar:
                shares = [(KC_ORIGIN_NAMES[o], float(kc_latest.get(f"KC-{o}-TOT", 0) or 0))
                          for o in KC_ORIGINS_NO_TOT if float(kc_latest.get(f"KC-{o}-TOT", 0) or 0) > 0]
                shares = sorted(shares, key=lambda x: x[1], reverse=True)
                if shares:
                    fig_kc_pie = go.Figure(data=[go.Pie(labels=[s[0] for s in shares],
                                                         values=[s[1] for s in shares], hole=0.55)])
                    fig_kc_pie.update_layout(title="Origin Share", height=260, margin=dict(l=20, r=20, t=40, b=20))
                    st.plotly_chart(fig_kc_pie, use_container_width=True)

            sel_origins = st.multiselect(
                "Select Arabica Origins", options=KC_ORIGINS_NO_TOT,
                default=[o for o in ["HON", "BRZ", "COL", "GUA"] if o in KC_ORIGINS_NO_TOT],
                format_func=lambda o: KC_ORIGIN_NAMES[o], key="kc_selected_origins",
            )
            vl, vr = st.columns(2)
            with vl:
                if sel_origins:
                    fig_kc_org = go.Figure()
                    for origin in sel_origins:
                        col = f"KC-{origin}-TOT"
                        if col in kc_filtered.columns:
                            fig_kc_org.add_trace(go.Scatter(x=kc_filtered["Date"], y=kc_filtered[col],
                                                             name=KC_ORIGIN_NAMES[origin]))
                    fig_kc_org.update_layout(title="Certified Stocks by Origin", height=300, margin=dict(l=20, r=20, t=40, b=20))
                    st.plotly_chart(fig_kc_org, use_container_width=True)
            with vr:
                if "KC-TOT-PENDING" in kc_filtered.columns:
                    fig_kc_pend = go.Figure()
                    fig_kc_pend.add_trace(go.Scatter(x=kc_filtered["Date"], y=kc_filtered["KC-TOT-PENDING"],
                                                      fill="tozeroy", name="Pending", line=dict(color="#d4a017")))
                    fig_kc_pend.update_layout(title="KC Total Pending Certification", height=300,
                                              margin=dict(l=20, r=20, t=40, b=20))
                    st.plotly_chart(fig_kc_pend, use_container_width=True)

    with st.expander("4) Grading Visuals", expanded=False):
        grade_cols_present = [f"KC-{p}-PASSGRAD" for p in KC_GRADE_PORTS] + [f"KC-{p}-FAILGRAD" for p in KC_GRADE_PORTS]
        if not all(c in cert_kc.columns for c in grade_cols_present):
            st.warning("KC grading columns not found in parquet.")
        else:
            grading_range = st.slider(
                "Grading Matrix Date Range",
                min_value=cert_kc["Date"].min().to_pydatetime(),
                max_value=cert_kc["Date"].max().to_pydatetime(),
                value=(cert_kc["Date"].min().to_pydatetime(), cert_kc["Date"].max().to_pydatetime()),
                key="kc_grading_range",
            )
            pass_cols = [f"KC-{p}-PASSGRAD" for p in KC_GRADE_PORTS]
            fail_cols = [f"KC-{p}-FAILGRAD" for p in KC_GRADE_PORTS]
            kc_grade_daily = cert_kc[["Date", *pass_cols, *fail_cols]].copy()
            for col in pass_cols + fail_cols:
                kc_grade_daily[col] = pd.to_numeric(kc_grade_daily[col], errors="coerce").fillna(0)
            kc_grade_daily["Pass Total"] = kc_grade_daily[pass_cols].sum(axis=1)
            kc_grade_daily["Fail Total"] = kc_grade_daily[fail_cols].sum(axis=1)
            kc_grade_daily = kc_grade_daily[
                (kc_grade_daily["Date"] >= pd.to_datetime(grading_range[0]))
                & (kc_grade_daily["Date"] <= pd.to_datetime(grading_range[1]))
            ].sort_values("Date")

            if not kc_grade_daily.empty:
                fig_kc_grade = go.Figure()
                fig_kc_grade.add_trace(go.Bar(x=kc_grade_daily["Date"], y=kc_grade_daily["Pass Total"],
                                              name="Pass", marker_color="rgba(34,139,34,0.85)"))
                fig_kc_grade.add_trace(go.Bar(x=kc_grade_daily["Date"], y=kc_grade_daily["Fail Total"],
                                              name="Fail", marker_color="rgba(192,57,43,0.85)"))
                fig_kc_grade.update_layout(title="Daily Pass vs Fail Grading", barmode="stack", height=320,
                                           margin=dict(l=20, r=20, t=40, b=20),
                                           legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                st.plotly_chart(fig_kc_grade, use_container_width=True)

            kc_grading_filtered = cert_kc[
                (cert_kc["Date"] >= pd.to_datetime(grading_range[0]))
                & (cert_kc["Date"] <= pd.to_datetime(grading_range[1]))
            ].copy()
            if not kc_grading_filtered.empty:
                matrix_rows = []
                total_pass = total_fail = 0
                for port in KC_GRADE_PORTS:
                    pc, fc = f"KC-{port}-PASSGRAD", f"KC-{port}-FAILGRAD"
                    pv = safe_num(kc_grading_filtered[pc].fillna(0).sum()) if pc in kc_grading_filtered.columns else 0
                    fv = safe_num(kc_grading_filtered[fc].fillna(0).sum()) if fc in kc_grading_filtered.columns else 0
                    tv = pv + fv
                    total_pass += pv
                    total_fail += fv
                    matrix_rows.append({"Port": KC_PORT_NAMES[port], "Pass": int(pv), "Fail": int(fv),
                                        "Total": int(tv), "Pass %": (pv / tv * 100) if tv else 0})
                ta = total_pass + total_fail
                matrix_rows.append({"Port": "TOTAL", "Pass": int(total_pass), "Fail": int(total_fail),
                                    "Total": int(ta), "Pass %": (total_pass / ta * 100) if ta else 0})
                kc_gm = pd.DataFrame(matrix_rows).set_index("Port")
                st.dataframe(
                    kc_gm.style
                    .set_properties(subset=["Total"], **{"font-weight": "700", "background-color": "#fff3cd", "color": "#3b2f00"})
                    .format({"Pass": "{:,.0f}", "Fail": "{:,.0f}", "Total": "{:,.0f}", "Pass %": "{:.1f}%"})
                    .apply(lambda row: ["font-weight: 700;" if row.name == "TOTAL" else "" for _ in row], axis=1),
                    use_container_width=False, width=620, height=min(420, 42 + len(kc_gm) * 35),
                )

    with st.expander("5) Fresh Pending / Implied Decerts", expanded=False):
        pending_range = st.slider(
            "Pending / Decerts Date Range",
            min_value=cert_kc["Date"].min().to_pydatetime(),
            max_value=cert_kc["Date"].max().to_pydatetime(),
            value=(cert_kc["Date"].min().to_pydatetime(), cert_kc["Date"].max().to_pydatetime()),
            key="kc_pending_range",
        )
        pass_cols = [f"KC-{p}-PASSGRAD" for p in KC_GRADE_PORTS]
        fail_cols = [f"KC-{p}-FAILGRAD" for p in KC_GRADE_PORTS]
        pending_df = cert_kc[["Date", "KC-TOT-PENDING", "KC-TOT-TOT", *pass_cols, *fail_cols]].copy()
        pending_df = pending_df[
            (pending_df["Date"] >= pd.to_datetime(pending_range[0]))
            & (pending_df["Date"] <= pd.to_datetime(pending_range[1]))
        ].sort_values("Date", ascending=False)

        if pending_df.empty:
            st.warning("No KC pending/decerts data for selected range.")
        else:
            for col in ["KC-TOT-PENDING", "KC-TOT-TOT", *pass_cols, *fail_cols]:
                pending_df[col] = pd.to_numeric(pending_df[col], errors="coerce")
            pending_df["Passed_raw"] = pending_df[pass_cols].fillna(0).sum(axis=1)
            pending_df["Failed_raw"] = pending_df[fail_cols].fillna(0).sum(axis=1)
            pending_df["Pending_raw"] = pending_df["KC-TOT-PENDING"].fillna(0)
            pending_df["Certs_raw"] = pending_df["KC-TOT-TOT"].fillna(0)
            pending_df["Certs Change_raw"] = pending_df["Certs_raw"] - pending_df["Certs_raw"].shift(-1)
            pending_df["Fresh Pending_raw"] = (
                pending_df["Pending_raw"] - pending_df["Pending_raw"].shift(-1)
                + pending_df["Passed_raw"] + pending_df["Failed_raw"]
            ).clip(lower=0)
            pending_df["Imp Decerts_raw"] = pending_df["Passed_raw"] - pending_df["Certs Change_raw"].fillna(0)

            pending_df["Passed"] = pending_df["Passed_raw"] / 1000
            pending_df["Failed"] = pending_df["Failed_raw"] / 1000
            pending_df["%"] = pending_df["Passed_raw"] / (pending_df["Passed_raw"] + pending_df["Failed_raw"])
            pending_df["Pending"] = pending_df["Pending_raw"] / 1000
            pending_df["Certs"] = pending_df["Certs_raw"] / 1000
            pending_df["Certs Change"] = pending_df["Certs Change_raw"] / 1000
            pending_df["Fresh Pending"] = pending_df["Fresh Pending_raw"] / 1000
            pending_df["Imp Decerts"] = pending_df["Imp Decerts_raw"] / 1000

            display_df = pending_df[["Date", "Passed", "Failed", "%", "Pending", "Certs",
                                     "Certs Change", "Fresh Pending", "Imp Decerts"]].copy()
            display_df["Date"] = pd.to_datetime(display_df["Date"]).dt.strftime("%d-%b-%y")
            zero_mask = (pending_df["Passed_raw"] + pending_df["Failed_raw"]) == 0
            display_df.loc[zero_mask, ["Passed", "Failed", "%"]] = pd.NA

            rolling_n = st.radio("Rolling Window", options=[5, 10, 20, 30], index=0, horizontal=True,
                                 format_func=lambda x: f"{x}D", key="kc_pending_roll_n")
            chart_df = pending_df.sort_values("Date").copy()
            chart_df["Rolling Passed"] = chart_df["Passed"].rolling(rolling_n, min_periods=1).sum()
            chart_df["Rolling Fresh Pending"] = chart_df["Fresh Pending"].rolling(rolling_n, min_periods=1).sum()
            chart_df["Rolling Imp Decerts"] = chart_df["Imp Decerts"].rolling(rolling_n, min_periods=1).sum()
            chart_df["Rolling Decerts Minus Fresh"] = chart_df["Rolling Imp Decerts"] - chart_df["Rolling Fresh Pending"]

            pl, pr = st.columns([1.2, 1])
            with pl:
                st.dataframe(style_kc_pending_table(display_df), use_container_width=True, hide_index=True,
                             height=min(1200, 42 + len(display_df) * 35))
            with pr:
                fig_roll = go.Figure()
                fig_roll.add_trace(go.Scatter(x=chart_df["Date"], y=chart_df["Rolling Imp Decerts"],
                                              name=f"Rolling {rolling_n}d Decerts", mode="lines+markers",
                                              line=dict(color="#ff6f61", width=2), marker=dict(size=5, symbol="diamond")))
                fig_roll.add_trace(go.Scatter(x=chart_df["Date"], y=chart_df["Rolling Fresh Pending"],
                                              name=f"Rolling {rolling_n}d Fresh Pending", mode="lines+markers",
                                              line=dict(color="#67b7e1", width=2), marker=dict(size=6, symbol="square")))
                fig_roll.add_trace(go.Scatter(x=chart_df["Date"], y=chart_df["Rolling Passed"],
                                              name=f"Rolling {rolling_n}d Passed", mode="lines+markers",
                                              line=dict(color="#70ad47", width=2), marker=dict(size=6, symbol="triangle-up")))
                fig_roll.update_layout(title=f"Rolling {rolling_n}D Metrics (k bags)", height=320,
                                       margin=dict(l=20, r=20, t=50, b=20),
                                       legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
                st.plotly_chart(fig_roll, use_container_width=True)

                fig_gap = go.Figure()
                fig_gap.add_trace(go.Scatter(x=chart_df["Date"], y=chart_df["Rolling Decerts Minus Fresh"],
                                             name="Imp Decerts - Fresh Pending", mode="lines+markers",
                                             line=dict(color="#444444", width=2), marker=dict(size=6, color="#222222")))
                fig_gap.add_hline(y=0, line_color="#bbbbbb", line_width=1)
                fig_gap.update_layout(title=f"Imp Decerts - Fresh Pending (Rolling {rolling_n}D in k bags)",
                                      height=320, margin=dict(l=20, r=20, t=50, b=20), showlegend=False)
                st.plotly_chart(fig_gap, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# COCOA US (CC) TAB
# ══════════════════════════════════════════════════════════════════════════════
with cc_tab:
    st.subheader("Cocoa US — Certified Stocks (CC)")

    cc_latest_row = cert_cc.dropna(subset=[CC_TOTAL_COL]).iloc[-1]
    cc_prev_frame = cert_cc.dropna(subset=[CC_TOTAL_COL])
    cc_prev_row = cc_prev_frame.iloc[-2] if len(cc_prev_frame) > 1 else cc_latest_row
    cc_total_change = safe_int(cc_latest_row[CC_TOTAL_COL]) - safe_int(cc_prev_row[CC_TOTAL_COL])

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Latest Date", cc_latest_row["Date"].strftime("%d-%b-%Y"))
    k2.metric("Total Certs", f"{safe_int(cc_latest_row[CC_TOTAL_COL]):,}", delta=f"{cc_total_change:+,}")
    for k, (col, name) in zip([k3, k4, k5, k6], list(CC_PORT_MAP.items())[:4]):
        val = safe_int(cc_latest_row.get(col, 0))
        chg = val - safe_int(cc_prev_row.get(col, 0))
        k.metric(name, f"{val:,}", delta=f"{chg:+,}")

    with st.expander("1) Certs Tables", expanded=False):
        cc_table_range = st.slider(
            "Tables Date Range",
            min_value=cert_cc["Date"].min().to_pydatetime(),
            max_value=cert_cc["Date"].max().to_pydatetime(),
            value=(cert_cc["Date"].min().to_pydatetime(), cert_cc["Date"].max().to_pydatetime()),
            key="cc_table_range",
        )
        cc_table_df = cert_cc[
            (cert_cc["Date"] >= pd.to_datetime(cc_table_range[0]))
            & (cert_cc["Date"] <= pd.to_datetime(cc_table_range[1]))
        ].copy()
        if cc_table_df.empty:
            st.warning("No data for selected range.")
        else:
            certs_table_section(cc_table_df, CC_TOTAL_COL, CC_PORT_MAP, "cc")

    with st.expander("2) Certs Analysis", expanded=True):
        cc_analysis_range = st.slider(
            "Analysis Date Range",
            min_value=cert_cc["Date"].min().to_pydatetime(),
            max_value=cert_cc["Date"].max().to_pydatetime(),
            value=(cert_cc["Date"].min().to_pydatetime(), cert_cc["Date"].max().to_pydatetime()),
            key="cc_analysis_range",
        )
        cc_df = cert_cc[
            (cert_cc["Date"] >= pd.to_datetime(cc_analysis_range[0]))
            & (cert_cc["Date"] <= pd.to_datetime(cc_analysis_range[1]))
        ].copy()

        if cc_df.empty:
            st.warning("No data for selected range.")
        else:
            cl, cr = st.columns([2, 1])
            with cl:
                st.markdown("### TOTAL CERTIFIED STOCKS")
                fig_cc_tot = go.Figure()
                fig_cc_tot.add_trace(go.Scatter(x=cc_df["Date"], y=cc_df[CC_TOTAL_COL], fill="tozeroy", name="Total"))
                fig_cc_tot.update_layout(height=280, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_cc_tot, use_container_width=True)
            with cr:
                st.markdown("### PORT SHARE (%)")
                port_vals = {name: safe_num(cc_df.iloc[-1].get(col, 0)) for col, name in CC_PORT_MAP.items()}
                fig_cc_pie = go.Figure(data=[go.Pie(labels=list(port_vals.keys()),
                                                     values=list(port_vals.values()), hole=0.6)])
                fig_cc_pie.update_layout(height=280, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_cc_pie, use_container_width=True)

            st.markdown("### PORT STOCKS OVER TIME")
            fig_cc_ports = go.Figure()
            for col, name in CC_PORT_MAP.items():
                fig_cc_ports.add_trace(go.Scatter(x=cc_df["Date"], y=cc_df[col], name=name))
            fig_cc_ports.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_cc_ports, use_container_width=True)

            st.markdown("### ROLLING CHANGE")
            cc_roll_preset = st.radio("Rolling Window", options=[5, 20, 60, 90], index=1, horizontal=True,
                                      format_func=lambda x: f"{x}D", key="cc_rolling_window")
            cc_roll = cc_df.copy()
            cc_roll["Total_roll"] = cc_roll[CC_TOTAL_COL].diff(cc_roll_preset)
            for col, name in CC_PORT_MAP.items():
                cc_roll[f"{name}_roll"] = cc_roll[col].diff(cc_roll_preset)
            fig_cc_roll = go.Figure()
            fig_cc_roll.add_trace(go.Scatter(x=cc_roll["Date"], y=cc_roll["Total_roll"], name=f"{cc_roll_preset}d Total"))
            for col, name in CC_PORT_MAP.items():
                fig_cc_roll.add_trace(go.Scatter(x=cc_roll["Date"], y=cc_roll[f"{name}_roll"], name=f"{cc_roll_preset}d {name}"))
            fig_cc_roll.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_cc_roll, use_container_width=True)

    with st.expander("3) Origin Analysis", expanded=False):
        cc_origin_range = st.slider(
            "Origin Date Range",
            min_value=cert_cc["Date"].min().to_pydatetime(),
            max_value=cert_cc["Date"].max().to_pydatetime(),
            value=(cert_cc["Date"].min().to_pydatetime(), cert_cc["Date"].max().to_pydatetime()),
            key="cc_origin_range",
        )
        cc_origin_df = cert_cc[
            (cert_cc["Date"] >= pd.to_datetime(cc_origin_range[0]))
            & (cert_cc["Date"] <= pd.to_datetime(cc_origin_range[1]))
        ].copy()

        if cc_origin_df.empty:
            st.warning("No data for selected range.")
        else:
            cc_origins = compute_cc_origin_totals(cc_origin_df)
            latest_origin_vals = cc_origins.iloc[-1].sort_values(ascending=False)
            latest_origin_vals = latest_origin_vals[latest_origin_vals > 0]

            oa, ob = st.columns([1, 1])
            with oa:
                st.markdown("### Origin Share — Latest")
                labels = [CC_ORIGIN_NAMES.get(o, o) for o in latest_origin_vals.index]
                fig_cc_opie = go.Figure(data=[go.Pie(labels=labels, values=latest_origin_vals.values, hole=0.5)])
                fig_cc_opie.update_layout(height=400, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_cc_opie, use_container_width=True)
            with ob:
                st.markdown("### Top Origins Over Time")
                top_origins = latest_origin_vals.head(8).index.tolist()
                fig_cc_otime = go.Figure()
                for origin in top_origins:
                    fig_cc_otime.add_trace(go.Scatter(x=cc_origin_df["Date"], y=cc_origins[origin],
                                                       name=CC_ORIGIN_NAMES.get(origin, origin)))
                fig_cc_otime.update_layout(height=400, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_cc_otime, use_container_width=True)

            st.markdown("### Latest Origin Breakdown (lots)")
            origin_table = pd.DataFrame({
                "Origin": [CC_ORIGIN_NAMES.get(o, o) for o in latest_origin_vals.index],
                "Code": latest_origin_vals.index,
                "Lots": latest_origin_vals.values.astype(int),
                "Share %": (latest_origin_vals / latest_origin_vals.sum() * 100).values,
            })
            st.dataframe(
                origin_table.style
                .format({"Lots": "{:,.0f}", "Share %": "{:.1f}%"})
                .background_gradient(cmap="Blues", subset=["Lots"]),
                use_container_width=False, width=480, hide_index=True,
                height=min(600, 42 + len(origin_table) * 35),
            )


# ══════════════════════════════════════════════════════════════════════════════
# COCOA UK (LCC) TAB
# ══════════════════════════════════════════════════════════════════════════════
with lcc_tab:
    st.subheader("Cocoa UK — Certified Stocks (LCC)")

    lcc_latest_row = cert_lcc.dropna(subset=[LCC_TOTAL_COL]).iloc[-1]
    lcc_prev_frame = cert_lcc.dropna(subset=[LCC_TOTAL_COL])
    lcc_prev_row = lcc_prev_frame.iloc[-2] if len(lcc_prev_frame) > 1 else lcc_latest_row
    lcc_total_change = safe_num(lcc_latest_row[LCC_TOTAL_COL]) - safe_num(lcc_prev_row[LCC_TOTAL_COL])

    # SDU/LDU/BDU totals (perpetual = fully tenderable)
    lcc_sdu = safe_num(lcc_latest_row.get("LCC-TOT-SDU_PERP", 0)) + safe_num(lcc_latest_row.get("LCC-TOT-SDU_INIT", 0))
    lcc_ldu = safe_num(lcc_latest_row.get("LCC-TOT-LDU_PERP", 0)) + safe_num(lcc_latest_row.get("LCC-TOT-LDU_INIT", 0))
    lcc_bdu = safe_num(lcc_latest_row.get("LCC-TOT-BDU_PERP", 0)) + safe_num(lcc_latest_row.get("LCC-TOT-BDU_INIT", 0))

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Latest Date", lcc_latest_row["Date"].strftime("%d-%b-%Y"))
    k2.metric("Total Certs", f"{safe_int(lcc_latest_row[LCC_TOTAL_COL]):,}", delta=f"{int(lcc_total_change):+,}")
    k3.metric("SDU (Tenderable)", f"{int(lcc_sdu):,}")
    k4.metric("LDU (Tenderable)", f"{int(lcc_ldu):,}")
    k5.metric("BDU (Tenderable)", f"{int(lcc_bdu):,}")

    with st.expander("1) Certs Tables", expanded=False):
        lcc_table_range = st.slider(
            "Tables Date Range",
            min_value=cert_lcc["Date"].min().to_pydatetime(),
            max_value=cert_lcc["Date"].max().to_pydatetime(),
            value=(cert_lcc["Date"].min().to_pydatetime(), cert_lcc["Date"].max().to_pydatetime()),
            key="lcc_table_range",
        )
        lcc_table_df = cert_lcc[
            (cert_lcc["Date"] >= pd.to_datetime(lcc_table_range[0]))
            & (cert_lcc["Date"] <= pd.to_datetime(lcc_table_range[1]))
        ].copy()
        if lcc_table_df.empty:
            st.warning("No data for selected range.")
        else:
            certs_table_section(lcc_table_df, LCC_TOTAL_COL, LCC_PORT_TOTAL_MAP, "lcc")

    with st.expander("2) Certs Analysis", expanded=True):
        lcc_analysis_range = st.slider(
            "Analysis Date Range",
            min_value=cert_lcc["Date"].min().to_pydatetime(),
            max_value=cert_lcc["Date"].max().to_pydatetime(),
            value=(cert_lcc["Date"].min().to_pydatetime(), cert_lcc["Date"].max().to_pydatetime()),
            key="lcc_analysis_range",
        )
        lcc_df = cert_lcc[
            (cert_lcc["Date"] >= pd.to_datetime(lcc_analysis_range[0]))
            & (cert_lcc["Date"] <= pd.to_datetime(lcc_analysis_range[1]))
        ].copy()

        if lcc_df.empty:
            st.warning("No data for selected range.")
        else:
            ll, lr = st.columns([2, 1])
            with ll:
                st.markdown("### TOTAL CERTIFIED STOCKS")
                fig_lcc_tot = go.Figure()
                fig_lcc_tot.add_trace(go.Scatter(x=lcc_df["Date"], y=lcc_df[LCC_TOTAL_COL], fill="tozeroy", name="Total"))
                fig_lcc_tot.update_layout(height=280, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_lcc_tot, use_container_width=True)
            with lr:
                st.markdown("### PORT SHARE (%)")
                lcc_port_vals = {p: safe_num(lcc_df.iloc[-1].get(f"LCC-{p}-TOTAL", 0)) for p in LCC_PORTS}
                lcc_port_vals = {k: v for k, v in lcc_port_vals.items() if pd.notna(v) and v > 0}
                fig_lcc_pie = go.Figure(data=[go.Pie(labels=[LCC_PORT_NAMES[p] for p in lcc_port_vals],
                                                      values=list(lcc_port_vals.values()), hole=0.6)])
                fig_lcc_pie.update_layout(height=280, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_lcc_pie, use_container_width=True)

            lcc_chart_ports = st.multiselect(
                "Ports to chart", options=LCC_PORTS,
                default=["ANT", "LON", "HAM", "ROT"],
                format_func=lambda p: f"{p} — {LCC_PORT_NAMES[p]}",
                key="lcc_chart_ports",
            )
            if lcc_chart_ports:
                fig_lcc_ports = go.Figure()
                for p in lcc_chart_ports:
                    col = f"LCC-{p}-TOTAL"
                    if col in lcc_df.columns:
                        fig_lcc_ports.add_trace(go.Scatter(x=lcc_df["Date"], y=lcc_df[col],
                                                            name=f"{p} — {LCC_PORT_NAMES[p]}"))
                fig_lcc_ports.update_layout(title="Port Stocks Over Time", height=300, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_lcc_ports, use_container_width=True)

            st.markdown("### ROLLING CHANGE")
            lcc_roll_preset = st.radio("Rolling Window", options=[5, 20, 60, 90], index=1, horizontal=True,
                                       format_func=lambda x: f"{x}D", key="lcc_rolling_window")
            lcc_roll = lcc_df.copy()
            lcc_roll["Total_roll"] = lcc_roll[LCC_TOTAL_COL].diff(lcc_roll_preset)
            fig_lcc_roll = go.Figure()
            fig_lcc_roll.add_trace(go.Scatter(x=lcc_roll["Date"], y=lcc_roll["Total_roll"], name=f"{lcc_roll_preset}d Total"))
            for p in ["ANT", "HAM", "ROT"]:
                col = f"LCC-{p}-TOTAL"
                if col in lcc_roll.columns:
                    lcc_roll[f"{p}_roll"] = lcc_roll[col].diff(lcc_roll_preset)
                    fig_lcc_roll.add_trace(go.Scatter(x=lcc_roll["Date"], y=lcc_roll[f"{p}_roll"], name=f"{lcc_roll_preset}d {p}"))
            fig_lcc_roll.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_lcc_roll, use_container_width=True)

    with st.expander("3) SDU / LDU / BDU Breakdown", expanded=False):
        lcc_du_range = st.slider(
            "Date Range",
            min_value=cert_lcc["Date"].min().to_pydatetime(),
            max_value=cert_lcc["Date"].max().to_pydatetime(),
            value=(cert_lcc["Date"].min().to_pydatetime(), cert_lcc["Date"].max().to_pydatetime()),
            key="lcc_du_range",
        )
        lcc_du_df = cert_lcc[
            (cert_lcc["Date"] >= pd.to_datetime(lcc_du_range[0]))
            & (cert_lcc["Date"] <= pd.to_datetime(lcc_du_range[1]))
        ].copy()

        if not lcc_du_df.empty:
            for du in ["SDU", "LDU", "BDU"]:
                perp = f"LCC-TOT-{du}_PERP"
                init = f"LCC-TOT-{du}_INIT"
                exp_ = f"LCC-TOT-{du}_EXP"
                nont = f"LCC-TOT-{du}_NONTEND"
                if perp in lcc_du_df.columns:
                    lcc_du_df[f"{du}_Tend"] = lcc_du_df[perp].fillna(0) + lcc_du_df.get(init, pd.Series(0, index=lcc_du_df.index)).fillna(0)
                    lcc_du_df[f"{du}_NonTend"] = lcc_du_df.get(exp_, pd.Series(0, index=lcc_du_df.index)).fillna(0) + lcc_du_df.get(nont, pd.Series(0, index=lcc_du_df.index)).fillna(0)

            fig_du = go.Figure()
            colors = {"SDU": "#2f5b89", "LDU": "#c62828", "BDU": "#4f7ac8"}
            for du in ["SDU", "LDU", "BDU"]:
                tend_col = f"{du}_Tend"
                if tend_col in lcc_du_df.columns:
                    fig_du.add_trace(go.Scatter(x=lcc_du_df["Date"], y=lcc_du_df[tend_col],
                                                name=f"{du} Tenderable", line=dict(color=colors[du], width=2)))
            fig_du.update_layout(title="Tenderable Certs by Delivery Unit Type", height=320, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig_du, use_container_width=True)

            latest_lcc_du = lcc_du_df.iloc[-1]
            du_table_rows = []
            for du in ["SDU", "LDU", "BDU"]:
                tend_col, nont_col = f"{du}_Tend", f"{du}_NonTend"
                tend_val = safe_num(latest_lcc_du.get(tend_col, 0))
                nont_val = safe_num(latest_lcc_du.get(nont_col, 0))
                total = tend_val + nont_val
                du_table_rows.append({"Type": du, "Tenderable": int(tend_val), "Non-Tenderable": int(nont_val),
                                      "Total": int(total), "% Tenderable": (tend_val / total * 100) if total else 0})
            du_table = pd.DataFrame(du_table_rows)
            st.markdown("### Latest Delivery Unit Summary")
            st.dataframe(
                du_table.style.format({"Tenderable": "{:,.0f}", "Non-Tenderable": "{:,.0f}",
                                       "Total": "{:,.0f}", "% Tenderable": "{:.1f}%"})
                .set_properties(subset=["Total"], **{"font-weight": "700", "background-color": "#fff3cd", "color": "#3b2f00"}),
                use_container_width=False, width=480, hide_index=True,
            )

    with st.expander("4) Grading Analysis", expanded=False):
        lcc_grad_range = st.slider(
            "Grading Date Range",
            min_value=grading_lcc["PublishedDate"].min().to_pydatetime(),
            max_value=grading_lcc["PublishedDate"].max().to_pydatetime(),
            value=(grading_lcc["PublishedDate"].min().to_pydatetime(), grading_lcc["PublishedDate"].max().to_pydatetime()),
            key="lcc_grading_range",
        )
        lcc_gf1, lcc_gf2, lcc_gf3 = st.columns(3)
        lcc_sel_ports = lcc_gf1.multiselect("Ports", options=sorted(grading_lcc["PortId"].dropna().unique()),
                                              default=sorted(grading_lcc["PortId"].dropna().unique()), key="lcc_grad_ports")
        lcc_sel_origins = lcc_gf2.multiselect("Origins", options=sorted(grading_lcc["Origin"].dropna().unique()),
                                               default=sorted(grading_lcc["Origin"].dropna().unique()), key="lcc_grad_origins")
        lcc_panel_types = sorted(grading_lcc["PanelType"].dropna().unique())
        lcc_sel_types = lcc_gf3.multiselect("Panel Type", options=lcc_panel_types, default=lcc_panel_types, key="lcc_grad_types")

        lcc_grad_filtered = grading_lcc[
            (grading_lcc["PublishedDate"] >= pd.to_datetime(lcc_grad_range[0]))
            & (grading_lcc["PublishedDate"] <= pd.to_datetime(lcc_grad_range[1]))
            & (grading_lcc["PortId"].isin(lcc_sel_ports))
            & (grading_lcc["Origin"].isin(lcc_sel_origins))
            & (grading_lcc["PanelType"].isin(lcc_sel_types))
        ].copy()

        if lcc_grad_filtered.empty:
            st.warning("No grading data for selected filters.")
        else:
            lcc_grad_monthly = (
                lcc_grad_filtered
                .assign(Month=lambda x: x["PublishedDate"].dt.to_period("M").dt.to_timestamp())
                .groupby(["Month", "PortId"], as_index=False)["TotalLots"].sum()
            )
            lcc_grad_origin = (
                lcc_grad_filtered
                .assign(Month=lambda x: x["PublishedDate"].dt.to_period("M").dt.to_timestamp())
                .groupby(["Month", "Origin"], as_index=False)["TotalLots"].sum()
            )

            fig_lcc_grad_port = go.Figure()
            for port in sorted(lcc_grad_monthly["PortId"].unique()):
                sub = lcc_grad_monthly[lcc_grad_monthly["PortId"] == port]
                fig_lcc_grad_port.add_trace(go.Bar(x=sub["Month"], y=sub["TotalLots"], name=port))
            fig_lcc_grad_port.update_layout(barmode="stack", title="Monthly Grading by Port (Lots)", height=340,
                                            margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig_lcc_grad_port, use_container_width=True)

            fig_lcc_grad_org = go.Figure()
            for origin in sorted(lcc_grad_origin["Origin"].unique()):
                sub = lcc_grad_origin[lcc_grad_origin["Origin"] == origin]
                fig_lcc_grad_org.add_trace(go.Bar(x=sub["Month"], y=sub["TotalLots"], name=origin))
            fig_lcc_grad_org.update_layout(barmode="stack", title="Monthly Grading by Origin (Lots)", height=340,
                                           margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig_lcc_grad_org, use_container_width=True)

            lcc_du_grad = (
                lcc_grad_filtered
                .assign(Month=lambda x: x["PublishedDate"].dt.to_period("M").dt.to_timestamp())
                .groupby("Month", as_index=False)[["SDU", "LDU", "BDU"]].sum()
            )
            fig_lcc_du_grad = go.Figure()
            for du, color in [("SDU", "#2f5b89"), ("LDU", "#c62828"), ("BDU", "#4f7ac8")]:
                fig_lcc_du_grad.add_trace(go.Bar(x=lcc_du_grad["Month"], y=lcc_du_grad[du], name=du,
                                                  marker_color=color))
            fig_lcc_du_grad.update_layout(barmode="stack", title="Monthly Grading by Delivery Unit Type", height=300,
                                          margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig_lcc_du_grad, use_container_width=True)

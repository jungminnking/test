import json
from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime

DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "bls_timeseries.csv"
META_PATH = DATA_DIR / "meta.json"

SERIES = {
    "LNS12000000": {"section": "Employment", "name": "Civilian Employment (Thousands, SA)", "freq": "M"},
    "CES0000000001": {"section": "Employment", "name": "Total Nonfarm Employment (Thousands, SA)", "freq": "M"},
    "LNS14000000": {"section": "Employment", "name": "Unemployment Rate (% SA)", "freq": "M"},
    "CES0500000002": {"section": "Employment", "name": "Avg Weekly Hours, Total Private (SA)", "freq": "M"},
    "CES0500000003": {"section": "Employment", "name": "Avg Hourly Earnings, Total Private ($, SA)", "freq": "M"},
    "PRS85006093": {"section": "Productivity", "name": "Output per Hour — Nonfarm Business (Q/Q %)", "freq": "Q"},
    "CUUR0000SA0": {"section": "Price Index", "name": "CPI-U All Items (NSA, 1982–84=100)", "freq": "M"},
    "CIU1010000000000I": {"section": "Compensation", "name": "ECI — Total Compensation, Private (Index, NSA)", "freq": "Q"},
    "CIU1010000000000A": {"section": "Compensation", "name": "ECI — Total Compensation, Private (12m % change, NSA)", "freq": "Q"},
}

SECTIONS = ["Employment", "Productivity", "Price Index", "Compensation"]

# NBER recession shading since 2006 (approximate monthly ranges)
RECESSIONS = [
    (pd.Timestamp(2007, 12, 1), pd.Timestamp(2009, 6, 1)),
    (pd.Timestamp(2020, 2, 1), pd.Timestamp(2020, 4, 1)),
]

@st.cache_data
def load_data():
    df = pd.read_csv(CSV_PATH, parse_dates=["date"])
    df["series_id"] = df["series_id"].astype("string")
    return df

def yoy_from_level(df, sid):
    freq = SERIES.get(sid, {}).get("freq", "M").upper()
    lag = 4 if freq.startswith("Q") else 12
    d = df[df.series_id == sid][["date", "value"]].sort_values("date").set_index("date").copy()
    d["YoY %"] = d["value"].pct_change(lag) * 100.0
    d = d.reset_index()
    d["series_id"] = sid
    return d

def add_recession_shading(fig):
    for (start, end) in RECESSIONS:
        fig.add_vrect(x0=start, x1=end, fillcolor="gray", opacity=0.15, line_width=0)
    return fig

def main():
    st.set_page_config(page_title="US Labor Dashboard", layout="wide")
    st.title("US Labor Dashboard")
    st.caption("Auto-updating BLS dashboard (Econ 8320 project)")

    with st.expander("About & rubric alignment", expanded=False):
        st.markdown(
            "- Uses BLS Public API via monthly/quarterly fetcher (stored to CSV; no live fetch on every app load).\n"
            "- Includes required series: Nonfarm Employment & Unemployment Rate; plus additional sections from proposal.\n"
            "- Updates via GitHub Actions twice monthly to catch major releases.\n"
            "- Filter by section/series and date range; optional YoY visuals for CPI, Wages, ECI index."
        )

    if META_PATH.exists():
        meta = json.loads(META_PATH.read_text())
        st.caption(f"Last updated (UTC): {meta.get('last_updated_utc', 'unknown')}")

    # Sidebar filters
    section = st.sidebar.multiselect("Sections", SECTIONS, default=SECTIONS)
    eligible = [sid for sid, m in SERIES.items() if m["section"] in section]
    pick = st.sidebar.multiselect(
        "Series",
        eligible,
        format_func=lambda x: f"{SERIES[x]['section']} — {SERIES[x]['name']}",
        default=eligible,
    )
    year_min, year_max = st.sidebar.slider("Year range", 2006, datetime.utcnow().year, (2006, datetime.utcnow().year))

    if not CSV_PATH.exists():
        st.error("Data file not found. Run bls_update.py first (or wait for GitHub Actions to populate it).")
        return
    df = load_data()
    df = df[df["series_id"].isin(pick)]
    df = df[(df["date"].dt.year >= year_min) & (df["date"].dt.year <= year_max)]

    # Download buttons
    st.download_button("Download full CSV", CSV_PATH.read_bytes(), file_name="bls_timeseries.csv")
    st.download_button("Download filtered CSV", df.to_csv(index=False).encode("utf-8"), file_name="bls_timeseries_filtered.csv")

    # Sectioned charts
    for sec in SECTIONS:
        sub_ids = [sid for sid in pick if SERIES[sid]["section"] == sec]
        if not sub_ids:
            continue
        st.subheader(sec)
        for sid in sub_ids:
            name = SERIES[sid]["name"]
            d = df[df.series_id == sid].sort_values("date")
            if d.empty:
                continue
            fig = px.line(d, x="date", y="value", title=name, labels={"value": "Value", "date": "Date"})
            fig = add_recession_shading(fig)
            st.plotly_chart(fig, use_container_width=True)

            if sid in ["CUUR0000SA0", "CES0500000003", "CIU1010000000000I"]:
                yoy = yoy_from_level(df, sid).dropna()
                if not yoy.empty:
                    fig2 = px.line(yoy, x="date", y="YoY %", title=f"{name} — YoY %")
                    fig2 = add_recession_shading(fig2)
                    st.plotly_chart(fig2, use_container_width=True)

    st.write("---")
    st.caption("Notes: CPI is NSA; productivity series is Q/Q %; ECI shown as official YoY and YoY computed from the index.")

if __name__ == "__main__":
    main()

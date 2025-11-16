import os, json, requests
from datetime import datetime
from pathlib import Path
import pandas as pd

START_YEAR = 2006
END_YEAR = datetime.utcnow().year

# Curated series aligned with proposal + rubric
SERIES = {
    # Employment (Monthly, SA)
    "LNS12000000": {"section": "Employment", "name": "Civilian Employment (Thousands, SA)", "freq": "M"},
    "CES0000000001": {"section": "Employment", "name": "Total Nonfarm Employment (Thousands, SA)", "freq": "M"},
    "LNS14000000": {"section": "Employment", "name": "Unemployment Rate (% SA)", "freq": "M"},
    "CES0500000002": {"section": "Employment", "name": "Avg Weekly Hours, Total Private (SA)", "freq": "M"},
    "CES0500000003": {"section": "Employment", "name": "Avg Hourly Earnings, Total Private ($, SA)", "freq": "M"},
    # Productivity (Quarterly, SA) — percent change from previous quarter
    "PRS85006093": {"section": "Productivity", "name": "Output per Hour — Nonfarm Business (Q/Q %)", "freq": "Q"},
    # Price Index (Monthly, NSA)
    "CUUR0000SA0": {"section": "Price Index", "name": "CPI-U All Items (NSA, 1982–84=100)", "freq": "M"},
    # Compensation (Quarterly, NSA)
    "CIU1010000000000I": {"section": "Compensation", "name": "ECI — Total Compensation, Private (Index, NSA)", "freq": "Q"},
    "CIU1010000000000A": {"section": "Compensation", "name": "ECI — Total Compensation, Private (12m % change, NSA)", "freq": "Q"},
}

BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "bls_timeseries.csv"
META_PATH = DATA_DIR / "meta.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

class BLSError(Exception): pass

def fetch_bls_timeseries(series_ids, start_year, end_year):
    payload = {"seriesid": series_ids, "startyear": str(start_year), "endyear": str(end_year)}
    key = os.getenv("BLS_API_KEY")
    if key:
        payload["registrationkey"] = key
    r = requests.post(BLS_URL, json=payload, timeout=60)
    if r.status_code != 200:
        raise BLSError(f"HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    if data.get("status") != "REQUEST_SUCCEEDED":
        raise BLSError(f"BLS error: {json.dumps(data)[:300]}")
    return data

def _q_to_month(q: int) -> int:
    return {1: 3, 2: 6, 3: 9, 4: 12}[q]

def series_payload_to_rows(series_json):
    sid = series_json["seriesID"]
    rows = []
    for item in series_json["data"]:
        p = item.get("period")
        if not p or p == "M13":
            continue
        year = int(item["year"])
        if p.startswith("M"):
            month = int(p[1:])
        elif p.startswith("Q"):
            month = _q_to_month(int(p[1:]))
        else:
            continue
        dt = pd.Timestamp(year=year, month=month, day=1)
        val = float(item["value"])
        rows.append({"series_id": sid, "date": dt, "value": val})
    return rows

def load_existing():
    if CSV_PATH.exists():
        return pd.read_csv(CSV_PATH, parse_dates=["date"])
    return pd.DataFrame(columns=["series_id", "date", "value"])

def union_and_dedupe(df_old, df_new):
    df = pd.concat([df_old, df_new], ignore_index=True)
    df = df.drop_duplicates(subset=["series_id", "date"], keep="last")
    return df.sort_values(["series_id", "date"]).reset_index(drop=True)

def run_full_or_incremental():
    df_old = load_existing()
    if df_old.empty:
        start = START_YEAR
    else:
        last_date = df_old["date"].max()
        start = max(START_YEAR, (last_date - pd.DateOffset(months=24)).year)  # backfill window for revisions

    api = fetch_bls_timeseries(list(SERIES.keys()), start, END_YEAR)
    rows = [r for s in api["Results"]["series"] for r in series_payload_to_rows(s)]
    df_new = pd.DataFrame(rows)
    df_out = union_and_dedupe(df_old, df_new)

    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(CSV_PATH, index=False)
    META_PATH.write_text(json.dumps({"last_updated_utc": datetime.utcnow().isoformat()}, indent=2))
    print(f"Updated {len(df_out)} rows → {CSV_PATH}")
    return df_out

if __name__ == "__main__":
    run_full_or_incremental()

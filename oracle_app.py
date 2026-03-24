"""
Oracle-style ME dashboard scaffold backed by Google Sheets.

Run:
    streamlit run oracle_app.py
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st

DEFAULT_SHEET_ID = "1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8"


def _extract_sheet_id(url_or_id: str) -> str:
    value = (url_or_id or "").strip()
    if "/spreadsheets/d/" not in value:
        return value
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    return match.group(1) if match else value


def _read_sheet_public(sheet_id: str, gid: int = 0) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    return pd.read_csv(url)


def _read_sheet_auth(sheet_id: str, service_account_bytes: bytes, worksheet_name: Optional[str] = None) -> pd.DataFrame:
    import gspread
    from google.oauth2.service_account import Credentials

    creds_info = json.loads(service_account_bytes.decode("utf-8"))
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name) if worksheet_name else sh.get_worksheet(0)
    return pd.DataFrame(ws.get_all_records())


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "Date": "date",
        "Country_Name": "country",
        "Country_Code": "country_code",
        "Channel_Gp": "channel",
        "Channel_Name": "channel",
        "Platform": "platform",
        "UTM_Source_Gp": "utm_source",
        "Utm_Source_L": "utm_source_l",
        "Utm_Source_O": "utm_source_o",
        "Cost": "cost",
        "Ad_Spend": "cost",
        "Spend": "cost",
        "Clicks___Gp": "clicks",
        "Impressions___Gp": "impressions",
        "Qualified": "qualified",
        "Leads": "leads",
        "Pitching": "pitching",
        "Closed_Won": "closed_won",
    }
    for old, new in rename.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        df["date"] = pd.NaT

    for c in ["cost", "clicks", "impressions", "leads", "qualified", "pitching", "closed_won"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        else:
            df[c] = 0

    for c in ["country", "country_code", "channel", "platform", "utm_source", "utm_source_l", "utm_source_o"]:
        if c not in df.columns:
            df[c] = "Unknown"
        df[c] = df[c].astype(str).replace("nan", "Unknown")

    df["month"] = df["date"].dt.to_period("M").astype(str)
    return df


@st.cache_data(ttl=300)
def load_marketing_data(sheet_id: str, gid: int, service_account_bytes: Optional[bytes], worksheet_name: Optional[str]) -> pd.DataFrame:
    if service_account_bytes:
        raw = _read_sheet_auth(sheet_id, service_account_bytes, worksheet_name)
    else:
        raw = _read_sheet_public(sheet_id, gid)
    return _normalize(raw)


def card(col, title: str, value: str) -> None:
    col.markdown(
        f"""
        <div style="border:1px solid #E6E6E6;padding:14px;border-radius:8px;background:#fff;">
            <div style="font-size:12px;color:#777;">{title}</div>
            <div style="font-size:30px;font-weight:700;line-height:1.2;">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.set_page_config(page_title="X-Ray Marketing Dashboard", page_icon="📊", layout="wide")

st.markdown("## X-Ray Marketing Dashboard")
top_nav = "MARKETING"

with st.sidebar:
    st.subheader("Data Source")
    sheet_url_or_id = st.text_input("X-Ray Google Sheet URL or ID", value=DEFAULT_SHEET_ID)
    sheet_id = _extract_sheet_id(sheet_url_or_id)
    gid = st.number_input("gid", value=0, step=1)
    worksheet_name = st.text_input("Worksheet name (optional)", value="")
    creds_file = st.file_uploader("Service account JSON (optional)", type=["json"])
    st.caption("Leave service account empty only if sheet is publicly readable.")

    st.markdown("---")
    st.subheader("Filters")
    default_start = date(2025, 9, 1)
    default_end = date.today()
    start_date = st.date_input("Start date", value=default_start, max_value=default_end)
    end_date = st.date_input("End date", value=default_end, min_value=start_date, max_value=default_end)

service_account_bytes = creds_file.read() if creds_file else None

try:
    df = load_marketing_data(
        sheet_id=sheet_id,
        gid=int(gid),
        service_account_bytes=service_account_bytes,
        worksheet_name=worksheet_name.strip() or None,
    )
except Exception as exc:
    st.error(f"Failed to load sheet data: {exc}")
    st.stop()

if df.empty:
    st.warning("No data loaded from this sheet/tab.")
    st.stop()

mask = (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
df = df.loc[mask].copy()

if df.empty:
    st.warning("No rows after date filter.")
    st.stop()

country_opts = sorted([x for x in df["country"].dropna().unique().tolist() if x and x != "Unknown"])
selected_countries = st.multiselect("Country", ["All Countries"] + country_opts, default=["All Countries"])
if "All Countries" not in selected_countries and selected_countries:
    df = df[df["country"].isin(selected_countries)]

if top_nav == "MARKETING":
    st.markdown("### Marketing Dashboard")

    total_spend = float(df["cost"].sum())
    total_impr = int(df["impressions"].sum())
    total_clicks = int(df["clicks"].sum())
    total_leads = int(df["leads"].sum())
    total_qualified = int(df["qualified"].sum())
    total_pitching = int(df["pitching"].sum())
    total_cw = int(df["closed_won"].sum())
    ctr = (total_clicks / total_impr * 100) if total_impr else 0

    r1 = st.columns(4)
    card(r1[0], "Total Spend", f"${total_spend:,.2f}")
    card(r1[1], "Total Impressions", f"{total_impr:,}")
    card(r1[2], "Total Clicks", f"{total_clicks:,}")
    card(r1[3], "Click-Through Rate", f"{ctr:.2f}%")

    r2 = st.columns(4)
    card(r2[0], "Leads", f"{total_leads:,}")
    card(r2[1], "Qualified Leads", f"{total_qualified:,}")
    card(r2[2], "Pitching", f"{total_pitching:,}")
    card(r2[3], "Closed Wons", f"{total_cw:,}")

    tab_dash, tab_region, tab_tcv, tab_funnel, tab_budgets, tab_lost = st.tabs(
        ["Dashboard", "Regional Analysis", "TCV Analysis", "Sales Funnel", "Budgets", "Lost Analysis"]
    )

    with tab_dash:
        st.markdown("#### Performance Trends")
        monthly = (
            df.groupby("month", as_index=False)
            .agg(cost=("cost", "sum"), clicks=("clicks", "sum"), impressions=("impressions", "sum"))
            .sort_values("month")
        )
        m1, m2 = st.columns(2)
        with m1:
            fig_cost = px.line(monthly, x="month", y="cost", markers=True, title="Monthly Cost")
            st.plotly_chart(fig_cost, use_container_width=True)
        with m2:
            fig_clicks = px.line(monthly, x="month", y="clicks", markers=True, title="Monthly Clicks")
            st.plotly_chart(fig_clicks, use_container_width=True)

        st.markdown("#### Breakdown")
        b1, b2 = st.columns(2)
        with b1:
            by_country = df.groupby("country", as_index=False)["cost"].sum().sort_values("cost", ascending=False).head(15)
            st.plotly_chart(px.bar(by_country, x="country", y="cost", title="Spend by Country"), use_container_width=True)
        with b2:
            by_channel = df.groupby("channel", as_index=False)["cost"].sum().sort_values("cost", ascending=False).head(15)
            st.plotly_chart(px.bar(by_channel, x="channel", y="cost", title="Spend by Channel"), use_container_width=True)

    with tab_region:
        by_region = (
            df.groupby(["country", "month"], as_index=False)
            .agg(cost=("cost", "sum"), leads=("leads", "sum"))
            .sort_values(["month", "cost"], ascending=[True, False])
        )
        st.dataframe(by_region, use_container_width=True)
    with tab_tcv:
        st.info("TCV analysis wiring can be connected once opportunity/TCV columns are added to this source.")
    with tab_funnel:
        funnel_df = pd.DataFrame(
            [
                {"stage": "Impressions", "value": total_impr},
                {"stage": "Clicks", "value": total_clicks},
                {"stage": "Leads", "value": total_leads},
                {"stage": "Qualified", "value": total_qualified},
                {"stage": "Pitching", "value": total_pitching},
                {"stage": "Closed Wons", "value": total_cw},
            ]
        )
        st.plotly_chart(px.funnel(funnel_df, x="value", y="stage", title="Funnel Overview"), use_container_width=True)
    with tab_budgets:
        st.info("Budget tracking can be enabled once budget target columns/tabs are shared.")
    with tab_lost:
        st.info("Lost analysis needs lost-opportunity fields from CRM source.")

    with st.expander("Show raw rows"):
        st.dataframe(df, use_container_width=True)


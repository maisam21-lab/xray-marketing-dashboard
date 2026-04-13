"""
EurasiaX Performance Marketing Dashboard (Streamlit).
Replicates and improves on the Vercel dashboard with Spend, Lead, and Post Lead (SF Opportunity) data.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime, timedelta
from typing import Optional
from data_loader import (
    load_from_bigquery,
    load_from_csv,
    load_from_sheets,
    build_aggregated_metrics,
    DEFAULT_SHEET_ID,
)

st.set_page_config(
    page_title="EurasiaX Marketing Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Source of truth: Looker Studio (MENA); raw data in Google Sheet
LOOKER_REPORT_URL = "https://lookerstudio.google.com/u/0/reporting/ea7c7905-baf6-40ad-ad97-f36bd67c4dbc/page/p_w4ql3rgi1d"
RAW_DATA_SHEET_URL = "https://docs.google.com/spreadsheets/d/1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8/edit?pli=1&gid=8109573#gid=8109573"

# ---------------------------------------------------------------------------
# Banner: Looker Studio is source of truth
# ---------------------------------------------------------------------------
st.sidebar.markdown("**Source of truth: Looker Studio (MENA)**")
st.sidebar.markdown(f"[📊 Open Looker Studio report]({LOOKER_REPORT_URL})")
st.sidebar.markdown(f"[📋 Raw data (Google Sheet)]({RAW_DATA_SHEET_URL})")
st.sidebar.markdown("---")
st.sidebar.title("📊 This app (optional)")
st.sidebar.caption("Local view only. Use Looker for official reporting.")
st.sidebar.markdown("---")

# ---------------------------------------------------------------------------
# Data source & filters
# ---------------------------------------------------------------------------

data_source = st.sidebar.radio(
    "Data source",
    ["Google Sheets", "BigQuery", "CSV upload (3 files)"],
    index=0,
    help="Google Sheets = 3 tabs refreshed hourly; BigQuery = live warehouse; CSV = manual upload.",
)

# Date range
default_end = date.today()
default_start = date(2025, 1, 1)
ds_start = st.sidebar.date_input("Start date", default_start, max_value=default_end)
ds_end = st.sidebar.date_input("End date", default_end, min_value=ds_start, max_value=default_end)
if ds_start > ds_end:
    ds_start, ds_end = ds_end, ds_start

# Region filter (country/platform options set after data load)
st.sidebar.markdown("---")
st.sidebar.subheader("Filters")
filter_region = st.sidebar.selectbox("Region", ["All Regions", "GCC", "MENA"], index=0)
st.sidebar.markdown("---")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600)
def fetch_bigquery(ds_start: date, ds_end: date):
    return load_from_bigquery(ds_start, ds_end)


# Google Sheets: cache 5 min so hourly refresh is reflected
@st.cache_data(ttl=300)
def fetch_sheets(sheet_id: str, creds_key: Optional[bytes] = None):
    return load_from_sheets(spreadsheet_id=sheet_id, credentials_json=creds_key)


# Google Sheets config (only when Sheets selected)
sheet_id = DEFAULT_SHEET_ID
sheets_creds = None
if data_source == "Google Sheets":
    st.sidebar.caption("Data refreshed hourly from your Sheet.")
    sheet_id = st.sidebar.text_input(
        "Spreadsheet ID",
        value=DEFAULT_SHEET_ID,
        help="From the Sheet URL: docs.google.com/spreadsheets/d / [THIS_ID] / edit",
    ).strip() or DEFAULT_SHEET_ID
    use_sheets_auth = st.sidebar.checkbox("Use service account (private sheet)", value=False)
    if use_sheets_auth:
        sheets_creds_file = st.sidebar.file_uploader("Service account JSON", type=["json"], key="sheets_creds")
        if sheets_creds_file:
            sheets_creds = sheets_creds_file.read()

# CSV uploaders (only when CSV source selected)
spend_file = lead_file = opp_file = None
if data_source == "CSV upload (3 files)":
    st.sidebar.info("Upload 3 CSV files (Spend, Lead, Post Lead).")
    spend_file = st.sidebar.file_uploader("Spend (platform data)", type=["csv"])
    lead_file = st.sidebar.file_uploader("Lead data", type=["csv"])
    opp_file = st.sidebar.file_uploader("Post Lead (Opportunity)", type=["csv"])


def get_data():
    if data_source == "Google Sheets":
        df_spend, df_lead, df_opp, err = fetch_sheets(sheet_id, sheets_creds)
        if err:
            st.sidebar.error(err)
            return None, None, None, None
    elif data_source == "BigQuery":
        df_spend, df_lead, df_opp, err = fetch_bigquery(ds_start, ds_end)
        if err:
            st.sidebar.error(err)
            return None, None, None, None
    else:
        if not (spend_file and lead_file and opp_file):
            return None, None, None, None
        df_spend, df_lead, df_opp, err = load_from_csv(spend_file, lead_file, opp_file)
        if err:
            st.sidebar.error(err)
            return None, None, None, None

    agg = build_aggregated_metrics(df_spend, df_lead, df_opp)
    return df_spend, df_lead, df_opp, agg


df_spend, df_lead, df_opportunity, df_agg = get_data()

# Show load status (row counts) so user can see why there might be no data
def _row_count(df) -> int:
    return len(df) if df is not None and isinstance(df, pd.DataFrame) else 0

n_spend, n_lead, n_opp = _row_count(df_spend), _row_count(df_lead), _row_count(df_opportunity)
if data_source == "Google Sheets":
    st.sidebar.caption(f"**Data loaded:** Spend {n_spend} rows · Lead {n_lead} rows · Post Lead {n_opp} rows")
elif data_source == "BigQuery":
    st.sidebar.caption(f"**Data loaded:** Spend {n_spend} · Lead {n_lead} · Opportunity {n_opp} rows")
else:
    st.sidebar.caption(f"**Data loaded:** Spend {n_spend} · Lead {n_lead} · Opportunity {n_opp} rows")

# Dynamic filter options from data
countries = ["All Countries", "UAE", "Kuwait", "Saudi Arabia", "Bahrain", "Qatar"]
if df_agg is not None and not df_agg.empty and "market" in df_agg.columns:
    countries = ["All Countries"] + sorted(df_agg["market"].dropna().unique().tolist())
platforms = ["All Platforms"]
if df_spend is not None and not df_spend.empty and "platform" in df_spend.columns:
    platforms = ["All Platforms"] + sorted(df_spend["platform"].dropna().unique().tolist())

filter_country = st.sidebar.multiselect("Country", options=countries, default=["All Countries"])
filter_platform = st.sidebar.multiselect("Platform", options=platforms, default=["All Platforms"])


def apply_filters(df: pd.DataFrame, country_key: str = "market", platform_key: str = "platform"):
    if df is None or df.empty:
        return df
    out = df.copy()
    if "All Countries" not in filter_country and filter_country and country_key in out.columns:
        out = out[out[country_key].isin(filter_country)]
    if "All Platforms" not in filter_platform and filter_platform and platform_key in out.columns:
        out = out[out[platform_key].isin(filter_platform)]
    return out


# ---------------------------------------------------------------------------
# Page routing
# ---------------------------------------------------------------------------
page = st.sidebar.radio(
    "Page",
    [
        "Dashboard",
        "Regional Analysis",
        "TCV Analysis",
        "Sales Funnel",
        "Lost Analysis",
        "Marketing Budgets",
    ],
    index=0,
)

if df_agg is None or (isinstance(df_agg, pd.DataFrame) and df_agg.empty):
    st.warning("**There is no data to display.**")
    if data_source == "Google Sheets":
        st.info(
            "**Google Sheets:** Check the row counts in the sidebar. If any are 0:\n"
            "- Confirm the first 3 tabs are named exactly **Spend**, **Lead**, **Post Lead**.\n"
            "- Ensure each tab has data with headers in row 1 (e.g. Date, Market/Country, Platform, Spend).\n"
            "- If the sheet is private, enable **Use service account** and upload your JSON key."
        )
    elif data_source == "BigQuery":
        st.info(
            "**BigQuery:** Set your date range and ensure application default credentials are set "
            "(e.g. `gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`). "
            "If you see an error in the sidebar, fix that first."
        )
    else:
        st.info("Upload the 3 CSV files (Spend, Lead, Post Lead) in the sidebar.")
    st.stop()

# Apply filters to aggregated data
agg_f = apply_filters(df_agg, "market", None)
spend_f = apply_filters(df_spend, "market", "platform") if df_spend is not None else pd.DataFrame()
opp_f = apply_filters(df_opportunity, "kitchen_country", None) if df_opportunity is not None else pd.DataFrame()
lead_f = apply_filters(df_lead, "market", None) if df_lead is not None else pd.DataFrame()

# ---------------------------------------------------------------------------
# Dashboard (overview)
# ---------------------------------------------------------------------------
# Reminder: Looker Studio is source of truth
st.markdown(f"**Source of truth:** [Looker Studio (MENA)]({LOOKER_REPORT_URL}) · Raw data: [Google Sheet]({RAW_DATA_SHEET_URL})")
st.markdown("---")

if page == "Dashboard":
    st.title("EurasiaX Performance Marketing Dashboard")
    st.caption(f"Date range: {ds_start} – {ds_end} | Region: {filter_region} | Countries: {', '.join(filter_country) if filter_country else 'All'}")

    # KPIs
    total_spend = agg_f["spend"].sum()
    total_cw = int(agg_f["cw_inc_approved"].sum())
    total_tcv = agg_f["actual_tcv"].sum()
    total_leads = int(agg_f["total_leads"].sum())
    total_qualified = int(agg_f["qualified"].sum())
    cpcw = total_spend / total_cw if total_cw else None
    cpl = total_spend / total_leads if total_leads else None
    cost_tcv_pct = 100 * total_spend / total_tcv if total_tcv else None
    sql_pct = 100 * total_qualified / total_leads if total_leads else None

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Spend (USD)", f"${total_spend:,.0f}")
    k2.metric("Closed Won (Approved)", total_cw)
    k3.metric("Actual TCV (USD)", f"${total_tcv:,.0f}")
    k4.metric("Total Leads", total_leads)
    k5.metric("Qualified", total_qualified)

    k6, k7, k8, k9 = st.columns(4)
    k6.metric("CpCW (USD)", f"${cpcw:,.0f}" if cpcw else "—")
    k7.metric("CPL (USD)", f"${cpl:,.0f}" if cpl else "—")
    k8.metric("Cost / TCV %", f"{cost_tcv_pct:.1f}%" if cost_tcv_pct else "—")
    k9.metric("SQL %", f"{sql_pct:.1f}%" if sql_pct else "—")

    st.markdown("---")
    # Spend over time by market
    if not agg_f.empty and "unified_date" in agg_f.columns:
        spend_by_month = agg_f.groupby("unified_date").agg({"spend": "sum", "cw_inc_approved": "sum", "actual_tcv": "sum"}).reset_index()
        fig = go.Figure()
        fig.add_trace(go.Bar(x=spend_by_month["unified_date"], y=spend_by_month["spend"], name="Spend (USD)", marker_color="#1f77b4"))
        fig.add_trace(go.Scatter(x=spend_by_month["unified_date"], y=spend_by_month["actual_tcv"], name="TCV (USD)", line=dict(color="#ff7f0e", width=2), yaxis="y2"))
        fig.update_layout(
            title="Spend vs TCV by month",
            xaxis_title="Month",
            yaxis_title="Spend (USD)",
            yaxis2=dict(title="TCV (USD)", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=400,
        )
        st.plotly_chart(fig, width="stretch")

    c1, c2 = st.columns(2)
    with c1:
        if not agg_f.empty:
            by_market = agg_f.groupby("market").agg({"spend": "sum", "cw_inc_approved": "sum", "actual_tcv": "sum"}).reset_index()
            fig2 = px.bar(by_market, x="market", y="spend", title="Spend by market", color="spend", color_continuous_scale="Blues")
            st.plotly_chart(fig2, width="stretch")
    with c2:
        if not spend_f.empty and "platform" in spend_f.columns:
            by_platform = spend_f.groupby("platform")["spend"].sum().reset_index()
            fig3 = px.pie(by_platform, values="spend", names="platform", title="Spend by platform")
            st.plotly_chart(fig3, width="stretch")

    with st.expander("View aggregated data (month × market)"):
        st.dataframe(agg_f.head(100), width="stretch")

# ---------------------------------------------------------------------------
# Regional Analysis (CpCW:LF style)
# ---------------------------------------------------------------------------
elif page == "Regional Analysis":
    st.title("Regional Analysis")
    st.caption("CpCW and cost efficiency by region/market")

    if agg_f.empty:
        st.warning("No data after filters.")
    else:
        by_market = agg_f.groupby("market").agg(
            spend=("spend", "sum"),
            cw_inc_approved=("cw_inc_approved", "sum"),
            actual_tcv=("actual_tcv", "sum"),
            total_leads=("total_leads", "sum"),
            qualified=("qualified", "sum"),
        ).reset_index()
        by_market["cpcw"] = by_market.apply(lambda r: r["spend"] / r["cw_inc_approved"] if r["cw_inc_approved"] else None, axis=1)
        by_market["cpl"] = by_market.apply(lambda r: r["spend"] / r["total_leads"] if r["total_leads"] else None, axis=1)
        by_market["sql_pct"] = by_market.apply(lambda r: 100 * r["qualified"] / r["total_leads"] if r["total_leads"] else None, axis=1)

        fmt = {"spend": "${:,.0f}", "actual_tcv": "${:,.0f}"}
        try:
            by_market_display = by_market.copy()
            for c in ["cpcw", "cpl"]:
                if c in by_market_display.columns:
                    fmt[c] = "${:,.0f}"
            if "sql_pct" in by_market_display.columns:
                fmt["sql_pct"] = "{:.1f}%"
        except Exception:
            pass
        st.dataframe(by_market.style.format(fmt, na_rep="—"), width="stretch")
        fig = px.bar(by_market, x="market", y="cpcw", title="Cost per Closed Won (CpCW) by market", color="cpcw", color_continuous_scale="Viridis")
        st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------------------------------
# TCV Analysis
# ---------------------------------------------------------------------------
elif page == "TCV Analysis":
    st.title("TCV Analysis")
    st.caption("Total Contract Value and Closed Won by market and time")

    if opp_f.empty and "actual_tcv" in agg_f.columns:
        tcv_by_month_market = agg_f.groupby(["unified_date", "market"])["actual_tcv"].sum().reset_index()
        fig = px.bar(tcv_by_month_market, x="unified_date", y="actual_tcv", color="market", title="TCV by month and market", barmode="stack")
        st.plotly_chart(fig, width="stretch")
        st.dataframe(agg_f[["unified_date", "market", "actual_tcv", "cw_inc_approved"]].drop_duplicates(), width="stretch")
    elif not opp_f.empty:
        opp_f = opp_f.copy()
        opp_f["close_date"] = pd.to_datetime(opp_f["close_date"], errors="coerce")
        opp_f["month"] = opp_f["close_date"].dt.to_period("M").astype(str)
        tcv_table = opp_f.groupby(["month", "kitchen_country"]).agg(tcv_usd=("tcv_usd", "sum"), count=("opportunity_name", "count")).reset_index()
        fig = px.bar(tcv_table, x="month", y="tcv_usd", color="kitchen_country", title="TCV by month and market", barmode="stack")
        st.plotly_chart(fig, width="stretch")
        st.dataframe(tcv_table, width="stretch")
    else:
        st.warning("No opportunity data available.")

# ---------------------------------------------------------------------------
# Sales Funnel
# ---------------------------------------------------------------------------
elif page == "Sales Funnel":
    st.title("Sales Funnel")
    st.caption("Spend → Leads → Qualified → Closed Won")

    funnel_data = []
    if not agg_f.empty:
        funnel_data.append({"Stage": "Spend (USD)", "Value": agg_f["spend"].sum()})
    if not agg_f.empty:
        funnel_data.append({"Stage": "Total Leads", "Value": agg_f["total_leads"].sum()})
    if not agg_f.empty:
        funnel_data.append({"Stage": "Qualified", "Value": agg_f["qualified"].sum()})
    if not agg_f.empty:
        funnel_data.append({"Stage": "Closed Won (Approved)", "Value": agg_f["cw_inc_approved"].sum()})
    if not agg_f.empty:
        funnel_data.append({"Stage": "TCV (USD)", "Value": agg_f["actual_tcv"].sum()})

    if funnel_data:
        funnel_df = pd.DataFrame(funnel_data)
        fig = px.funnel(funnel_df, x="Value", y="Stage", title="Funnel overview")
        st.plotly_chart(fig, width="stretch")
        st.dataframe(funnel_df, width="stretch")
    else:
        st.warning("No data for funnel.")

# ---------------------------------------------------------------------------
# Lost Analysis (stub)
# ---------------------------------------------------------------------------
elif page == "Lost Analysis":
    st.title("Lost Analysis")
    st.info("Lost opportunities and reasons can be added when Salesforce lost-reason data is available. For now, use the Opportunity table filtered by stage = Closed Lost.")
    if not opp_f.empty and "stage" in opp_f.columns:
        lost = opp_f[opp_f["stage"].astype(str).str.lower().str.contains("lost", na=False)]
        if not lost.empty:
            st.dataframe(lost, width="stretch")
        else:
            st.caption("Current data contains only won opportunities. Add a query for lost opportunities to populate this view.")

# ---------------------------------------------------------------------------
# Marketing Budgets (stub)
# ---------------------------------------------------------------------------
elif page == "Marketing Budgets":
    st.title("Marketing Budgets")
    st.info("Budget vs actual can be added when budget targets are available (e.g. from Google Sheets or a budget table).")
    if not agg_f.empty:
        by_month = agg_f.groupby("unified_date")["spend"].sum().reset_index()
        fig = px.bar(by_month, x="unified_date", y="spend", title="Actual spend by month (budget comparison when available)")
        st.plotly_chart(fig, width="stretch")

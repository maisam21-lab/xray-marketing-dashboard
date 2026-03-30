"""
Oracle-style ME dashboard scaffold backed by Google Sheets.

Run:
    streamlit run oracle_app.py
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, time
from typing import Any, Optional, Union

import pandas as pd
import plotly.express as px
import streamlit as st

DEFAULT_SHEET_ID = "1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8"


def _default_sheet_id_from_secrets() -> str:
    """Optional Streamlit secret XRAY_SHEET_ID overrides default workbook."""
    try:
        s = st.secrets
        v = (s.get("XRAY_SHEET_ID") or s.get("xray_sheet_id") or "").strip()
        return v if v else DEFAULT_SHEET_ID
    except Exception:
        return DEFAULT_SHEET_ID


def _extract_sheet_id(url_or_id: str) -> str:
    value = (url_or_id or "").strip()
    if "/spreadsheets/d/" not in value:
        return value
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    return match.group(1) if match else value


def _coerce_worksheet_gid(gid: Union[int, float, None]) -> Optional[int]:
    """URL gid must be a positive integer. 0 or invalid → use first worksheet (index 0)."""
    if gid is None:
        return None
    try:
        g = int(gid)
    except (TypeError, ValueError):
        return None
    if g <= 0:
        return None
    return g


def _read_sheet_public(sheet_id: str, gid: int = 0) -> pd.DataFrame:
    gid_safe = max(0, int(gid))
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_safe}"
    try:
        return pd.read_csv(url)
    except Exception as e:
        err = str(e)
        if "401" in err or "Unauthorized" in err:
            raise RuntimeError(
                "Anonymous Google Sheet export returned 401 (sheet is private). "
                "This Streamlit app is not using a service account yet.\n\n"
                "Fix: In Streamlit Cloud → this app → Settings → Secrets, add the same "
                "`[gsheet_service_account]` block as your KSA tracker (full `private_key` PEM), "
                "OR add `[GCP_SERVICE_ACCOUNT]` with the same fields. "
                "Then share the spreadsheet with the service account `client_email` (Viewer). "
                "Enable Google Sheets API on that GCP project.\n\n"
                f"Technical detail: {e}"
            ) from e
        raise


def _coerce_service_account_dict(service_account_data: Union[bytes, dict, str]) -> dict:
    if isinstance(service_account_data, bytes):
        return json.loads(service_account_data.decode("utf-8"))
    if isinstance(service_account_data, str):
        return json.loads(service_account_data)
    if isinstance(service_account_data, dict):
        return dict(service_account_data)
    # Streamlit secrets (nested TOML) — try several shapes
    try:
        return dict(service_account_data.items())
    except Exception:
        pass
    out: dict[str, Any] = {}
    for attr in (
        "type",
        "project_id",
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
        "auth_provider_x509_cert_url",
        "client_x509_cert_url",
        "universe_domain",
    ):
        try:
            if hasattr(service_account_data, attr):
                v = getattr(service_account_data, attr)
                if v is not None and v != "":
                    out[attr] = v
        except Exception:
            continue
    if out.get("private_key") and out.get("client_email"):
        return out
    raise TypeError(f"Cannot convert service account secrets: {type(service_account_data)!r}")


def _validate_service_account_dict(d: dict) -> None:
    pk = (d.get("private_key") or "").strip()
    if not pk:
        raise ValueError("service account is missing private_key")
    if "BEGIN PRIVATE KEY" not in pk or "END PRIVATE KEY" not in pk:
        raise ValueError(
            "private_key must include a full PEM (lines between BEGIN PRIVATE KEY and END PRIVATE KEY)"
        )
    # Real RSA keys are much longer than a placeholder header-only string
    if len(pk) < 400:
        raise ValueError(
            "private_key looks truncated (too short). Paste the full key from the JSON downloaded in GCP."
        )
    if not (d.get("client_email") or "").strip():
        raise ValueError("service account is missing client_email")


def _service_account_from_streamlit_secrets() -> Optional[dict]:
    """Support GCP_SERVICE_ACCOUNT, gsheet_service_account, and any top-level key *service_account*."""
    try:
        s = st.secrets
    except Exception:
        return None

    keys_to_try: list[str] = []
    for k in ("GCP_SERVICE_ACCOUNT", "gsheet_service_account", "GSHEET_SERVICE_ACCOUNT"):
        if k not in keys_to_try:
            keys_to_try.append(k)
    try:
        for k in s:
            kl = str(k).lower().replace("-", "_")
            if kl in ("gcp_service_account", "gsheet_service_account"):
                if k not in keys_to_try:
                    keys_to_try.insert(0, k)
            elif "service_account" in kl and k not in keys_to_try:
                keys_to_try.append(k)
    except Exception:
        pass

    last_err: Optional[Exception] = None
    for key in keys_to_try:
        try:
            if key not in s:
                continue
            block = s[key]
            if block is None or block == "":
                continue
            d = _coerce_service_account_dict(block)
            _validate_service_account_dict(d)
            return d
        except Exception as e:
            last_err = e
            continue

    if last_err:
        st.session_state["_last_sa_secret_error"] = str(last_err)
    return None


def _secret_fingerprint(secret_dict: Optional[dict]) -> str:
    if not secret_dict:
        return "none"
    em = (secret_dict.get("client_email") or "").strip()
    pk = secret_dict.get("private_key") or ""
    h = hashlib.sha256(f"{em}|{len(pk)}".encode()).hexdigest()[:20]
    return f"{em.split('@')[0] if '@' in em else 'sa'}_{h}"


def _read_sheet_auth(
    sheet_id: str,
    service_account_data: Union[bytes, dict, str],
    worksheet_name: Optional[str] = None,
    worksheet_gid: Optional[int] = None,
) -> pd.DataFrame:
    import gspread
    from google.oauth2.service_account import Credentials

    creds_info = _coerce_service_account_dict(service_account_data)
    _validate_service_account_dict(creds_info)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    if worksheet_name:
        ws = sh.worksheet(worksheet_name)
    elif worksheet_gid is not None:
        try:
            ws = sh.get_worksheet_by_id(int(worksheet_gid))
        except Exception as e:
            raise RuntimeError(
                f"Could not open the tab with gid={worksheet_gid}. "
                "In the sheet URL, copy the number after `gid=` (e.g. …#gid=8109573 → 8109573). "
                "Set gid to 0 to use the first tab."
            ) from e
    else:
        ws = sh.get_worksheet(0)
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


def _filter_by_date_range(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Inclusive calendar-day filter; avoids pandas dtype errors comparing dt.date vs Timestamp."""
    if df.empty or "date" not in df.columns:
        return df
    s = pd.to_datetime(df["date"], errors="coerce")
    # tz-aware → naive UTC (safe compare with date pickers)
    try:
        if getattr(s.dtype, "tz", None) is not None:
            s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    except Exception:
        pass
    start_ts = pd.Timestamp(datetime.combine(start, time.min))
    end_ts = pd.Timestamp(datetime.combine(end, time.max))
    mask = (s >= start_ts) & (s <= end_ts)
    return df.loc[mask].copy()


@st.cache_data(ttl=300)
def load_marketing_data(
    sheet_id: str,
    gid: int,
    service_account_bytes: Optional[bytes],
    worksheet_name: Optional[str],
    _secret_fp: str,
) -> pd.DataFrame:
    """_secret_fp must change when Streamlit Secrets change, or cache keeps stale auth behavior."""
    secret_creds = _service_account_from_streamlit_secrets()

    creds_to_use = service_account_bytes if service_account_bytes else secret_creds
    if creds_to_use:
        raw = _read_sheet_auth(
            sheet_id,
            creds_to_use,
            worksheet_name,
            worksheet_gid=_coerce_worksheet_gid(gid),
        )
    else:
        raw = _read_sheet_public(sheet_id, gid)
    return _normalize(raw)


def card(col, title: str, value: str) -> None:
    col.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.set_page_config(
    page_title="X-Ray Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
    /* KSA-style: no left sidebar chrome — controls live in main area */
    section[data-testid="stSidebar"] { display: none !important; }
    [data-testid="collapsedControl"] { display: none !important; }
    .stApp { background: #FAFBFC; font-family: sans-serif; font-size: 0.8125rem !important; }
    header[data-testid="stHeader"] { background: #F1F3F4 !important; border-bottom: 1px solid #E2E8F0; }
    header[data-testid="stHeader"] * { color: #1E293B !important; }
    .topbar {
        background: #0F766E;
        color: #ffffff;
        border-radius: 0 10px 10px 0;
        padding: 14px 18px;
        margin-bottom: 14px;
        border: none;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    .topbar-wrap {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        flex-wrap: wrap;
    }
    .brand {
        border: 2px solid #ffffff;
        color: #ffffff;
        font-size: 13px;
        letter-spacing: 1px;
        font-weight: 700;
        border-radius: 5px;
        padding: 4px 9px;
    }
    .navhint { color: #D1FAE5; font-size: 11px; }
    .title-main { font-size: 1.35rem; font-weight: 700; margin-top: 4px; }
    .title-sub { font-size: 12px; color: #CCFBF1; margin-top: 2px; }
    .section-chip {
        display: inline-block;
        background: #F1F5F9;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 4px 10px;
        font-size: 11px;
        margin: 0 6px 8px 0;
        color: #475569;
        font-weight: 600;
    }
    .metric-card {
        border: 1px solid #E2E8F0;
        padding: 14px 16px;
        border-radius: 10px;
        background: linear-gradient(145deg, #f0fdf4 0%, #e0f2fe 100%);
        border-left: 4px solid #0F766E;
        box-shadow: 0 1px 3px rgba(0,0,0,.08);
        min-height: 92px;
    }
    .metric-title { font-size: 12px; color: #374151; margin-bottom: 8px; font-weight: 600; }
    .metric-value { font-size: 28px; font-weight: 700; line-height: 1.1; color: #111827; }
    .block-title { font-size: 18px; font-weight: 700; color: #0f172a; margin: 8px 0 4px 0; }
    .block-subtitle { font-size: 12px; color: #64748b; margin-bottom: 12px; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #F1F5F9;
        padding: 8px;
        border-radius: 10px;
        overflow-x: auto !important;
        overflow-y: hidden !important;
        flex-wrap: nowrap !important;
    }
    .stTabs [data-baseweb="tab"] { padding: 10px 18px; border-radius: 8px; font-weight: 500; color: #475569; flex-shrink: 0; }
    .stTabs [aria-selected="true"] { background: #0F766E !important; color: white !important; }
    .stTabs [aria-selected="true"] span { color: white !important; }
    .streamlit-expanderHeader { background: #F8FAFC; border-radius: 8px; border-left: 4px solid #0F766E; }
    .stTextInput input, .stSelectbox > div, .stDateInput input {
        border-radius: 6px !important;
        background: #F8FAFC !important;
        border: 1px solid #E2E8F0 !important;
    }
    .stDataFrame { border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: 1px solid #E2E8F0; }
    [data-testid="stMetricValue"] { color: #1E293B !important; }
    [data-testid="stMetricLabel"] { color: #64748B !important; }
    .stCaption { color: #64748B !important; }
    .stAlert { border-radius: 8px; border-left: 4px solid #0F766E; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="topbar">
      <div class="topbar-wrap">
        <div>
          <div style="display:flex; align-items:center; gap:14px;">
            <span class="brand">X-RAY</span>
            <span class="navhint">RANKING · PIPELINE · BOB · REP · COUNTRY · EMAILS · MARKETING</span>
          </div>
          <div class="title-main">Middle East Revenue & Marketing Cockpit</div>
          <div class="title-sub">Executive view for paid media efficiency and commercial outcomes</div>
        </div>
        <div class="navhint">ME Dashboard</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("Data source & filters (KSA-style: controls in main area, sidebar hidden)", expanded=False):
    st.caption(
        "The **service account JSON** (or Streamlit Secrets) only proves **who** is calling Google. "
        "The **spreadsheet URL or ID** below chooses **which workbook** to read — same as pointing the KSA tracker at a sheet ID."
    )
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        sheet_url_or_id = st.text_input(
            "Google Sheet URL or ID",
            value=_default_sheet_id_from_secrets(),
            help="Paste full URL or just the ID from /spreadsheets/d/<ID>/",
        )
    with c2:
        gid = st.number_input(
            "gid (tab ID from URL #gid=…)",
            value=0,
            min_value=0,
            step=1,
            help="Paste the number after gid= in the sheet URL (e.g. 8109573). Use 0 for the first tab.",
        )
    with c3:
        worksheet_name = st.text_input("Worksheet name (optional)", value="", help="Leave empty for first sheet")

    creds_file = st.file_uploader(
        "Service account JSON (optional — or use Streamlit Secrets)",
        type=["json"],
        help="Upload only if not using Cloud Secrets. Does not select which sheet; use the field above.",
    )
    default_start = date(2025, 9, 1)
    default_end = date.today()
    d1, d2 = st.columns(2)
    with d1:
        start_date = st.date_input("Start date", value=default_start, max_value=default_end)
    with d2:
        end_date = st.date_input("End date", value=default_end, min_value=start_date, max_value=default_end)

    sheet_id = _extract_sheet_id(sheet_url_or_id)
    service_account_bytes = creds_file.read() if creds_file else None
    _preload_secret = _service_account_from_streamlit_secrets()
    _secret_fp = _secret_fingerprint(_preload_secret)
    if service_account_bytes:
        _secret_fp = f"upload_{hashlib.sha256(service_account_bytes).hexdigest()[:16]}"

    st.markdown("**Auth**")
    if service_account_bytes:
        st.success("Using uploaded service account JSON (identity only — workbook is chosen above).")
    elif _preload_secret:
        st.success(
            f"Using Streamlit Secrets: `{_preload_secret.get('client_email', '?')}` "
            f"(fingerprint `{_secret_fp}`)"
        )
    else:
        st.warning(
            "No valid service account in **this** app's Streamlit Secrets. "
            "Anonymous export only works if the sheet is public."
        )
        if st.session_state.get("_last_sa_secret_error"):
            st.caption(f"Last parse error: {st.session_state['_last_sa_secret_error']}")

main_section = st.radio(
    "Section",
    ["Country", "Rep", "BoB", "Marketing"],
    horizontal=True,
    index=3,
    label_visibility="collapsed",
)

try:
    df = load_marketing_data(
        sheet_id=sheet_id,
        gid=int(gid),
        service_account_bytes=service_account_bytes,
        worksheet_name=worksheet_name.strip() or None,
        _secret_fp=_secret_fp,
    )
except Exception as exc:
    st.error(f"Failed to load sheet data: {exc}")
    st.stop()

if df.empty:
    st.warning("No data loaded from this sheet/tab.")
    st.stop()

df = _filter_by_date_range(df, start_date, end_date)

if df.empty:
    st.warning("No rows after date filter.")
    st.stop()

country_opts = sorted([x for x in df["country"].dropna().unique().tolist() if x and x != "Unknown"])
selected_countries = st.multiselect("Country", ["All Countries"] + country_opts, default=["All Countries"])
if "All Countries" not in selected_countries and selected_countries:
    df = df[df["country"].isin(selected_countries)]

platform_opts = sorted([x for x in df["platform"].dropna().unique().tolist() if x and x != "Unknown"])
selected_platforms = st.multiselect("Platform", ["All Platforms"] + platform_opts, default=["All Platforms"])
if "All Platforms" not in selected_platforms and selected_platforms:
    df = df[df["platform"].isin(selected_platforms)]

if main_section == "Marketing":
    st.markdown('<div class="block-title">Marketing Command Center</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="block-subtitle">Track media efficiency, lead quality, and funnel progression in one view.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <span class="section-chip">Dashboard</span>
        <span class="section-chip">Regional Analysis</span>
        <span class="section-chip">TCV Analysis</span>
        <span class="section-chip">Sales Funnel</span>
        <span class="section-chip">Budgets</span>
        <span class="section-chip">Lost Analysis</span>
        """,
        unsafe_allow_html=True,
    )

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
            fig_cost.update_traces(line_color="#2563eb", marker_color="#2563eb")
            fig_cost.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_cost, use_container_width=True)
        with m2:
            fig_clicks = px.line(monthly, x="month", y="clicks", markers=True, title="Monthly Clicks")
            fig_clicks.update_traces(line_color="#0f766e", marker_color="#0f766e")
            fig_clicks.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_clicks, use_container_width=True)

        st.markdown("#### Breakdown")
        b1, b2 = st.columns(2)
        with b1:
            by_country = df.groupby("country", as_index=False)["cost"].sum().sort_values("cost", ascending=False).head(15)
            fig_country = px.bar(by_country, x="country", y="cost", title="Spend by Country")
            fig_country.update_traces(marker_color="#334155")
            fig_country.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_country, use_container_width=True)
        with b2:
            by_channel = df.groupby("channel", as_index=False)["cost"].sum().sort_values("cost", ascending=False).head(15)
            fig_channel = px.bar(by_channel, x="channel", y="cost", title="Spend by Channel")
            fig_channel.update_traces(marker_color="#16a34a")
            fig_channel.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_channel, use_container_width=True)

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

elif main_section == "Country":
    st.markdown('<div class="block-title">Country Quality Analysis</div>', unsafe_allow_html=True)
    by_country = (
        df.groupby("country", as_index=False)
        .agg(
            spend=("cost", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            leads=("leads", "sum"),
            qualified=("qualified", "sum"),
            closed_won=("closed_won", "sum"),
        )
        .sort_values("spend", ascending=False)
    )
    if by_country.empty:
        st.info("No country data for the current filters.")
    else:
        top_country = by_country.iloc[0]["country"]
        c1, c2, c3, c4 = st.columns(4)
        card(c1, "Top Country", str(top_country))
        card(c2, "Markets", f"{len(by_country)}")
        card(c3, "Total Spend", f"${by_country['spend'].sum():,.0f}")
        card(c4, "Closed Won", f"{int(by_country['closed_won'].sum()):,}")
        l1, l2 = st.columns(2)
        with l1:
            st.plotly_chart(px.bar(by_country, x="country", y="spend", title="Spend by Country"), use_container_width=True)
        with l2:
            st.plotly_chart(px.bar(by_country, x="country", y="closed_won", title="Closed Won by Country"), use_container_width=True)
        st.dataframe(by_country, use_container_width=True)

elif main_section == "Rep":
    st.markdown('<div class="block-title">Rep Quality Analysis</div>', unsafe_allow_html=True)
    rep_df = df.copy()
    rep_df["rep"] = rep_df.get("rep_name", "Unassigned")
    rep_agg = (
        rep_df.groupby("rep", as_index=False)
        .agg(
            spend=("cost", "sum"),
            leads=("leads", "sum"),
            qualified=("qualified", "sum"),
            closed_won=("closed_won", "sum"),
        )
        .sort_values("closed_won", ascending=False)
    )
    if rep_agg.empty:
        st.info("No rep-level rows. Add a rep column in sheet (e.g., rep_name) to fully match the screenshot.")
    else:
        r1, r2, r3, r4 = st.columns(4)
        card(r1, "Active Reps", f"{len(rep_agg)}")
        card(r2, "Total Leads", f"{int(rep_agg['leads'].sum()):,}")
        card(r3, "Qualified", f"{int(rep_agg['qualified'].sum()):,}")
        card(r4, "Closed Won", f"{int(rep_agg['closed_won'].sum()):,}")
        st.plotly_chart(px.bar(rep_agg.head(20), x="rep", y="closed_won", title="Rep Performance (Closed Won)"), use_container_width=True)
        st.dataframe(rep_agg, use_container_width=True)

elif main_section == "BoB":
    st.markdown('<div class="block-title">Book of Business</div>', unsafe_allow_html=True)
    bob_df = df.copy()
    if "account" not in bob_df.columns:
        bob_df["account"] = "Account not provided"
    if "owner" not in bob_df.columns:
        bob_df["owner"] = "Owner not provided"
    bob_df["status"] = bob_df.get("status", "Unknown")
    summary1, summary2, summary3, summary4 = st.columns(4)
    card(summary1, "Filtered Accounts", f"{bob_df['account'].nunique():,}")
    card(summary2, "Touched", f"{int((bob_df['clicks'] > 0).sum()):,}")
    card(summary3, "With Open Opps", f"{int((bob_df['pitching'] > 0).sum()):,}")
    card(summary4, "Untouched", f"{int((bob_df['clicks'] == 0).sum()):,}")
    display_cols = [c for c in ["account", "country", "channel", "owner", "status", "clicks", "leads", "qualified", "pitching", "closed_won"] if c in bob_df.columns]
    st.dataframe(bob_df[display_cols].head(500), use_container_width=True)


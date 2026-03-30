"""
Oracle-style ME dashboard scaffold backed by Google Sheets.

Run:
    streamlit run oracle_app.py
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, time, timedelta
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
            try:
                tab_lines = [f"  • \"{w.title}\" → gid **{w.id}**" for w in sh.worksheets()]
                tab_help = "\n".join(tab_lines) if tab_lines else "  (none)"
            except Exception:
                tab_help = "  (could not list tabs)"
            raise RuntimeError(
                f"Could not open a tab with gid={worksheet_gid}. "
                "**gid is not tab order (1,2,3).** It must match the number after `gid=` in the URL when that tab is open, "
                "or use **0** for the **first (leftmost) tab**.\n\n"
                f"Tabs in this file:\n{tab_help}"
            ) from e
    else:
        ws = sh.get_worksheet(0)
    return pd.DataFrame(ws.get_all_records())


def _norm_header_key(name: str) -> str:
    """Lowercase, collapse spaces/underscores so e.g. Clicks___Gp and clicks_gp match."""
    s = str(name).strip().lower()
    s = re.sub(r"[\s_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# Normalized header → canonical column (covers X-Ray export names + plain English headers)
_NORM_TO_FIELD: dict[str, str] = {
    "date": "date",
    "day": "date",
    "period": "date",
    "week": "date",
    "country_name": "country",
    "country": "country",
    "market": "country",
    "geo": "country",
    "country_code": "country_code",
    "channel_gp": "channel",
    "channel_name": "channel",
    "channel": "channel",
    "media_type": "channel",
    "platform": "platform",
    "cost": "cost",
    "ad_spend": "cost",
    "spend": "cost",
    "cost_usd": "cost",
    "adspend": "cost",
    "clicks_gp": "clicks",
    "clicks": "clicks",
    "impressions_gp": "impressions",
    "impressions": "impressions",
    "impr": "impressions",
    "leads": "leads",
    "qualified": "qualified",
    "pitching": "pitching",
    "closed_won": "closed_won",
    "closedwon": "closed_won",
    "closed_won_deals": "closed_won",
    "utm_source_gp": "utm_source",
    "utm_source": "utm_source",
    "utm_source_l": "utm_source_l",
    "utm_source_o": "utm_source_o",
}

_NUM_FIELDS = frozenset({"cost", "clicks", "impressions", "leads", "qualified", "pitching", "closed_won"})


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    raw_cols = [str(c).strip() for c in df.columns]
    attrs: dict[str, Any] = {"sheet_columns": raw_cols}

    if df.empty:
        out = pd.DataFrame()
        attrs["fields_mapped"] = []
        out.attrs.update(attrs)
        return out

    df = df.copy()
    df.columns = raw_cols

    # Map each sheet column to at most one canonical field (first wins for dims; sum for dup metrics)
    field_to_sources: dict[str, list[str]] = {}
    for col in df.columns:
        nk = _norm_header_key(col)
        field = _NORM_TO_FIELD.get(nk)
        if field:
            field_to_sources.setdefault(field, []).append(col)

    out = pd.DataFrame(index=df.index)
    for field, srcs in field_to_sources.items():
        if len(srcs) == 1:
            out[field] = df[srcs[0]]
        elif field in _NUM_FIELDS:
            acc = pd.to_numeric(df[srcs[0]], errors="coerce").fillna(0)
            for c in srcs[1:]:
                acc = acc + pd.to_numeric(df[c], errors="coerce").fillna(0)
            out[field] = acc
        else:
            out[field] = df[srcs[0]]

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    else:
        out["date"] = pd.NaT

    for c in ["cost", "clicks", "impressions", "leads", "qualified", "pitching", "closed_won"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
        else:
            out[c] = 0

    for c in ["country", "country_code", "channel", "platform", "utm_source", "utm_source_l", "utm_source_o"]:
        if c not in out.columns:
            out[c] = "Unknown"
        out[c] = out[c].astype(str).replace("nan", "Unknown")

    out["month"] = out["date"].dt.to_period("M").astype(str)
    attrs["fields_mapped"] = sorted(field_to_sources.keys())
    out.attrs.update(attrs)
    return out


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
def list_worksheet_meta(sheet_id: str, _secret_fp: str) -> list[tuple[str, int]]:
    """Spreadsheet worksheet titles and numeric ids, **same order as in Google Sheets** (left → right)."""
    import gspread
    from google.oauth2.service_account import Credentials

    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        raise RuntimeError(
            "No service account in Streamlit Secrets. Add a `[gsheet_service_account]` block "
            "(or `GCP_SERVICE_ACCOUNT`) in this app’s Secrets, then reboot."
        )
    creds_info = _coerce_service_account_dict(secret_creds)
    _validate_service_account_dict(creds_info)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return [(w.title, int(w.id)) for w in sh.worksheets()]


@st.cache_data(ttl=300)
def load_all_worksheets_combined(sheet_id: str, _secret_fp: str) -> pd.DataFrame:
    """Read every worksheet in the spreadsheet (backend) and stack rows with `source_tab` set to the tab title."""
    meta = list_worksheet_meta(sheet_id, _secret_fp)
    frames: list[pd.DataFrame] = []
    tab_stats: list[tuple[str, int]] = []
    for title, ws_gid in meta:
        try:
            df = load_marketing_data(sheet_id, ws_gid, _secret_fp)
        except Exception:
            tab_stats.append((title, -1))
            continue
        if df.empty:
            tab_stats.append((title, 0))
            continue
        df = df.copy()
        df["source_tab"] = (title.strip() if title.strip() else "Sheet")
        frames.append(df)
        tab_stats.append((title, len(df)))
    if not frames:
        out = pd.DataFrame()
        out.attrs["tab_stats"] = tab_stats
        out.attrs["worksheet_order"] = [t for t, _ in meta]
        return out
    combined = pd.concat(frames, ignore_index=True)
    combined.attrs["tab_stats"] = tab_stats
    combined.attrs["worksheet_order"] = [t for t, _ in meta]
    try:
        combined.attrs["fields_mapped"] = list(frames[0].attrs.get("fields_mapped", []) or [])
    except Exception:
        combined.attrs["fields_mapped"] = []
    combined.attrs["sheet_columns"] = list(combined.columns)
    return combined


@st.cache_data(ttl=300)
def load_marketing_data(
    sheet_id: str,
    worksheet_gid: int,
    _secret_fp: str,
) -> pd.DataFrame:
    """Reads one worksheet by Google’s numeric worksheet id (gid in URL). Cached per tab."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        raise RuntimeError(
            "No service account in Streamlit Secrets. Add a `[gsheet_service_account]` block "
            "(or `GCP_SERVICE_ACCOUNT`) in this app’s Secrets, then reboot."
        )
    raw = _read_sheet_auth(
        sheet_id,
        secret_creds,
        worksheet_name=None,
        worksheet_gid=int(worksheet_gid),
    )
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


def render_main_dashboard(
    df_loaded: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> None:
    """Metrics from all worksheets combined; no filter banners — date slice falls back to all rows silently."""
    key_suffix = "main"
    if df_loaded.empty:
        return

    df_filtered = _filter_by_date_range(df_loaded, start_date, end_date)
    df_date = df_loaded if df_filtered.empty else df_filtered
    if df_date.empty:
        return

    st.markdown('<div class="block-title">Marketing dashboard</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="block-subtitle">Scorecards and breakdowns from the sheet columns (spend, delivery, funnel).</div>',
        unsafe_allow_html=True,
    )

    country_opts = sorted([x for x in df_date["country"].dropna().unique().tolist() if x and x != "Unknown"])
    selected_countries = st.multiselect(
        "Country",
        ["All Countries"] + country_opts,
        default=["All Countries"],
        key=f"{key_suffix}_country",
    )
    df = df_date.copy()
    if "All Countries" not in selected_countries and selected_countries:
        df = df[df["country"].isin(selected_countries)]

    platform_opts = sorted([x for x in df_date["platform"].dropna().unique().tolist() if x and x != "Unknown"])
    selected_platforms = st.multiselect(
        "Platform",
        ["All Platforms"] + platform_opts,
        default=["All Platforms"],
        key=f"{key_suffix}_platform",
    )
    if "All Platforms" not in selected_platforms and selected_platforms:
        df = df[df["platform"].isin(selected_platforms)]

    if "source_tab" in df.columns:
        st_opts = sorted([x for x in df["source_tab"].dropna().unique().tolist() if x])
        selected_tabs = st.multiselect(
            "Source tab",
            ["All tabs"] + st_opts,
            default=["All tabs"],
            key=f"{key_suffix}_source_tab",
        )
        if "All tabs" not in selected_tabs and selected_tabs:
            df = df[df["source_tab"].isin(selected_tabs)]

    total_spend = float(df["cost"].sum())
    total_impr = int(df["impressions"].sum())
    total_clicks = int(df["clicks"].sum())
    total_leads = int(df["leads"].sum())
    total_qualified = int(df["qualified"].sum())
    total_pitching = int(df["pitching"].sum())
    total_cw = int(df["closed_won"].sum())
    ctr = (total_clicks / total_impr * 100) if total_impr else 0
    cpc = (total_spend / total_clicks) if total_clicks else 0.0
    cpm = (total_spend / total_impr * 1000) if total_impr else 0.0
    cpl = (total_spend / total_leads) if total_leads else 0.0
    cpsql = (total_spend / total_qualified) if total_qualified else 0.0

    st.markdown("#### Scorecards")
    r1 = st.columns(4)
    card(r1[0], "Total spend", f"${total_spend:,.2f}")
    card(r1[1], "Impressions", f"{total_impr:,}")
    card(r1[2], "Clicks", f"{total_clicks:,}")
    card(r1[3], "CTR", f"{ctr:.2f}%")

    r2 = st.columns(4)
    card(r2[0], "CPC", f"${cpc:,.2f}")
    card(r2[1], "CPM", f"${cpm:,.2f}")
    card(r2[2], "CPL (cost / lead)", f"${cpl:,.2f}")
    card(r2[3], "Cost / qualified", f"${cpsql:,.2f}")

    r3 = st.columns(4)
    card(r3[0], "Leads", f"{total_leads:,}")
    card(r3[1], "Qualified", f"{total_qualified:,}")
    card(r3[2], "Pitching", f"{total_pitching:,}")
    card(r3[3], "Closed won", f"{total_cw:,}")

    tab_overview, tab_region, tab_funnel = st.tabs(
        ["Overview", "Regional", "Funnel"],
    )

    with tab_overview:
        st.markdown("#### Trends")
        monthly = (
            df.groupby("month", as_index=False)
            .agg(cost=("cost", "sum"), clicks=("clicks", "sum"), impressions=("impressions", "sum"))
            .sort_values("month")
        )
        m1, m2 = st.columns(2)
        with m1:
            fig_cost = px.line(monthly, x="month", y="cost", markers=True, title="Cost by month")
            fig_cost.update_traces(line_color="#2563eb", marker_color="#2563eb")
            fig_cost.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_cost, use_container_width=True, key=f"{key_suffix}_pl_cost_mo")
        with m2:
            fig_clicks = px.line(monthly, x="month", y="clicks", markers=True, title="Clicks by month")
            fig_clicks.update_traces(line_color="#0f766e", marker_color="#0f766e")
            fig_clicks.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_clicks, use_container_width=True, key=f"{key_suffix}_pl_clicks_mo")

        st.markdown("#### Spend breakdown")
        b1, b2 = st.columns(2)
        with b1:
            by_country = (
                df.groupby("country", as_index=False)["cost"].sum().sort_values("cost", ascending=False).head(15)
            )
            fig_country = px.bar(by_country, x="country", y="cost", title="Spend by country")
            fig_country.update_traces(marker_color="#334155")
            fig_country.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_country, use_container_width=True, key=f"{key_suffix}_pl_country")
        with b2:
            by_channel = (
                df.groupby("channel", as_index=False)["cost"].sum().sort_values("cost", ascending=False).head(15)
            )
            fig_channel = px.bar(by_channel, x="channel", y="cost", title="Spend by channel")
            fig_channel.update_traces(marker_color="#16a34a")
            fig_channel.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
            st.plotly_chart(fig_channel, use_container_width=True, key=f"{key_suffix}_pl_channel")

    with tab_region:
        by_region = (
            df.groupby(["country", "month"], as_index=False)
            .agg(cost=("cost", "sum"), leads=("leads", "sum"))
            .sort_values(["month", "cost"], ascending=[True, False])
        )
        st.dataframe(by_region, use_container_width=True, key=f"{key_suffix}_df_region")

    with tab_funnel:
        funnel_df = pd.DataFrame(
            [
                {"stage": "Impressions", "value": total_impr},
                {"stage": "Clicks", "value": total_clicks},
                {"stage": "Leads", "value": total_leads},
                {"stage": "Qualified", "value": total_qualified},
                {"stage": "Pitching", "value": total_pitching},
                {"stage": "Closed won", "value": total_cw},
            ]
        )
        st.plotly_chart(
            px.funnel(funnel_df, x="value", y="stage", title="Funnel"),
            use_container_width=True,
            key=f"{key_suffix}_pl_funnel",
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
            <span class="navhint">Marketing performance</span>
          </div>
          <div class="title-main">Marketing dashboard</div>
          <div class="title-sub">Scorecards and trends from your connected Google Sheet</div>
        </div>
        <div class="navhint">ME</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Backend-only: spreadsheet from secrets (`XRAY_SHEET_ID` / default), last ~24 months date window, no filter UI.
_end = date.today()
_start = _end - timedelta(days=730)
sheet_id = _extract_sheet_id(_default_sheet_id_from_secrets())
_secret_fp = _secret_fingerprint(_service_account_from_streamlit_secrets())

try:
    df_loaded = load_all_worksheets_combined(sheet_id, _secret_fp)
except Exception as exc:
    st.error(f"Failed to load spreadsheet: {exc}")
    st.stop()

render_main_dashboard(df_loaded, _start, _end)

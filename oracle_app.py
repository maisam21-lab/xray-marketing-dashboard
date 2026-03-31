"""
Oracle-style ME dashboard scaffold backed by Google Sheets.

Run:
    streamlit run oracle_app.py
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd
import plotly.express as px
import streamlit as st

DEFAULT_SHEET_ID = "1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8"
DEFAULT_SOURCE_TRUTH_GID = 8109573
# Default empty on Streamlit Cloud; set `XRAY_EXCEL_PATH` in secrets or `XRAY_EXCEL_PATH_DEFAULT` locally.
DEFAULT_LOCAL_EXCEL_PATH = (os.environ.get("XRAY_EXCEL_PATH_DEFAULT") or "").strip()


def _default_sheet_id_from_secrets() -> str:
    """Optional Streamlit secret XRAY_SHEET_ID overrides default workbook."""
    try:
        s = st.secrets
        v = (s.get("XRAY_SHEET_ID") or s.get("xray_sheet_id") or "").strip()
        return v if v else DEFAULT_SHEET_ID
    except Exception:
        return DEFAULT_SHEET_ID


def _default_truth_gid_from_secrets() -> int:
    """Optional Streamlit secret XRAY_TRUTH_GID overrides default source-of-truth tab gid."""
    try:
        s = st.secrets
        v = (s.get("XRAY_TRUTH_GID") or s.get("xray_truth_gid") or "").strip()
        return int(v) if v else DEFAULT_SOURCE_TRUTH_GID
    except Exception:
        return DEFAULT_SOURCE_TRUTH_GID


def _default_excel_path_from_secrets() -> str:
    """Optional Streamlit secret XRAY_EXCEL_PATH overrides default local workbook path."""
    try:
        s = st.secrets
        v = (s.get("XRAY_EXCEL_PATH") or s.get("xray_excel_path") or "").strip()
        return v if v else DEFAULT_LOCAL_EXCEL_PATH
    except Exception:
        return DEFAULT_LOCAL_EXCEL_PATH


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
    """Lowercase; non-alphanumeric → underscores (matches ME X-Ray Excel headers like `CPCW:LF`, `Cost/TCV%`)."""
    s = str(name).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# Normalized header → canonical column (covers X-Ray export names + ME X-Ray Excel template)
_NORM_TO_FIELD: dict[str, str] = {
    "date": "date",
    "day": "date",
    "period": "date",
    "week": "date",
    "create_date": "date",
    "date_formatted": "date",
    "close_date": "date",
    "country_name": "country",
    "country": "country",
    "market": "country",
    "geo": "country",
    "kitchen_country": "country",
    "country_code": "country_code",
    "channel_gp": "channel",
    "channel_name": "channel",
    "channel": "channel",
    "media_type": "channel",
    "lead_source": "channel",
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
    "cw_including_approved": "closed_won",
    "utm_source_gp": "utm_source",
    "utm_source": "utm_source",
    "utm_source_l": "utm_source_l",
    "utm_source_o": "utm_source_o",
    "month": "report_month",
    "tcv": "tcv",
    "tcv_usd": "tcv",
    "tcv_converted": "tcv",
    "1st_month_lf": "first_month_lf",
    "monthly_lf_usd": "first_month_lf",
    "cpcw": "cpcw",
    "cpcw_lf": "cpcw_lf",
    "cost_tcv": "cost_tcv_pct",
    "sql": "sql_pct",
    "q_win_rate": "q_win_rate",
}

_NUM_FIELDS = frozenset(
    {
        "cost",
        "clicks",
        "impressions",
        "leads",
        "qualified",
        "pitching",
        "closed_won",
        "tcv",
        "first_month_lf",
        "cpcw",
        "cpcw_lf",
        "cost_tcv_pct",
        "sql_pct",
        "q_win_rate",
    }
)


def _to_number_series(s: pd.Series) -> pd.Series:
    """Robust numeric parser for Sheets/CSV text like '$1,234.50', '3.4%', '(120)'."""
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").fillna(0)
    txt = s.astype(str).str.strip()
    neg_paren = txt.str.match(r"^\(.*\)$", na=False)
    cleaned = (
        txt.str.replace(r"[\$,]", "", regex=True)
        .str.replace("%", "", regex=False)
        .str.replace(r"^\((.*)\)$", r"\1", regex=True)
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    out = pd.to_numeric(cleaned, errors="coerce").fillna(0)
    out.loc[neg_paren] = -out.loc[neg_paren]
    return out


def _parse_report_month_series(s: pd.Series) -> pd.Series:
    """Parse Excel `Month` cells: datetimes, `Sept`, `December`, etc."""
    raw = s.copy()
    out = pd.to_datetime(raw, errors="coerce")
    mask = out.isna() & raw.notna()
    if not mask.any():
        return out
    for idx in raw.index[mask]:
        v = raw.loc[idx]
        val = str(v).strip()
        parsed: Optional[pd.Timestamp] = None
        vl = val.lower()
        # Ambiguous month-only labels: prefer FY order (Sep–Dec 2025, then Jan 2026, …).
        if vl.startswith("jan"):
            years = (2026, 2025, 2024)
        else:
            years = (2025, 2026, 2024)
        for y in years:
            t = pd.to_datetime(f"{val} 1, {y}", errors="coerce")
            if pd.notna(t):
                parsed = t
                break
        out.loc[idx] = parsed if parsed is not None else pd.NaT
    return out


def _preprocess_excel_sheet(df: pd.DataFrame, tab_name: str) -> pd.DataFrame:
    """ME X-Ray template: forward-fill month blocks on CW Summary; drop regional subtotals."""
    df = df.copy()
    t = tab_name.strip().lower()
    if "Month" in df.columns and "cw summary" in t:
        df["Month"] = df["Month"].ffill()
    if "Market" in df.columns:
        m = df["Market"].astype(str)
        df = df[~m.str.contains("TOTAL", case=False, na=False)]
    if t == "raw leads":
        # Convert lead rows into additive metrics so they can be combined with spend.
        if "Leads" not in df.columns:
            df["Leads"] = 1
        status = df.get("Lead Status", pd.Series(index=df.index, dtype=str)).astype(str).str.lower()
        if "Qualified" not in df.columns:
            df["Qualified"] = status.str.contains("qualified", na=False).astype(int)
        if "Date Formatted" in df.columns and "Date" not in df.columns:
            df["Date"] = pd.to_datetime(df["Date Formatted"], errors="coerce")
    if t == "raw post qualification":
        # Stage rows become pipeline counters (post-lead funnel).
        stage = df.get("Stage", pd.Series(index=df.index, dtype=str)).astype(str).str.lower().str.strip()
        if "Qualified" not in df.columns:
            df["Qualified"] = 1
        if "Pitching" not in df.columns:
            df["Pitching"] = stage.str.contains("pitch", na=False).astype(int)
        if "Closed Won" not in df.columns:
            df["Closed Won"] = stage.str.contains("closed won", na=False).astype(int)
        if "Date" not in df.columns:
            if "Formatted Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Formatted Date"], errors="coerce")
            elif "Created Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
    if t == "raw cw":
        # RAW CW can contain repeated rows for the same opportunity; dedupe before aggregation.
        dedupe_cols = [c for c in ("Opportunity Name", "Close Date", "Kitchen Country", "Stage") if c in df.columns]
        if dedupe_cols:
            df = df.drop_duplicates(subset=dedupe_cols, keep="first")
        stage = df.get("Stage", pd.Series(index=df.index, dtype=str)).astype(str).str.lower().str.strip()
        if "Closed Won" not in df.columns:
            df["Closed Won"] = stage.str.contains("closed won", na=False).astype(int)
        if "Date" not in df.columns:
            if "Close Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Close Date"], errors="coerce", dayfirst=True)
            elif "Created Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        # Training definition: TCV = Monthly LF x Contract Length (fallback if TCV (USD) absent/zero).
        if "TCV (USD)" in df.columns:
            tcv_num = pd.to_numeric(df["TCV (USD)"], errors="coerce").fillna(0)
        else:
            tcv_num = pd.Series(0, index=df.index, dtype=float)
        lf_num = pd.to_numeric(df.get("Monthly LF (USD)", 0), errors="coerce").fillna(0)
        term_num = pd.to_numeric(df.get("License Initial Term (Months)", 0), errors="coerce").fillna(0)
        df["TCV (USD)"] = tcv_num.where(tcv_num > 0, lf_num * term_num)
    return df


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
            acc = _to_number_series(df[srcs[0]])
            for c in srcs[1:]:
                acc = acc + _to_number_series(df[c])
            out[field] = acc
        else:
            out[field] = df[srcs[0]]

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    else:
        out["date"] = pd.NaT

    if "report_month" in out.columns:
        rm = _parse_report_month_series(out["report_month"])
        rm = rm.ffill()
        out["date"] = out["date"].fillna(rm)

    for c in _NUM_FIELDS:
        if c in out.columns:
            out[c] = _to_number_series(out[c])
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
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        raise RuntimeError(
            "No service account in Streamlit Secrets. Add a `[gsheet_service_account]` block "
            "(or `GCP_SERVICE_ACCOUNT`) in this app’s Secrets, then reboot."
        )
    frames: list[pd.DataFrame] = []
    tab_stats: list[tuple[str, int]] = []
    for title, ws_gid in meta:
        try:
            raw = _read_sheet_auth(
                sheet_id,
                secret_creds,
                worksheet_name=None,
                worksheet_gid=int(ws_gid),
            )
            raw = _preprocess_excel_sheet(raw, title)
            df = _normalize(raw)
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


@st.cache_data(ttl=300)
def load_source_of_truth_tab(sheet_id: str, worksheet_gid: int, _secret_fp: str) -> pd.DataFrame:
    """Load one canonical source-of-truth tab by gid from Google Sheets."""
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
    raw = _preprocess_excel_sheet(raw, "source_of_truth")
    out = _normalize(raw)
    out["source_tab"] = f"gid:{worksheet_gid}"
    return out


@st.cache_data(ttl=300)
def load_excel_all_sheets(_content_hash: str, xlsx_bytes: bytes) -> pd.DataFrame:
    """Load and combine core ME X-Ray tabs (spend, leads, post-leads) into one dataset."""
    bio = io.BytesIO(xlsx_bytes)
    xl = pd.ExcelFile(bio)
    preferred_tabs = ["Raw Spend", "Raw Leads", "Raw Post Qualification", "RAW CW"]
    selected_tabs = [t for t in preferred_tabs if t in xl.sheet_names]
    if not selected_tabs:
        selected_tabs = [t for t in xl.sheet_names if str(t).strip()]
    frames: list[pd.DataFrame] = []
    tab_stats: list[tuple[str, int]] = []
    for title in selected_tabs:
        raw = pd.read_excel(xl, sheet_name=title)
        if raw.empty or len(raw.columns) == 0:
            tab_stats.append((title, 0))
            continue
        raw = _preprocess_excel_sheet(raw, title)
        norm = _normalize(raw)
        if norm.empty:
            tab_stats.append((title, 0))
            continue
        df = norm.copy()
        df["source_tab"] = title.strip() if str(title).strip() else "Sheet"
        frames.append(df)
        tab_stats.append((title, len(df)))
    if not frames:
        out = pd.DataFrame()
        out.attrs["tab_stats"] = tab_stats
        out.attrs["worksheet_order"] = selected_tabs
        return out
    combined = pd.concat(frames, ignore_index=True)
    combined.attrs["tab_stats"] = tab_stats
    combined.attrs["worksheet_order"] = selected_tabs
    try:
        combined.attrs["fields_mapped"] = list(frames[0].attrs.get("fields_mapped", []) or [])
    except Exception:
        combined.attrs["fields_mapped"] = []
    combined.attrs["sheet_columns"] = list(combined.columns)
    return combined


LOOKER_PAGES: tuple[str, ...] = (
    "Marketing Performance Overview",
    "Market MoM View",
    "Performance Marketing Channels Overview",
    "All Inbound Channels Overview",
)


def _format_currency(v: float) -> str:
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v:,.0f}"
    return f"${v:,.2f}"


def _heatmap_bg(series: pd.Series, *, good_low: bool = True) -> list[str]:
    """Return rgba background strings for column (min=green-ish, max=red-ish or inverse)."""
    s = pd.to_numeric(series, errors="coerce")
    lo, hi = float(s.min(skipna=True)), float(s.max(skipna=True))
    if pd.isna(lo) or pd.isna(hi) or hi <= lo:
        return ["background-color: #ffffff"] * len(s)
    out: list[str] = []
    for x in s:
        if pd.isna(x):
            out.append("background-color: #f8fafc")
            continue
        t = (float(x) - lo) / (hi - lo)
        if not good_low:
            t = 1.0 - t
        # green #dcfce7 -> yellow #fef9c3 -> rose #fecaca
        if t <= 0.5:
            g = 0.85 + 0.15 * (t * 2)
            r = 0.86 - 0.1 * (t * 2)
        else:
            u = (t - 0.5) * 2
            r = 0.76 + 0.2 * u
            g = 0.85 - 0.35 * u
        out.append(f"background-color: rgba({int(r * 255)},{int(g * 255)},{int(0.75 * 255)},0.65)")
    return out


def _apply_sheet_filters(
    df_date: pd.DataFrame,
    *,
    key_suffix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (filtered for metrics/charts, df_for_tabs mirror)."""
    country_opts = sorted([x for x in df_date["country"].dropna().unique().tolist() if x and x != "Unknown"])
    selected_countries = st.multiselect(
        "Country / market",
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

    df_for_tabs = df.copy()

    return df, df_for_tabs


def _kpi_block(
    *,
    total_spend: float,
    total_impr: int,
    total_clicks: int,
    ctr: float,
    total_leads: int,
    total_qualified: int,
    total_cw: int,
    total_tcv: float,
    total_first_month_lf: float,
    cpc: float,
    cpl: float,
    cpsql: float,
) -> None:
    """Looker-style scorecards: `st.metric` so values always render on Streamlit Cloud (HTML can be stripped)."""
    vals = [
        _format_currency(total_spend),
        f"{total_impr:,}",
        f"{total_clicks:,}",
        f"{ctr:.2f}%",
        f"{total_qualified:,}",
        f"{total_leads:,}",
    ]
    titles = ["Spend", "Impressions", "Clicks", "CTR", "Qualified", "Leads"]
    r1 = st.columns(6)
    for i, c in enumerate(r1):
        with c:
            st.metric(titles[i], vals[i])

    q_rate = (total_cw / total_qualified * 100) if total_qualified else 0.0
    cpcw = (total_spend / total_cw) if total_cw else 0.0
    # Training deck formulas:
    # CpCW:LF = CpCW / 1st Month LF(avg) == Marketing Spend / total 1st Month LF
    cpcw_lf = (total_spend / total_first_month_lf) if total_first_month_lf else 0.0
    spend_tcv_pct = (total_spend / total_tcv * 100) if total_tcv else 0.0
    pills = [
        ("CPCW (Spend/CW)", f"${cpcw:,.2f}" if total_cw else "—"),
        ("CpCW:LF", f"{cpcw_lf:.2f}" if total_first_month_lf else "—"),
        ("Spend / TCV %", f"{spend_tcv_pct:.2f}%" if total_tcv else "—"),
        ("CPL", f"${cpl:,.2f}" if total_leads else "—"),
        ("CPSQL", f"${cpsql:,.2f}" if total_qualified else "—"),
        ("Q → Win %", f"{q_rate:.2f}%"),
    ]
    r2 = st.columns(5)
    for i, c in enumerate(r2):
        lbl, val = pills[i]
        with c:
            st.metric(lbl, val)


def _master_performance_table(
    df: pd.DataFrame,
    *,
    key_suffix: str,
    section_title: Optional[str] = "Marketing Performance Master View",
) -> None:
    """Month × market pivot; ME X-Ray columns (TCV, LF, CPCW:LF, Cost/TCV%) when present in the data."""
    if section_title:
        st.markdown(f'<div class="looker-table-title">{section_title}</div>', unsafe_allow_html=True)
    agg: dict[str, tuple[str, str]] = {
        "spend": ("cost", "sum"),
        "cw": ("closed_won", "sum"),
        "clicks": ("clicks", "sum"),
        "leads": ("leads", "sum"),
        "qualified": ("qualified", "sum"),
    }
    if "tcv" in df.columns and float(df["tcv"].sum()) > 0:
        agg["tcv"] = ("tcv", "sum")
    if "first_month_lf" in df.columns and float(df["first_month_lf"].sum()) > 0:
        agg["lf"] = ("first_month_lf", "sum")

    g = df.groupby(["month", "country"], as_index=False).agg(**agg).sort_values(
        ["month", "country"], ascending=[False, True]
    )
    g["Unified Date"] = g["month"].apply(lambda m: pd.Period(m, freq="M").strftime("%b %Y") if pd.notna(m) else "")
    g["Market"] = g["country"]
    g["Spend"] = g["spend"]
    g["CW (Inc Approved)"] = g["cw"].astype(int)
    g["CPCW"] = g.apply(
        lambda r: (r["spend"] / r["cw"]) if r["cw"] and r["cw"] > 0 else float("nan"),
        axis=1,
    )
    if "lf" in g.columns:
        g["1st Month LF"] = g["lf"]
        g["CPCW:LF"] = g.apply(
            lambda r: (r["spend"] / r["lf"]) if r["lf"] and r["lf"] > 0 else float("nan"),
            axis=1,
        )
    if "tcv" in g.columns:
        g["Actual TCV"] = g["tcv"]
        g["Cost/TCV%"] = g.apply(
            lambda r: (r["spend"] / r["tcv"] * 100) if r["tcv"] and r["tcv"] > 0 else float("nan"),
            axis=1,
        )
    g["CPL"] = g.apply(
        lambda r: (r["spend"] / r["leads"]) if r["leads"] and r["leads"] > 0 else float("nan"),
        axis=1,
    )
    g["SQL %"] = g.apply(
        lambda r: (r["qualified"] / r["leads"] * 100) if r["leads"] and r["leads"] > 0 else float("nan"),
        axis=1,
    )
    g["Total Leads"] = g["leads"]

    cols = [
        "Unified Date",
        "Market",
        "Spend",
        "CW (Inc Approved)",
        "CPCW",
    ]
    if "1st Month LF" in g.columns:
        cols.append("1st Month LF")
    if "Actual TCV" in g.columns:
        cols.append("Actual TCV")
    if "CPCW:LF" in g.columns:
        cols.append("CPCW:LF")
    if "Cost/TCV%" in g.columns:
        cols.append("Cost/TCV%")
    cols.extend(["CPL", "SQL %", "Total Leads"])

    show = g[cols].copy()
    fmt: dict[str, Any] = {
        "Spend": "${:,.2f}",
        "CW (Inc Approved)": "{:,.0f}",
        "CPCW": lambda x: f"${x:,.2f}" if pd.notna(x) else "—",
        "CPL": lambda x: f"${x:,.2f}" if pd.notna(x) else "—",
        "SQL %": lambda x: f"{x:.2f}%" if pd.notna(x) else "—",
        "Total Leads": "{:,.0f}",
    }
    if "1st Month LF" in show.columns:
        fmt["1st Month LF"] = "${:,.2f}"
    if "Actual TCV" in show.columns:
        fmt["Actual TCV"] = "${:,.2f}"
    if "CPCW:LF" in show.columns:
        fmt["CPCW:LF"] = lambda x: f"{x:,.2f}" if pd.notna(x) else "—"
    if "Cost/TCV%" in show.columns:
        fmt["Cost/TCV%"] = lambda x: f"{x:.2f}%" if pd.notna(x) else "—"

    styler = show.style.format(fmt)
    heat_cols = [c for c in ("CPCW", "CPCW:LF", "Cost/TCV%", "CPL") if c in show.columns]
    for hc in heat_cols:
        styler = styler.apply(lambda s, col=hc: _heatmap_bg(show[col], good_low=True), subset=[hc])
    st.dataframe(styler, use_container_width=True, hide_index=True, key=f"{key_suffix}_df_master")


def render_page_marketing_performance(
    df_loaded: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> None:
    key_suffix = "mpo"
    df_filtered = _filter_by_date_range(df_loaded, start_date, end_date)
    df_date = df_loaded if df_filtered.empty else df_filtered
    if df_date.empty:
        st.info("No rows in the selected date range.")
        return

    st.markdown('<h1 class="looker-page-h1">Marketing Performance Overview</h1>', unsafe_allow_html=True)
    df, _ = _apply_sheet_filters(df_date, key_suffix=key_suffix)

    c1, c2 = st.columns([3, 1])
    with c1:
        st.caption("Filters apply to scorecards, master table, and charts below.")
    with c2:
        with st.expander("Applied filters", expanded=False):
            st.caption("Country, platform, and source tab mirror Looker’s control strip.")

    st.caption("All worksheet tabs are auto-combined into one model; no manual tab selection is required.")

    total_spend = float(df["cost"].sum())
    total_impr = int(df["impressions"].sum())
    total_clicks = int(df["clicks"].sum())
    total_leads = int(df["leads"].sum())
    total_qualified = int(df["qualified"].sum())
    total_pitching = int(df["pitching"].sum())
    total_cw = int(df["closed_won"].sum())
    total_tcv = float(df["tcv"].sum()) if "tcv" in df.columns else 0.0
    total_first_month_lf = float(df["first_month_lf"].sum()) if "first_month_lf" in df.columns else 0.0
    ctr = (total_clicks / total_impr * 100) if total_impr else 0
    cpc = (total_spend / total_clicks) if total_clicks else 0.0
    cpl = (total_spend / total_leads) if total_leads else 0.0
    cpsql = (total_spend / total_qualified) if total_qualified else 0.0

    # Quick health check so "no numbers" issues are visible immediately.
    if total_spend == 0 and total_leads == 0 and total_cw == 0:
        st.warning(
            "Loaded rows but key totals are zero (Spend, Leads, Closed Won). "
            "Check selected source tabs and verify raw sheets contain numeric values."
        )
    st.caption(
        f"Data health — rows: {len(df):,} | spend: {total_spend:,.2f} | "
        f"leads: {total_leads:,} | qualified: {total_qualified:,} | closed won: {total_cw:,}"
    )

    _kpi_block(
        total_spend=total_spend,
        total_impr=total_impr,
        total_clicks=total_clicks,
        ctr=ctr,
        total_leads=total_leads,
        total_qualified=total_qualified,
        total_cw=total_cw,
        total_tcv=total_tcv,
        total_first_month_lf=total_first_month_lf,
        cpc=cpc,
        cpl=cpl,
        cpsql=cpsql,
    )

    _master_performance_table(df, key_suffix=key_suffix)

    tab_overview, tab_region, tab_funnel = st.tabs(["Overview", "Regional", "Funnel"])
    with tab_overview:
        st.markdown("#### Trends")
        _nt = int(df["source_tab"].nunique()) if "source_tab" in df.columns else 0
        if _nt > 1:
            monthly_by_tab = (
                df.groupby(["month", "source_tab"], as_index=False)
                .agg(cost=("cost", "sum"), clicks=("clicks", "sum"), impressions=("impressions", "sum"))
                .sort_values(["month", "source_tab"])
            )
            m1, m2 = st.columns(2)
            with m1:
                fig_cost = px.line(
                    monthly_by_tab,
                    x="month",
                    y="cost",
                    color="source_tab",
                    markers=True,
                    title="Cost by month (by worksheet tab)",
                )
                fig_cost.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    margin=dict(l=8, r=8, t=45, b=8),
                    legend_title_text="Tab",
                )
                st.plotly_chart(fig_cost, use_container_width=True, key=f"{key_suffix}_pl_cost_mo")
            with m2:
                fig_clicks = px.line(
                    monthly_by_tab,
                    x="month",
                    y="clicks",
                    color="source_tab",
                    markers=True,
                    title="Clicks by month (by worksheet tab)",
                )
                fig_clicks.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    margin=dict(l=8, r=8, t=45, b=8),
                    legend_title_text="Tab",
                )
                st.plotly_chart(fig_clicks, use_container_width=True, key=f"{key_suffix}_pl_clicks_mo")
        else:
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
        st.markdown("#### Country × month")
        by_region = (
            df.groupby(["country", "month"], as_index=False)
            .agg(cost=("cost", "sum"), leads=("leads", "sum"))
            .sort_values(["month", "cost"], ascending=[True, False])
        )
        st.dataframe(by_region, use_container_width=True, key=f"{key_suffix}_df_region")
        if "source_tab" in df.columns and df["source_tab"].nunique() > 1:
            st.markdown("#### Worksheet tab × month")
            by_tab_m = (
                df.groupby(["source_tab", "month"], as_index=False)
                .agg(cost=("cost", "sum"), leads=("leads", "sum"))
                .sort_values(["month", "source_tab"])
            )
            st.dataframe(by_tab_m, use_container_width=True, key=f"{key_suffix}_df_tab_month")

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


def render_page_market_mom(
    df_loaded: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> None:
    key_suffix = "mom"
    df_filtered = _filter_by_date_range(df_loaded, start_date, end_date)
    df_date = df_loaded if df_filtered.empty else df_filtered
    if df_date.empty:
        st.info("No rows in the selected date range.")
        return

    st.markdown('<h1 class="looker-page-h1">Market MoM View</h1>', unsafe_allow_html=True)
    df, _ = _apply_sheet_filters(df_date, key_suffix=key_suffix)

    mk_opts = sorted([x for x in df_date["country"].dropna().unique().tolist() if x and x != "Unknown"])
    pick = st.selectbox(
        "Market",
        ["All markets"] + mk_opts,
        key=f"{key_suffix}_market",
    )
    if pick != "All markets":
        df = df[df["country"] == pick]

    _master_performance_table(df, key_suffix=f"{key_suffix}_mom", section_title="")

    grand = pd.DataFrame(
        [
            {
                "Unified Date": "Grand total",
                "Market": "—",
                "Spend": float(df["cost"].sum()),
                "CW (Inc Approved)": int(df["closed_won"].sum()),
                "Total Leads": int(df["leads"].sum()),
            }
        ]
    )
    st.caption("Grand total (filtered)")
    st.dataframe(grand, use_container_width=True, hide_index=True, key=f"{key_suffix}_df_grand")

    monthly = (
        df.groupby("month", as_index=False)
        .agg(
            cw=("closed_won", "sum"),
            leads=("leads", "sum"),
            qualified=("qualified", "sum"),
        )
        .sort_values("month")
    )
    monthly["sql_pct"] = monthly.apply(
        lambda r: (r["qualified"] / r["leads"] * 100) if r["leads"] else 0.0,
        axis=1,
    )
    monthly["q_win_pct"] = monthly.apply(
        lambda r: (r["cw"] / r["qualified"] * 100) if r["qualified"] else 0.0,
        axis=1,
    )

    ch1, ch2 = st.columns(2)
    with ch1:
        fig = px.bar(
            monthly,
            x="month",
            y=["cw", "leads", "qualified"],
            barmode="group",
            title="CW vs leads vs qualified (by month)",
        )
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
        st.plotly_chart(fig, use_container_width=True, key=f"{key_suffix}_pl_combo")
    with ch2:
        fig2 = px.line(
            monthly,
            x="month",
            y=["sql_pct", "q_win_pct"],
            markers=True,
            title="SQL % and Q → Win % (by month)",
        )
        fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
        st.plotly_chart(fig2, use_container_width=True, key=f"{key_suffix}_pl_lines")


def render_page_channels(df_loaded: pd.DataFrame, start_date: date, end_date: date, *, inbound: bool) -> None:
    key_suffix = "inb" if inbound else "pmc"
    df_filtered = _filter_by_date_range(df_loaded, start_date, end_date)
    df_date = df_loaded if df_filtered.empty else df_filtered
    if df_date.empty:
        st.info("No rows in the selected date range.")
        return

    title = "All Inbound Channels Overview" if inbound else "Performance Marketing Channels Overview"
    st.markdown(f'<h1 class="looker-page-h1">{title}</h1>', unsafe_allow_html=True)
    df, _ = _apply_sheet_filters(df_date, key_suffix=key_suffix)

    group_col = "utm_source" if inbound else "channel"
    if group_col not in df.columns:
        st.warning(f"Column `{group_col}` missing; showing channel breakdown instead.")
        group_col = "channel"

    st.markdown("#### Spend & efficiency by channel / source")
    agg = (
        df.groupby(group_col, as_index=False)
        .agg(spend=("cost", "sum"), clicks=("clicks", "sum"), leads=("leads", "sum"), cw=("closed_won", "sum"))
        .sort_values("spend", ascending=False)
    )
    agg["CPL"] = agg.apply(lambda r: (r["spend"] / r["leads"]) if r["leads"] else float("nan"), axis=1)
    st.dataframe(agg, use_container_width=True, hide_index=True, key=f"{key_suffix}_df_ch")

    m1, m2 = st.columns(2)
    with m1:
        fig = px.bar(agg.head(20), x=group_col, y="spend", title="Spend")
        fig.update_traces(marker_color="#0F766E")
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
        st.plotly_chart(fig, use_container_width=True, key=f"{key_suffix}_pl_spend")
    with m2:
        trend = (
            df.groupby(["month", group_col], as_index=False)
            .agg(spend=("cost", "sum"))
            .sort_values(["month", group_col])
        )
        top = trend.groupby(group_col)["spend"].sum().nlargest(8).index.tolist()
        trend = trend[trend[group_col].isin(top)]
        fig2 = px.line(trend, x="month", y="spend", color=group_col, markers=True, title="Spend trend (top groups)")
        fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
        st.plotly_chart(fig2, use_container_width=True, key=f"{key_suffix}_pl_trend")


def render_main_dashboard(
    start_date: date,
    end_date: date,
) -> None:
    """Load Google Sheets or ME X-Ray Excel template, then route to Looker-named report pages."""
    st.markdown(
        """
        <div class="ksa-nav">
          <div class="ksa-pill">Kitchen Master Data</div>
          <div class="ksa-pill active">Dashboard</div>
          <div class="ksa-pill">Discussions</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.container():
        t1, t2, t3 = st.columns([1.0, 1.4, 2.2])
        with t1:
            src = st.radio(
                "Data source",
                ["Google Sheets", "Excel (.xlsx)"],
                horizontal=True,
                key="data_src",
            )
        with t2:
            page = st.selectbox("Report page", LOOKER_PAGES, key="looker_page")
        with t3:
            excel_path = ""
            if src == "Excel (.xlsx)":
                excel_path = st.text_input(
                    "Workbook path",
                    value=_default_excel_path_from_secrets(),
                    key="xlsx_path",
                    placeholder="Full path to .xlsx — on Cloud set XRAY_EXCEL_PATH in secrets",
                )

    df_loaded: pd.DataFrame
    if src == "Excel (.xlsx)":
        if not (excel_path or "").strip():
            st.error("Enter the full path to your .xlsx file, or set **XRAY_EXCEL_PATH** in Streamlit secrets (required on Streamlit Cloud).")
            return
        p = Path(excel_path.strip()).expanduser()
        if not p.exists():
            st.error(f"Excel file not found: {p}")
            return
        if p.suffix.lower() != ".xlsx":
            st.error("Excel source must be a .xlsx file.")
            return
        xbytes = p.read_bytes()
        fp = hashlib.md5(xbytes).hexdigest()
        df_loaded = load_excel_all_sheets(fp, xbytes)
    else:
        sheet_id = _extract_sheet_id(_default_sheet_id_from_secrets())
        _fp = _secret_fingerprint(_service_account_from_streamlit_secrets())
        truth_gid = _default_truth_gid_from_secrets()
        try:
            df_loaded = load_source_of_truth_tab(sheet_id, truth_gid, _fp)
        except Exception as exc:
            st.error(f"Failed to load spreadsheet: {exc}")
            return

    if df_loaded.empty:
        st.warning("No data rows were returned. Check tabs and column headers against the ME X-Ray template.")
        return
    if page == "Marketing Performance Overview":
        render_page_marketing_performance(df_loaded, start_date, end_date)
    elif page == "Market MoM View":
        render_page_market_mom(df_loaded, start_date, end_date)
    elif page == "Performance Marketing Channels Overview":
        render_page_channels(df_loaded, start_date, end_date, inbound=False)
    else:
        render_page_channels(df_loaded, start_date, end_date, inbound=True)


def main() -> None:
    st.set_page_config(
        page_title="KitchenPark Marketing Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(
        """
    <style>
    .stApp { background: #f4f6f8; font-family: 'Segoe UI', system-ui, sans-serif; font-size: 0.772rem !important; }
    section[data-testid="stSidebar"] { display: none !important; }
    [data-testid="collapsedControl"] { display: none !important; }
    header[data-testid="stHeader"] { background: #FFFFFF !important; border-bottom: 1px solid #E2E8F0; }
    header[data-testid="stHeader"] * { color: #1E293B !important; }
    .looker-header {
        background: #ffffff;
        border: 1px solid #e6ebef;
        border-radius: 10px;
        padding: 14px 20px;
        margin: -1rem -1rem 12px -1rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 8px;
        color: #1f2937;
    }
    .looker-header-title { font-size: 1.05rem; font-weight: 700; color: #111827; margin: 0; }
    .looker-header-badge {
        width: 28px; height: 28px; border-radius: 50%;
        background: #1A73E8; color: #fff; display: inline-flex; align-items: center; justify-content: center;
        font-size: 14px; font-weight: 700; margin-right: 10px; vertical-align: middle;
    }
    .looker-header-actions { font-size: 12px; color: #6b7280; }
    .ksa-nav {
        display: grid;
        grid-template-columns: 1fr 1fr 1fr;
        gap: 10px;
        margin: 10px 0 8px 0;
    }
    .ksa-pill {
        background: #ffffff;
        border: 1px solid #dde4ea;
        color: #4b5563;
        border-radius: 6px;
        text-align: center;
        padding: 8px 12px;
        font-size: 12px;
        font-weight: 500;
    }
    .ksa-pill.active {
        background: #19766f;
        color: #ffffff;
        border-color: #19766f;
    }
    .looker-page-h1 { font-size: 1.5rem; font-weight: 400; color: #202124; margin: 8px 0 16px 0; }
    .looker-table-title { font-size: 1rem; font-weight: 600; color: #202124; margin: 20px 0 8px 0; }
    .looker-kpi-big {
        background: linear-gradient(180deg, #0D9488 0%, #0F766E 100%);
        color: #fff;
        border-radius: 8px;
        padding: 14px 12px;
        text-align: center;
        min-height: 76px;
        box-shadow: 0 1px 2px rgba(0,0,0,.12);
    }
    .looker-kpi-big-val { font-size: 1.35rem; font-weight: 600; line-height: 1.2; }
    .looker-kpi-big-lbl { font-size: 11px; opacity: 0.92; margin-top: 6px; font-weight: 500; }
    .looker-kpi-pill {
        border: 1px solid #0F766E;
        background: #F0FDFA;
        border-radius: 999px;
        padding: 8px 12px;
        text-align: center;
        font-size: 12px;
        margin-top: 10px;
    }
    .looker-pill-lbl { color: #115E59; display: block; font-weight: 600; }
    .looker-pill-val { color: #134E4A; font-weight: 700; font-size: 13px; }
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
    [data-testid="stMetric"] {
        background: #e9f3f8;
        border: 1px solid #d5e4ec;
        border-left: 3px solid #19766f;
        border-radius: 8px;
        padding: 6px 10px;
    }
    [data-testid="stMetricLabel"] { font-size: 11px !important; color: #4b5563 !important; }
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; color: #1f2937 !important; }
    .stRadio [role="radiogroup"] { gap: 14px; }
    .stSelectbox > label, .stRadio > label, .stTextInput > label { font-size: 11px !important; color: #6b7280 !important; }
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
    <div class="looker-header">
      <div style="display:flex;align-items:center;">
        <span class="looker-header-badge">K</span>
        <h1 class="looker-header-title">KitchenPark Marketing Dashboard</h1>
      </div>
      <div class="looker-header-actions">● Delayed &nbsp;&nbsp; Refreshed 31 min ago</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    _end = date.today()
    _start = _end - timedelta(days=730)

    render_main_dashboard(_start, _end)


if __name__ == "__main__":
    main()

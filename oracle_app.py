"""
Oracle-style ME dashboard scaffold backed by Google Sheets.

Run (Windows: use ``py -m`` if ``streamlit`` is not on PATH):
    py -m streamlit run oracle_app.py
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import html
import io
import json
import os
import re
import base64
import urllib.error
import urllib.request
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal, Optional, Union

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# Bump when you ship UI/logic changes — used for cache keys and the header “Build:” pill.
# If the hosted app shows an older string, Streamlit Cloud has not deployed the latest GitHub ``main`` yet (check branch + reboot).
DASHBOARD_BUILD = "2026-04-27-lock-cw-approved-headline-scope"

# T3B3: optional CPCW:LF goal-scope table (UAE · Saudi · Kuwait + Bahrain). Set True to show again.
_SHOW_T3B3_CPCW_LF_GOALS_TABLE = False

DEFAULT_SHEET_ID = "1tcjVk7UD-4LG3DG-73ELTNCfzD2XnwnEYqdS8NoH71I"
ME_XRAY_SPEND_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{DEFAULT_SHEET_ID}/edit"
# **Spend by channel** month × channel grid is scoped to this workbook (``spreadsheet_id`` on stacked loads).
PMC_MONTH_GRID_SHEET_ID = DEFAULT_SHEET_ID
# Optional workbook: set Streamlit secret ``XRAY_SHEET_ID`` to the id or full URL below, then set
# XRAY_TRUTH_GID / XRAY_SPEND_GID / XRAY_LEADS_GID / XRAY_POST_QUAL_GID / XRAY_RAW_CW_GID to each tab’s
# ``gid`` from the URL when that tab is open (example tab gid=279936880):
# https://docs.google.com/spreadsheets/d/1tcjVk7UD-4LG3DG-73ELTNCfzD2XnwnEYqdS8NoH71I/edit?gid=279936880
#
# **Paid media (clicks / impressions / per-platform Ads Data tabs)** defaults to this Supermetrics workbook
# (Kitchen Park connector). It is stacked after the primary ME X-Ray workbook. Override with
# ``PAID_MEDIA_SHEET_ID`` / ``SUPERMETRICS_SHEET_ID`` / ``XRAY_ADS_SHEET_ID``, or set ``PAID_MEDIA_SHEET_ID``
# to ``none`` to disable the second load. ``DISABLE_PAID_MEDIA_SECOND_WORKBOOK=1`` also disables it.
DEFAULT_PAID_MEDIA_SHEET_ID = "1tcjVk7UD-4LG3DG-73ELTNCfzD2XnwnEYqdS8NoH71I"
# Default paid-media platform Ads Data tabs in the Supermetrics workbook.
# 0=Google Ads, 1802364778=Meta, 1720904536=Snapchat, 279936880=LinkedIn.
DEFAULT_PAID_MEDIA_PLATFORM_GIDS: tuple[int, ...] = (
    0,
    1802364778,
    1720904536,
    279936880,
)
DEFAULT_SOURCE_TRUTH_GID = 1212330729
# Canonical source-of-truth tab in the Supermetrics workbook:
# https://docs.google.com/spreadsheets/d/1tcjVk7UD-4LG3DG-73ELTNCfzD2XnwnEYqdS8NoH71I/edit?gid=1212330729
ME_XRAY_SOURCE_OF_TRUTH_URL = (
    f"https://docs.google.com/spreadsheets/d/{DEFAULT_SHEET_ID}/edit?gid={DEFAULT_SOURCE_TRUTH_GID}"
)
# Raw / detail leads tab (all lead rows + qualifying flags for SQL counts):
# https://docs.google.com/spreadsheets/d/1tcjVk7UD-4LG3DG-73ELTNCfzD2XnwnEYqdS8NoH71I/edit?gid=1359284016
DEFAULT_LEADS_WORKSHEET_GID = 1359284016
DEFAULT_POST_QUAL_WORKSHEET_GID = 2124231650
DEFAULT_RAW_CW_WORKSHEET_GID = 2126759408
DEFAULT_SPEND_WORKSHEET_GID = 1666828602
DEFAULT_CW_SOURCE_TRUTH_GID = 1871946442
# Default empty on Streamlit Cloud; set `XRAY_EXCEL_PATH` in secrets or `XRAY_EXCEL_PATH_DEFAULT` locally.
DEFAULT_LOCAL_EXCEL_PATH = (os.environ.get("XRAY_EXCEL_PATH_DEFAULT") or "").strip()
DEFAULT_LOGO_PATH = (
    os.environ.get("XRAY_LOGO_PATH_DEFAULT")
    or str((Path(__file__).resolve().parent / "assets" / "logo.png"))
    or ""
).strip()


def _default_sheet_id_from_secrets() -> str:
    """Optional Streamlit secret XRAY_SHEET_ID overrides default workbook (id or full Sheets URL)."""
    try:
        s = st.secrets
        v = (s.get("XRAY_SHEET_ID") or s.get("xray_sheet_id") or "").strip()
        if v:
            return _extract_sheet_id(v)
        return DEFAULT_SHEET_ID
    except Exception:
        return DEFAULT_SHEET_ID


def _workbook_id_resolution() -> tuple[str, str]:
    """(canonical_sheet_id, label for debug: default vs secret override)."""
    try:
        s = st.secrets
        v = (s.get("XRAY_SHEET_ID") or s.get("xray_sheet_id") or "").strip()
        if v:
            return _extract_sheet_id(v), "XRAY_SHEET_ID (Streamlit secret)"
    except Exception:
        pass
    return DEFAULT_SHEET_ID, "DEFAULT_SHEET_ID in code (ME X-Ray workbook)"


def _paid_media_sheet_id_value_disables_second_workbook(v: str) -> bool:
    return (v or "").strip().lower() in ("none", "false", "0", "-", "off", "disable", "disabled")


def _optional_paid_media_sheet_id_from_secrets() -> str:
    """Second spreadsheet: per-platform paid media / Supermetrics (clicks, impressions, Ads Data tabs).

    Defaults to ``DEFAULT_PAID_MEDIA_SHEET_ID`` so ME X-Ray primary + Supermetrics connector merge without
    extra config. Secrets/env override: ``PAID_MEDIA_SHEET_ID``, ``SUPERMETRICS_SHEET_ID``, ``XRAY_ADS_SHEET_ID``.
    Set ``PAID_MEDIA_SHEET_ID`` to ``none`` (or env ``DISABLE_PAID_MEDIA_SECOND_WORKBOOK=1``) to disable.
    """
    try:
        d = (st.secrets.get("DISABLE_PAID_MEDIA_SECOND_WORKBOOK") or "").strip().lower()
        if d in ("1", "true", "yes", "on"):
            return ""
    except Exception:
        pass
    if (os.environ.get("DISABLE_PAID_MEDIA_SECOND_WORKBOOK") or "").strip().lower() in ("1", "true", "yes", "on"):
        return ""

    keys = (
        "PAID_MEDIA_SHEET_ID",
        "paid_media_sheet_id",
        "SUPERMETRICS_SHEET_ID",
        "supermetrics_sheet_id",
        "XRAY_ADS_SHEET_ID",
        "xray_ads_sheet_id",
    )
    try:
        s = st.secrets
        for k in keys:
            v = (s.get(k) or "").strip()
            if not v:
                continue
            if _paid_media_sheet_id_value_disables_second_workbook(v):
                return ""
            return _extract_sheet_id(v)
    except Exception:
        pass
    for k in ("PAID_MEDIA_SHEET_ID", "SUPERMETRICS_SHEET_ID", "XRAY_ADS_SHEET_ID"):
        v = (os.environ.get(k) or "").strip()
        if not v:
            continue
        if _paid_media_sheet_id_value_disables_second_workbook(v):
            return ""
        return _extract_sheet_id(v)
    return DEFAULT_PAID_MEDIA_SHEET_ID


def _dataframe_with_spreadsheet_id(df: pd.DataFrame, spreadsheet_id: str) -> pd.DataFrame:
    """Tag stacked rows so ``worksheet_gid`` slices stay unambiguous across merged workbooks."""
    if df.empty or not (spreadsheet_id or "").strip():
        return df
    out = df.copy()
    out["spreadsheet_id"] = str(_extract_sheet_id(spreadsheet_id))
    return out


def _rows_for_workbook_id(df: pd.DataFrame, spreadsheet_id: str) -> pd.DataFrame:
    """Restrict stacked rows to one workbook (``spreadsheet_id``). If the column is missing, returns ``df`` unchanged."""
    if df.empty or not (spreadsheet_id or "").strip():
        return df
    sid = str(_extract_sheet_id(spreadsheet_id))
    if "spreadsheet_id" not in df.columns:
        return df
    m = df["spreadsheet_id"].astype(str) == sid
    if not bool(m.any()):
        return df.iloc[0:0].copy()
    return df.loc[m].copy()


def _default_truth_gid_from_secrets() -> int:
    """Optional Streamlit secret XRAY_TRUTH_GID overrides default source-of-truth tab gid."""
    try:
        s = st.secrets
        v = (s.get("XRAY_TRUTH_GID") or s.get("xray_truth_gid") or "").strip()
        return int(v) if v else DEFAULT_SOURCE_TRUTH_GID
    except Exception:
        return DEFAULT_SOURCE_TRUTH_GID


def _default_leads_gid_from_secrets() -> int:
    """Optional secret/env overrides default leads tab gid (URL ``#gid=``).

    Accepts ``XRAY_LEADS_GID`` or the first entry of comma-separated ``XRAY_LEAD_WORKSHEET_GIDS``
    (same as newer configs) in secrets or environment.
    """
    try:
        s = st.secrets
        v = (s.get("XRAY_LEADS_GID") or s.get("xray_leads_gid") or "").strip()
        if v.isdigit():
            return int(v)
        multi = (s.get("XRAY_LEAD_WORKSHEET_GIDS") or s.get("xray_lead_worksheet_gids") or "").strip()
        if multi:
            first = multi.split(",")[0].strip()
            if first.isdigit():
                return int(first)
    except Exception:
        pass
    ev = (os.environ.get("XRAY_LEADS_GID") or "").strip()
    if ev.isdigit():
        return int(ev)
    evm = (os.environ.get("XRAY_LEAD_WORKSHEET_GIDS") or "").strip()
    if evm:
        fst = evm.split(",")[0].strip()
        if fst.isdigit():
            return int(fst)
    return DEFAULT_LEADS_WORKSHEET_GID


# Tab-name patterns for inbound lead sheets (tab ``Leads`` / ``Raw Leads``, ``AB post lead``, etc.).
_MPO_LEAD_TAB_PATTERNS: list[str] = [
    r"raw\s*leads?",
    r"lead\s*sheet",
    r"^\s*leads?\s*$",
    r"leads?\s+report",
    r"leads?\s+export",
    r"copy\s+of\s+.*lead",
    r"post\s*lead",  # e.g. AB post lead
    r"ab\s*post",
    r"\binbound\b",
    r"\bmql\b",
    r"sql\s*lead",
    r"form\s*fill",
]


def _rows_by_worksheet_id(
    frame: pd.DataFrame,
    gid: int,
    spreadsheet_id: Optional[str] = None,
) -> pd.DataFrame:
    """Slice stacked workbook rows for worksheet id (``worksheet_gid`` or ``source_ws_gid``).

    When ``spreadsheet_id`` is set and ``frame`` has a ``spreadsheet_id`` column, only rows from that
    workbook are returned (avoids gid collisions when two spreadsheets are concatenated).
    """
    if frame.empty or int(gid) <= 0:
        return frame.iloc[0:0].copy()
    for col in ("worksheet_gid", "source_ws_gid"):
        if col not in frame.columns:
            continue
        wg = pd.to_numeric(frame[col], errors="coerce")
        mask = wg == int(gid)
        if spreadsheet_id is not None and "spreadsheet_id" in frame.columns:
            mask = mask & (frame["spreadsheet_id"].astype(str) == str(spreadsheet_id))
        sub = frame.loc[mask].copy()
        if not sub.empty:
            return sub
    return frame.iloc[0:0].copy()


def _optional_post_qual_gid_from_secrets() -> Optional[int]:
    """Optional Streamlit secret XRAY_POST_QUAL_GID: load this tab alone for pipeline KPIs (Total Live)."""
    try:
        s = st.secrets
        v = (s.get("XRAY_POST_QUAL_GID") or s.get("xray_post_qual_gid") or "").strip()
        return int(v) if v else DEFAULT_POST_QUAL_WORKSHEET_GID
    except Exception:
        return DEFAULT_POST_QUAL_WORKSHEET_GID


def _optional_raw_cw_gid_from_secrets() -> Optional[int]:
    """Optional Streamlit secret XRAY_RAW_CW_GID: TCV / 1st Month LF tab (same as Looker Actual TCV & CPCW:LF)."""
    try:
        s = st.secrets
        v = (s.get("XRAY_RAW_CW_GID") or s.get("xray_raw_cw_gid") or "").strip()
        return int(v) if v else DEFAULT_RAW_CW_WORKSHEET_GID
    except Exception:
        return DEFAULT_RAW_CW_WORKSHEET_GID


def _optional_spend_gid_from_secrets() -> Optional[int]:
    """Optional Streamlit secret XRAY_SPEND_GID: worksheet id from the tab URL when Spend is not on gid=0."""
    try:
        s = st.secrets
        v = (s.get("XRAY_SPEND_GID") or s.get("xray_spend_gid") or "").strip()
        return int(v) if v else DEFAULT_SPEND_WORKSHEET_GID
    except Exception:
        return DEFAULT_SPEND_WORKSHEET_GID


def _optional_cw_source_truth_gid_from_secrets() -> Optional[int]:
    """Optional Streamlit secret XRAY_CW_GID: worksheet id for CW source-of-truth."""
    try:
        s = st.secrets
        v = (s.get("XRAY_CW_GID") or s.get("xray_cw_gid") or "").strip()
        return int(v) if v else DEFAULT_CW_SOURCE_TRUTH_GID
    except Exception:
        return DEFAULT_CW_SOURCE_TRUTH_GID


def _optional_spend_column_header_from_secrets() -> str:
    """Exact (or substring) spend column header on the sheet, e.g. ``Marketing Spend`` — ``XRAY_SPEND_COLUMN``."""
    try:
        s = st.secrets
        return (s.get("XRAY_SPEND_COLUMN") or s.get("xray_spend_column") or "").strip()
    except Exception:
        return ""


def _inject_cost_from_named_sheet_column(raw: pd.DataFrame, cand: pd.DataFrame, header: str) -> pd.DataFrame:
    """Force ``cost`` from a raw column when auto-mapping fails (same row order as ``cand``)."""
    if not header or raw.empty or cand.empty or len(raw) != len(cand):
        return cand
    hl = header.strip().lower()
    match: Optional[str] = None
    for c in raw.columns:
        if str(c).strip().lower() == hl:
            match = c
            break
    if match is None:
        for c in raw.columns:
            if hl in str(c).strip().lower():
                match = c
                break
    if match is None:
        return cand
    out = cand.copy()
    out["cost"] = _to_number_series(raw[match].reset_index(drop=True)).values
    return out


def _default_excel_path_from_secrets() -> str:
    """Optional Streamlit secret XRAY_EXCEL_PATH overrides default local workbook path."""
    try:
        s = st.secrets
        v = (s.get("XRAY_EXCEL_PATH") or s.get("xray_excel_path") or "").strip()
        return v if v else DEFAULT_LOCAL_EXCEL_PATH
    except Exception:
        return DEFAULT_LOCAL_EXCEL_PATH


def _default_logo_path_from_secrets() -> str:
    """Optional path for header logo image."""
    try:
        s = st.secrets
        v = (s.get("XRAY_LOGO_PATH") or s.get("xray_logo_path") or "").strip()
        return v if v else DEFAULT_LOGO_PATH
    except Exception:
        return DEFAULT_LOGO_PATH


def _logo_data_uri(path_str: str) -> str:
    p = Path(path_str).expanduser()
    if not path_str or not p.exists():
        return ""
    raw = p.read_bytes()
    b64 = base64.b64encode(raw).decode("ascii")
    ext = p.suffix.lower().lstrip(".") or "png"
    if ext == "jpg":
        ext = "jpeg"
    return f"data:image/{ext};base64,{b64}"


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
    from gspread.utils import ValueRenderOption

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
    try:
        recs = ws.get_all_records(value_render_option=ValueRenderOption.unformatted)
        return pd.DataFrame(recs)
    except Exception:
        return pd.DataFrame(ws.get_all_records())


def _read_sheet_auth_loose(
    sheet_id: str,
    service_account_data: Union[bytes, dict, str],
    *,
    worksheet_gid: int,
) -> pd.DataFrame:
    """Fallback reader for tabs where get_all_records() fails due unusual header rows."""
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.utils import ValueRenderOption

    creds_info = _coerce_service_account_dict(service_account_data)
    _validate_service_account_dict(creds_info)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.get_worksheet_by_id(int(worksheet_gid))
    try:
        grid = ws.get_all_values(value_render_option=ValueRenderOption.unformatted) or []
    except Exception:
        grid = ws.get_all_values() or []
    if not grid:
        return pd.DataFrame()
    # Prefer a row that looks like Supermetrics metric headers (often row 2 when row 1 is only ``Date``).
    best_idx: Optional[int] = None
    best_score = -1
    for i, row in enumerate(grid[:12]):
        if not any(str(cell).strip() for cell in row):
            continue
        score = sum(1 for cell in row if _cell_looks_like_supermetrics_metric_title(cell))
        non_empty = sum(1 for c in row if str(c).strip())
        if score >= 3 and non_empty >= 3 and score > best_score:
            best_score = score
            best_idx = i
    if best_idx is None:
        # Fallback: first non-empty row (legacy behavior).
        for i, row in enumerate(grid):
            if any(str(cell).strip() for cell in row):
                best_idx = i
                break
    if best_idx is None:
        return pd.DataFrame()
    header_idx = best_idx
    headers = [str(h).strip() or f"col_{j+1}" for j, h in enumerate(grid[header_idx])]
    rows = grid[header_idx + 1 :]
    if not rows:
        return pd.DataFrame(columns=headers)
    width = len(headers)
    fixed_rows = [(r + [""] * max(0, width - len(r)))[:width] for r in rows]
    return pd.DataFrame(fixed_rows, columns=headers)


def _read_sheet_grid_values(
    sheet_id: str,
    service_account_data: Union[bytes, dict, str],
    worksheet_gid: int,
) -> list[list[Any]]:
    """Raw cell grid; prefers **unformatted** values so formulas/currency read as numbers (fixes $0 spend)."""
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.utils import ValueRenderOption

    creds_info = _coerce_service_account_dict(service_account_data)
    _validate_service_account_dict(creds_info)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.get_worksheet_by_id(int(worksheet_gid))
    try:
        return ws.get_all_values(value_render_option=ValueRenderOption.unformatted) or []
    except Exception:
        return ws.get_all_values() or []


def _normalized_spend_cost_sum(frame: pd.DataFrame) -> float:
    if frame.empty or "cost" not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame["cost"], errors="coerce").fillna(0).sum())


def _dataframe_from_grid_with_keyword_header(grid: list[list[str]], keyword: str) -> pd.DataFrame:
    """Build a dataframe by detecting a header row containing a keyword (e.g. 'spend')."""
    if not grid:
        return pd.DataFrame()
    kw = _norm_header_key(keyword)
    best_idx = None
    best_score = -1
    for i, row in enumerate(grid[:55]):  # title rows often push the real header down
        if not any(str(c).strip() for c in row):
            continue
        norm_cells = [_norm_header_key(c) for c in row]
        score = sum(1 for c in norm_cells if kw in c)
        # Prefer rows that also look tabular (multiple non-empty cells)
        non_empty = sum(1 for c in row if str(c).strip())
        if non_empty >= 2 and score > best_score:
            best_score = score
            best_idx = i
    if best_idx is None or best_score <= 0:
        return pd.DataFrame()
    header = [str(h).strip() or f"col_{j+1}" for j, h in enumerate(grid[best_idx])]
    rows = grid[best_idx + 1 :]
    if not rows:
        return pd.DataFrame(columns=header)
    width = len(header)
    fixed_rows = [(r + [""] * max(0, width - len(r)))[:width] for r in rows]
    return pd.DataFrame(fixed_rows, columns=header)


def _make_unique_headers_row(row: list[Any]) -> list[str]:
    names = [str(h).strip() or f"col_{j+1}" for j, h in enumerate(row)]
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        seen[n] = seen.get(n, 0) + 1
        out.append(n if seen[n] == 1 else f"{n}__{seen[n]}")
    return out


def _dataframe_from_grid_header_at(grid: list[list[str]], header_idx: int) -> pd.DataFrame:
    """Build a frame using an arbitrary grid row as the header (for tabs where the real header is not row 1)."""
    if header_idx < 0 or header_idx >= len(grid):
        return pd.DataFrame()
    header = _make_unique_headers_row(grid[header_idx])
    rows = grid[header_idx + 1 :]
    if not rows:
        return pd.DataFrame(columns=header)
    width = len(header)
    fixed_rows = [(r + [""] * max(0, width - len(r)))[:width] for r in rows]
    return pd.DataFrame(fixed_rows, columns=header)


def _sheet_value_looks_like_metric_subheader(v: Any) -> bool:
    nk = _norm_header_key(str(v))
    if nk in {
        "date",
        "day",
        "month",
        "year",
        "campaign",
        "campaign_name",
        "impressions",
        "impr",
        "clicks",
        "cost",
        "spend",
        "total_spend",
        "ctr",
        "cpc",
    }:
        return True
    return any(
        k in nk
        for k in (
            "impression",
            "click",
            "spend",
            "cost",
            "ctr",
            "date",
            "campaign",
        )
    )


def _cell_looks_like_supermetrics_metric_title(val: Any) -> bool:
    """True for Supermetrics-style column titles, e.g. ``… (Impressions)``, ``… (Cost)``, ``… (Clicks (all))``."""
    s = str(val).strip().lower()
    if not s or s in ("nan", "none", "nat"):
        return False
    if "(" not in s:
        return False
    # Plain ``Date`` / ``Month`` in the header row — not a metric column.
    if re.match(r"^\s*(date|month|year|day|week)\s*(\([^)]*\))?\s*$", s):
        return False
    return any(
        k in s
        for k in (
            "impression",
            "click",
            "cost",
            "cpc",
            "ctr",
            "conversion",
            "spend",
            "swipe",
            "lead",
            "engagement",
            "traffic",
            "awareness",
        )
    )


def _score_supermetrics_metric_header_row(row: list[Any]) -> int:
    """How many cells in a grid row look like Supermetrics metric column titles."""
    return sum(1 for cell in row if _cell_looks_like_supermetrics_metric_title(cell))


def _dataframe_from_grid_best_supermetrics_header(
    sheet_id: str,
    service_account_data: Union[bytes, dict, str],
    worksheet_gid: int,
) -> pd.DataFrame:
    """Pick the best header row by scanning raw grid cells — fixes ``get_all_records`` breaking wide Snap/LinkedIn tabs."""
    grid = _read_sheet_grid_values(sheet_id, service_account_data, int(worksheet_gid))
    if not grid:
        return pd.DataFrame()
    best_i = -1
    best_s = -1
    best_ne = -1
    for i in range(min(25, len(grid))):
        row = grid[i]
        if not any(str(c).strip() for c in row):
            continue
        s = _score_supermetrics_metric_header_row(row)
        ne = sum(1 for c in row if str(c).strip())
        if s > best_s or (s == best_s and ne > best_ne):
            best_s, best_i, best_ne = s, i, ne
    if best_i < 0 or best_s < 3:
        return pd.DataFrame()
    return _dataframe_from_grid_header_at(grid, best_i)


def _promote_wide_metric_header_row_if_needed(raw: pd.DataFrame) -> pd.DataFrame:
    """When ``get_all_records`` used row 1 as headers but Supermetrics metric titles are on row 2 (Snap/LinkedIn).

    Meta/Google often put campaign metrics in row 1 — then the first **data** row is numeric and this no-ops.
    """
    if raw.empty or len(raw) < 2:
        return raw
    first = raw.iloc[0]
    row_hits = sum(1 for v in first if _cell_looks_like_supermetrics_metric_title(v))
    if row_hits < 3:
        return raw
    col_hits = sum(1 for c in raw.columns if _cell_looks_like_supermetrics_metric_title(str(c)))
    if col_hits >= max(3, row_hits - 1):
        return raw
    new_cols = _make_unique_headers_row([str(x).strip() or f"col_{i+1}" for i, x in enumerate(first.tolist())])
    out = raw.iloc[1:].copy()
    out.columns = new_cols
    return out.reset_index(drop=True)


def _coerce_two_row_sheet_headers(raw: pd.DataFrame) -> pd.DataFrame:
    """Handle sheets where row1=group labels and row2=actual metric headers (e.g. campaign exports)."""
    if raw.empty or len(raw.columns) < 2 or len(raw.index) < 1:
        return raw
    first_row = raw.iloc[0].tolist()
    metric_hits = sum(1 for v in first_row if _sheet_value_looks_like_metric_subheader(v))
    if metric_hits < max(2, int(0.12 * len(raw.columns))):
        return raw

    new_cols: list[str] = []
    for col_name, sub in zip(raw.columns.tolist(), first_row):
        top = str(col_name).strip()
        sub_s = str(sub).strip()
        if _sheet_value_looks_like_metric_subheader(sub_s):
            # Prefer canonical metric/date labels from the 2nd header row.
            name = sub_s
        elif sub_s and sub_s.lower() not in ("nan", "none", "nat"):
            name = f"{top}__{sub_s}" if top else sub_s
        else:
            name = top
        new_cols.append(name if name else top)
    new_cols = _make_unique_headers_row(new_cols)
    out = raw.iloc[1:].copy()
    out.columns = new_cols
    return out.reset_index(drop=True)


def _parse_european_money_scalar(val: Any) -> float:
    """Parse ``1.234,56`` / ``1234,56`` style amounts when US-style parsing yields nothing."""
    t = str(val).strip()
    if not t or t.lower() in ("-", "—", "n/a", "na", "#ref!", "#value!", "null"):
        return 0.0
    paren_neg = t.startswith("(") and t.endswith(")")
    if paren_neg:
        t = t[1:-1].strip()
    sign = -1.0 if t.startswith("-") else 1.0
    t = t.lstrip("-").strip()
    t = re.sub(r"[\$€£\s\xa0]", "", t)
    if not t:
        return 0.0
    if re.search(r",\d{1,2}$", t) and t.count(",") == 1:
        t = t.replace(",", ".")
    elif "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    try:
        v = sign * float(t)
        return -abs(v) if paren_neg else v
    except ValueError:
        return 0.0


def _guess_spend_value_column_raw(raw: pd.DataFrame) -> Optional[str]:
    """If headers do not contain spend/cost, pick the column with the largest plausible money total (excludes ids)."""
    if raw.empty or len(raw.columns) < 2:
        return None
    best_c: Optional[str] = None
    best_sm = -1.0
    for c in raw.columns:
        nk = _norm_header_key(str(c))
        if any(
            x in nk
            for x in (
                "id",
                "opp_",
                "opportunity",
                "email",
                "phone",
                "url",
                "link",
                "uuid",
                "record",
                "case_id",
                "percent",
                "pct",
                "rate",
                "ratio",
            )
        ):
            continue
        if nk in {"month", "year", "week", "day", "date", "market", "country", "region", "channel", "platform"}:
            continue
        sm = float(_to_number_series(raw[c]).abs().sum())
        if sm > best_sm:
            best_sm = sm
            best_c = c
    if best_c is None or best_sm < 1e-6:
        return None
    return best_c


def _first_best_metric_column_by_keyword(
    df: pd.DataFrame,
    keywords: tuple[str, ...],
    exclude_substrings: tuple[str, ...],
) -> Optional[str]:
    """Pick the raw column whose normalized name contains all ``keywords`` (as substrings) and has largest numeric mass."""
    best_c: Optional[str] = None
    best_sum = -1.0
    for c in df.columns:
        nk = _norm_header_key(c)
        if not all(k in nk for k in keywords):
            continue
        if any(ex in nk for ex in exclude_substrings):
            continue
        sm = float(_to_number_series(df[c]).abs().sum())
        if sm > best_sum:
            best_sum = sm
            best_c = c
    return best_c


def _best_ctr_column_raw(df: pd.DataFrame) -> Optional[str]:
    """Best CTR-like raw column for click recovery when explicit click fields are missing."""
    best_c: Optional[str] = None
    best_sum = -1.0
    for c in df.columns:
        nk = _norm_header_key(c)
        if "ctr" not in nk and "click_through_rate" not in nk:
            continue
        if any(ex in nk for ex in ("cpc", "cpm", "cost", "conv", "quality", "rank", "position", "share")):
            continue
        s = _to_number_series(df[c]).abs()
        sm = float(s.sum())
        if sm > best_sum:
            best_sum = sm
            best_c = c
    return best_c


def _sum_metric_columns_by_keywords(
    df: pd.DataFrame,
    *,
    include_keywords: tuple[str, ...],
    exclude_keywords: tuple[str, ...] = (),
) -> pd.Series:
    """Sum all raw columns whose normalized headers match metric keywords."""
    if df.empty:
        return pd.Series(dtype=float)
    acc = pd.Series(0.0, index=df.index, dtype=float)
    matched = False
    for c in df.columns:
        nk = _norm_header_key(str(c))
        if not any(k in nk for k in include_keywords):
            continue
        if any(k in nk for k in exclude_keywords):
            continue
        acc = acc + _to_number_series(df[c]).fillna(0.0)
        matched = True
    return acc if matched else pd.Series(0.0, index=df.index, dtype=float)


def _norm_header_key(name: str) -> str:
    """Lowercase; non-alphanumeric → underscores (matches ME X-Ray Excel headers like `CPCW:LF`, `Cost/TCV%`)."""
    s = str(name).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _resolve_post_lead_stage_column(df: pd.DataFrame) -> Optional[str]:
    """Prefer Post Lead Stage / opportunity stage over the first generic *stage* column."""
    if df.empty or not len(df.columns):
        return None

    def _is_stage_header_noise(nk: str) -> bool:
        """Exclude timestamp / audit columns that happen to contain the substring ``stage``."""
        if "stage" not in nk:
            return True
        if "date" in nk or "time" in nk or "timestamp" in nk or "age" in nk:
            return True
        if "change" in nk or "history" in nk or "audit" in nk:
            return True
        return False

    for c in df.columns:
        if _norm_header_key(c) == "post_lead_stage":
            return c
    best: list[tuple[int, str]] = []
    for c in df.columns:
        nk = _norm_header_key(c)
        if _is_stage_header_noise(nk):
            continue
        if "post_lead" in nk and "stage" in nk:
            best.append((0, c))
        elif nk in ("stagename", "stage_name", "opportunity_stage", "deal_stage", "sales_stage", "lead_stage"):
            best.append((1, c))
    if best:
        best.sort(key=lambda x: (x[0], x[1]))
        return best[0][1]
    for c in df.columns:
        nk = _norm_header_key(c)
        if nk == "stage" and not _is_stage_header_noise(nk):
            return c
    for c in df.columns:
        nk = _norm_header_key(c)
        if "stage" in nk and not _is_stage_header_noise(nk):
            return c
    return None


def _is_closed_won_stage_text(val: Any) -> bool:
    """Count rows in Closed Won, including formally approved; exclude Not Approved / Closed Lost."""
    t = str(val).lower().strip()
    if not t or t in ("nan", "none"):
        return False
    if "closed lost" in t:
        return False
    if "closed won" in t or re.search(r"\bclosed\s*[-_/]*won\b", t):
        return True
    if re.search(r"\bcw\b", t) and "closed lost" not in t:
        return True
    if "not approved" in t or "unapproved" in t:
        return False
    if re.search(r"\bapproved\b", t):
        return True
    return False


def _is_post_lead_pipeline_tab(tab_name: str) -> bool:
    """True for Post Lead / Post Qualification sheets; False for Raw Leads (name variants)."""
    t = tab_name.strip().lower()
    if re.search(r"raw\s*leads?", t):
        return False
    if "raw" in t and "post" in t and "qual" in t:
        return True
    if re.search(r"post\s*lead", t):
        return True
    if "post" in t and "qual" in t:
        return True
    return False

# Tabs whose rows contribute to CW / post-lead funnel (must not include RAW CW or Spend).
_POST_LEAD_SOURCE_TAB_PATTERNS: tuple[str, ...] = (
    # Historical KPI scope: extra patterns pulled a second tab and doubled CW (e.g. 61 -> 122).
    r"raw.*post.*qual",
    r"post\s*leads?",
    # "Post Qualification" / "Post Qual" (no "Raw" or "Leads" in the tab title) — was excluded by the two patterns above.
    r"post\s+qual",
    r"post.*qualif",
)


def _sheet_title_matches_leads_or_post_lead(title: str) -> bool:
    """True for **Leads** and **Post lead / Post qual** tabs — Sep reporting floor must not apply here."""
    tl = str(title).strip().lower()
    if not tl:
        return False
    for pat in _MPO_LEAD_TAB_PATTERNS:
        if re.search(pat, tl, flags=re.I):
            return True
    for pat in _POST_LEAD_SOURCE_TAB_PATTERNS:
        if re.search(pat, tl, flags=re.I):
            return True
    return False


# RAW CW / TCV tab — Actual TCV, 1st Month LF, CPCW:LF (Looker-aligned: CpCW ÷ avg LF per CW = Spend ÷ Σ 1st Month LF).
_RAW_CW_TAB_PATTERNS: tuple[str, ...] = (
    r"raw\s*cw",
    r"raw.*\bcw\b",
    r"^\s*cw\s*$",
    r"sum\s*cw",
    r"cw\s*summary",
)

# Master View: regional subtotal (same markets as typical ME X-Ray).
_MIDDLE_EAST_MARKET_KEYS: frozenset[str] = frozenset(
    {
        "bahrain",
        "kuwait",
        "saudi arabia",
        "uae",
        "united arab emirates",
        "oman",
        "qatar",
        "jordan",
        "lebanon",
        "iraq",
    }
)
def _norm_market_key(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().lower())


_COUNTRY_JOIN_ALIASES: dict[str, str] = {
    "uae": "united arab emirates",
    "u.a.e": "united arab emirates",
    "u.a.e.": "united arab emirates",
    "the uae": "united arab emirates",
    "sa": "saudi arabia",
    "ksa": "saudi arabia",
    "ksa sa": "saudi arabia",
    "saudi": "saudi arabia",
    "kw": "kuwait",
    "bh": "bahrain",
    "kingdom of saudi arabia": "saudi arabia",
    "kingdom of saudi": "saudi arabia",
    # Spend tabs often use a regional row; keep one key so it merges and can roll down to countries.
    "middle east": "middle east",
    "mena": "middle east",
    "mea": "middle east",
    "gcc": "middle east",
    "gulf": "middle east",
}


def _country_join_key(name: str) -> str:
    """Align Spend vs CRM market labels (e.g. UAE ↔ United Arab Emirates) for filtering."""
    k = _norm_market_key(name)
    if k in ("", "unknown", "nan", "<na>"):
        return k
    if k in _COUNTRY_JOIN_ALIASES:
        return _COUNTRY_JOIN_ALIASES[k]
    return k


def _canonical_country_label(name: Any) -> str:
    """Canonical display country for month × country × platform outputs."""
    k = _country_join_key(str(name))
    if k in ("", "unknown", "nan", "<na>", "none"):
        return "Unknown"
    if k == "united arab emirates":
        return "UAE"
    if k == "saudi arabia":
        return "Saudi Arabia"
    if k == "bahrain":
        return "Bahrain"
    if k == "kuwait":
        return "Kuwait"
    return _market_display_from_join_key(k)


# Canonical join-key → CRM-style label in Master View (after merge on normalized ``country``).
_MARKET_DISPLAY_FROM_KEY: dict[str, str] = {
    "united arab emirates": "UAE",
    "saudi arabia": "Saudi Arabia",
    "bahrain": "Bahrain",
    "kuwait": "Kuwait",
    "oman": "Oman",
    "qatar": "Qatar",
    "jordan": "Jordan",
    "lebanon": "Lebanon",
    "iraq": "Iraq",
}


def _market_display_from_join_key(country_key: str) -> str:
    k = _norm_market_key(str(country_key))
    if k == "middle east":
        return _MIDDLE_EAST_REGION_LABEL
    if k in _MARKET_DISPLAY_FROM_KEY:
        return _MARKET_DISPLAY_FROM_KEY[k]
    if not k or k in ("unknown", "nan", "<na>"):
        return "Unknown"
    return " ".join(w.capitalize() for w in k.split())


def _canonical_platform_label(name: Any) -> str:
    """Normalize platform labels so grouping is stable across tabs."""
    s = str(name).strip()
    if not s or s.lower() in {"unknown", "nan", "<na>", "none"}:
        return "Unknown"
    sl = s.lower()
    if re.search(r"\bgoogle\b", sl):
        return "Google Ads"
    if re.search(r"\bmeta\b|facebook|instagram", sl):
        return "Meta Ads"
    if re.search(r"\bsnap(?:chat)?\b", sl):
        return "Snapchat Ads"
    if re.search(r"linked\s*in|linkedin", sl):
        return "LinkedIn Ads"
    return s


def _normalize_master_merge_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Unify ``month`` + ``country`` so spend + CRM + CW outer merges land on one row per market."""
    if df.empty:
        return df
    out = df.copy()
    if "country" in out.columns:
        out["country"] = out["country"].map(_country_join_key)
    if "month" in out.columns:
        mk = out["month"].map(_month_norm_key)
        if "date" in out.columns:
            bad = mk.eq("") | mk.isna()
            if bool(bad.any()):
                d = pd.to_datetime(
                    _scrub_pre_2000_dates(_coerce_sheet_serial_dates(out["date"])),
                    errors="coerce",
                )
                fill = pd.Series("", index=out.index, dtype=object)
                dok = d.notna() & (d >= pd.Timestamp("2000-01-01"))
                if bool(dok.any()):
                    fill.loc[dok] = d.loc[dok].dt.to_period("M").astype(str)
                mk = mk.where(~bad, fill.map(_month_norm_key))
        out["month"] = mk.map(_month_norm_key)
    return out


def _master_df_coalesce_month_country(master_df: pd.DataFrame) -> pd.DataFrame:
    """Sum duplicate rows that share the same canonical ``month`` × ``country`` (outer-merge key drift across tabs)."""
    if master_df.empty or "month" not in master_df.columns or "country" not in master_df.columns:
        return master_df
    m = master_df.copy()
    m["month"] = m["month"].map(lambda x: _month_norm_key(x) if pd.notna(x) else "")
    m = m.loc[m["month"].astype(str).str.strip().ne("")]
    if m.empty:
        return master_df
    try:
        out = m.groupby(["month", "country"], as_index=False, dropna=False).sum(numeric_only=True)
    except TypeError:
        out = m.groupby(["month", "country"], as_index=False, dropna=False).sum()
    return out


# General month keys (CRM, merges): allow full history in loaded sheets.
_MIN_DASHBOARD_PERIOD = pd.Period("1990-01", freq="M")
_MAX_DASHBOARD_PERIOD = pd.Period(date.today(), freq="M")
# **All fetched sheets except Leads / Post lead–Post qual**: Jan 2025 onward on ``month`` / ``date`` (applied after load).
_MIN_FETCHED_SHEET_PERIOD = pd.Period("2025-01", freq="M")
_MIN_FETCHED_SHEET_TS = pd.Timestamp("2025-01-01", tz=None)


def _yyyymm_calendar_to_key(ni: int) -> str:
    """Map integer ``YYYYMM`` (e.g. ``202509``) to canonical ``YYYY-MM`` (ME exports / pivot downloads)."""
    if ni < 199001 or ni > 210012:
        return ""
    y, mo = ni // 100, ni % 100
    if mo < 1 or mo > 12 or y < 1990 or y > 2100:
        return ""
    try:
        p = pd.Period(year=int(y), month=int(mo), freq="M")
        if p >= _MIN_DASHBOARD_PERIOD:
            return str(p)
    except (ValueError, TypeError):
        pass
    return ""


def _month_only_calendar_month_to_key(month_1_12: int) -> str:
    """Map bare month index 1..12 (no year in cell) to ``YYYY-MM``.

    Picks the latest calendar month **not after** the current month (same year first, then prior years).
    """
    if month_1_12 < 1 or month_1_12 > 12:
        return ""
    nowp = pd.Period(date.today(), freq="M")
    y0 = date.today().year
    for y in range(y0, y0 - 8, -1):
        try:
            p = pd.Period(year=y, month=month_1_12, freq="M")
            if p < _MIN_DASHBOARD_PERIOD:
                continue
            if p <= nowp:
                return str(p)
        except (ValueError, TypeError):
            continue
    return ""


def _dashboard_month_plausible(m: Any) -> bool:
    """False for empty month and for pre-2000 periods (epoch junk); True if unparseable (keep row)."""
    if m is None or (isinstance(m, float) and pd.isna(m)):
        return False
    ms = str(m).strip().lower()
    if not ms or ms in ("nat", "nan", "none"):
        return False
    try:
        return bool(pd.Period(str(m), freq="M") >= _MIN_DASHBOARD_PERIOD)
    except Exception:
        return True


def _coerce_sheet_serial_dates(s: pd.Series) -> pd.Series:
    """Google Sheets ``UNFORMATTED_VALUE`` often returns calendar columns as **serial day counts** (Excel epoch).

    Plain ``pd.to_datetime`` on those floats is wrong/NaT, which blanks ``month`` and the Master View filter drops every row.
    """
    if s is None or getattr(s, "empty", True):
        return s
    d1 = pd.to_datetime(s, errors="coerce")
    n = pd.to_numeric(s, errors="coerce")
    ser_mask = n.notna() & (n > 20000) & (n < 100000)
    if not bool(ser_mask.any()):
        return d1
    base = pd.Timestamp("1899-12-30")
    conv = base + pd.to_timedelta(n.loc[ser_mask], unit="D")
    out = d1.copy()
    out.loc[ser_mask] = conv
    return out


def _scrub_pre_2000_dates(s: pd.Series) -> pd.Series:
    """Turn ancient timestamps (often from ``0`` or blank mis-read as epoch) into NaT."""
    dt = pd.to_datetime(s, errors="coerce")
    bad = dt.notna() & (dt < pd.Timestamp("2000-01-01"))
    return dt.where(~bad, pd.NaT)


def _month_norm_key(m: Any) -> str:
    """Canonical ``YYYY-MM`` period string for matching Spend rows to dashboard ``month``."""
    if m is None or (isinstance(m, float) and pd.isna(m)):
        return ""
    # Sheets ``UNFORMATTED_VALUE`` often puts calendar months in the cell as **serial day counts** (Excel epoch).
    # ``pd.to_datetime(45321)`` treats that as ns/ms → 1970 junk → we return "" and Master View drops spend rows.
    try:
        if not isinstance(m, bool):
            n = float(m)
            if pd.notna(n):
                ni = int(round(n))
                if abs(n - ni) < 0.02 or abs(n - round(n)) < 1e-6:
                    yk = _yyyymm_calendar_to_key(ni)
                    if yk:
                        try:
                            p_yk = pd.Period(str(yk), freq="M")
                            if p_yk < _MIN_DASHBOARD_PERIOD or p_yk > _MAX_DASHBOARD_PERIOD:
                                return ""
                        except Exception:
                            return ""
                        return yk
                    # Calendar month 1-12 only (often paired with a separate Year column — see spend preprocess).
                    if 1 <= ni <= 12 and not (20000 < n < 100000):
                        mk = _month_only_calendar_month_to_key(ni)
                        if mk:
                            try:
                                p_mk = pd.Period(str(mk), freq="M")
                                if p_mk < _MIN_DASHBOARD_PERIOD or p_mk > _MAX_DASHBOARD_PERIOD:
                                    return ""
                            except Exception:
                                return ""
                            return mk
                # Excel / Sheets day-count serial (not YYYYMM — those are > 100k).
                if 20000 < n < 100000:
                    if abs(n - ni) < 0.02 or abs(n - round(n)) < 1e-6:
                        base = pd.Timestamp("1899-12-30")
                        ts = base + pd.to_timedelta(ni, unit="D")
                        if ts.year >= 2000:
                            p = ts.to_period("M")
                            if _MIN_DASHBOARD_PERIOD <= p <= _MAX_DASHBOARD_PERIOD:
                                return str(p)
    except (TypeError, ValueError, OverflowError):
        pass
    ms = str(m).strip()
    if not ms or ms.lower() in ("nat", "none", "nan"):
        return ""
    ms6 = ms.replace(",", "").replace(" ", "")
    if ms6.isdigit() and len(ms6) == 6:
        yk6 = _yyyymm_calendar_to_key(int(ms6))
        if yk6:
            try:
                p6 = pd.Period(str(yk6), freq="M")
                if p6 < _MIN_DASHBOARD_PERIOD or p6 > _MAX_DASHBOARD_PERIOD:
                    return ""
            except Exception:
                return ""
            return yk6
    if ms6.isdigit() and 1 <= len(ms6) <= 2:
        mi = int(ms6)
        if 1 <= mi <= 12:
            mk = _month_only_calendar_month_to_key(mi)
            if mk:
                try:
                    pm = pd.Period(str(mk), freq="M")
                    if pm < _MIN_DASHBOARD_PERIOD or pm > _MAX_DASHBOARD_PERIOD:
                        return ""
                except Exception:
                    return ""
                return mk
    try:
        p = pd.Period(str(m), freq="M")
        if p < _MIN_DASHBOARD_PERIOD or p > _MAX_DASHBOARD_PERIOD:
            return ""
        return str(p)
    except Exception:
        try:
            ts = pd.to_datetime(m, errors="coerce")
            if pd.isna(ts) or ts < pd.Timestamp("2000-01-01"):
                return ""
            p = ts.to_period("M")
            if p < _MIN_DASHBOARD_PERIOD or p > _MAX_DASHBOARD_PERIOD:
                return ""
            return str(p)
        except Exception:
            return ""


def _spend_slice_for_dashboard_filters(spend_master: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
    """Apply Market/Month filters without brittle merge; if filters zero out real spend, keep full slice."""
    if spend_master.empty or "cost" not in spend_master.columns:
        return spend_master
    full_sum = float(pd.to_numeric(spend_master["cost"], errors="coerce").fillna(0).sum())
    if df_ref.empty or full_sum == 0.0:
        return spend_master.copy()
    out = spend_master.copy()
    if "country" in out.columns and "country" in df_ref.columns:
        allow_c = {
            x for x in df_ref["country"].map(_country_join_key).unique().tolist() if x and x not in ("unknown", "nan", "")
        }
        if allow_c:
            out = out[out["country"].map(_country_join_key).isin(allow_c)]
    if "month" in out.columns and "month" in df_ref.columns:
        allow_m = {x for x in df_ref["month"].map(_month_norm_key).unique().tolist() if x}
        if allow_m:
            km = out["month"].map(_month_norm_key)
            out = out[km.isin(allow_m) | (km == "")]
    filt_sum = float(pd.to_numeric(out["cost"], errors="coerce").fillna(0).sum())
    if filt_sum == 0.0 and full_sum > 0.0:
        return spend_master.copy()
    return out


def _month_norm_keys_in_reporting_window(start: date, end: date) -> set[str]:
    """``YYYY-MM`` keys for every calendar month from ``start`` through ``end`` (inclusive)."""
    try:
        s = pd.Period(start, freq="M")
        e = pd.Period(end, freq="M")
        out: set[str] = set()
        cur = s
        while cur <= e:
            out.add(str(cur))
            cur = cur + 1
        return out
    except Exception:
        return set()


def _enforce_global_reporting_floor(df: pd.DataFrame) -> pd.DataFrame:
    """No-op: keep **all** loaded rows (historical Sep-2025-only floor removed — use full workbook history)."""
    return df.copy() if not df.empty else df


def _mpo_slice_by_dashboard_ref(frame: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
    """Apply Market/Month filters from ``df_ref`` (same keys as spend KPI slice, without spend-sum fallback)."""
    if frame.empty or df_ref.empty:
        return frame.copy()
    out = frame.copy()
    if "country" in out.columns and "country" in df_ref.columns:
        allow_c = {
            x for x in df_ref["country"].map(_country_join_key).unique().tolist() if x and x not in ("unknown", "nan", "")
        }
        if allow_c:
            out = out[out["country"].map(_country_join_key).isin(allow_c)]
    if "month" in out.columns and "month" in df_ref.columns:
        allow_m = {x for x in df_ref["month"].map(_month_norm_key).unique().tolist() if x}
        if allow_m:
            km = out["month"].map(_month_norm_key)
            out = out[km.isin(allow_m) | (km == "")]
    return out


def _mpo_post_qual_closed_won_rows_for_kpis(post_df: pd.DataFrame, df_scope: pd.DataFrame) -> pd.DataFrame:
    """Post-qual / post-lead rows in **Market × Month** scope with **Closed Won + Approved** (``closed_won`` > 0).

    Shared by the **CW (inc. approved)** count and the **CpCW:LF** Σ first-month LF denominator so both use the
    **same deal row set** (then LF is one value per opportunity when keys exist).
    """
    if post_df.empty or df_scope.empty:
        return pd.DataFrame()
    sl = _mpo_slice_by_dashboard_ref(post_df, df_scope)
    if sl.empty:
        return sl
    w = _ensure_closed_won_from_text_flags(sl.copy())
    if "closed_won" not in w.columns:
        return pd.DataFrame()
    hit = pd.to_numeric(w["closed_won"], errors="coerce").fillna(0) > 0
    return w.loc[hit].copy()


def _mpo_first_month_lf_sum_same_deals_as_post_qual_cw_rows(cw_only: pd.DataFrame, cw_kpi: pd.DataFrame) -> float:
    """Σ ``first_month_lf`` for the **same opportunities** as ``cw_only`` (CW card slice).

    If post-qual carries ``first_month_lf``, use **max LF per opportunity** on those rows. Otherwise join **cw_kpi**
    (RAW CW / deal tab) on shared opportunity keys and sum one LF per key (rent / licence fee for those deals).
    """
    if cw_only.empty:
        return 0.0
    if "first_month_lf" in cw_only.columns:
        lf = pd.to_numeric(cw_only["first_month_lf"], errors="coerce").fillna(0)
        keys = _opp_key_columns_for_post_lead(cw_only)
        if keys:
            tmp = cw_only.loc[:, list(keys)].copy()
            tmp["_lf"] = lf
            return float(tmp.groupby(keys, dropna=False)["_lf"].max().sum())
        return float(lf.sum())
    if cw_kpi.empty or "first_month_lf" not in cw_kpi.columns:
        return 0.0
    keys = [c for c in _opp_key_columns_for_post_lead(cw_only) if c in cw_only.columns and c in cw_kpi.columns]
    if not keys:
        return 0.0
    left = cw_only.loc[:, keys].drop_duplicates()
    right = cw_kpi.copy()
    if "closed_won" in right.columns:
        right = right.loc[pd.to_numeric(right["closed_won"], errors="coerce").fillna(0) > 0].copy()
    if "first_month_lf" not in right.columns:
        return 0.0
    use_cols = list(dict.fromkeys([c for c in keys if c in right.columns] + ["first_month_lf"]))
    right = right.loc[:, use_cols].copy()
    right["first_month_lf"] = pd.to_numeric(right["first_month_lf"], errors="coerce").fillna(0)
    agg = right.groupby(keys, dropna=False)["first_month_lf"].max().reset_index()
    merged = left.merge(agg, on=keys, how="left")
    return float(pd.to_numeric(merged["first_month_lf"], errors="coerce").fillna(0).sum())


def _mpo_cw_kpi_post_lead_record_count(post_df: pd.DataFrame, df_scope: pd.DataFrame) -> int:
    """**CW (inc. approved)** for Marketing performance — fixed definition (do not swap for source-truth or CpCW B2).

    Count **records** on the post-qualification / post-lead worksheet in ``post_df`` after the same **Market × Month**
    filters as the tab (``df_scope``), where **Stage** is Closed Won or Approved (via ``_ensure_closed_won_from_text_flags``).
    """
    return int(len(_mpo_post_qual_closed_won_rows_for_kpis(post_df, df_scope)))


def _mpo_slice_supermetrics_pool_for_kpis(
    pool: pd.DataFrame,
    df_scope: pd.DataFrame,
    *,
    allowed_month_keys: set[str],
) -> pd.DataFrame:
    """Apply **country** from Data scope when paid rows have geo; **month** from ``allowed_month_keys`` only.

    Supermetrics rows often have ``country`` = Unknown — then we skip the CRM country filter.
    Months must **not** come from ``df_scope`` alone: CRM can miss months where ads ran, understating
    totals vs agency / ad-platform reports for the same reporting window.
    """
    if pool.empty or df_scope.empty:
        return pool.copy() if not pool.empty else pool
    out = pool.copy()
    has_geo = False
    if "country" in out.columns:
        ck = out["country"].map(_country_join_key).astype(str).str.strip().str.lower()
        has_geo = bool(((ck != "unknown") & (ck != "") & (ck != "nan") & (ck != "none")).any())
    if has_geo and "country" in df_scope.columns:
        allow_c = {
            x
            for x in df_scope["country"].map(_country_join_key).unique().tolist()
            if x and x not in ("unknown", "nan", "")
        }
        if allow_c:
            out = out[out["country"].map(_country_join_key).isin(allow_c)]
    if "month" in out.columns and allowed_month_keys:
        km = out["month"].map(_month_norm_key)
        out = out[km.isin(allowed_month_keys) | (km == "")]
    return out


def _mpo_traffic_totals_from_sm_pool(
    df_loaded: pd.DataFrame,
    df_scope: pd.DataFrame,
    *,
    primary_sheet_id: str,
    start_date: date,
    end_date: date,
    headline_month_keys: list[str],
    key_suffix: str = "mpo",
) -> Optional[tuple[int, int, float]]:
    """Hero **impressions / clicks / CTR** from Supermetrics pool — months aligned to reporting window + month UI."""
    pool = _mpo_supermetrics_pool_for_clicks_impressions(df_loaded, primary_sheet_id=str(primary_sheet_id))
    if pool.empty:
        return None
    if df_scope.empty:
        return 0, 0, 0.0
    window_m = _month_norm_keys_in_reporting_window(start_date, end_date)
    months_sel = st.session_state.get(f"{key_suffix}_month", [_MPO_ALL_MONTHS_SENTINEL])
    if _mpo_month_multiselect_is_all(months_sel):
        allowed_m = window_m
    else:
        hk = {
            str(_month_norm_key(x)).strip()
            for x in headline_month_keys
            if x and str(x).strip().lower() not in ("", "nan", "nat", "none")
        }
        allowed_m = hk & window_m
        if not allowed_m:
            allowed_m = window_m
    p = _mpo_slice_supermetrics_pool_for_kpis(pool, df_scope, allowed_month_keys=allowed_m)
    ti = int(pd.to_numeric(p["impressions"], errors="coerce").fillna(0).sum()) if "impressions" in p.columns else 0
    tc = int(pd.to_numeric(p["clicks"], errors="coerce").fillna(0).sum()) if "clicks" in p.columns else 0
    if ti == 0 and tc == 0:
        return None
    ctr = (tc / ti * 100.0) if ti else 0.0
    return ti, tc, ctr


def _spend_sheet_month_is_blank(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip().str.lower()
    return ~(t.str.len() > 0) | t.isin(["nan", "nat", "none"])


def _best_norm_month_series_from_normalized_frame(df: pd.DataFrame) -> Optional[pd.Series]:
    """Pick the column whose values most often normalize to ``YYYY-MM`` (unmapped / odd ME X-Ray headers)."""
    if df.empty or len(df.columns) < 2:
        return None
    skip_nk = frozenset(
        {
            "country",
            "market",
            "geo",
            "kitchen_country",
            "country_code",
            "channel",
            "platform",
            "media_type",
            "lead_source",
            "cost",
            "clicks",
            "impressions",
            "cost_tcv_pct",
        }
    ) | _NUM_FIELDS
    best: Optional[pd.Series] = None
    best_ct = 0
    n = len(df)
    for c in df.columns:
        sc = str(c)
        nk = _norm_header_key(sc)
        if nk in skip_nk:
            continue
        if any(p in nk for p in ("opportunity", "deal", "record", "case", "utm_", "lead_status", "opp_")):
            continue
        if nk.endswith("_id") or nk.endswith("_name"):
            continue
        if sc.startswith("_") or sc in ("source_tab", "worksheet_gid"):
            continue
        s = df[c].map(_month_norm_key)
        ct = int(s.ne("").sum())
        if ct > best_ct:
            best_ct = ct
            best = s
    need = max(3, int(0.015 * n))
    if best is None or best_ct < need:
        return None
    return best


def _canonicalize_spend_month_column(df: pd.DataFrame) -> pd.DataFrame:
    """Set ``month`` (YYYY-MM) from row ``date`` when valid; else forward-filled ``report_month`` (merged Month cells).

    ``normalize()`` fills missing ``date`` from **per-row** ``report_month``. Blank Month cells stay blank there,
    but a **preprocess ffill** on Month (removed) or a single filled cell used to collapse all spend into one
    period. Here **date wins**; ``report_month.ffill()`` applies only where month is still unknown.
    """
    if df.empty:
        return df
    out = df.copy()
    idx = out.index
    m_new = pd.Series(pd.NA, index=idx, dtype=object)
    if "date" in out.columns:
        # One map covers YYYYMM ints, sheet serials, ISO strings, etc. (avoids ``to_datetime(202509)`` junk).
        from_date = out["date"].map(_month_norm_key)
        okd = from_date.ne("")
        if bool(okd.any()):
            m_new.loc[okd] = from_date.loc[okd]
    blank = m_new.isna() | m_new.astype(str).str.strip().str.lower().isin(["", "nan", "nat", "none"])
    if "report_month" in out.columns and bool(blank.any()):
        rm_ff = out["report_month"].ffill()
        direct_rm = rm_ff.map(_month_norm_key)
        hit_direct = blank & direct_rm.ne("")
        if bool(hit_direct.any()):
            m_new.loc[hit_direct] = direct_rm.loc[hit_direct]
        blank = m_new.isna() | m_new.astype(str).str.strip().str.lower().isin(["", "nan", "nat", "none"])
        rser = _parse_report_month_series(rm_ff)
        rser = rser.fillna(_coerce_sheet_serial_dates(rm_ff))
        rser = _scrub_pre_2000_dates(rser)
        okr = blank & rser.notna() & (rser >= pd.Timestamp("2000-01-01"))
        if bool(okr.any()):
            m_new.loc[okr] = rser.loc[okr].dt.to_period("M").astype(str)
    blank2 = m_new.isna() | m_new.astype(str).str.strip().str.lower().isin(["", "nan", "nat", "none"])
    if "month" in out.columns and bool(blank2.any()):
        ex = out["month"].map(_month_norm_key)
        use = blank2 & ex.ne("")
        if bool(use.any()):
            m_new.loc[use] = ex.loc[use]
    tmp_k = m_new.map(lambda v: _month_norm_key(v) if pd.notna(v) and str(v).strip() else "")
    if bool(tmp_k.eq("").all()) and "cost" in out.columns:
        if float(pd.to_numeric(out["cost"], errors="coerce").fillna(0).sum()) > 1e-6:
            guess = _best_norm_month_series_from_normalized_frame(out)
            if guess is not None:
                m_new = guess
    out["month"] = m_new.map(lambda v: _month_norm_key(v) if pd.notna(v) and str(v).strip() else "")
    return out


def _attach_spend_pool_debug_attrs(frame: pd.DataFrame) -> None:
    """Stash diagnostics on the normalized spend pool for the MPO debug expander."""
    if frame is None or getattr(frame, "empty", True):
        return
    try:
        frame.attrs["spend_debug_norm_columns"] = list(frame.columns)
        frame.attrs["spend_debug_fields_mapped"] = list(frame.attrs.get("fields_mapped", []) or [])
        frame.attrs["spend_debug_has_report_month"] = bool("report_month" in frame.columns)
        frame.attrs["spend_debug_report_month_filled"] = (
            int(
                (
                    frame["report_month"].notna()
                    & frame["report_month"].astype(str).str.strip().ne("")
                    & ~frame["report_month"].astype(str).str.strip().str.lower().isin(["nan", "none", "nat"])
                ).sum()
            )
            if "report_month" in frame.columns
            else 0
        )
        frame.attrs["spend_debug_date_filled"] = int(frame["date"].notna().sum()) if "date" in frame.columns else 0
        frame.attrs["spend_debug_month_key_rows"] = (
            int(frame["month"].map(_month_norm_key).ne("").sum()) if "month" in frame.columns else 0
        )
        if "report_month" in frame.columns:
            u = frame["report_month"].dropna().astype(str).str.strip()
            u = u[~u.str.lower().isin(["nan", "none", "nat"])]
            frame.attrs["spend_debug_report_month_sample"] = sorted(set(u.tolist()))[:24]
        else:
            frame.attrs["spend_debug_report_month_sample"] = []
    except Exception:
        pass


def _spend_sheet_pivot_by_month_country(spend_df: pd.DataFrame) -> pd.DataFrame:
    """Treat the spend tab as a **pivot**: ``SUM(cost)`` [+ clicks/impressions] per ``month`` × ``country``."""
    metrics = ["cost", "clicks", "impressions"]
    if spend_df.empty or "cost" not in spend_df.columns:
        return pd.DataFrame(columns=["month", "country"] + metrics)
    x0 = spend_df.copy()
    x0 = _canonicalize_spend_month_column(x0)
    x = _normalize_master_merge_frame(x0)
    if "month" not in x.columns or "country" not in x.columns:
        return pd.DataFrame(columns=["month", "country"] + metrics)

    x["month"] = x["month"].map(_month_norm_key)
    x = x.loc[~_spend_sheet_month_is_blank(x["month"])]

    def _not_epoch_jan(m: Any) -> bool:
        try:
            p = pd.Period(str(m), freq="M")
            return not (p.year == 1970 and p.month == 1)
        except Exception:
            return True

    x = x.loc[x["month"].map(_not_epoch_jan)]

    if x.empty and _normalized_spend_cost_sum(spend_df) > 1e-9:
        y = _normalize_master_merge_frame(_canonicalize_spend_month_column(spend_df.copy()))
        cols = [c for c in metrics if c in y.columns]
        if cols and "month" in y.columns and "country" in y.columns and not y.empty:
            g = y.groupby(["month", "country"], as_index=False, dropna=False)[cols].sum()
            for c in metrics:
                if c not in g.columns:
                    g[c] = 0 if c in {"clicks", "impressions"} else 0.0
            if _normalized_spend_cost_sum(g) > 1e-9:
                g["month"] = g["month"].map(_month_norm_key)
                return g[["month", "country"] + metrics]

    if x.empty:
        return pd.DataFrame(columns=["month", "country"] + metrics)
    cols = [c for c in metrics if c in x.columns]
    g = x.groupby(["month", "country"], as_index=False, dropna=False)[cols].sum()
    for c in metrics:
        if c not in g.columns:
            g[c] = 0 if c in {"clicks", "impressions"} else 0.0
    g["month"] = g["month"].map(_month_norm_key)
    return g[["month", "country"] + metrics]


def _spend_sheet_pivot_by_month_channel(spend_df: pd.DataFrame) -> pd.DataFrame:
    """``SUM(cost)`` / clicks / impressions per ``month`` × **sheet channel** (``country`` column holds channel for master merge)."""
    metrics = ["cost", "clicks", "impressions"]
    if spend_df.empty or "cost" not in spend_df.columns:
        return pd.DataFrame(columns=["month", "country"] + metrics)
    x0 = spend_df.copy()
    x0 = _canonicalize_spend_month_column(x0)
    x = _normalize_master_merge_frame(x0)
    x["sheet_channel"] = _pmc_sheet_channel_series(x)
    if "month" not in x.columns:
        return pd.DataFrame(columns=["month", "country"] + metrics)
    x["month"] = x["month"].map(_month_norm_key)
    x = x.loc[~_spend_sheet_month_is_blank(x["month"])]

    def _not_epoch_jan(m: Any) -> bool:
        try:
            p = pd.Period(str(m), freq="M")
            return not (p.year == 1970 and p.month == 1)
        except Exception:
            return True

    x = x.loc[x["month"].map(_not_epoch_jan)]
    if x.empty:
        return pd.DataFrame(columns=["month", "country"] + metrics)
    cols = [c for c in metrics if c in x.columns]
    g = x.groupby(["month", "sheet_channel"], as_index=False, dropna=False)[cols].sum()
    for c in metrics:
        if c not in g.columns:
            g[c] = 0 if c in {"clicks", "impressions"} else 0.0
    g["month"] = g["month"].map(_month_norm_key)
    g = g.rename(columns={"sheet_channel": "country"})
    return g[["month", "country"] + metrics]


def _is_middle_east_market(name: str) -> bool:
    k = _norm_market_key(name)
    if k == "uae":
        k = "united arab emirates"
    return k in _MIDDLE_EAST_MARKET_KEYS


def _pmc_is_middle_east_row(df: pd.DataFrame) -> pd.Series:
    """Paid / PMC rows scoped to GCC/ME countries or sheet **Middle East** regional key."""
    if df.empty or "country" not in df.columns:
        return pd.Series(False, index=df.index)
    jk = df["country"].map(_country_join_key).astype(str).str.strip().str.lower()
    disp = df["country"].map(_market_display_from_join_key)
    return disp.map(_is_middle_east_market) | jk.eq("middle east")


def _pmc_filter_middle_east(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.loc[_pmc_is_middle_east_row(df)].copy()


def _pmc_row_is_regional_middle_east_summary(df: pd.DataFrame) -> pd.Series:
    """True for sheet **Middle East** / MENA / GCC **rollup** rows (join key ``middle east``), not a country line."""
    if df.empty or "country" not in df.columns:
        return pd.Series(False, index=df.index)
    jk = df["country"].astype(str).str.strip().str.lower()
    return jk.eq("middle east")


def _pmc_row_is_me_country(df: pd.DataFrame) -> pd.Series:
    """GCC/ME **country** rows only (excludes the regional **Middle East** aggregate row)."""
    if df.empty or "country" not in df.columns:
        return pd.Series(False, index=df.index)
    disp = df["country"].map(_market_display_from_join_key)
    return disp.map(_is_middle_east_market) & ~_pmc_row_is_regional_middle_east_summary(df)


def _pmc_dedupe_regional_vs_country_spend(df: pd.DataFrame) -> pd.DataFrame:
    """Per month: if ME **countries** have spend, drop **Middle East** regional rows so cost is not summed twice.

    Same rule as the Master View ME row: use country detail when present, otherwise keep the regional line.
    """
    if df.empty or "country" not in df.columns or "month" not in df.columns or "cost" not in df.columns:
        return df
    mk = df["month"].map(_month_norm_key).astype(str).str.strip()
    reg_m = _pmc_row_is_regional_middle_east_summary(df)
    cc_m = _pmc_row_is_me_country(df)
    cost = pd.to_numeric(df["cost"], errors="coerce").fillna(0.0)
    drop: list[Any] = []
    for m in mk.loc[mk.ne("")].unique():
        m_mask = mk.eq(m)
        if not bool(m_mask.any()):
            continue
        c_country = float(cost.loc[m_mask & cc_m].sum())
        if c_country <= 1e-6:
            continue
        ix_reg = df.index[m_mask & reg_m]
        drop.extend(ix_reg.tolist())
    if not drop:
        return df
    return df.drop(index=drop).copy()


def _pmc_rows_workbook(df: pd.DataFrame, spreadsheet_id: str) -> pd.DataFrame:
    """Keep rows tagged with ``spreadsheet_id`` = ME marketing workbook (excludes second Supermetrics workbook)."""
    if df.empty:
        return df
    sid = str(_extract_sheet_id(spreadsheet_id))
    if "spreadsheet_id" not in df.columns:
        return df.iloc[0:0].copy()
    return df.loc[df["spreadsheet_id"].astype(str) == sid].copy()


# Master View regional roll-up (first row under each month for ME markets).
_MIDDLE_EAST_REGION_LABEL = "Middle East"
_REGION_SUBTOTAL_NAMES = frozenset(
    {_MIDDLE_EAST_REGION_LABEL, "middle east", "mena", "mea", "gcc", "gulf"}
)
_REGION_SUBTOTAL_NAMES_LOWER = frozenset(str(x).strip().lower() for x in _REGION_SUBTOTAL_NAMES)
# Sheet-level metrics to move from regional aggregate rows onto ME country rows when country-level sums are zero.
_REGIONAL_ROLL_METRICS = frozenset({"spend", "clicks", "impressions", "cw", "tcv", "lf"})


def _tab_subset_by_patterns(frame: pd.DataFrame, tab_keywords: list[str]) -> pd.DataFrame:
    """Filter rows by ``source_tab`` regex patterns (shared by KPI + master merges)."""
    if frame.empty or "source_tab" not in frame.columns:
        return frame
    s = frame["source_tab"].astype(str).str.lower()
    mask = pd.Series(False, index=frame.index)
    for k in tab_keywords:
        mask = mask | s.str.contains(k.lower(), na=False, regex=True)
    return frame[mask].copy()


def _disambiguate_raw_cw_tabs(frame: pd.DataFrame) -> pd.DataFrame:
    """When several tabs match RAW CW patterns, keep one worksheet so TCV/LF are not double-counted.

    Prefer a tab whose title matches ``raw cw``; drop other matches (e.g. ``CW Summary`` + ``RAW CW``).
    If none are explicitly ``raw cw``, prefer non-summary titles, then the tab with the most rows
    (deal-level exports are usually larger than rollups).
    """
    if frame.empty or "source_tab" not in frame.columns:
        return frame
    tabs = frame["source_tab"].dropna().astype(str).str.strip()
    ut = tabs.unique().tolist()
    if len(ut) <= 1:
        return frame
    raw_named = [u for u in ut if re.search(r"raw\s*cw", u.lower())]
    pool = raw_named if raw_named else list(ut)
    if len(pool) > 1:
        non_sum = [u for u in pool if "summary" not in u.lower()]
        if non_sum:
            pool = non_sum
    best = max(pool, key=lambda tab: int((tabs == tab).sum()))
    return frame.loc[tabs.eq(best)].copy()


def _resolve_cw_tcv_dataframe(df_loaded: pd.DataFrame, df_filtered: pd.DataFrame) -> pd.DataFrame:
    """Resolve rows from the RAW CW worksheet(s). KPI totals apply ``_cw_dataframe_for_kpis`` (closed won + filters)."""
    gid = _optional_raw_cw_gid_from_secrets()
    if gid is not None and not df_loaded.empty and "worksheet_gid" in df_loaded.columns:
        wg = pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce")
        by_g = df_loaded.loc[wg == int(gid)].copy()
        if not by_g.empty:
            return by_g
    if not df_loaded.empty:
        t = _tab_subset_by_patterns(df_loaded, list(_RAW_CW_TAB_PATTERNS))
        if not t.empty:
            return _disambiguate_raw_cw_tabs(t)
    # Fallback: filtered frame + loose pattern
    if df_filtered.empty:
        return df_filtered
    s = df_filtered["source_tab"].astype(str).str.lower() if "source_tab" in df_filtered.columns else None
    if s is not None:
        mask = pd.Series(False, index=df_filtered.index)
        for k in _RAW_CW_TAB_PATTERNS:
            mask = mask | s.str.contains(k.lower(), na=False, regex=True)
        sub = df_filtered.loc[mask].copy()
        if not sub.empty:
            return _disambiguate_raw_cw_tabs(sub)
    return df_filtered


def _is_raw_cw_style_tab(tab_name: str) -> bool:
    """Worksheets that hold deal-level TCV / LF (not only tabs literally named RAW CW)."""
    t = tab_name.strip().lower()
    if "raw" in t and "cw" in t:
        return True
    if re.match(r"^\s*cw\s*$", t):
        return True
    if re.search(r"cw\s*summary", t):
        return True
    return False


def _cw_dataframe_for_kpis(cw_df: pd.DataFrame, df_dashboard: pd.DataFrame) -> pd.DataFrame:
    """Looker-style scope for Actual TCV and 1st Month LF: **closed-won rows only**, same Month × Market as the dashboard.

    ``closed_won`` includes ``Is_CW`` / ``is_cw`` after normalization. If flags would zero out TCV incorrectly,
    we fall back to the unfiltered CW frame (misaligned sheet).
    """
    out = cw_df.copy() if not cw_df.empty else cw_df
    if out.empty:
        return out
    # Apply stage-derived CW flags (Closed Won + Approved) with robust fallbacks.
    out = _ensure_closed_won_from_text_flags(out)
    # Hard-safe for CW truth layouts: include rows if ANY plausible stage/status column indicates CW/Approved.
    # This avoids undercount when a single detected status column is incomplete/misaligned.
    _stage_union_mask: Optional[pd.Series] = None
    _stage_cols: list[str] = []
    _st = _resolve_post_lead_stage_column(out)
    if _st is not None and _st in out.columns:
        _stage_cols.append(_st)
    for _c in out.columns:
        _nk = _norm_header_key(str(_c))
        if _nk in {
            "stage",
            "stagename",
            "stage_name",
            "opportunity_stage",
            "deal_stage",
            "lead_status",
            "status",
            "deal_status",
            "opportunity_status",
        }:
            _stage_cols.append(_c)
    _stage_cols = list(dict.fromkeys(_stage_cols))
    for _c in _stage_cols:
        _s = out[_c]
        if not (pd.api.types.is_object_dtype(_s) or pd.api.types.is_string_dtype(_s)):
            continue
        _m = _s.map(_is_closed_won_stage_text).fillna(False)
        _stage_union_mask = _m if _stage_union_mask is None else (_stage_union_mask | _m)
    if out.shape[1] >= 16:
        _m_p = out.iloc[:, 15].map(_is_closed_won_stage_text).fillna(False)
        _stage_union_mask = _m_p if _stage_union_mask is None else (_stage_union_mask | _m_p)
    # Final fallback: if explicit stage candidates produce no hits, scan all text columns row-wise.
    # This handles shifted/missing headers in the CW truth sheet while keeping matching strict
    # to Closed Won/Approved text only.
    if _stage_union_mask is None or not bool(_stage_union_mask.any()):
        _row_any = pd.Series(False, index=out.index)
        for _c in out.columns:
            _s = out[_c]
            if not (pd.api.types.is_object_dtype(_s) or pd.api.types.is_string_dtype(_s)):
                continue
            _row_any = _row_any | _s.map(_is_closed_won_stage_text).fillna(False)
        if bool(_row_any.any()):
            _stage_union_mask = _row_any
    if _stage_union_mask is not None and bool(_stage_union_mask.any()):
        _cw_existing = pd.to_numeric(out.get("closed_won", 0), errors="coerce").fillna(0).gt(0)
        out["closed_won"] = _to_int_series_safe(_cw_existing | _stage_union_mask)
    if "closed_won" in out.columns:
        s = pd.to_numeric(out["closed_won"], errors="coerce").fillna(0)
        if float(s.sum()) > 0:
            filt = out.loc[s > 0].copy()
            if "tcv" in filt.columns and "tcv" in out.columns:
                t_f = float(pd.to_numeric(filt["tcv"], errors="coerce").fillna(0).sum())
                t_all = float(pd.to_numeric(out["tcv"], errors="coerce").fillna(0).sum())
                if t_f == 0.0 and t_all > 0.0:
                    pass
                else:
                    out = filt
            else:
                out = filt
    if not df_dashboard.empty and not out.empty:
        if "month" in out.columns and "month" in df_dashboard.columns:
            _mset = {
                str(x).strip()
                for x in df_dashboard["month"].map(_month_norm_key).dropna().astype(str).tolist()
                if str(x).strip()
            }
            if _mset:
                _om = out["month"].map(_month_norm_key).fillna("").astype(str).str.strip()
                out = out.loc[_om.isin(_mset)].copy()
        if "country" in out.columns and "country" in df_dashboard.columns and not out.empty:
            _cset = {
                str(x).strip().casefold()
                for x in df_dashboard["country"].dropna().astype(str).tolist()
                if str(x).strip()
            }
            if _cset:
                _oc = out["country"].fillna("").astype(str).str.strip().str.casefold()
                out = out.loc[_oc.isin(_cset)].copy()
    return out


def _dedupe_post_lead_rows(df: pd.DataFrame) -> pd.DataFrame:
    """When the same opportunity appears on **more than one** post-qual tab, keep one row (prefer Raw Post Qualification).

    Duplicate rows **within the same tab** are kept so SUM(Qualifying)+SUM(Pitching)+… matches the X-Ray sheet.
    (CW is still de-risked via ``_sum_closed_won_unique_opportunities``.)
    """
    if df.empty or "source_tab" not in df.columns:
        return df
    out = df.copy()

    def _tab_pri(x: Any) -> int:
        s = str(x).lower()
        if re.search(r"raw.*post.*qual", s):
            return 0
        return 1

    out["_tab_pri"] = out["source_tab"].map(_tab_pri)
    key_cols = _opp_key_columns_for_post_lead(out)
    if key_cols:
        out = out.sort_values(by=["_tab_pri"] + key_cols)
        miss = out[out[key_cols].isna().any(axis=1)].copy()
        valid = out[~out[key_cols].isna().any(axis=1)].copy()
        if valid.empty:
            return miss.drop(columns=["_tab_pri"], errors="ignore")
        parts: list[pd.DataFrame] = []
        for _, g in valid.groupby(key_cols, dropna=False):
            n_tabs = g["source_tab"].dropna().astype(str).str.strip().nunique()
            if n_tabs <= 1:
                parts.append(g)
            else:
                parts.append(g.sort_values("_tab_pri").head(1))
        merged = pd.concat(parts + ([miss] if not miss.empty else []), ignore_index=True)
        return merged.drop(columns=["_tab_pri"], errors="ignore")
    ut = out["source_tab"].dropna().astype(str).unique().tolist()
    if len(ut) <= 1:
        return out.drop(columns=["_tab_pri"], errors="ignore")
    best = min(ut, key=_tab_pri)
    return out.loc[out["source_tab"].astype(str) == best].drop(columns=["_tab_pri"], errors="ignore")


def _opp_key_columns_for_post_lead(df: pd.DataFrame) -> list[str]:
    """Columns that identify one Salesforce opportunity (for de-duping CW)."""
    keys: list[str] = []
    for c in df.columns:
        nk = _norm_header_key(c)
        if nk in {
            "opportunity_id",
            "opportunity_id_18",
            "opportunity_name",
            "record_id",
            "case_id",
            "opp_id",
            "opp_name",
            "deal_id",
            "deal_name",
        }:
            keys.append(c)
            continue
        if "opportunity" in nk and ("id" in nk or "name" in nk):
            keys.append(c)
    return list(dict.fromkeys(keys))


def _sum_closed_won_unique_opportunities(df: pd.DataFrame) -> int:
    """Sum CW without double-counting duplicate rows for the same deal (e.g. 122 vs 61)."""
    if df.empty or "closed_won" not in df.columns:
        return 0
    cw = pd.to_numeric(df["closed_won"], errors="coerce").fillna(0)
    cw_bin = _to_int_series_safe(cw > 0)
    keys = _opp_key_columns_for_post_lead(df)
    if keys:
        tmp = df.loc[:, keys].copy()
        tmp["_cw"] = cw_bin
        return int(tmp.groupby(keys, dropna=False)["_cw"].max().sum())
    return int(cw_bin.sum())


def _ensure_closed_won_from_text_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Derive ``closed_won`` from stage/status text when numeric CW is missing/zero."""
    if df.empty:
        return df
    out = df.copy()
    has_cw = "closed_won" in out.columns
    cw_existing = (
        _to_int_series_safe(pd.to_numeric(out.get("closed_won", 0), errors="coerce").fillna(0) > 0)
        if has_cw
        else pd.Series(0, index=out.index, dtype=int)
    )

    # Score candidate stage/status columns and pick the strongest CW/Approved signal.
    candidate_cols: list[str] = []
    st_col = _resolve_post_lead_stage_column(out)
    if st_col is not None and st_col in out.columns:
        candidate_cols.append(st_col)
    for c in out.columns:
        nk = _norm_header_key(c)
        if nk in {"stage", "stagename", "stage_name", "opportunity_stage", "deal_stage"}:
            candidate_cols.append(c)
    for c in out.columns:
        nk = _norm_header_key(c)
        if nk in {"lead_status", "status", "deal_status", "opportunity_status"}:
            candidate_cols.append(c)
    for c in out.columns:
        nk = _norm_header_key(c)
        if "status" in nk and not any(x in nk for x in ("date", "time", "timestamp", "history", "change")):
            candidate_cols.append(c)

    # Keep order while de-duplicating.
    candidate_cols = list(dict.fromkeys(candidate_cols))

    derived: Optional[pd.Series] = None
    best_hits = 0
    for c in candidate_cols:
        s = out[c]
        if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
            continue
        cand = _to_int_series_safe(s.map(_is_closed_won_stage_text))
        hits = int(cand.sum())
        if hits > best_hits:
            best_hits = hits
            derived = cand

    # ME Post Lead / source-truth: Stage is often **column P** (index 15). Always pit it against named headers
    # so a weaker wrong column cannot win and skip P (previously P ran only when best_hits == 0).
    if out.shape[1] >= 16:
        col_p = _to_int_series_safe(out.iloc[:, 15].map(_is_closed_won_stage_text))
        hits_p = int(col_p.sum())
        if hits_p > best_hits:
            best_hits = hits_p
            derived = col_p

    # Last-resort fallback: infer from any object/string column.
    if derived is None or best_hits == 0:
        for c in out.columns:
            s = out[c]
            if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
                continue
            cand = _to_int_series_safe(s.map(_is_closed_won_stage_text))
            hits = int(cand.sum())
            if hits > best_hits:
                best_hits = hits
                derived = cand

    if derived is None:
        out["closed_won"] = cw_existing if has_cw else 0
        return out
    out["closed_won"] = _to_int_series_safe((cw_existing > 0) | (derived > 0))
    return out


def _closed_won_kpi_count_from_leads_gid(
    df_loaded: pd.DataFrame,
    sheet_id: str,
    leads_gid: int,
    _fp_mpo: str,
    *,
    close_date_min: Optional[pd.Timestamp] = None,
) -> int:
    """CW (inc. approved) from Leads tab: count matching rows after optional date floor + stage."""
    base = _rows_by_worksheet_id(df_loaded, int(leads_gid), sheet_id)
    if base.empty:
        try:
            base = load_worksheet_by_gid_preprocessed(sheet_id, int(leads_gid), _fp_mpo)
        except Exception:
            base = pd.DataFrame()
    if base.empty:
        return 0
    work = _ensure_closed_won_from_text_flags(base.copy())
    if close_date_min is not None and "date" in work.columns:
        d = pd.to_datetime(work["date"], errors="coerce")
        work = work.loc[d.isna() | (d >= close_date_min)].copy()
    work = work.loc[pd.to_numeric(work.get("closed_won", 0), errors="coerce").fillna(0) > 0].copy()
    return int(len(work.index))


def _closed_won_kpi_count_from_source_truth_gid(
    sheet_id: str,
    worksheet_gid: int,
) -> int:
    """CW source-of-truth count from one worksheet (each matching row = 1 CW)."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return 0
    try:
        raw = _read_sheet_auth(
            sheet_id,
            secret_creds,
            worksheet_name=None,
            worksheet_gid=int(worksheet_gid),
        )
    except Exception:
        raw = pd.DataFrame()
    if raw.empty:
        try:
            raw = _read_sheet_auth_loose(
                sheet_id,
                secret_creds,
                worksheet_gid=int(worksheet_gid),
            )
        except Exception:
            raw = pd.DataFrame()
    if raw.empty:
        return 0

    raw_orig = raw.copy()
    raw = _promote_wide_metric_header_row_if_needed(raw)
    raw = _coerce_two_row_sheet_headers(raw)

    stage_s: Optional[pd.Series] = None
    for c in raw.columns:
        nk = _norm_header_key(str(c))
        if nk in {"stage", "stagename", "stage_name", "opportunity_stage", "deal_stage", "lead_status", "status"}:
            stage_s = raw[c].astype(str)
            break
    # User confirmed stage is in column P (16th column) on this source-truth tab.
    if stage_s is None and raw.shape[1] >= 16:
        stage_s = raw.iloc[:, 15].astype(str)
    if stage_s is None and raw_orig.shape[1] >= 16:
        # Keep a strict fallback to source column P from the unmodified sheet extract.
        stage_s = raw_orig.iloc[:, 15].astype(str)
    if stage_s is None:
        stage_s = None

    if stage_s is not None:
        cw_mask = stage_s.map(_is_closed_won_stage_text).fillna(False)
        # Requested rule: publish CW as raw matching row count (no dedupe by opportunity).
        return int(_to_int_series_safe(cw_mask).sum())

    # Fallback: normalized parse path from the same worksheet gid.
    try:
        norm = load_worksheet_by_gid_preprocessed(sheet_id, int(worksheet_gid), _secret_fingerprint(_service_account_from_streamlit_secrets()))
    except Exception:
        norm = pd.DataFrame()
    if norm.empty:
        return 0
    norm = _ensure_closed_won_from_text_flags(norm)
    if "closed_won" not in norm.columns:
        return 0
    return int(pd.to_numeric(norm["closed_won"], errors="coerce").fillna(0).gt(0).sum())


def _closed_won_tcv_lf_sums_from_source_truth_gid(
    sheet_id: str,
    worksheet_gid: int,
) -> tuple[int, float, float]:
    """(row_count, tcv_sum, first_month_lf_sum) from source-truth worksheet for Closed Won + Approved rows.

    TCV and LF use the **same** ``stage_mask`` row set so headline **CpCW:LF = Spend ÷ Σ LF** matches the
    **Actual TCV** card when both are sourced from this tab (avoids Σ LF from a wider ``cw_kpi`` merge).
    """
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return 0, 0.0, 0.0
    try:
        raw = _read_sheet_auth(
            sheet_id,
            secret_creds,
            worksheet_name=None,
            worksheet_gid=int(worksheet_gid),
        )
    except Exception:
        raw = pd.DataFrame()
    if raw.empty:
        try:
            raw = _read_sheet_auth_loose(
                sheet_id,
                secret_creds,
                worksheet_gid=int(worksheet_gid),
            )
        except Exception:
            raw = pd.DataFrame()
    if raw.empty:
        return 0, 0.0, 0.0

    raw_orig = raw.copy()
    raw = _promote_wide_metric_header_row_if_needed(raw)
    raw = _coerce_two_row_sheet_headers(raw)

    # Stage mask: union across plausible stage/status columns, plus strict col-P fallback.
    stage_mask = pd.Series(False, index=raw.index)
    cand_cols: list[str] = []
    for c in raw.columns:
        nk = _norm_header_key(str(c))
        if nk in {"stage", "stagename", "stage_name", "opportunity_stage", "deal_stage", "lead_status", "status"}:
            cand_cols.append(c)
    cand_cols = list(dict.fromkeys(cand_cols))
    for c in cand_cols:
        s = raw[c]
        if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            stage_mask = stage_mask | s.map(_is_closed_won_stage_text).fillna(False)
    if raw.shape[1] >= 16:
        stage_mask = stage_mask | raw.iloc[:, 15].astype(str).map(_is_closed_won_stage_text).fillna(False)
    if raw_orig.shape[1] >= 16:
        stage_mask = stage_mask | raw_orig.iloc[:, 15].astype(str).map(_is_closed_won_stage_text).fillna(False)
    if not bool(stage_mask.any()):
        for c in raw.columns:
            s = raw[c]
            if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
                continue
            stage_mask = stage_mask | s.map(_is_closed_won_stage_text).fillna(False)

    # TCV source: prefer "TCV (converted)" (column T in source), then TCV USD.
    tcv_col: Optional[str] = None
    for c in raw.columns:
        nk = _norm_header_key(str(c))
        if nk == "tcv_converted":
            tcv_col = c
            break
    if tcv_col is None:
        for c in raw.columns:
            nk = _norm_header_key(str(c))
            if nk in {"tcv", "tcv_usd"}:
                tcv_col = c
                break
    if tcv_col is None and raw.shape[1] >= 20:
        tcv_s = pd.to_numeric(raw.iloc[:, 19], errors="coerce").fillna(0.0)
    else:
        tcv_s = pd.to_numeric(raw[tcv_col], errors="coerce").fillna(0.0) if tcv_col is not None else pd.Series(0.0, index=raw.index)

    lf_col: Optional[str] = None
    _lf_keys = {
        "first_month_lf",
        "1st_month_lf",
        "monthly_lf_usd",
        "monthly_license_fee",
        "monthly_license_fee_converted",
        "monthly_license_fee_usd",
        "license_fee",
    }
    for c in raw.columns:
        nk = _norm_header_key(str(c))
        if nk in _lf_keys:
            lf_col = c
            break

    rows = int(_to_int_series_safe(stage_mask).sum())
    total_tcv = float(tcv_s.loc[stage_mask].sum()) if rows > 0 else 0.0
    if lf_col is not None:
        lf_s = pd.to_numeric(raw[lf_col], errors="coerce").fillna(0.0)
        total_lf = float(lf_s.loc[stage_mask].sum()) if rows > 0 else 0.0
    else:
        total_lf = 0.0
    return rows, total_tcv, total_lf


def _closed_won_tcv_sum_from_source_truth_gid(
    sheet_id: str,
    worksheet_gid: int,
) -> tuple[int, float]:
    """Backward-compatible wrapper: (row_count, tcv_sum) only."""
    r, t, _ = _closed_won_tcv_lf_sums_from_source_truth_gid(sheet_id, worksheet_gid)
    return r, t


def _sum_closed_won_sheet_style(df: pd.DataFrame) -> int:
    """``SUM(CW)`` as in Sheets / Looker — sums the metric column across rows (includes duplicate rows)."""
    if df.empty or "closed_won" not in df.columns:
        return 0
    return int(pd.to_numeric(df["closed_won"], errors="coerce").fillna(0).sum())


def _qualified_denominator_for_qwin(post_df: pd.DataFrame, leads_df: pd.DataFrame) -> int:
    """``SUM(Qualified)`` on post-qual when the sheet has a real 0/1 column; else Lead Status=Qualified on leads tab."""
    q_leads = _qualified_count_from_leads(leads_df)
    if post_df.empty or "qualified" not in post_df.columns:
        return q_leads
    s = pd.to_numeric(post_df["qualified"], errors="coerce").fillna(0)
    n = len(post_df)
    if n == 0:
        return q_leads
    q_sum = int(s.sum())
    # Preprocess default for post tabs: Qualified=1 on every row → sum == row count (not SQL count).
    if q_sum == n and float(s.max()) <= 1.0:
        return q_leads
    return q_sum if q_sum > 0 else q_leads


def _q_win_rate_inputs(post_df: pd.DataFrame, leads_df: pd.DataFrame) -> tuple[int, int]:
    """(CW numerator, Qualified denominator) to mirror X-Ray ``SUM(CW)/SUM(Qualified)``."""
    _pq = post_df if not post_df.empty else pd.DataFrame()
    cw_sheet = _sum_closed_won_sheet_style(_pq)
    cw_uniq = _sum_closed_won_unique_opportunities(_pq)
    # Sheets SUM(CW) counts duplicate rows; unique is used when there are no extras.
    cw_num = cw_sheet if cw_sheet > cw_uniq else cw_uniq
    qual_den = _qualified_denominator_for_qwin(_pq, leads_df)
    return cw_num, qual_den


# Normalized header → canonical column (covers X-Ray export names + ME X-Ray Excel template)
_NORM_TO_FIELD: dict[str, str] = {
    "date": "date",
    "day": "date",
    "period": "date",
    "week": "date",
    "create_date": "date",
    "created_date": "date",
    "first_lead_created_date": "date",
    "1st_lead_created_date": "date",
    "date_formatted": "date",
    "close_date": "date",
    "reporting_date": "date",
    "activity_date": "date",
    "spend_date": "date",
    "campaign_date": "date",
    "week_start": "date",
    "week_starting": "date",
    "country_name": "country",
    "country": "country",
    "market": "country",
    "geo": "country",
    "kitchen_country": "country",
    "country_code": "country_code",
    "channel_gp": "channel",
    "channel_name": "channel",
    "channel": "channel",
    # ME X-Ray / Looker spend pivots — **this** is the channel grain (Organic, Meta, Google Search, …).
    "unified_channel": "channel",
    "media_type": "channel",
    "lead_source": "channel",
    # Google / Meta exports often put pivot channel here instead of **Channel**
    "campaign_type": "channel",
    "advertising_channel_type": "channel",
    "advertising_channel": "channel",
    "platform": "platform",
    "cost": "cost",
    "ad_spend": "cost",
    "spend": "cost",
    "total_spend": "cost",
    "sum_spend": "cost",
    "spend_usd": "cost",
    "amount_spent": "cost",
    "amount": "cost",
    "amount_usd": "cost",
    "total_amount": "cost",
    "spent": "cost",
    "marketing_spend": "cost",
    "cost_usd": "cost",
    "adspend": "cost",
    "clicks_gp": "clicks",
    "clicks": "clicks",
    "link_clicks": "clicks",
    "inline_link_clicks": "clicks",
    "unique_link_clicks": "clicks",
    "outbound_clicks": "clicks",
    "all_clicks": "clicks",
    "clicks_all": "clicks",
    "total_clicks": "clicks",
    "swipe_ups": "clicks",
    "swipe_up": "clicks",
    "swipes": "clicks",
    "landing_page_clicks": "clicks",
    "interactions": "clicks",
    "post_clicks": "clicks",
    "paid_clicks": "clicks",
    "link_click": "clicks",
    "impressions_gp": "impressions",
    "impressions": "impressions",
    "impr": "impressions",
    "post_impressions": "impressions",
    "paid_impressions": "impressions",
    "ctr": "ctr",
    "click_through_rate": "ctr",
    "ctr_percent": "ctr",
    "link_ctr": "ctr",
    "all_ctr": "ctr",
    "leads": "leads",
    "qualified": "qualified",
    "pitching": "pitching",
    "closed_won": "closed_won",
    "closedwon": "closed_won",
    "closed_won_deals": "closed_won",
    "cw_including_approved": "closed_won",
    # Post-lead X-Ray / Salesforce export (binary 0/1 column)
    "is_cw": "closed_won",
    # Sheet / pivot header variants (often col AA on Post Lead)
    "cw_inc_approved": "closed_won",
    "cw_inc_app": "closed_won",
    "utm_source_gp": "utm_source",
    "utm_source": "utm_source",
    "utm_source_l": "utm_source_l",
    "utm_source_o": "utm_source_o",
    "month": "report_month",
    "report_month": "report_month",
    "calendar_month": "report_month",
    "month_year": "report_month",
    "year_month": "report_month",
    "reporting_month": "report_month",
    "billing_month": "report_month",
    "posting_month": "report_month",
    "reporting_period": "report_month",
    "time_period": "report_month",
    "calendar_period": "report_month",
    "tcv": "tcv",
    "tcv_usd": "tcv",
    "tcv_converted": "tcv",
    "actual_tcv": "tcv",
    "actual_tcv_usd": "tcv",
    "1st_month_lf": "first_month_lf",
    "monthly_lf_usd": "first_month_lf",
    # Canonical column from a prior ``_normalize`` pass — must map through so CpCW / B3 re-ingest does not zero LF.
    "first_month_lf": "first_month_lf",
    # Salesforce exports often label first-month LF as "Monthly License Fee" (sometimes with (converted)/(USD)).
    "monthly_license_fee": "first_month_lf",
    "monthly_license_fee_converted": "first_month_lf",
    "monthly_license_fee_usd": "first_month_lf",
    "license_fee": "first_month_lf",
    "cpcw": "cpcw",
    "cpcw_lf": "cpcw_lf",
    "cost_tcv": "cost_tcv_pct",
    "sql": "sql_pct",
    "q_win_rate": "q_win_rate",
    "new": "new",
    "working": "working",
    "qualifying": "qualifying",
    "total_live": "total_live",
    "negotiation": "negotiation",
    "commitment": "commitment",
    "closed_lost": "closed_lost",
    # Pass through Salesforce opportunity keys (otherwise dropped — CW sums duplicate rows → 122 vs 61).
    "opportunity_id": "opportunity_id",
    "opportunity_name": "opportunity_name",
    "opportunity_id_18": "opportunity_id_18",
    "record_id": "record_id",
    "case_id": "case_id",
    "opp_id": "opp_id",
    "opp_name": "opp_name",
    "deal_id": "deal_id",
    "deal_name": "deal_name",
    "lead_status": "lead_status_text",
}

_NUM_FIELDS = frozenset(
    {
        "cost",
        "clicks",
        "impressions",
        "ctr",
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
        "new",
        "working",
        "qualifying",
        "total_live",
        "negotiation",
        "commitment",
        "closed_lost",
    }
)


def _lf_column_sort_key(column_name: str) -> tuple[int, str]:
    """Order LF source columns: converted/USD before local ``Monthly License Fee`` (max of both was wrong)."""
    nk = _norm_header_key(str(column_name))
    if nk == "monthly_license_fee_converted":
        pri = 0
    elif nk in ("monthly_license_fee_usd", "monthly_lf_usd"):
        pri = 1
    elif nk in ("1st_month_lf", "first_month_lf"):
        pri = 2
    elif nk == "monthly_license_fee":
        pri = 3
    elif nk == "license_fee":
        pri = 4
    else:
        pri = 5
    return (pri, str(column_name).lower())


def _to_number_series(s: pd.Series) -> pd.Series:
    """Robust numeric parser for Sheets/CSV text like '$1,234.50', '3.4%', '(120)'."""
    if pd.api.types.is_numeric_dtype(s):
        out_num = pd.to_numeric(s, errors="coerce")
        out_num = out_num.replace([float("inf"), float("-inf")], pd.NA)
        return out_num.fillna(0)
    txt = s.astype(str).str.strip()
    neg_paren = txt.str.match(r"^\(.*\)$", na=False)
    cleaned = (
        txt.str.replace(r"[\$,]", "", regex=True)
        .str.replace("%", "", regex=False)
        .str.replace(r"^\((.*)\)$", r"\1", regex=True)
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    out = pd.to_numeric(cleaned, errors="coerce")
    out = out.replace([float("inf"), float("-inf")], pd.NA).fillna(0)
    us_abs = float(out.abs().sum())
    used_eu = False
    if us_abs < 1e-12 and txt.str.contains(",", na=False).any():
        eu = txt.map(_parse_european_money_scalar).astype(float)
        if float(eu.abs().sum()) > us_abs:
            out = eu
            used_eu = True
    if not used_eu:
        out.loc[neg_paren] = -out.loc[neg_paren]
    return out.replace([float("inf"), float("-inf")], pd.NA).fillna(0)


def _to_int_series_safe(s: pd.Series) -> pd.Series:
    """Safe integer conversion for dashboard metrics (handles NaN/NA/inf)."""
    out = pd.to_numeric(s, errors="coerce")
    out = out.replace([float("inf"), float("-inf")], pd.NA).fillna(0)
    return out.round().astype(int)


def _closed_won_to_numeric_series(s: pd.Series) -> pd.Series:
    """Post-Lead ``Is_CW`` / boolean / TRUE-FALSE text → 0/1; leave integer counts >1 as-is (RAW CW tabs)."""
    if s.dtype == bool:
        return s.astype(float)
    n = pd.to_numeric(s, errors="coerce")
    nn = n.dropna()
    if len(nn) and float(nn.max()) > 1.0 + 1e-9:
        return _to_number_series(s)
    t = s.astype(str).str.strip().str.lower()
    out = n.fillna(0).astype(float)
    out.loc[t.isin(("true", "t", "yes", "y", "x"))] = 1.0
    out.loc[t.isin(("false", "f", "no", "n", "", "nan", "none", "nat"))] = 0.0
    return (out > 0).astype(float)


def _best_spend_column_raw(df: pd.DataFrame) -> Optional[str]:
    """When normalize maps ``cost`` to zero, pick the raw column that looks like spend (by header + numeric mass)."""
    if df.empty or not len(df.columns):
        return None
    best_c: Optional[str] = None
    best_abs = 0.0
    for c in df.columns:
        nk = _norm_header_key(str(c))
        if nk in {"cost_tcv", "cost_tcv_pct"}:
            continue
        if nk in {"leads", "qualified", "clicks", "impressions", "impr"}:
            continue
        if nk.startswith("tcv") and "cost" not in nk:
            continue
        if nk in {"first_month_lf", "monthly_lf_usd", "cpcw", "cpcw_lf", "contract_length"}:
            continue
        if not (
            "spend" in nk
            or "cost" in nk
            or "amount" in nk
            or nk in ("investment", "budget", "media_spend", "paid", "fee", "fees")
            or ("investment" in nk and "tcv" not in nk)
            or ("budget" in nk and "tcv" not in nk)
        ):
            continue
        sm = float(_to_number_series(df[c]).abs().sum())
        if sm > best_abs:
            best_abs = sm
            best_c = c
    return best_c


def _sum_cost_columns_raw(df: pd.DataFrame, nrows: int) -> pd.Series:
    """Row-wise sum of all spend/cost amount headers (excluding unit/rate fields)."""
    if df.empty or nrows <= 0:
        return pd.Series(0.0, index=range(max(nrows, 0)), dtype=float)
    include: list[str] = []
    for c in df.columns:
        nk = _norm_header_key(str(c))
        is_spend_amount = (
            "cost" in nk
            or "spend" in nk
            or "amount_spent" in nk
            or nk in ("amount", "investment", "budget", "media_spend", "paid")
        )
        if not is_spend_amount:
            continue
        if any(
            x in nk
            for x in (
                "cpu",
                "cost_per",
                "cost_per_conversion",
                "conversion_cost",
                "cost_conversion",
                "cpc",
                "cpm",
                "cpv",
                "cpa",
                "cost_tcv",
                "cost_tcv_pct",
                "percent",
                "pct",
                "rate",
            )
        ):
            continue
        include.append(c)
    if not include:
        return pd.Series(0.0, index=range(nrows), dtype=float)
    out = pd.Series(0.0, index=range(nrows), dtype=float)
    for c in include:
        s = _to_number_series(df[c].reset_index(drop=True))
        if len(s) < nrows:
            s = s.reindex(range(nrows), fill_value=0.0)
        out = out + pd.to_numeric(s, errors="coerce").fillna(0.0)
    return out


def _market_key_from_cost_header(header: str) -> str:
    """Infer market key from a cost column header token."""
    nk = _norm_header_key(str(header))
    if "saudi" in nk or "ksa" in nk or re.search(r"(^|_)sa($|_)", nk):
        return "saudi arabia"
    if "kuwait" in nk or re.search(r"(^|_)kw($|_)", nk):
        return "kuwait"
    if "bahrain" in nk or re.search(r"(^|_)bh($|_)", nk):
        return "bahrain"
    if "uae" in nk or "emirates" in nk:
        return "united arab emirates"
    return ""


def _infer_country_from_cost_headers_raw(df: pd.DataFrame, nrows: int) -> pd.Series:
    """Infer row market from cost headers when raw rows lack a usable country column."""
    if df.empty or nrows <= 0:
        return pd.Series(["Unknown"] * max(nrows, 0), index=range(max(nrows, 0)), dtype=object)
    picks = pd.Series(["Unknown"] * nrows, index=range(nrows), dtype=object)
    best_abs = pd.Series(0.0, index=range(nrows), dtype=float)
    for c in df.columns:
        nk = _norm_header_key(str(c))
        if "cost" not in nk:
            continue
        if any(
            x in nk
            for x in ("cpu", "cost_per", "cost_per_conversion", "conversion_cost", "cost_conversion", "cpc", "cpm", "cpv", "cpa")
        ):
            continue
        mk = _market_key_from_cost_header(str(c))
        if not mk:
            continue
        s = _to_number_series(df[c].reset_index(drop=True))
        if len(s) < nrows:
            s = s.reindex(range(nrows), fill_value=0.0)
        abs_s = pd.to_numeric(s, errors="coerce").fillna(0.0).abs()
        use = abs_s > best_abs
        if bool(use.any()):
            picks.loc[use] = mk
            best_abs.loc[use] = abs_s.loc[use]
    return picks.map(_country_join_key)


def _apply_equal_split_all_market_engagement(df: pd.DataFrame) -> pd.DataFrame:
    """Split unknown-country 'All Market (Engagement)' rows equally across UAE/SA/KW/BH."""
    if df.empty or "cost" not in df.columns:
        return df
    out = df.copy()
    if "country" not in out.columns:
        out["country"] = "Unknown"
    ck = out["country"].map(_country_join_key).astype(str).str.strip().str.lower()
    unknown_mask = ck.isin({"", "unknown", "nan", "none", "<na>"})
    if not bool(unknown_mask.any()):
        return out

    campaign_cols = [c for c in ("campaign_name", "campaign", "utm_campaign") if c in out.columns]
    if not campaign_cols:
        return out
    camp = pd.Series("", index=out.index, dtype=object)
    for c in campaign_cols:
        s = out[c].astype(str).str.strip()
        camp = camp.where(camp.astype(str).str.len() > 0, s)
    camp_l = camp.astype(str).str.lower()
    split_mask = unknown_mask & camp_l.str.contains("all market", na=False) & camp_l.str.contains("engagement", na=False)
    if not bool(split_mask.any()):
        return out

    base = out.loc[split_mask].copy()
    if base.empty:
        return out
    targets = ["united arab emirates", "saudi arabia", "kuwait", "bahrain"]
    parts: list[pd.DataFrame] = []
    for t in targets:
        p = base.copy()
        p["country"] = t
        p["cost"] = pd.to_numeric(p["cost"], errors="coerce").fillna(0.0) * 0.25
        if "clicks" in p.columns:
            p["clicks"] = pd.to_numeric(p["clicks"], errors="coerce").fillna(0.0) * 0.25
        if "impressions" in p.columns:
            p["impressions"] = pd.to_numeric(p["impressions"], errors="coerce").fillna(0.0) * 0.25
        parts.append(p)
    rest = out.loc[~split_mask].copy()
    return pd.concat([rest] + parts, ignore_index=True)


def _scan_frame_for_spend_sum(raw: pd.DataFrame) -> float:
    """Best single-column spend total on a **raw** sheet (headers not yet normalized)."""
    if raw.empty or len(raw.columns) == 0:
        return 0.0
    best_sum = 0.0
    for c in raw.columns:
        nk = _norm_header_key(c)
        if nk in {"cost_tcv", "cost_tcv_pct"}:
            continue
        if not (
            "spend" in nk
            or "cost" in nk
            or "amount" in nk
            or nk in ("investment", "budget")
            or ("investment" in nk and "tcv" not in nk)
            or ("budget" in nk and "tcv" not in nk)
        ):
            continue
        sm = float(_to_number_series(raw[c]).sum())
        if abs(sm) > abs(best_sum):
            best_sum = sm
    return best_sum


def _parse_report_month_series(s: pd.Series) -> pd.Series:
    """Parse Excel / Sheets ``Month`` cells: serial day counts, datetimes, ``Sept``, ``December``, etc.

    Plain ``pd.to_datetime`` on numeric 45xxx treats values as ns-since-epoch → junk; we coerce sheet serials first.
    """
    raw = s.copy()
    ser = _coerce_sheet_serial_dates(raw)
    out = pd.to_datetime(ser, errors="coerce")
    mask = out.isna() & raw.notna()
    if not mask.any():
        return out
    for idx in raw.index[mask]:
        v = raw.loc[idx]
        parsed: Optional[pd.Timestamp] = None
        try:
            if not isinstance(v, bool):
                n = float(v)
                if pd.notna(n):
                    ni = int(round(n))
                    if abs(n - ni) < 0.02 or abs(n - round(n)) < 1e-6:
                        yk = _yyyymm_calendar_to_key(ni)
                        if yk:
                            parsed = pd.Timestamp(yk + "-01")
                        elif 1 <= ni <= 12 and not (20000 < n < 100000):
                            mkey = _month_only_calendar_month_to_key(ni)
                            if mkey:
                                parsed = pd.Timestamp(mkey + "-01")
                        elif 20000 < n < 100000:
                            base = pd.Timestamp("1899-12-30")
                            ts = base + pd.to_timedelta(ni, unit="D")
                            if ts.year >= 2000:
                                parsed = ts
        except (TypeError, ValueError, OverflowError):
            pass
        if parsed is None:
            val = str(v).strip()
            if not val or val.lower() in ("nan", "none", "nat"):
                out.loc[idx] = pd.NaT
                continue
            vd = val.replace(",", "").replace(" ", "")
            if vd.isdigit() and 1 <= len(vd) <= 2:
                mi = int(vd)
                if 1 <= mi <= 12:
                    mkey = _month_only_calendar_month_to_key(mi)
                    if mkey:
                        parsed = pd.Timestamp(mkey + "-01")
            if parsed is None:
                _cy = date.today().year
                for y in (_cy, _cy - 1, _cy - 2, _cy + 1):
                    t = pd.to_datetime(f"{val} 1, {y}", errors="coerce")
                    if pd.notna(t):
                        parsed = t
                        break
        out.loc[idx] = parsed if parsed is not None else pd.NaT
    return out


def _try_period_from_column_header(name: str) -> Optional[pd.Period]:
    """True month columns in wide spend matrices (header = ``Jan 2025``, ``2025-03``, etc.)."""
    s = str(name).strip()
    if not s:
        return None
    sl = s.lower()
    if sl in ("total", "sum", "grand total", "ytd") or sl.startswith("vs ") or "variance" in sl:
        return None
    try:
        return pd.Period(s, freq="M")
    except Exception:
        pass
    ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(ts):
        try:
            return ts.to_period("M")
        except Exception:
            return None
    return None


def _try_melt_wide_month_spend_df(df: pd.DataFrame) -> pd.DataFrame:
    """If the tab is a pivot matrix (geo rows × month columns), melt so ``normalize()`` can see ``Spend`` + ``Month``."""
    if df is None or df.empty or len(df.columns) < 4:
        return df
    raw = df.copy()
    cols = [str(c).strip() for c in raw.columns]
    raw.columns = cols
    period_by_col: dict[str, Optional[pd.Period]] = {c: _try_period_from_column_header(c) for c in cols}
    value_vars = [c for c in cols if period_by_col[c] is not None]
    id_vars = [c for c in cols if period_by_col[c] is None]
    if len(value_vars) < 3 or not id_vars:
        return df
    geo_ok = any(
        any(x in _norm_header_key(c) for x in ("market", "country", "region", "geo", "location", "kitchen"))
        for c in id_vars
    )
    if not geo_ok:
        return df
    try:
        long = pd.melt(
            raw,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="__hdr_month",
            value_name="Spend",
        )
    except Exception:
        return df
    long["Month"] = long["__hdr_month"].map(
        lambda h: str(period_by_col[str(h)]) if period_by_col.get(str(h)) is not None else ""
    )
    long = long.drop(columns=["__hdr_month"], errors="ignore")
    long["Spend"] = _to_number_series(long["Spend"])
    long = long.loc[long["Spend"].abs() > 1e-12].copy()
    if long.empty:
        return df
    return long


def _spend_combine_year_month_columns(df: pd.DataFrame) -> pd.DataFrame:
    """When the Spend tab has ``Year`` + numeric ``Month`` (1-12), write ``YYYY-MM-01`` into the Month column.

    ``Year`` is not part of ``normalize()`` output — it is only used here on the raw grid so ``report_month`` parses.
    """
    ycols = [
        c
        for c in df.columns
        if _norm_header_key(str(c)) in ("year", "calendar_year", "report_year")
    ]
    mcols = [c for c in df.columns if _norm_header_key(str(c)) == "month"]
    if len(ycols) != 1 or len(mcols) != 1:
        return df
    yc, mc = ycols[0], mcols[0]
    if yc == mc:
        return df
    yv = pd.to_numeric(df[yc], errors="coerce")
    mv = pd.to_numeric(df[mc], errors="coerce")
    ok = yv.notna() & mv.notna() & mv.between(1, 12) & yv.between(1990, 2100)
    if not bool(ok.any()):
        return df
    combined = df[mc].astype(object).copy()
    for ix in df.index[ok]:
        try:
            combined.loc[ix] = f"{int(yv.loc[ix])}-{int(mv.loc[ix]):02d}-01"
        except (ValueError, TypeError, OverflowError):
            pass
    df[mc] = combined
    return df


def _preprocess_excel_sheet(df: pd.DataFrame, tab_name: str) -> pd.DataFrame:
    """ME X-Ray template: forward-fill month blocks on CW Summary; drop regional subtotals."""
    df = df.copy()
    t = tab_name.strip().lower()
    if "Month" in df.columns and "cw summary" in t:
        df["Month"] = df["Month"].ffill()
    if "Market" in df.columns:
        m = df["Market"].astype(str)
        df = df[~m.str.contains("TOTAL", case=False, na=False)]
    if "spend" in t:
        df = _try_melt_wide_month_spend_df(df)
        df = _spend_combine_year_month_columns(df)
        # Do **not** ffill Month here (merged-cell collapse). Also tell ``normalize()`` not to
        # ``date.fillna(report_month)`` on spend — Sheets sometimes repeats one Month on every row; that would
        # stamp the same calendar month onto all undated rows before we can prefer real per-row dates.
        df.attrs["spend_skip_date_fill_from_month"] = True
        # ``period`` maps to ``date`` only; ``reporting_period`` / ``Time period`` etc. may not map at all.
        # Mirror the best *period* column into ``Month`` so ``report_month`` is populated.
        period_cols = sorted(
            (c for c in df.columns if "period" in _norm_header_key(str(c))),
            key=lambda c: (0 if _norm_header_key(str(c)) == "period" else 1, str(c).lower()),
        )
        month_cols = [c for c in df.columns if _norm_header_key(str(c)) == "month"]
        if period_cols:
            pcol = period_cols[0]
            if not month_cols:
                df["Month"] = df[pcol].values
            else:
                mcol = month_cols[0]
                mser = df[mcol].astype(str).str.strip()
                empty_m = mser.eq("") | mser.str.lower().isin(["nan", "none", "nat"])
                if bool(empty_m.all()):
                    df[mcol] = df[pcol].values
    if ("lead" in t and "post" not in t):
        # Convert lead rows into additive metrics so they can be combined with spend.
        if "Leads" not in df.columns:
            df["Leads"] = 1
        status = df.get("Lead Status", pd.Series(index=df.index, dtype=str)).astype(str).str.lower()
        if "Qualified" not in df.columns:
            df["Qualified"] = _to_int_series_safe(status.str.contains("qualified", na=False))
        if "Date Formatted" in df.columns and "Date" not in df.columns:
            df["Date"] = pd.to_datetime(df["Date Formatted"], errors="coerce")
        # Salesforce exports often use ``Close Date`` for CW timing — map into ``Date`` when primary dates are blank.
        if "Close Date" in df.columns:
            cd = pd.to_datetime(df["Close Date"], errors="coerce", dayfirst=True)
            if "Date" not in df.columns:
                df["Date"] = cd
            else:
                base = pd.to_datetime(df["Date"], errors="coerce")
                df["Date"] = base.where(base.notna(), cd)
    if _is_post_lead_pipeline_tab(t):
        # Stage rows become pipeline counters (post-lead funnel). Prefer Post Lead Stage column.
        stage_col = _resolve_post_lead_stage_column(df)
        raw_stage = df.get(stage_col or "Stage", pd.Series(index=df.index, dtype=str))
        stage = raw_stage.astype(str).str.lower().str.strip()
        if "Qualified" not in df.columns:
            df["Qualified"] = 1
        if "Pitching" not in df.columns:
            df["Pitching"] = _to_int_series_safe(stage.str.contains("pitch", na=False))
        if "Qualifying" not in df.columns:
            # Distinct from Qualified SQL / Disqualified — match Qualifying + Qualification (Salesforce labels).
            _qual_core = stage.str.contains("qualifying", na=False) | stage.str.contains(
                "qualification", na=False
            )
            df["Qualifying"] = _to_int_series_safe(
                _qual_core
                & ~stage.str.contains("qualified", na=False)
                & ~stage.str.contains("disqualif", na=False)
            )
        _has_is_cw_col = any(_norm_header_key(c) == "is_cw" for c in df.columns)
        if "Closed Won" not in df.columns and not _has_is_cw_col:
            df["Closed Won"] = _to_int_series_safe(raw_stage.map(_is_closed_won_stage_text))
        if "Negotiation" not in df.columns:
            df["Negotiation"] = _to_int_series_safe(stage.str.contains("negotiation", na=False))
        if "Commitment" not in df.columns:
            df["Commitment"] = _to_int_series_safe(stage.str.contains("commitment", na=False))
        if "Closed Lost" not in df.columns:
            df["Closed Lost"] = _to_int_series_safe(stage.str.contains("closed lost", na=False))
        if "Total Live" not in df.columns:
            df["Total Live"] = _to_int_series_safe(
                stage.str.contains("new|working|qualifying|pitch|negotiation|commitment", na=False)
            )
        _flc = next(
            (
                c
                for c in df.columns
                if _norm_header_key(str(c)) in ("first_lead_created_date", "1st_lead_created_date")
            ),
            None,
        )
        if _flc is not None:
            _flc_parsed = pd.to_datetime(df[_flc], errors="coerce")
            if "Date" not in df.columns:
                df["Date"] = _flc_parsed
            else:
                df["Date"] = _flc_parsed.fillna(pd.to_datetime(df["Date"], errors="coerce"))
        elif "Date" not in df.columns:
            date_col = next(
                (
                    c
                    for c in df.columns
                    if _norm_header_key(c)
                    in {"formatted_date", "created_date", "create_date", "date", "date_formatted"}
                ),
                None,
            )
            if date_col:
                df["Date"] = pd.to_datetime(df[date_col], errors="coerce")
        # Do not drop duplicate opportunity rows here — SUM(Pitching)+… must match the sheet; CW uses unique opps in code.
    if _is_raw_cw_style_tab(t):
        # RAW CW / CW deal tabs can contain repeated rows for the same opportunity; dedupe before aggregation.
        dedupe_cols = [c for c in ("Opportunity Name", "Close Date", "Kitchen Country", "Stage") if c in df.columns]
        if dedupe_cols:
            df = df.drop_duplicates(subset=dedupe_cols, keep="first")
        stage_col = next((c for c in df.columns if "stage" in _norm_header_key(c)), None)
        stage = df.get(stage_col or "Stage", pd.Series(index=df.index, dtype=str)).astype(str).str.lower().str.strip()
        _has_is_cw_raw = any(_norm_header_key(c) == "is_cw" for c in df.columns)
        if "Closed Won" not in df.columns and not _has_is_cw_raw:
            df["Closed Won"] = _to_int_series_safe(stage.str.contains("closed won", na=False))
        if "Date" not in df.columns:
            if "Close Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Close Date"], errors="coerce", dayfirst=True)
            elif "Created Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        # Source of truth: prefer column T "TCV (converted)" when present; fallback to TCV (USD), then LF*term.
        tcv_converted_col = next((c for c in df.columns if _norm_header_key(str(c)) == "tcv_converted"), None)
        if tcv_converted_col is not None:
            tcv_num = pd.to_numeric(df[tcv_converted_col], errors="coerce").fillna(0)
        elif "TCV (USD)" in df.columns:
            tcv_num = pd.to_numeric(df["TCV (USD)"], errors="coerce").fillna(0)
        else:
            tcv_num = pd.Series(0, index=df.index, dtype=float)
        _lf_col = next(
            (
                c
                for c in df.columns
                if _norm_header_key(str(c))
                in {
                    "monthly_lf_usd",
                    "monthly_license_fee",
                    "monthly_license_fee_converted",
                    "monthly_license_fee_usd",
                    "license_fee",
                }
            ),
            None,
        )
        if _lf_col is not None:
            lf_num = pd.to_numeric(df[_lf_col], errors="coerce").fillna(0)
        else:
            lf_num = pd.to_numeric(df.get("Monthly LF (USD)", 0), errors="coerce").fillna(0)
        term_num = pd.to_numeric(df.get("License Initial Term (Months)", 0), errors="coerce").fillna(0)
        df["TCV (USD)"] = tcv_num.where(tcv_num > 0, lf_num * term_num)

    # Global fallback: if any sheet has a Stage-like column, derive Closed Won (inc approved).
    # This prevents CW from dropping to zero when tab naming differs in source files.
    stage_col_any = _resolve_post_lead_stage_column(df)
    if stage_col_any is None:
        stage_col_any = next((c for c in df.columns if "stage" in _norm_header_key(c)), None)
    _has_is_cw_g = any(_norm_header_key(c) == "is_cw" for c in df.columns)
    if stage_col_any and "Closed Won" not in df.columns and not _has_is_cw_g:
        df["Closed Won"] = _to_int_series_safe(df[stage_col_any].map(_is_closed_won_stage_text))
    return df


# Headers that populate ``cw_close_date`` (ME CpCW Analysis close-date gate) beyond exact ``close_date``.
_CLOSE_DATE_SOURCE_HEADER_KEYS: frozenset[str] = frozenset(
    {
        "close_date",
        "opportunity_close_date",
        "deal_close_date",
        "contract_close_date",
        "sales_close_date",
        "actual_close_date",
        "closed_date",
        "close_date_time",
    }
)


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
    _spend_skip_date_fill = bool(df.attrs.get("spend_skip_date_fill_from_month", False))

    # Map each sheet column to at most one canonical field (first wins for dims; sum for dup metrics)
    field_to_sources: dict[str, list[str]] = {}
    for col in df.columns:
        nk = _norm_header_key(col)
        field = _NORM_TO_FIELD.get(nk)
        if field:
            field_to_sources.setdefault(field, []).append(col)

    if "country" in field_to_sources:
        # Prefer **Market** over ``Kitchen Country`` / other geo keys so Post-Lead CW rolls up to sheet Market.
        _srcs = field_to_sources["country"]
        field_to_sources["country"] = sorted(
            _srcs,
            key=lambda c: (
                0 if _norm_header_key(str(c)) == "market" else 1 if _norm_header_key(str(c)) == "kitchen_country" else 2,
                str(c).lower(),
            ),
        )

    if "date" in field_to_sources:
        # Post-Lead: attribute CW / funnel to the **first touch** month (sheet ``First Lead Created Date``).
        _ds = field_to_sources["date"]
        field_to_sources["date"] = sorted(
            _ds,
            key=lambda c: (
                0
                if _norm_header_key(str(c))
                in ("first_lead_created_date", "1st_lead_created_date")
                else 1,
                str(c).lower(),
            ),
        )

    if "channel" in field_to_sources:
        _ch_src = field_to_sources["channel"]
        field_to_sources["channel"] = sorted(
            _ch_src,
            key=lambda c: (
                0 if _norm_header_key(str(c)) == "unified_channel" else 1,
                str(c).lower(),
            ),
        )
    if "tcv" in field_to_sources:
        _tcv_src = field_to_sources["tcv"]
        field_to_sources["tcv"] = sorted(
            _tcv_src,
            key=lambda c: (
                0 if _norm_header_key(str(c)) == "tcv_converted" else 1,
                str(c).lower(),
            ),
        )

    if "first_month_lf" in field_to_sources:
        field_to_sources["first_month_lf"] = sorted(
            field_to_sources["first_month_lf"],
            key=_lf_column_sort_key,
        )

    out = pd.DataFrame(index=df.index)

    def _col_as_series(frame: pd.DataFrame, col_name: str) -> pd.Series:
        """Return one column as Series even when duplicate headers exist."""
        got = frame[col_name]
        if isinstance(got, pd.DataFrame):
            if got.shape[1] == 0:
                return pd.Series(index=frame.index, dtype=object)
            return got.iloc[:, 0]
        return got
    # Row-wise max dedupes duplicate TCV columns; do **not** use it for LF — local vs converted must pick converted, not max.
    _DEDUPE_NUMERIC_MAX_FIELDS = frozenset({"tcv"})
    for field, srcs in field_to_sources.items():
        if field == "closed_won":
            is_cw_cols = [c for c in srcs if _norm_header_key(str(c)) == "is_cw"]
            if is_cw_cols:
                if len(is_cw_cols) == 1:
                    out[field] = _col_as_series(df, is_cw_cols[0])
                else:
                    acc = _closed_won_to_numeric_series(_col_as_series(df, is_cw_cols[0]))
                    for c in is_cw_cols[1:]:
                        acc = acc.combine(_closed_won_to_numeric_series(_col_as_series(df, c)), max)
                    out[field] = acc
                continue
        if field == "first_month_lf" and len(srcs) > 1:
            out[field] = _to_number_series(_col_as_series(df, srcs[0]))
            continue
        if len(srcs) == 1:
            out[field] = _col_as_series(df, srcs[0])
        elif field in _NUM_FIELDS:
            acc = _to_number_series(_col_as_series(df, srcs[0]))
            for c in srcs[1:]:
                nxt = _to_number_series(_col_as_series(df, c))
                if field == "closed_won":
                    acc = acc.combine(nxt, max)
                elif field in _DEDUPE_NUMERIC_MAX_FIELDS:
                    acc = acc.combine(nxt, max)
                else:
                    acc = acc + nxt
            out[field] = acc
        else:
            out[field] = _col_as_series(df, srcs[0])

    # Actual Close Date — ME **CpCW Analysis** (B2/B3) filters on this; ``date`` may prefer First Lead Created on post-lead tabs.
    _close_src: Optional[str] = None
    for col in df.columns:
        if _norm_header_key(str(col)) == "close_date":
            _close_src = col
            break
    if _close_src is None:
        for col in df.columns:
            nk = _norm_header_key(str(col))
            if nk in _CLOSE_DATE_SOURCE_HEADER_KEYS:
                _close_src = col
                break
    if _close_src is None:
        for col in df.columns:
            nk = _norm_header_key(str(col))
            if "close" in nk and "date" in nk and "created" not in nk and "start" not in nk:
                _close_src = col
                break
    if _close_src is not None:
        _cds = pd.to_datetime(_col_as_series(df, _close_src), errors="coerce", dayfirst=True)
        out["cw_close_date"] = _scrub_pre_2000_dates(_coerce_sheet_serial_dates(_cds))

    # Hard fallback for spend: if explicit mapping missed it, infer from any spend/cost/amount-like header.
    if "cost" not in out.columns:
        spend_like_cols = []
        for c in df.columns:
            nk = _norm_header_key(c)
            if nk in {"cost_tcv", "cost_tcv_pct"}:
                continue
            if (
                "spend" in nk
                or "cost" in nk
                or "amount" in nk
                or nk in ("investment", "budget", "media_spend", "fee", "fees", "paid")
                or ("investment" in nk and "tcv" not in nk)
                or ("budget" in nk and "tcv" not in nk)
            ):
                spend_like_cols.append(c)
        if spend_like_cols:
            inferred = _to_number_series(df[spend_like_cols[0]])
            for c in spend_like_cols[1:]:
                cand = _to_number_series(df[c])
                # Prefer column with larger non-zero signal.
                if float(cand.abs().sum()) > float(inferred.abs().sum()):
                    inferred = cand
            out["cost"] = inferred

    _cost_pre_num = out["cost"] if "cost" in out.columns else None
    if _cost_pre_num is None or float(_to_number_series(_cost_pre_num).sum()) < 1e-9:
        bc = _best_spend_column_raw(df)
        if bc is not None and float(_to_number_series(df[bc]).abs().sum()) > 1e-9:
            out["cost"] = _to_number_series(df[bc])

    # Hard fallback for first-month LF on CW tabs: Salesforce headers vary ("Monthly License Fee (converted)" etc.).
    if "first_month_lf" not in out.columns or float(_to_number_series(out["first_month_lf"]).sum()) < 1e-9:
        lf_like_cols: list[str] = []
        for c in df.columns:
            nk = _norm_header_key(str(c))
            if nk in {
                "first_month_lf",
                "monthly_lf_usd",
                "monthly_license_fee",
                "monthly_license_fee_converted",
                "monthly_license_fee_usd",
                "license_fee",
            }:
                lf_like_cols.append(c)
            elif ("license" in nk and "fee" in nk) and all(x not in nk for x in ("tcv", "cost", "spend", "tax")):
                lf_like_cols.append(c)
        if lf_like_cols:
            lf_like_cols = sorted(lf_like_cols, key=_lf_column_sort_key)
            out["first_month_lf"] = _to_number_series(df[lf_like_cols[0]])

    if "date" in out.columns:
        s_dt = out["date"].copy()
        dn = pd.to_numeric(s_dt, errors="coerce")
        int_ok = dn.notna() & (dn - dn.round()).abs() < 0.02
        if bool(int_ok.any()):
            ni = (
                pd.to_numeric(dn.loc[int_ok].round(), errors="coerce")
                .replace([float("inf"), float("-inf")], pd.NA)
                .dropna()
                .astype("int64")
            )
            yk = ni.map(_yyyymm_calendar_to_key)
            for ix in yk.index[yk.ne("")]:
                s_dt.loc[ix] = pd.Timestamp(str(yk.loc[ix]) + "-01")
        out["date"] = _scrub_pre_2000_dates(_coerce_sheet_serial_dates(s_dt))
    else:
        out["date"] = pd.NaT

    if "report_month" in out.columns:
        rm = _parse_report_month_series(out["report_month"])
        rm = rm.fillna(_coerce_sheet_serial_dates(out["report_month"]))
        # Never ``ffill`` here: one non-blank month + many empty cells (merged headers in Sheets/Excel)
        # would stamp the same month on every row and collapse the whole spend tab into one period.
        rm = _scrub_pre_2000_dates(rm)
        if not _spend_skip_date_fill:
            out["date"] = out["date"].fillna(rm)

    _still_ancient = out["date"].notna() & (out["date"] < pd.Timestamp("2000-01-01"))
    out.loc[_still_ancient, "date"] = pd.NaT

    def _metric_sum_zero(col: str) -> bool:
        if col not in out.columns:
            return True
        return float(_to_number_series(out[col]).abs().sum()) < 1e-9

    if _metric_sum_zero("clicks"):
        ic = _first_best_metric_column_by_keyword(
            df,
            ("click",),
            ("ctr", "cpc", "cost", "rate", "conv", "position", "share", "quality", "rank"),
        )
        if ic is None:
            ic = _first_best_metric_column_by_keyword(
                df,
                ("engagement",),
                ("rate", "cost", "ctr", "cpc", "conv"),
            )
        if ic is not None:
            out["clicks"] = _to_number_series(df[ic])
        # Wide campaign exports: many click columns (one per campaign) need summing, not one-column pick.
        if _metric_sum_zero("clicks"):
            out["clicks"] = _sum_metric_columns_by_keywords(
                df,
                include_keywords=("click",),
                exclude_keywords=("ctr", "cpc", "cost", "rate", "conv", "position", "share", "quality", "rank"),
            )
    else:
        ic = _first_best_metric_column_by_keyword(
            df,
            ("click",),
            ("ctr", "cpc", "cost", "rate", "conv", "position", "share", "quality", "rank"),
        )
        if ic is not None:
            cand = _to_number_series(df[ic])
            if float(cand.abs().sum()) > float(pd.to_numeric(out.get("clicks", 0), errors="coerce").fillna(0).abs().sum()) + 1e-6:
                out["clicks"] = cand
    if _metric_sum_zero("impressions"):
        ii = _first_best_metric_column_by_keyword(
            df,
            ("impr",),
            ("ctr", "cpc", "cost", "rate", "share", "position", "frequency", "quality", "rank"),
        )
        if ii is None:
            ii = _first_best_metric_column_by_keyword(
                df,
                ("impression",),
                ("ctr", "cpc", "cost", "rate", "share", "position", "frequency", "quality", "rank", "share_of"),
            )
        if ii is not None:
            out["impressions"] = _to_number_series(df[ii])
        # Wide campaign exports: many impression columns (one per campaign) need summing.
        if _metric_sum_zero("impressions"):
            out["impressions"] = _sum_metric_columns_by_keywords(
                df,
                include_keywords=("impr", "impression"),
                exclude_keywords=("ctr", "cpc", "cost", "rate", "share", "position", "frequency", "quality", "rank"),
            )
    else:
        ii = _first_best_metric_column_by_keyword(
            df,
            ("impr",),
            ("ctr", "cpc", "cost", "rate", "share", "position", "frequency", "quality", "rank"),
        )
        if ii is None:
            ii = _first_best_metric_column_by_keyword(
                df,
                ("impression",),
                ("ctr", "cpc", "cost", "rate", "share", "position", "frequency", "quality", "rank", "share_of"),
            )
        if ii is not None:
            cand_i = _to_number_series(df[ii])
            if float(cand_i.abs().sum()) > float(pd.to_numeric(out.get("impressions", 0), errors="coerce").fillna(0).abs().sum()) + 1e-6:
                out["impressions"] = cand_i
    # Always compare against "sum all matching columns" for wide campaign exports.
    _sum_clicks_all = _sum_metric_columns_by_keywords(
        df,
        include_keywords=("click", "swipe"),
        exclude_keywords=("ctr", "cpc", "cost", "rate", "conv", "position", "share", "quality", "rank"),
    )
    if float(_sum_clicks_all.abs().sum()) > float(pd.to_numeric(out.get("clicks", 0), errors="coerce").fillna(0).abs().sum()) + 1e-6:
        out["clicks"] = _sum_clicks_all

    _sum_impr_all = _sum_metric_columns_by_keywords(
        df,
        include_keywords=("impr", "impression"),
        exclude_keywords=("ctr", "cpc", "cost", "rate", "share", "position", "frequency", "quality", "rank"),
    )
    if float(_sum_impr_all.abs().sum()) > float(pd.to_numeric(out.get("impressions", 0), errors="coerce").fillna(0).abs().sum()) + 1e-6:
        out["impressions"] = _sum_impr_all
    # Supermetrics variants may expose impressions + CTR only; recover clicks when click fields are absent.
    if _metric_sum_zero("clicks") and not _metric_sum_zero("impressions"):
        ctr_col = _best_ctr_column_raw(df)
        if ctr_col is not None:
            ctr_s = _to_number_series(df[ctr_col]).fillna(0.0)
            q95 = float(ctr_s.abs().quantile(0.95)) if len(ctr_s) else 0.0
            ctr_ratio = ctr_s if q95 <= 1.0 else (ctr_s / 100.0)
            impr_s = pd.to_numeric(out.get("impressions", 0), errors="coerce").fillna(0.0)
            out["clicks"] = (impr_s * ctr_ratio).clip(lower=0)

    for c in _NUM_FIELDS:
        if c in out.columns:
            if c == "closed_won":
                out[c] = _closed_won_to_numeric_series(out[c])
            else:
                out[c] = _to_number_series(out[c])
        else:
            out[c] = 0

    for c in ["country", "country_code", "channel", "platform", "utm_source", "utm_source_l", "utm_source_o"]:
        if c not in out.columns:
            out[c] = "Unknown"
        out[c] = out[c].astype(str).replace("nan", "Unknown")
    out["country"] = out["country"].map(_canonical_country_label)
    out["platform"] = out["platform"].map(_canonical_platform_label)

    for dim in (
        "opportunity_id",
        "opportunity_id_18",
        "opportunity_name",
        "record_id",
        "case_id",
        "opp_id",
        "opp_name",
        "deal_id",
        "deal_name",
    ):
        if dim not in out.columns:
            out[dim] = ""
        else:
            out[dim] = out[dim].astype(str).replace("nan", "").replace("None", "")

    out["month"] = ""
    _dok = out["date"].notna()
    if bool(_dok.any()):
        out.loc[_dok, "month"] = out.loc[_dok, "date"].map(lambda t: _month_norm_key(t) if pd.notna(t) else "")
    _bad_m = out["month"].astype(str).str.strip().str.lower().isin(["", "nan", "nat", "none"])
    if bool(_bad_m.any()) and "report_month" in out.columns:
        rm_fix = _parse_report_month_series(out["report_month"])
        rm_fix = rm_fix.fillna(_coerce_sheet_serial_dates(out["report_month"]))
        rm_fix = _scrub_pre_2000_dates(rm_fix)
        ok_rm = rm_fix.notna() & (rm_fix >= pd.Timestamp("2000-01-01"))
        hit = _bad_m & ok_rm
        if bool(hit.any()):
            out.loc[hit, "month"] = rm_fix.loc[hit].map(lambda t: _month_norm_key(t) if pd.notna(t) else "")
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
    # Keep undated rows so stage-based metrics (e.g., post-qualification CW) don't get zeroed.
    mask = ((s >= start_ts) & (s <= end_ts)) | s.isna()
    return df.loc[mask].copy()


def _filter_spend_for_dashboard(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Prefer ``month``-period filtering for spend (sheet dates often mis-parse); fall back to calendar ``date``."""
    if df.empty:
        return df
    allow_m = set(_month_norm_keys_in_reporting_window(start, end))

    def _month_in_range(m: Any) -> bool:
        if m is None or (isinstance(m, float) and pd.isna(m)):
            return True
        k = _month_norm_key(m)
        if not k:
            return True
        return k in allow_m

    sub_m = pd.DataFrame()
    if "month" in df.columns:
        sub_m = df.loc[df["month"].map(_month_in_range)].copy()
        if _normalized_spend_cost_sum(sub_m) > 0.0:
            return sub_m

    sub_d = _filter_by_date_range(df, start, end)
    if _normalized_spend_cost_sum(sub_d) > 0.0:
        return sub_d

    if not sub_m.empty:
        return sub_m
    return sub_d if not sub_d.empty else df


def _apply_sep2025_month_date_floor(df: pd.DataFrame) -> pd.DataFrame:
    """Jan 2025 onward by ``month`` and/or ``date`` (undated rows kept)."""
    if df.empty:
        return df
    out = df.copy()
    if "month" in out.columns:
        def _m_ok(m: Any) -> bool:
            try:
                k = _month_norm_key(m)
                if not k:
                    return True
                return bool(pd.Period(str(k), freq="M") >= _MIN_FETCHED_SHEET_PERIOD)
            except Exception:
                return True

        out = out.loc[out["month"].map(_m_ok).fillna(True)].copy()
    if "date" in out.columns:
        d = pd.to_datetime(out["date"], errors="coerce")
        out = out.loc[d.isna() | (d >= _MIN_FETCHED_SHEET_TS)].copy()
    return out


def _apply_sep2025_all_sheets_except_leads_postlead(df: pd.DataFrame) -> pd.DataFrame:
    """After workbook merge: **Jan 2025+** on every tab’s rows except **Leads** and **Post lead / Post qual** sheets."""
    if df.empty or "source_tab" not in df.columns:
        return df
    out = df.copy()
    st = out["source_tab"].astype(str)
    excl = st.map(_sheet_title_matches_leads_or_post_lead).fillna(False)
    ix_ex = out.index[excl]
    ix_ke = out.index[~excl]
    if len(ix_ke) == 0:
        return out.loc[ix_ex].reset_index(drop=True)
    floored = _apply_sep2025_month_date_floor(out.loc[ix_ke].copy())
    return pd.concat([out.loc[ix_ex].copy(), floored], ignore_index=True)


def _restrict_paid_media_workbook_to_date_range(
    df: pd.DataFrame,
    ads_workbook_id: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Keep only rows from the Supermetrics workbook whose ``month`` or ``date`` falls in the app reporting window.

    CRM tabs elsewhere use ``_filter_by_date_range`` which keeps undated rows; for paid-media exports we
    strictly scope by period so 2025–2026 does not mix in other years from wide connector tabs.
    """
    if df.empty or "spreadsheet_id" not in df.columns:
        return df
    aid = str(_extract_sheet_id(str(ads_workbook_id)))
    is_sm = df["spreadsheet_id"].astype(str) == aid
    if not bool(is_sm.any()):
        return df
    start_p = pd.Period(start_date, freq="M")
    end_p = pd.Period(end_date, freq="M")
    sub = df.loc[is_sm].copy()
    in_window = pd.Series(False, index=sub.index)
    if "month" in sub.columns:

        def _month_ok(m: Any) -> bool:
            mk = _month_norm_key(m)
            if not mk:
                return False
            try:
                p = pd.Period(str(mk), freq="M")
                return bool(start_p <= p <= end_p)
            except Exception:
                return False

        in_window = sub["month"].map(_month_ok)
    if "date" in sub.columns:
        ts = pd.to_datetime(sub["date"], errors="coerce")
        try:
            if getattr(ts.dtype, "tz", None) is not None:
                ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
        except Exception:
            pass
        start_ts = pd.Timestamp(datetime.combine(start_date, time.min))
        end_ts = pd.Timestamp(datetime.combine(end_date, time.max))
        in_window = in_window | (ts.notna() & (ts >= start_ts) & (ts <= end_ts))
    kept = sub.loc[in_window].copy()
    rest = df.loc[~is_sm].copy()
    if kept.empty:
        return rest
    return pd.concat([rest, kept], ignore_index=True)


def _impute_master_df_cost_from_spend_pool(
    master_df: pd.DataFrame,
    spend_pool: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """When outer-merge leaves ``cost`` at zero but the spend tab has dollars, allocate by month using CW weights."""
    if master_df.empty or spend_pool.empty or "cost" not in master_df.columns:
        return master_df
    cur = float(pd.to_numeric(master_df["cost"], errors="coerce").fillna(0).sum())
    if cur > 1e-6:
        return master_df
    if "cost" not in spend_pool.columns or "month" not in spend_pool.columns:
        return master_df

    sp = _normalize_master_merge_frame(spend_pool.copy())
    sp["month"] = sp["month"].map(_month_norm_key)
    start_p = pd.Period(start_date, freq="M")
    end_p = pd.Period(end_date, freq="M")

    def _m_dashboard(m: Any) -> bool:
        if not m or str(m).strip().lower() in ("", "nat", "nan"):
            return True
        try:
            p = pd.Period(str(m), freq="M")
            return bool(start_p <= p <= end_p)
        except Exception:
            return True

    sp_f = sp.loc[sp["month"].map(_m_dashboard)].copy()
    if sp_f.empty or _normalized_spend_cost_sum(sp_f) < 1e-9:
        sp_f = sp
    cost_num = pd.to_numeric(sp_f["cost"], errors="coerce").fillna(0)
    by_m = cost_num.groupby(sp_f["month"], dropna=False).sum()
    month_totals: dict[str, float] = {}
    unassigned_spend = 0.0
    for k, v in by_m.items():
        fv = float(v)
        if fv < 1e-9:
            continue
        nk = _month_norm_key(k)
        if not nk:
            unassigned_spend += fv
        else:
            month_totals[nk] = month_totals.get(nk, 0) + fv

    out = master_df.copy()
    out["month"] = out["month"].map(_month_norm_key)
    if unassigned_spend > 1e-9:
        mkeys = sorted(
            {
                str(m).strip()
                for m in out["month"].dropna().unique()
                if str(m).strip() and _dashboard_month_plausible(str(m))
            }
        )
        if mkeys:
            share = unassigned_spend / len(mkeys)
            for mk in mkeys:
                month_totals[mk] = month_totals.get(mk, 0) + share

    if not month_totals:
        return master_df

    for mk, total in month_totals.items():
        if total < 1e-9:
            continue
        m_ix = out.index[out["month"].astype(str) == mk]
        if len(m_ix) == 0:
            continue
        sub = out.loc[m_ix]
        cw = pd.to_numeric(sub["closed_won"], errors="coerce").fillna(0).astype(float)
        if (cw > 1e-9).any():
            use_ix = sub.index[cw > 1e-9]
        else:
            use_ix = m_ix
        w = pd.to_numeric(out.loc[use_ix, "closed_won"], errors="coerce").fillna(0).astype(float)
        if float(w.sum()) < 1e-9:
            w = pd.Series(1.0, index=use_ix, dtype=float)
        share = w / float(w.sum())
        out.loc[use_ix, "cost"] = (share * total).values
    return out


def _coalesce_master_cost_from_spend_pivot(master_df: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    """Left-fill ``cost`` from a fresh month×country pivot of the spend pool when merge keys missed."""
    if master_df.empty or pool.empty or "cost" not in master_df.columns:
        return master_df
    piv = _spend_sheet_pivot_by_month_country(pool)
    if piv.empty or "cost" not in piv.columns:
        return master_df
    out = master_df.copy()
    out["month"] = out["month"].map(_month_norm_key)
    out["country"] = out["country"].map(_country_join_key)
    part = piv.copy()
    part["month"] = part["month"].map(_month_norm_key)
    part["country"] = part["country"].map(_country_join_key)
    agg = part.groupby(["month", "country"], as_index=False)["cost"].sum()
    m2 = out.merge(agg.rename(columns={"cost": "_spend_tab"}), on=["month", "country"], how="left")
    c0 = pd.to_numeric(m2["cost"], errors="coerce").fillna(0)
    cs = pd.to_numeric(m2["_spend_tab"], errors="coerce").fillna(0)
    m2["cost"] = c0.where(c0 >= 1e-9, cs)
    return m2.drop(columns=["_spend_tab"], errors="ignore")


def _allocate_spend_pool_by_country_and_cw(master_df: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    """If merged ``cost`` is still zero, allocate pool totals by CRM country (× CW within country)."""
    if master_df.empty or pool.empty or "cost" not in master_df.columns:
        return master_df
    if float(pd.to_numeric(master_df["cost"], errors="coerce").fillna(0).sum()) > 1e-6:
        return master_df
    gross = float(pd.to_numeric(pool["cost"], errors="coerce").fillna(0).sum())
    if gross < 1e-9:
        return master_df
    pn = _normalize_master_merge_frame(pool.copy())
    if "country" not in pn.columns:
        return master_df
    by_c = pd.to_numeric(pn["cost"], errors="coerce").fillna(0).groupby(pn["country"].map(_country_join_key)).sum()
    out = master_df.copy()
    out["month"] = out["month"].map(_month_norm_key)
    out["country"] = out["country"].map(_country_join_key)

    nonu = {
        str(k): float(v)
        for k, v in by_c.items()
        if float(v) > 1e-9 and str(k).strip().lower() not in ("", "unknown", "nan")
    }
    if not nonu:
        ix = out.index
        cw = pd.to_numeric(out["closed_won"], errors="coerce").fillna(0).astype(float)
        if float(cw.sum()) < 1e-9:
            add = gross / max(len(ix), 1)
            out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0) + add
        else:
            out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0) + gross * (cw / float(cw.sum()))
        return out

    matched_total = 0.0
    for ckey, T in nonu.items():
        ix = out.index[out["country"] == ckey]
        if len(ix) == 0:
            continue
        matched_total += T
        cw = pd.to_numeric(out.loc[ix, "closed_won"], errors="coerce").fillna(0).astype(float)
        if float(cw.sum()) < 1e-9:
            add = pd.Series(T / max(len(ix), 1), index=ix, dtype=float)
        else:
            add = T * (cw / float(cw.sum()))
        base = pd.to_numeric(out.loc[ix, "cost"], errors="coerce").fillna(0)
        out.loc[ix, "cost"] = base + add.reindex(ix).fillna(0)

    # Pool countries that do not appear on the master grid (e.g. Egypt, UK) used to drop their spend entirely.
    residual = gross - matched_total
    if residual > 1e-6:
        ix = out.index
        cw = pd.to_numeric(out["closed_won"], errors="coerce").fillna(0).astype(float)
        if float(cw.sum()) < 1e-9:
            add_r = residual / max(len(ix), 1)
            out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0) + add_r
        else:
            out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0) + residual * (cw / float(cw.sum()))
    return out


def _tab_title_looks_like_spend_worksheet(title: str) -> bool:
    tl = str(title).strip().lower()
    return bool(
        "spend" in tl
        or "paid media" in tl
        or "media spend" in tl
        or tl == "ppc"
        or "paid search" in tl
        or "marketing investment" in tl
    )


def _tab_title_looks_like_ads_data_sheet(title: str) -> bool:
    """Per-platform export tabs, e.g. ``Google Ads Data``, ``Meta Ads Data``, ``Snapchat Ads Data``."""
    tl = str(title).strip().lower()
    platform_core = r"(google|meta|snapchat|linked\s*in|linkedin)"
    return bool(
        re.search(platform_core + r"\s*ads?\s*(data)?", tl)
        or re.search(platform_core + r"\b", tl)
        or tl.endswith(" ads data")
    )


def _mpo_tab_title_to_platform_label(title: str) -> str:
    """Display label from tab title: ``Google Ads Data`` → ``Google Ads``."""
    t = str(title).strip()
    if t.lower().endswith(" data"):
        return t[: -len(" data")].strip()
    return t


def _tab_title_is_spend_rollup_tab(title: str) -> bool:
    """ME X-Ray / connector **aggregate** spend tabs — not per-network ``* Ads Data`` exports."""
    if _tab_title_looks_like_ads_data_sheet(title):
        return False
    tl = str(title).strip().lower()
    if re.match(r"^gid:\d+_spend$", tl):
        return True
    if tl in ("spend", "raw spend", "sum spend", "media spend", "rawspend"):
        return True
    if re.fullmatch(r"raw\s*spend", tl):
        return True
    return _tab_title_looks_like_spend_worksheet(title)


def _mpo_platform_label_from_source_tab(title: str) -> str:
    """Stable platform / utm label from ``source_tab`` (overrides misleading sheet ``Platform`` cells)."""
    t = str(title).strip()
    tl = t.lower()
    if re.search(r"\bgoogle\b", tl):
        return "Google Ads"
    if re.search(r"\bmeta\b|facebook|instagram", tl):
        return "Meta Ads"
    if re.search(r"\bsnap(?:chat)?\b", tl):
        return "Snapchat Ads"
    if re.search(r"linked\s*in|linkedin", tl):
        return "LinkedIn Ads"
    if _tab_title_looks_like_ads_data_sheet(t):
        return _mpo_tab_title_to_platform_label(t)
    if re.match(r"^gid:\d+_spend$", tl):
        return "Spend (rollup)"
    if _tab_title_is_spend_rollup_tab(t):
        return "Spend (rollup)"
    return _mpo_tab_title_to_platform_label(t)


def _best_spend_pool_from_df_loaded(
    df_loaded: pd.DataFrame,
    primary_sheet_id: Optional[str] = None,
) -> pd.DataFrame:
    """Use spend rows already in the combined workbook load (often non-empty when gid=0 reload is empty).

    When ``primary_sheet_id`` is set, only rows tagged with that workbook are considered (**ME X-Ray spend**).
    """
    if primary_sheet_id is not None and "spreadsheet_id" in df_loaded.columns:
        df_loaded = _rows_for_workbook_id(df_loaded, primary_sheet_id)
        if df_loaded.empty:
            return pd.DataFrame()
    if df_loaded.empty or "cost" not in df_loaded.columns:
        return pd.DataFrame()
    best = pd.DataFrame()
    best_sum = -1.0

    def _take(sub: pd.DataFrame) -> None:
        nonlocal best, best_sum
        if sub is None or sub.empty:
            return
        sm = _normalized_spend_cost_sum(sub)
        if sm > best_sum:
            best_sum = sm
            best = sub.copy()

    if "source_tab" in df_loaded.columns:
        s_tab = df_loaded["source_tab"].astype(str)
        sl = s_tab.str.lower()
        mask = sl.str.contains(
            r"spend|paid\s*media|media\s*spend|ppc|paid\s*search|marketing\s*investment",
            na=False,
            regex=True,
        ) | s_tab.str.match(r"^gid:\d+_spend$", na=False)
        _take(df_loaded.loc[mask])
    if "worksheet_gid" in df_loaded.columns:
        wg = pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce")
        _take(df_loaded.loc[wg == 0])
        sg = _optional_spend_gid_from_secrets()
        if sg is not None:
            _take(df_loaded.loc[wg == int(sg)])
    out = best if best_sum > 1e-9 else pd.DataFrame()
    if not out.empty:
        out = _canonicalize_spend_month_column(out)
    return out


@st.cache_data(ttl=300)
def _scan_workbook_for_best_spend_frame(sheet_id: str, _secret_fp: str) -> pd.DataFrame:
    """Last resort: reload each spend-like tab by gid with full preprocess (cached)."""
    try:
        meta = list_worksheet_meta(sheet_id, _secret_fp)
    except Exception:
        return pd.DataFrame()
    best = pd.DataFrame()
    best_sum = -1.0
    for title, ws_gid in meta:
        if not _tab_title_looks_like_spend_worksheet(title):
            continue
        try:
            cand = load_worksheet_by_gid_preprocessed(sheet_id, int(ws_gid), _secret_fp)
        except Exception:
            continue
        if cand.empty:
            continue
        sm = _normalized_spend_cost_sum(cand)
        if sm > best_sum:
            best_sum = sm
            best = cand
    return best if best_sum > 1e-9 else pd.DataFrame()


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


def _load_one_tab_row_for_combined_workbook(
    sheet_id: str,
    secret_creds: dict,
    title: str,
    ws_gid: int,
) -> tuple[Optional[pd.DataFrame], tuple[str, int]]:
    """Load + normalize one tab for ``load_all_worksheets_combined``. No Streamlit calls — safe for thread pools."""
    prefer_grid = int(ws_gid) in DEFAULT_PAID_MEDIA_PLATFORM_GIDS or _tab_title_looks_like_ads_data_sheet(
        title
    )
    raw = pd.DataFrame()
    if prefer_grid:
        raw = _dataframe_from_grid_best_supermetrics_header(sheet_id, secret_creds, int(ws_gid))
    try:
        if raw.empty or len(raw.columns) < 4:
            raw = _read_sheet_auth(
                sheet_id,
                secret_creds,
                worksheet_name=None,
                worksheet_gid=int(ws_gid),
            )
            if raw.empty or len(raw.columns) == 0:
                raw = _read_sheet_auth_loose(
                    sheet_id,
                    secret_creds,
                    worksheet_gid=int(ws_gid),
                )
    except Exception:
        try:
            raw = _read_sheet_auth_loose(
                sheet_id,
                secret_creds,
                worksheet_gid=int(ws_gid),
            )
        except Exception:
            return None, (title, -1)
    raw = _promote_wide_metric_header_row_if_needed(raw)
    raw = _coerce_two_row_sheet_headers(raw)
    raw = _preprocess_excel_sheet(raw, title)
    df = _normalize(raw)
    if not df.empty and int(ws_gid) in DEFAULT_PAID_MEDIA_PLATFORM_GIDS:
        df = df.copy()
        df["cost"] = _sum_cost_columns_raw(raw, len(df)).values
        if "country" not in df.columns:
            df["country"] = "Unknown"
        ck = df["country"].astype(str).map(_country_join_key).fillna("unknown").astype(str).str.strip().str.lower()
        bad_country = ck.isin({"", "unknown", "nan", "none", "<na>"})
        if bool(bad_country.any()):
            inferred = _infer_country_from_cost_headers_raw(raw, len(df))
            df.loc[bad_country, "country"] = inferred.loc[bad_country].values
        df = _apply_equal_split_all_market_engagement(df)
        df = _canonicalize_spend_month_column(df)
    if df.empty:
        return None, (title, 0)
    df = df.copy()
    df["source_tab"] = (title.strip() if title.strip() else "Sheet")
    df["worksheet_gid"] = int(ws_gid)
    return df, (title, len(df))


@st.cache_data(ttl=300)
def load_all_worksheets_combined(
    sheet_id: str,
    _secret_fp: str,
    *,
    _ingest_version: str = DASHBOARD_BUILD,
) -> pd.DataFrame:
    """Read every worksheet in the spreadsheet (backend) and stack rows with `source_tab` set to the tab title."""
    _ = _ingest_version  # cache key only — bump ``DASHBOARD_BUILD`` to invalidate after ingest fixes
    meta = list_worksheet_meta(sheet_id, _secret_fp)
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        raise RuntimeError(
            "No service account in Streamlit Secrets. Add a `[gsheet_service_account]` block "
            "(or `GCP_SERVICE_ACCOUNT`) in this app’s Secrets, then reboot."
        )
    frames: list[pd.DataFrame] = []
    tab_stats: list[tuple[str, int]] = []
    use_parallel = len(meta) >= 3
    max_workers = min(8, max(1, len(meta)))
    if use_parallel and max_workers > 1:

        def _job(tpl: tuple[str, int]) -> tuple[Optional[pd.DataFrame], tuple[str, int]]:
            t, g = tpl
            return _load_one_tab_row_for_combined_workbook(sheet_id, secret_creds, t, int(g))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            for df, stat in pool.map(_job, meta):
                tab_stats.append(stat)
                if df is not None:
                    frames.append(df)
    else:
        for title, ws_gid in meta:
            df, stat = _load_one_tab_row_for_combined_workbook(sheet_id, secret_creds, title, int(ws_gid))
            tab_stats.append(stat)
            if df is not None:
                frames.append(df)
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
    *,
    _ingest_version: str = DASHBOARD_BUILD,
) -> pd.DataFrame:
    """Reads one worksheet by Google’s numeric worksheet id (gid in URL). Cached per tab."""
    _ = _ingest_version
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        raise RuntimeError(
            "No service account in Streamlit Secrets. Add a `[gsheet_service_account]` block "
            "(or `GCP_SERVICE_ACCOUNT`) in this app’s Secrets, then reboot."
        )
    raw = pd.DataFrame()
    if int(worksheet_gid) in DEFAULT_PAID_MEDIA_PLATFORM_GIDS:
        raw = _dataframe_from_grid_best_supermetrics_header(sheet_id, secret_creds, int(worksheet_gid))
    if raw.empty or len(raw.columns) < 4:
        raw = _read_sheet_auth(
            sheet_id,
            secret_creds,
            worksheet_name=None,
            worksheet_gid=int(worksheet_gid),
        )
    raw = _promote_wide_metric_header_row_if_needed(raw)
    raw = _coerce_two_row_sheet_headers(raw)
    out = _normalize(raw)
    if not out.empty and int(worksheet_gid) in DEFAULT_PAID_MEDIA_PLATFORM_GIDS:
        out = out.copy()
        out["cost"] = _sum_cost_columns_raw(raw, len(out)).values
        if "country" not in out.columns:
            out["country"] = "Unknown"
        ck = out["country"].astype(str).map(_country_join_key).fillna("unknown").astype(str).str.strip().str.lower()
        bad_country = ck.isin({"", "unknown", "nan", "none", "<na>"})
        if bool(bad_country.any()):
            inferred = _infer_country_from_cost_headers_raw(raw, len(out))
            out.loc[bad_country, "country"] = inferred.loc[bad_country].values
        out = _apply_equal_split_all_market_engagement(out)
        out = _canonicalize_spend_month_column(out)
    return out


def _tab_title_for_worksheet_gid(sheet_id: str, worksheet_gid: int, _secret_fp: str) -> str:
    try:
        for title, gid in list_worksheet_meta(sheet_id, _secret_fp):
            if int(gid) == int(worksheet_gid):
                return (title or "").strip() or "sheet"
    except Exception:
        pass
    return "post_qual"


@st.cache_data(ttl=300)
def load_worksheet_by_gid_preprocessed(
    sheet_id: str,
    worksheet_gid: int,
    _secret_fp: str,
    *,
    _ingest_version: str = DASHBOARD_BUILD,
) -> pd.DataFrame:
    """Read one tab by gid with the same preprocess + normalize path as ``load_all_worksheets_combined``."""
    _ = _ingest_version
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        raise RuntimeError(
            "No service account in Streamlit Secrets. Add a `[gsheet_service_account]` block "
            "(or `GCP_SERVICE_ACCOUNT`) in this app’s Secrets, then reboot."
        )
    raw = pd.DataFrame()
    if int(worksheet_gid) in DEFAULT_PAID_MEDIA_PLATFORM_GIDS:
        raw = _dataframe_from_grid_best_supermetrics_header(sheet_id, secret_creds, int(worksheet_gid))
    try:
        if raw.empty or len(raw.columns) < 4:
            raw = _read_sheet_auth(
                sheet_id,
                secret_creds,
                worksheet_name=None,
                worksheet_gid=int(worksheet_gid),
            )
            if raw.empty or len(raw.columns) == 0:
                raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=int(worksheet_gid))
    except Exception:
        try:
            raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=int(worksheet_gid))
        except Exception:
            return pd.DataFrame()
    raw = _promote_wide_metric_header_row_if_needed(raw)
    raw = _coerce_two_row_sheet_headers(raw)
    title = _tab_title_for_worksheet_gid(sheet_id, worksheet_gid, _secret_fp)
    raw = _preprocess_excel_sheet(raw, title)
    out = _normalize(raw)
    if not out.empty and int(worksheet_gid) in DEFAULT_PAID_MEDIA_PLATFORM_GIDS:
        out = out.copy()
        out["cost"] = _sum_cost_columns_raw(raw, len(out)).values
        if "country" not in out.columns:
            out["country"] = "Unknown"
        ck = out["country"].astype(str).map(_country_join_key).fillna("unknown").astype(str).str.strip().str.lower()
        bad_country = ck.isin({"", "unknown", "nan", "none", "<na>"})
        if bool(bad_country.any()):
            inferred = _infer_country_from_cost_headers_raw(raw, len(out))
            out.loc[bad_country, "country"] = inferred.loc[bad_country].values
        out = _apply_equal_split_all_market_engagement(out)
        out = _canonicalize_spend_month_column(out)
    if not out.empty and (
        _tab_title_looks_like_spend_worksheet(str(title)) or _tab_title_looks_like_ads_data_sheet(str(title))
    ):
        out = _canonicalize_spend_month_column(out)
    out["source_tab"] = title
    out["worksheet_gid"] = int(worksheet_gid)
    return out


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

    def _truth_tab_month_country_platform_cost(df_raw: pd.DataFrame) -> pd.DataFrame:
        """Parse truth tab shaped as Month/Country + platform spend columns."""
        if df_raw.empty:
            return pd.DataFrame()
        cols = list(df_raw.columns)
        nk_map = {c: _norm_header_key(str(c)) for c in cols}

        month_col = next((c for c in cols if nk_map[c] in {"month", "report_month", "period"}), None)
        country_col = next((c for c in cols if nk_map[c] in {"country", "market"}), None)
        if month_col is None or country_col is None:
            return pd.DataFrame()

        platform_cols: list[tuple[str, str]] = []
        for c in cols:
            k = nk_map[c]
            if "google" in k and "ads" in k:
                platform_cols.append((c, "Google Ads"))
            elif "meta" in k and "ads" in k:
                platform_cols.append((c, "Meta Ads"))
            elif "snapchat" in k and "ads" in k:
                platform_cols.append((c, "Snapchat Ads"))
            elif "linkedin" in k and "ads" in k:
                platform_cols.append((c, "LinkedIn Ads"))
        if not platform_cols:
            return pd.DataFrame()

        base = pd.DataFrame(index=df_raw.index)
        base["month"] = df_raw[month_col].map(_month_norm_key)
        base["country"] = df_raw[country_col].map(_canonical_country_label)
        base = base.loc[base["month"].astype(str).str.len() > 0].copy()
        if base.empty:
            return pd.DataFrame()

        parts: list[pd.DataFrame] = []
        for col, plat in platform_cols:
            p = base.copy()
            p["platform"] = plat
            p["cost"] = _to_number_series(df_raw.loc[p.index, col]).fillna(0.0)
            parts.append(p)
        if not parts:
            return pd.DataFrame()
        out_truth = pd.concat(parts, ignore_index=True)
        out_truth["date"] = pd.to_datetime(out_truth["month"] + "-01", errors="coerce")
        return out_truth

    parsed_truth = _truth_tab_month_country_platform_cost(raw)
    out = parsed_truth if not parsed_truth.empty else _normalize(raw)
    out["source_tab"] = f"gid:{worksheet_gid}"
    out["worksheet_gid"] = int(worksheet_gid)
    return out


def _load_paid_media_platform_tabs_by_gid(sheet_id: str, _secret_fp: str) -> pd.DataFrame:
    """Explicitly load the 4 paid-media platform tabs by gid (Google/Meta/Snapchat/LinkedIn)."""
    parts: list[pd.DataFrame] = []
    for gid in DEFAULT_PAID_MEDIA_PLATFORM_GIDS:
        try:
            sub = load_worksheet_by_gid_preprocessed(sheet_id, int(gid), _secret_fp)
        except Exception:
            continue
        if sub.empty:
            continue
        sub = sub.copy()
        sub["worksheet_gid"] = int(gid)
        parts.append(sub)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


@st.cache_data(ttl=300)
def load_named_worksheet_normalized(sheet_id: str, worksheet_name: str, _secret_fp: str) -> pd.DataFrame:
    """Load one worksheet by exact title, preprocess + normalize, and tag source_tab."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        raise RuntimeError(
            "No service account in Streamlit Secrets. Add a `[gsheet_service_account]` block "
            "(or `GCP_SERVICE_ACCOUNT`) in this app’s Secrets, then reboot."
        )
    raw = _read_sheet_auth(
        sheet_id,
        secret_creds,
        worksheet_name=worksheet_name,
        worksheet_gid=None,
    )
    if raw.empty or len(raw.columns) == 0:
        try:
            import gspread
            from google.oauth2.service_account import Credentials

            creds_info = _coerce_service_account_dict(secret_creds)
            _validate_service_account_dict(creds_info)
            creds = Credentials.from_service_account_info(
                creds_info,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(sheet_id)
            ws = sh.worksheet(worksheet_name)
            from gspread.utils import ValueRenderOption

            try:
                grid = ws.get_all_values(value_render_option=ValueRenderOption.unformatted) or []
            except Exception:
                grid = ws.get_all_values() or []
            raw = _dataframe_from_grid_with_keyword_header(grid, "spend")
        except Exception:
            return pd.DataFrame()
    raw = _preprocess_excel_sheet(raw, worksheet_name)
    out = _normalize(raw)
    if (out.empty or "cost" not in out.columns or float(out["cost"].sum()) == 0.0) and worksheet_name.strip().lower() == "spend":
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            from gspread.utils import ValueRenderOption

            creds_info = _coerce_service_account_dict(secret_creds)
            _validate_service_account_dict(creds_info)
            creds = Credentials.from_service_account_info(
                creds_info,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(sheet_id)
            ws = sh.worksheet(worksheet_name)
            try:
                grid = ws.get_all_values(value_render_option=ValueRenderOption.unformatted) or []
            except Exception:
                grid = ws.get_all_values() or []
            raw2 = _dataframe_from_grid_with_keyword_header(grid, "spend")
            if not raw2.empty:
                out = _normalize(_preprocess_excel_sheet(raw2, worksheet_name))
        except Exception:
            pass
    if out.empty:
        return out
    out["source_tab"] = worksheet_name
    return out


_GRID_KEYWORDS_FOR_SPEND: tuple[str, ...] = (
    "spend",
    "cost_usd",
    "cost",
    "amount",
    "investment",
    "budget",
    "media",
    "paid",
    "fee",
    "fees",
    "aed",
    "sar",
)


def _worksheet_title_is_spend_like(title: str) -> bool:
    t = str(title).strip().lower()
    if "spend" in t or "paid media" in t or "media spend" in t:
        return True
    if "ppc" in t or "paid search" in t:
        return True
    return False


def _collect_spend_raw_candidates_for_gid(
    sheet_id: str,
    secret_creds: Any,
    worksheet_gid: int,
) -> list[pd.DataFrame]:
    raws: list[pd.DataFrame] = []
    try:
        raw = _read_sheet_auth(
            sheet_id,
            secret_creds,
            worksheet_name=None,
            worksheet_gid=int(worksheet_gid),
        )
        if not raw.empty and len(raw.columns) > 0:
            raws.append(raw)
    except Exception:
        pass
    try:
        loose = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=int(worksheet_gid))
        if not loose.empty:
            raws.append(loose)
    except Exception:
        pass
    try:
        grid = _read_sheet_grid_values(sheet_id, secret_creds, int(worksheet_gid))
        for kw in _GRID_KEYWORDS_FOR_SPEND:
            gdf = _dataframe_from_grid_with_keyword_header(grid, kw)
            if not gdf.empty:
                raws.append(gdf)
    except Exception:
        pass
    return raws


@st.cache_data(ttl=300)
def load_spend_worksheet_fallback(sheet_id: str, _secret_fp: str) -> pd.DataFrame:
    """Find spend-like worksheets by title, load each, keep the normalized frame with the largest ``cost`` sum."""
    meta = list_worksheet_meta(sheet_id, _secret_fp)
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return pd.DataFrame()
    spend_candidates = [(t, gid) for t, gid in meta if _worksheet_title_is_spend_like(t)]
    if not spend_candidates:
        return pd.DataFrame()
    best = pd.DataFrame()
    best_sum = -1.0
    best_title = ""
    for title, ws_gid in spend_candidates:
        try:
            raw = _read_sheet_auth(
                sheet_id,
                secret_creds,
                worksheet_name=None,
                worksheet_gid=int(ws_gid),
            )
            if raw.empty or len(raw.columns) == 0:
                raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=int(ws_gid))
        except Exception:
            try:
                raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=int(ws_gid))
            except Exception:
                continue
        if raw.empty or len(raw.columns) == 0:
            continue
        raw = _preprocess_excel_sheet(raw, str(title))
        out = _normalize(raw)
        if out.empty:
            continue
        s = _normalized_spend_cost_sum(out)
        if s > best_sum:
            best_sum = s
            best = out.copy()
            best_title = str(title)
    if best.empty:
        return pd.DataFrame()
    best = _canonicalize_spend_month_column(best)
    best["source_tab"] = best_title
    _attach_spend_pool_debug_attrs(best)
    return best


@st.cache_data(ttl=300)
def load_spend_gid0_normalized(sheet_id: str, _secret_fp: str) -> pd.DataFrame:
    """Spend worksheet: optional ``XRAY_SPEND_GID`` first, then ``gid=0``. Several parses per tab; keeps best ``cost``."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return pd.DataFrame()

    gids: list[int] = []
    sg = _optional_spend_gid_from_secrets()
    if sg is not None:
        gids.append(int(sg))
    gids.append(0)
    gids = list(dict.fromkeys(gids))

    raws_tagged: list[tuple[pd.DataFrame, int]] = []
    for gid in gids:
        for raw in _collect_spend_raw_candidates_for_gid(sheet_id, secret_creds, gid):
            raws_tagged.append((raw, gid))

    best = pd.DataFrame()
    best_sum = 0.0
    best_gid = 0
    _spend_col_secret = _optional_spend_column_header_from_secrets()
    for raw, src_gid in raws_tagged:
        if raw.empty or len(raw.columns) == 0:
            continue
        try:
            raw_orig = raw.copy()
            cand = _normalize(_preprocess_excel_sheet(raw.copy(), "spend"))
            if _spend_col_secret:
                cand = _inject_cost_from_named_sheet_column(raw_orig, cand, _spend_col_secret)
            s = _normalized_spend_cost_sum(cand)
            if s < 1e-9:
                gx = _guess_spend_value_column_raw(raw)
                if (
                    gx
                    and gx in raw.columns
                    and not cand.empty
                    and len(cand) == len(raw)
                ):
                    cand = cand.copy()
                    cand["cost"] = _to_number_series(raw[gx].reset_index(drop=True)).values
                    s = _normalized_spend_cost_sum(cand)
            if s > best_sum:
                best_sum = s
                best = cand
                best_gid = int(src_gid)
        except Exception:
            continue

    # Many workbooks have title rows: get_all_records used the wrong header. Try each grid row as header.
    if best_sum < 1e-6:
        for gid in gids:
            try:
                grid = _read_sheet_grid_values(sheet_id, secret_creds, gid)
            except Exception:
                continue
            for hi in range(0, min(55, len(grid))):
                if not any(str(x).strip() for x in grid[hi]):
                    continue
                raw = _dataframe_from_grid_header_at(grid, hi)
                if raw.empty or len(raw.columns) < 2:
                    continue
                raw = raw.dropna(axis=1, how="all")
                if raw.empty:
                    continue
                try:
                    raw_orig = raw.copy()
                    cand = _normalize(_preprocess_excel_sheet(raw.copy(), "spend"))
                    if _spend_col_secret:
                        cand = _inject_cost_from_named_sheet_column(raw_orig, cand, _spend_col_secret)
                    s = _normalized_spend_cost_sum(cand)
                    if s < 1e-9:
                        gx = _guess_spend_value_column_raw(raw)
                        if (
                            gx
                            and gx in raw.columns
                            and not cand.empty
                            and len(cand) == len(raw)
                        ):
                            cand = cand.copy()
                            cand["cost"] = _to_number_series(raw[gx].reset_index(drop=True)).values
                            s = _normalized_spend_cost_sum(cand)
                    if s > best_sum:
                        best_sum = s
                        best = cand
                        best_gid = int(gid)
                except Exception:
                    continue

    if best.empty:
        return pd.DataFrame()
    best = _canonicalize_spend_month_column(best)
    best["source_tab"] = f"gid:{best_gid}_spend"
    _attach_spend_pool_debug_attrs(best)
    return best


@st.cache_data(ttl=300)
def load_spend_gid0_raw_sum(sheet_id: str, _secret_fp: str) -> float:
    """Raw-grid spend sum: optional ``XRAY_SPEND_GID`` then ``gid=0`` (keyword headers + loose read)."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return 0.0

    gids: list[int] = []
    _sg = _optional_spend_gid_from_secrets()
    if _sg is not None:
        gids.append(int(_sg))
    gids.append(0)
    gids = list(dict.fromkeys(gids))

    best = 0.0
    for gid in gids:
        try:
            grid = _read_sheet_grid_values(sheet_id, secret_creds, gid)
            for kw in _GRID_KEYWORDS_FOR_SPEND:
                df = _dataframe_from_grid_with_keyword_header(grid, kw)
                sm = _scan_frame_for_spend_sum(df)
                if abs(sm) > abs(best):
                    best = sm
        except Exception:
            pass
        try:
            raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=gid)
            sm = _scan_frame_for_spend_sum(raw)
            if abs(sm) > abs(best):
                best = sm
        except Exception:
            pass
    return best


def _load_first_matching_worksheet_from_meta(
    sheet_id: str,
    name_patterns: tuple[str, ...],
    _secret_fp: str,
    meta: list[tuple[str, int]],
) -> pd.DataFrame:
    """Load first worksheet whose title matches any regex pattern; pass ``meta`` from ``list_worksheet_meta`` to avoid duplicate API calls."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return pd.DataFrame()

    picked: Optional[tuple[str, int]] = None
    for title, ws_gid in meta:
        tl = str(title).strip().lower()
        if any(re.search(p, tl) for p in name_patterns):
            picked = (title, ws_gid)
            break
    if not picked:
        return pd.DataFrame()

    title, ws_gid = picked
    try:
        raw = _read_sheet_auth(
            sheet_id,
            secret_creds,
            worksheet_name=None,
            worksheet_gid=int(ws_gid),
        )
        if raw.empty or len(raw.columns) == 0:
            raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=int(ws_gid))
    except Exception:
        try:
            raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=int(ws_gid))
        except Exception:
            return pd.DataFrame()
    if raw.empty or len(raw.columns) == 0:
        return pd.DataFrame()
    out = _normalize(_preprocess_excel_sheet(raw, str(title)))
    if out.empty:
        return out
    if _tab_title_looks_like_spend_worksheet(str(title)):
        out = _canonicalize_spend_month_column(out)
        _attach_spend_pool_debug_attrs(out)
    out["source_tab"] = str(title)
    out["worksheet_gid"] = int(ws_gid)
    return out


@st.cache_data(ttl=300)
def load_first_matching_worksheet_normalized(
    sheet_id: str,
    name_patterns: tuple[str, ...],
    _secret_fp: str,
) -> pd.DataFrame:
    """Load first worksheet whose title matches any regex pattern."""
    meta = list_worksheet_meta(sheet_id, _secret_fp)
    return _load_first_matching_worksheet_from_meta(sheet_id, name_patterns, _secret_fp, meta)


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


def _format_currency(v: float) -> str:
    return f"${v:,.2f}"


def _format_spend_k(v: float) -> str:
    """Display cost/spend in thousands: ``$1.22K`` (2 decimals; values under 1K stay full)."""
    if pd.isna(v):
        return "—"
    n = float(v)
    if n == 0.0 or abs(n) < 1e-9:
        return "$0"
    if abs(n) < 1_000.0:
        return f"${n:,.2f}"
    k = n / 1_000.0
    return f"${k:,.2f}K"


def _format_tcv_short(v: float) -> str:
    """Display TCV compactly: ``$3.24M`` or ``$500.00K`` (2 decimals; under 1K full)."""
    if pd.isna(v):
        return "—"
    n = float(v)
    if n == 0.0 or abs(n) < 1e-9:
        return "$0"
    if abs(n) < 1_000.0:
        return f"${n:,.2f}"
    if abs(n) >= 1_000_000.0:
        return f"${n / 1_000_000.0:,.2f}M"
    k = n / 1_000.0
    return f"${k:,.2f}K"


def _format_compact_k(v: float) -> str:
    """Compact currency for KPI tiles (e.g. 1.8K)."""
    if pd.isna(v):
        return "—"
    n = float(v)
    if abs(n) >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"${n / 1_000:.1f}K"
    return f"${n:,.2f}"


def _format_ratio_cpcw_lf(v: float) -> str:
    """Unitless ratio like CpCW:LF — always two decimal places."""
    if pd.isna(v) or v != v:
        return "—"
    n = float(v)
    if abs(n) < 1e-12:
        return "0.00"
    return f"{n:.2f}"


def _lead_rows_count(frame: pd.DataFrame) -> int:
    """Lead count = data-row count of the resolved leads slice (sheet rows, header excluded)."""
    return int(len(frame)) if isinstance(frame, pd.DataFrame) else 0


def _ensure_leads_metric_for_master(ld: pd.DataFrame) -> pd.DataFrame:
    """Master view sums ``leads``; inbound sheets often have no numeric Leads column (all zeros). One row = one lead."""
    if ld.empty:
        return ld
    out = ld.copy()
    if "leads" not in out.columns:
        out["leads"] = 1
    else:
        s = pd.to_numeric(out["leads"], errors="coerce").fillna(0)
        if float(s.sum()) == 0 and len(out) > 0:
            out["leads"] = 1
    return out


def _leads_pivot_rowcount_by_month_country(ld: pd.DataFrame) -> pd.DataFrame:
    """``month`` × ``country`` counts from lead **rows** (matches scorecard row-count when the sheet has no Leads metric)."""
    x = _normalize_master_merge_frame(ld)
    if x.empty or "month" not in x.columns or "country" not in x.columns:
        return pd.DataFrame(columns=["month", "country", "leads"])
    x = x.copy()
    x["_mk"] = x["month"].map(_month_norm_key).astype(str).str.strip()
    x = x.loc[x["_mk"].ne("")].copy()
    if x.empty:
        return pd.DataFrame(columns=["month", "country", "leads"])
    x["month"] = x["_mk"]
    x = x.drop(columns=["_mk"], errors="ignore")
    cnt = x.groupby(["month", "country"], as_index=False, dropna=False).size().rename(columns={"size": "leads"})
    cnt["leads"] = _to_int_series_safe(cnt["leads"])
    return cnt


def _leads_qualified_overlay_frame_by_month_market(ld: pd.DataFrame) -> pd.DataFrame:
    """Month × Market **leads** (row count) and **qualified** (``lead_status_text``), same basis as master drill / scorecard."""
    x = _normalize_master_merge_frame(ld)
    if x.empty or "month" not in x.columns or "country" not in x.columns:
        return pd.DataFrame(columns=["month", "Market", "leads", "qualified"])
    x = x.copy()
    x["_mk"] = x["month"].map(_month_norm_key).astype(str).str.strip()
    x = x.loc[x["_mk"].ne("")].copy()
    if x.empty:
        return pd.DataFrame(columns=["month", "Market", "leads", "qualified"])
    x["month"] = x["_mk"]
    x = x.drop(columns=["_mk"], errors="ignore")
    x["_qual"] = _to_int_series_safe(_leads_is_qualified_mask(x))
    _gb = x.groupby(["month", "country"], dropna=False)
    g = _gb.size().reset_index(name="leads")
    g = g.merge(_gb["_qual"].sum().reset_index(name="qualified"), on=["month", "country"], how="left")
    g["Market"] = g["country"].map(_market_display_from_join_key)
    out = g.groupby(["month", "Market"], as_index=False, dropna=False).agg(
        leads=("leads", "sum"),
        qualified=("qualified", "sum"),
    )
    out["leads"] = pd.to_numeric(out["leads"], errors="coerce").fillna(0)
    out["qualified"] = pd.to_numeric(out["qualified"], errors="coerce").fillna(0)
    return out


def _overlay_gm_leads_qualified_from_raw_leads(gm: pd.DataFrame, leads_df: pd.DataFrame) -> pd.DataFrame:
    """Replace ``leads`` / ``qualified`` where raw leads rows exist (aligns grid + T3B3 with master **cell drill** source)."""
    if gm.empty or leads_df is None or (isinstance(leads_df, pd.DataFrame) and leads_df.empty):
        return gm
    if "Market" not in gm.columns or "month" not in gm.columns:
        return gm
    ov = _leads_qualified_overlay_frame_by_month_market(leads_df)
    if ov.empty:
        return gm
    ov = ov.copy()
    ov["month_key"] = ov["month"].map(lambda m: _month_norm_key(m) or (str(m).strip() if pd.notna(m) else ""))
    ov = ov.loc[ov["month_key"].astype(str).str.len() > 0].drop_duplicates(subset=["month_key", "Market"], keep="last")
    out = gm.copy()
    out["month_key"] = out["month"].map(_month_norm_key)
    ov_m = ov.rename(columns={"leads": "_lr", "qualified": "_qr"}).drop(columns=["month"], errors="ignore")
    out = out.merge(ov_m, on=["month_key", "Market"], how="left")
    m = out["_lr"].notna()
    if m.any():
        out.loc[m, "leads"] = pd.to_numeric(out.loc[m, "_lr"], errors="coerce").fillna(0)
        out.loc[m, "qualified"] = pd.to_numeric(out.loc[m, "_qr"], errors="coerce").fillna(0)
    out = out.drop(columns=["_lr", "_qr", "month_key"], errors="ignore")
    out["CPL"] = out.apply(
        lambda r: (r["spend"] / r["leads"]) if r["leads"] and r["leads"] > 0 else float("nan"),
        axis=1,
    )
    out["SQL %"] = out.apply(
        lambda r: (r["qualified"] / r["leads"] * 100) if r["leads"] and r["leads"] > 0 else float("nan"),
        axis=1,
    )
    if "Total Leads" in out.columns:
        out["Total Leads"] = out["leads"]
    return out


def _leads_is_qualifying_column_name(cols: Any) -> Optional[str]:
    """Detect spreadsheet SQL flag column (e.g. ``Is_Qualifying`` on AB post lead tab)."""
    for c in cols:
        key = re.sub(r"[^a-z0-9]", "", str(c).strip().lower())
        if key == "isqualifying":
            return str(c)
    return None


def _leads_is_qualified_mask(frame: pd.DataFrame) -> pd.Series:
    """True when lead counts as SQL/qualified: status ``Qualified`` **or** ``Is_Qualifying``-style flag = 1."""
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)
    out = pd.Series(False, index=frame.index, dtype=bool)
    if "lead_status_text" in frame.columns:
        st = frame["lead_status_text"].astype(str).str.strip().str.lower()
        out = out | st.eq("qualified")
    if "Lead Status" in frame.columns:
        st = frame["Lead Status"].astype(str).str.strip().str.lower()
        out = out | st.eq("qualified")
    _iq = _leads_is_qualifying_column_name(frame.columns)
    if _iq is not None:
        v = frame[_iq]
        num = pd.to_numeric(v, errors="coerce")
        yn = v.astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})
        out = out | (num.fillna(0) > 0) | yn
    return out


def _new_working_count_from_leads(frame: pd.DataFrame) -> int:
    """Count leads where Lead Status is exactly New or Working."""
    if frame.empty or "lead_status_text" not in frame.columns:
        return 0
    s = frame["lead_status_text"].astype(str).str.strip().str.lower()
    return int(s.isin({"new", "working"}).sum())


def _qualified_count_from_leads(frame: pd.DataFrame) -> int:
    """Count SQL/qualified leads (status **Qualified** and/or ``Is_Qualifying`` = 1)."""
    if frame.empty:
        return 0
    return int(_leads_is_qualified_mask(frame).sum())


def _master_view_drop_empty_months(gm: pd.DataFrame) -> pd.DataFrame:
    """Drop months whose summed activity is all zeros (no rows in source for that period).

    Uses a broad set of metrics so pipeline/clicks/qualified still count as activity.
    If the filter would remove every row, returns ``gm`` unchanged so the table does not go blank.
    """
    if gm.empty:
        return gm
    before_n = len(gm)
    gm_ok = gm[gm["month"].notna()].copy()
    if gm_ok.empty:
        return gm
    num_cols = [
        c
        for c in (
            "spend",
            "cw",
            "leads",
            "tcv",
            "lf",
            "qualified",
            "clicks",
            "impressions",
            "pitching",
            "new",
            "working",
            "qualifying",
            "negotiation",
            "commitment",
            "closed_lost",
        )
        if c in gm_ok.columns
    ]
    if not num_cols:
        return gm_ok
    by_m = gm_ok.groupby("month", as_index=True)[num_cols].sum()
    keep = by_m.index[(by_m > 0).any(axis=1)]
    out = gm_ok[gm_ok["month"].isin(keep)].copy()
    if out.empty and before_n > 0:
        return gm
    return out


def _master_view_append_middle_east_first(gm: pd.DataFrame) -> pd.DataFrame:
    """Per month: country markets (by spend high → low), then **Middle East** subtotal row last."""
    if gm.empty:
        return gm
    parts: list[pd.DataFrame] = []

    def _month_sort_key(m: Any) -> Any:
        try:
            return pd.Period(str(m), freq="M")
        except Exception:
            return str(m)

    def _is_regional_totals_market(name: str) -> bool:
        return _norm_market_key(name) in _REGION_SUBTOTAL_NAMES_LOWER

    def _frame_numeric_sum(frame: pd.DataFrame, col: str) -> float:
        if frame.empty or col not in frame.columns:
            return 0.0
        return float(pd.to_numeric(frame[col], errors="coerce").fillna(0).sum())

    months_sorted = sorted(gm["month"].dropna().unique(), key=_month_sort_key, reverse=True)
    for month in months_sorted:
        grp_all = gm[gm["month"] == month].copy()
        reg_mask = grp_all["Market"].map(_is_regional_totals_market)
        regional = grp_all.loc[reg_mask]
        grp = grp_all.loc[~reg_mask]
        if grp.empty and regional.empty:
            continue

        reg_totals: dict[str, float] = {}
        for col in _REGIONAL_ROLL_METRICS:
            if col in gm.columns:
                reg_totals[col] = _frame_numeric_sum(regional, col)

        if not grp.empty:
            grp = grp.copy()
            if "spend" in grp.columns:
                grp["_spend_sort"] = pd.to_numeric(grp["spend"], errors="coerce").fillna(0.0)
            else:
                grp["_spend_sort"] = 0.0
            country_block = grp.sort_values(
                ["_spend_sort", "Market"],
                ascending=[False, True],
                kind="mergesort",
            ).drop(columns="_spend_sort")
        else:
            country_block = pd.DataFrame(columns=gm.columns)

        me_mask = country_block["Market"].map(_is_middle_east_market) if not country_block.empty else pd.Series(
            dtype=bool
        )
        me_idx = country_block.index[me_mask] if not country_block.empty else pd.Index([], dtype=int)

        # Clicks/impressions may still roll from a regional row onto ME countries; **spend** stays on **Middle East** as month total.
        if len(me_idx) > 0:
            country_block = country_block.copy()
            for col in _REGIONAL_ROLL_METRICS:
                if col == "spend":
                    continue
                if col not in country_block.columns:
                    continue
                rtot = reg_totals.get(col, 0.0)
                if rtot <= 0.0:
                    continue
                csum = _frame_numeric_sum(country_block.loc[me_idx], col)
                if csum > 1e-6:
                    continue
                w = pd.to_numeric(country_block.loc[me_idx, "cw"], errors="coerce").fillna(0).astype(float)
                if float(w.sum()) > 0.0:
                    alloc = rtot * (w / w.sum())
                else:
                    alloc = pd.Series(rtot / max(len(me_idx), 1), index=me_idx, dtype=float)
                country_block.loc[me_idx, col] = alloc.reindex(me_idx).fillna(0.0).values

        me_slice = country_block[me_mask] if not country_block.empty else country_block.iloc[0:0]

        blocks: list[pd.DataFrame] = []
        if not country_block.empty:
            blocks.append(country_block)

        if not me_slice.empty:
            # Middle East row: spend = regional/sheet total for the month when countries have no spend; else sum of countries.
            row: dict[str, Any] = {"month": month, "Market": _MIDDLE_EAST_REGION_LABEL}
            for c in gm.columns:
                if c in ("month", "Market"):
                    continue
                if c in me_slice.columns:
                    summed = float(pd.to_numeric(me_slice[c], errors="coerce").fillna(0).sum())
                    if c == "spend":
                        r_spend = float(reg_totals.get("spend", 0.0))
                        row[c] = summed if summed > 1e-6 else r_spend
                    else:
                        row[c] = summed
            mena_df = pd.DataFrame([row])
            for c in gm.columns:
                if c not in mena_df.columns:
                    mena_df[c] = float("nan")
            mena_df = mena_df[gm.columns]
            blocks.append(mena_df)
        elif reg_totals.get("spend", 0.0) > 0.0 or any(v > 0.0 for v in reg_totals.values()):
            # No ME country rows this month but sheet had regional spend — show one aggregate row.
            row = {c: float("nan") for c in gm.columns}
            row["month"] = month
            row["Market"] = _MIDDLE_EAST_REGION_LABEL
            for c in _REGIONAL_ROLL_METRICS:
                if c in row:
                    row[c] = reg_totals.get(c, 0.0)
            mena_df = pd.DataFrame([row])[gm.columns]
            blocks.append(mena_df)
        if blocks:
            parts.append(pd.concat(blocks, ignore_index=True))
    if not parts:
        return gm
    return pd.concat(parts, ignore_index=True)


def _master_view_style_css(
    df: pd.DataFrame,
    *,
    month_block_first: Optional[list[bool]] = None,
    month_block_last: Optional[list[bool]] = None,
) -> pd.DataFrame:
    """Looker-like fills: cyan inputs, white leads, R/G/Y ratios; bold Middle East region row.

    When ``month`` labels sit on **Middle East** only, pass ``month_block_first`` / ``month_block_last`` (one entry per
    row in display order) so thick borders still separate calendar months.
    """
    _align_c = "text-align: center; vertical-align: middle;"
    _align_l = "text-align: left; vertical-align: middle; padding-left: 8px;"

    def _cell(base: str, *, center: bool = True) -> str:
        a = _align_c if center else _align_l
        b = base.strip().rstrip(";")
        return f"{b}; {a}" if b else a

    css = pd.DataFrame("", index=df.index, columns=df.columns)
    is_region = pd.Series(False, index=df.index)
    if "Market" in df.columns:
        is_region = is_region | df["Market"].astype(str).str.strip().str.lower().isin({"middle east", "mena"})
    if "Country / platform" in df.columns:
        _cp = df["Country / platform"].astype(str).str.strip().str.lower()
        is_region = is_region | _cp.str.startswith("middle east")
    if "Unified Channel" in df.columns:
        _uc = df["Unified Channel"].astype(str).str.strip().str.lower()
        is_region = is_region | _uc.eq("middle east")
    if "Channel" in df.columns:
        _chm = df["Channel"].astype(str).str.strip().str.lower()
        is_region = is_region | _chm.eq("middle east")
    non_me = ~is_region
    cyan = "background-color: #e8f4f8; color: #0f172a;"
    white = "background-color: #ffffff; color: #0f172a;"
    me_bold = "font-weight: 700;"
    ratio_me = "background-color: #ffffff; font-weight: 700; color: #0f172a;"
    empty_cell = "background-color: #fafafa; color: #94a3b8;"

    def _rgy(val: Any, lo: float, hi: float) -> str:
        try:
            v = float(val)
        except (TypeError, ValueError):
            return _cell("background-color: #fee2e2; color: #991b1b;")
        if pd.isna(v) or v == 0.0:
            return _cell("background-color: #fee2e2; color: #991b1b;")
        if v <= lo:
            return _cell("background-color: #dcfce7; color: #166534;")
        if v <= hi:
            return _cell("background-color: #fef9c3; color: #854d0e;")
        return _cell("background-color: #fee2e2; color: #b91c1c;")

    lf_lo = lf_hi = 1.0
    ct_lo = ct_hi = 5.0
    if "CPCW:LF" in df.columns:
        s_lf = pd.to_numeric(df.loc[non_me, "CPCW:LF"], errors="coerce").dropna()
        if len(s_lf) >= 2:
            lf_lo, lf_hi = float(s_lf.quantile(0.33)), float(s_lf.quantile(0.66))
        else:
            lf_lo, lf_hi = 1.0, 2.5
    if "Cost/TCV%" in df.columns:
        s_ct = pd.to_numeric(df.loc[non_me, "Cost/TCV%"], errors="coerce").dropna()
        if len(s_ct) >= 2:
            ct_lo, ct_hi = float(s_ct.quantile(0.33)), float(s_ct.quantile(0.66))
        else:
            ct_lo, ct_hi = 5.0, 12.0

    cyan_cols = {"Spend", "CW (Inc Approved)", "CPCW", "1st Month LF", "Actual TCV"}
    idx_list = list(df.index)
    n = len(idx_list)
    month_first: list[bool] = []
    month_last: list[bool] = []
    if (
        month_block_first is not None
        and month_block_last is not None
        and len(month_block_first) == n
        and len(month_block_last) == n
    ):
        month_first = list(month_block_first)
        month_last = list(month_block_last)
    elif "Month" in df.columns:
        for i in idx_list:
            v = df.loc[i, "Month"]
            month_first.append(bool(pd.notna(v) and str(v).strip() != ""))
        for pos in range(n):
            month_last.append(pos == n - 1 or (month_first[pos + 1] if pos + 1 < n else False))
    else:
        month_first = [p == 0 for p in range(n)]
        month_last = [p == n - 1 for p in range(n)]

    for pos, i in enumerate(idx_list):
        me = bool(is_region.loc[i])
        row_edge = ""
        if month_first[pos] and pos > 0:
            row_edge = "border-top: 4px solid #64748b"
        if month_last[pos]:
            row_edge = (row_edge + "; " if row_edge else "") + "border-bottom: 4px solid #64748b"

        def _rx(s: str) -> str:
            if not row_edge:
                return s
            return f"{s.rstrip().rstrip(';')}; {row_edge}"

        def _ew(st: str) -> str:
            if not row_edge:
                return st
            return f"{st.rstrip().rstrip(';')}; {row_edge}"

        for col in df.columns:
            if col in {"Month", "Unified Date"}:
                v = df.loc[i, col]
                if v == "" or (isinstance(v, str) and not str(v).strip()):
                    css.loc[i, col] = _cell(_rx(empty_cell))
                else:
                    css.loc[i, col] = _cell(
                        _rx(
                            "background-color: #f1f5f9; font-weight: 600; color: #334155; border-bottom: 1px solid #e2e8f0;"
                        )
                    )
            elif col == "Market":
                base = (me_bold + " background-color: #ffffff; color: #0f172a;") if me else white
                css.loc[i, col] = _cell(_rx(base), center=False)
            elif col == "Country / platform":
                base = (me_bold + " background-color: #ffffff; color: #0f172a;") if me else white
                css.loc[i, col] = _cell(_rx(base), center=False)
            elif col == "Channel":
                base = (me_bold + " background-color: #ffffff; color: #0f172a;") if me else white
                css.loc[i, col] = _cell(_rx(base), center=False)
            elif col == "Unified Channel":
                base = (me_bold + " background-color: #ffffff; color: #0f172a;") if me else white
                css.loc[i, col] = _cell(_rx(base), center=False)
            elif col in cyan_cols:
                base = (cyan + me_bold) if me else cyan
                css.loc[i, col] = _cell(_rx(base))
            elif col in {"Total Leads", "Qualified"}:
                base = (white + me_bold) if me else white
                css.loc[i, col] = _cell(_rx(base))
            elif col == "CPCW:LF":
                if me:
                    css.loc[i, col] = _cell(_rx(ratio_me))
                else:
                    css.loc[i, col] = _ew(_rgy(df.loc[i, col], lf_lo, lf_hi))
            elif col == "Cost/TCV%":
                if me:
                    css.loc[i, col] = _cell(_rx(ratio_me))
                else:
                    css.loc[i, col] = _ew(_rgy(df.loc[i, col], ct_lo, ct_hi))
            else:
                css.loc[i, col] = _cell(_rx(white))
    return css


def _apply_sheet_filters(
    df_date: pd.DataFrame,
    *,
    key_suffix: str,
    filters_in_row: bool = False,
    include_country_filter: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (filtered for metrics/charts, df_for_tabs mirror)."""
    country_opts = sorted([x for x in df_date["country"].dropna().unique().tolist() if x and x != "Unknown"])
    platform_opts = sorted([x for x in df_date["platform"].dropna().unique().tolist() if x and x != "Unknown"])

    selected_countries = ["All markets"]
    if filters_in_row:
        if include_country_filter:
            fc, fp = st.columns(2, gap="small")
            with fc:
                selected_countries = st.multiselect(
                    "Market",
                    ["All markets"] + country_opts,
                    default=["All markets"],
                    key=f"{key_suffix}_country",
                )
            with fp:
                selected_platforms = st.multiselect(
                    "Platform",
                    ["All Platforms"] + platform_opts,
                    default=["All Platforms"],
                    key=f"{key_suffix}_platform",
                )
        else:
            selected_platforms = st.multiselect(
                "Platform",
                ["All Platforms"] + platform_opts,
                default=["All Platforms"],
                key=f"{key_suffix}_platform",
            )
    else:
        selected_countries = st.multiselect(
            "Market",
            ["All markets"] + country_opts,
            default=["All markets"],
            key=f"{key_suffix}_country",
        )
        selected_platforms = st.multiselect(
            "Platform",
            ["All Platforms"] + platform_opts,
            default=["All Platforms"],
            key=f"{key_suffix}_platform",
        )

    df = df_date.copy()
    if include_country_filter and "All markets" not in selected_countries and selected_countries:
        df = df[df["country"].isin(selected_countries)]
    if "All Platforms" not in selected_platforms and selected_platforms:
        df = df[df["platform"].isin(selected_platforms)]

    df_for_tabs = df.copy()

    return df, df_for_tabs


def _dashboard_tab_page_header(heading: Optional[str] = None) -> None:
    """Kicker (Marketing · RevOps) and optional bold tab title — omit ``heading`` to show kicker only."""
    _kicker = (
        '<p class="dash-tab-kicker-wrap">'
        '<span class="dash-tab-kicker-stamp" title="KitchenPark Marketing · RevOps">'
        '<span class="dash-tab-kicker-marketing">Marketing</span>'
        '<span class="dash-tab-kicker-sep"> · </span>'
        '<span class="dash-tab-kicker-revops">RevOps</span>'
        "</span></p>"
    )
    _parts: list[str] = ['<div class="dash-tab-head-cluster">', _kicker]
    if heading and str(heading).strip():
        _parts.append(f'<p class="dash-tab-heading">{html.escape(str(heading).strip())}</p>')
    _parts.append("</div>")
    st.markdown("".join(_parts), unsafe_allow_html=True)


# Marketing Performance: one multiselect token per dimension = full data (no slice on that dimension).
_MPO_ALL_GEO_SENTINEL = "All markets"
_MPO_ALL_GEO_LEGACY: frozenset[str] = frozenset(
    {
        "All Markets",
        "All Countries",
        "All markets & countries",
        "All data",
        _MPO_ALL_GEO_SENTINEL,
    }
)
_MPO_ALL_MONTHS_SENTINEL = "All months"
_MPO_ALL_MONTHS_LEGACY: frozenset[str] = frozenset(
    {"All Months", "All months", "All data", _MPO_ALL_MONTHS_SENTINEL}
)
_MPO_ALL_CHANNELS_SENTINEL = "All channels"
_MPO_ALL_CHANNELS_LEGACY: frozenset[str] = frozenset(
    {"All Channels", "All channels", "All data", _MPO_ALL_CHANNELS_SENTINEL}
)


def _mpo_normalize_market_multiselect_state(key_suffix: str) -> None:
    """Migrate older session values so options always match the multiselect (Streamlit requires values ⊆ options)."""
    k = f"{key_suffix}_market"
    if k not in st.session_state:
        return
    v = st.session_state[k]
    if not isinstance(v, list):
        return
    if any(str(x) in _MPO_ALL_GEO_LEGACY for x in v):
        st.session_state[k] = [_MPO_ALL_GEO_SENTINEL]


def _mpo_normalize_month_multiselect_state(key_suffix: str) -> None:
    k = f"{key_suffix}_month"
    if k not in st.session_state:
        return
    v = st.session_state[k]
    if not isinstance(v, list):
        return
    if any(str(x) in _MPO_ALL_MONTHS_LEGACY for x in v):
        st.session_state[k] = [_MPO_ALL_MONTHS_SENTINEL]


def _mpo_normalize_channel_multiselect_state(key_suffix: str) -> None:
    k = f"{key_suffix}_channel_scope"
    if k not in st.session_state:
        return
    v = st.session_state[k]
    if not isinstance(v, list):
        return
    if any(str(x) in _MPO_ALL_CHANNELS_LEGACY for x in v):
        st.session_state[k] = [_MPO_ALL_CHANNELS_SENTINEL]


def _mpo_month_multiselect_is_all(sel: Any) -> bool:
    if not sel:
        return True
    if not isinstance(sel, list):
        return str(sel) in _MPO_ALL_MONTHS_LEGACY
    return any(str(x) in _MPO_ALL_MONTHS_LEGACY for x in sel)


def _mpo_month_multiselect_explicit(sel: Any) -> list[Any]:
    if not isinstance(sel, list):
        return []
    return [m for m in sel if str(m) not in _MPO_ALL_MONTHS_LEGACY]


def _mpo_market_scope_is_all(sel: Any) -> bool:
    if not sel:
        return True
    if not isinstance(sel, list):
        return str(sel) in _MPO_ALL_GEO_LEGACY
    return any(str(x) in _MPO_ALL_GEO_LEGACY for x in sel)


def _mpo_market_scope_countries_only(sel: Any) -> list[str]:
    if not isinstance(sel, list):
        return []
    return [str(x) for x in sel if x and str(x) not in _MPO_ALL_GEO_LEGACY]


def _mpo_channel_multiselect_is_all(sel: Any) -> bool:
    if not sel:
        return True
    if not isinstance(sel, list):
        return str(sel) in _MPO_ALL_CHANNELS_LEGACY
    return any(str(x) in _MPO_ALL_CHANNELS_LEGACY for x in sel)


def _mpo_channel_scope_explicit(sel: Any) -> list[str]:
    if not isinstance(sel, list):
        return []
    return [str(x) for x in sel if x and str(x) not in _MPO_ALL_CHANNELS_LEGACY]


def _mpo_scorecard_compare_label(opt: str) -> str:
    """Labels for scorecard comparison: MoM vs YoY only."""
    return {
        "mom": "Month vs month",
        "yoy": "Year vs year",
    }.get(str(opt), str(opt))


def _mpo_sorted_month_key_list(months_raw: list[Any]) -> list[str]:
    """Sorted unique ``YYYY-MM`` keys from raw month values (filters + compare pickers)."""
    seen: set[str] = set()
    out: list[str] = []
    for m in months_raw:
        k = _month_norm_key(m)
        sk = str(k).strip() if k is not None else ""
        if not sk or sk.lower() in ("nan", "nat", "none"):
            continue
        if sk not in seen:
            seen.add(sk)
            out.append(sk)
    return sorted(out, key=_mpo_month_ts_for_sort)


def _mpo_ensure_scorecard_compare_session(key_suffix: str) -> str:
    """Session key ``{suffix}_scorecard_compare``: ``mom`` | ``yoy`` (default **mom**)."""
    k = f"{key_suffix}_scorecard_compare"
    _mig = f"{key_suffix}_scorecard_compare_v3_mom_yoy"
    if _mig not in st.session_state:
        raw = str(st.session_state.get(k, "mom") or "mom")
        st.session_state[k] = "yoy" if raw == "yoy" else "mom"
        st.session_state[_mig] = True
    elif k not in st.session_state:
        st.session_state[k] = "mom"
    v = str(st.session_state.get(k) or "mom")
    if v not in ("mom", "yoy"):
        st.session_state[k] = "mom"
        v = "mom"
    st.session_state[f"{key_suffix}_scorecard_basis"] = "filtered"
    return v


def _mpo_month_picker_options(
    df_date: pd.DataFrame,
    *,
    reporting_start: date,
    reporting_end: date,
) -> list[Any]:
    """Month values present in data only (deduped, newest first, limited to reporting window)."""
    allow_keys = set(_month_norm_keys_in_reporting_window(reporting_start, reporting_end))
    by_k: dict[str, Any] = {}
    if not df_date.empty and "month" in df_date.columns:
        for x in df_date["month"].dropna().unique().tolist():
            if x is None or str(x) in ("NaT", "nan", "None"):
                continue
            nk = str(_month_norm_key(x)).strip()
            if not nk or nk.lower() in ("nan", "nat", "none"):
                continue
            if allow_keys and nk not in allow_keys:
                continue
            if nk not in by_k:
                by_k[nk] = x
    return [by_k[k] for k in sorted(by_k.keys(), key=_mpo_month_ts_for_sort, reverse=True)]


def _apply_marketing_performance_filters(
    df_date: pd.DataFrame,
    *,
    key_suffix: str,
    reporting_start: date,
    reporting_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Performance-tab filters: **market** × **month**."""

    _market_option_blocklist = {
        "unknown",
        "other",
        "country",
        "country breakdown",
        "select all",
    }

    mk_raw = [x for x in df_date["country"].dropna().unique().tolist() if x and x != "Unknown"]
    if mk_raw:
        _mk = (
            df_date.loc[df_date["country"].isin(mk_raw)]
            .assign(
                _ck=lambda d: d["country"].map(_country_join_key),
                _disp=lambda d: d["country"].map(_country_join_key).map(_market_display_from_join_key),
            )
            .loc[
                lambda d: ~d["_disp"]
                .astype(str)
                .str.strip()
                .str.lower()
                .isin(_market_option_blocklist)
            ]
        )
        if "cost" in _mk.columns and not _mk.empty:
            _tot = _mk.assign(_co=lambda d: pd.to_numeric(d["cost"], errors="coerce").fillna(0)).groupby("_disp")["_co"].sum()
            market_opts = sorted(_mk["_disp"].dropna().unique().tolist(), key=lambda c: (-float(_tot.get(c, 0)), str(c).lower()))
        else:
            market_opts = sorted(_mk["_disp"].dropna().unique().tolist())
    else:
        market_opts = []
    # Month picker must follow Supermetrics Ads sheet availability (not CRM-only months).
    _month_source = _mpo_rows_paid_media_from_combined(df_date)
    if _month_source.empty:
        _month_source = df_date
    month_opts = _mpo_month_picker_options(_month_source, reporting_start=reporting_start, reporting_end=reporting_end)

    _k_mpo_market = f"{key_suffix}_market"
    _k_mpo_month = f"{key_suffix}_month"
    if _k_mpo_market not in st.session_state:
        st.session_state[_k_mpo_market] = [_MPO_ALL_GEO_SENTINEL]
    if _k_mpo_month not in st.session_state:
        st.session_state[_k_mpo_month] = [_MPO_ALL_MONTHS_SENTINEL]
    _mpo_normalize_market_multiselect_state(key_suffix)
    _mpo_normalize_month_multiselect_state(key_suffix)
    _mpo_filter_panel = st.container()
    with _mpo_filter_panel:
        _c_mk, _c_mo = st.columns(2, gap="medium")
        with _c_mk:
            st.multiselect(
                "Market",
                [_MPO_ALL_GEO_SENTINEL] + market_opts,
                key=_k_mpo_market,
            )
        with _c_mo:
            st.multiselect(
                "Month",
                [_MPO_ALL_MONTHS_SENTINEL] + month_opts,
                key=_k_mpo_month,
            )

    selected_markets = st.session_state.get(f"{key_suffix}_market", [_MPO_ALL_GEO_SENTINEL])
    selected_months = st.session_state.get(f"{key_suffix}_month", [_MPO_ALL_MONTHS_SENTINEL])

    df = df_date.copy()
    _geo_picks = _mpo_market_scope_countries_only(selected_markets)
    if _geo_picks:
        _pick_keys = {str(_country_join_key(x)).strip().lower() for x in _geo_picks if str(x).strip()}
        _country_keys = df["country"].map(_country_join_key).astype(str).str.strip().str.lower()
        df = df[_country_keys.isin(_pick_keys)]
    if not _mpo_month_multiselect_is_all(selected_months):
        _mo_pick = _mpo_month_multiselect_explicit(selected_months)
        if _mo_pick and "month" in df.columns:
            allow_k = {str(_month_norm_key(m)) for m in _mo_pick if _month_norm_key(m)}
            if allow_k:
                km = df["month"].map(_month_norm_key)
                df = df[km.isin(allow_k) | (km == "")]

    return df, df.copy()


def _mpo_dataframe_from_session_filters(
    df_date: pd.DataFrame,
    *,
    key_suffix: str = "mpo",
) -> pd.DataFrame:
    """Apply **Market** + **Month** session state (same rules as the Marketing performance tab) without rendering widgets."""
    df = df_date.copy()
    selected_markets = st.session_state.get(f"{key_suffix}_market", [_MPO_ALL_GEO_SENTINEL])
    selected_months = st.session_state.get(f"{key_suffix}_month", [_MPO_ALL_MONTHS_SENTINEL])
    _geo_picks = _mpo_market_scope_countries_only(selected_markets)
    if _geo_picks:
        _pick_keys = {str(_country_join_key(x)).strip().lower() for x in _geo_picks if str(x).strip()}
        _country_keys = df["country"].map(_country_join_key).astype(str).str.strip().str.lower()
        df = df[_country_keys.isin(_pick_keys)]
    if not _mpo_month_multiselect_is_all(selected_months):
        _mo_pick = _mpo_month_multiselect_explicit(selected_months)
        if _mo_pick and "month" in df.columns:
            allow_k = {str(_month_norm_key(m)) for m in _mo_pick if _month_norm_key(m)}
            if allow_k:
                km = df["month"].map(_month_norm_key)
                df = df[km.isin(allow_k) | (km == "")]
    return df


def _mpo_load_spend_sheet_for_kpis(
    df_loaded: pd.DataFrame,
    df_date: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str, str]:
    """Load spend/traffic for KPIs.

    Prefer canonical truth-tab rows (XRAY_TRUTH_GID) when they already include spend + clicks/impressions.
    Otherwise blend paid-media spend pools from Supermetrics Ads sheets and merge clicks/impressions.

    Returns ``(spend_sheet_for_kpis, spend_sheet_master, spend_pool_full, sheet_id, fingerprint)``.
    """
    sheet_id, _ = _workbook_id_resolution()
    _fp_mpo = _secret_fingerprint(_service_account_from_streamlit_secrets())
    truth_gid = _default_truth_gid_from_secrets()

    truth_rows = _rows_by_worksheet_id(df_date, int(truth_gid), sheet_id)
    if truth_rows.empty:
        truth_rows = _rows_by_worksheet_id(df_loaded, int(truth_gid), sheet_id)

    if not truth_rows.empty:
        tr = _normalize_master_merge_frame(truth_rows.copy())
        has_cost = "cost" in tr.columns and float(pd.to_numeric(tr["cost"], errors="coerce").fillna(0).abs().sum()) > 1e-9
        has_traffic = (
            "clicks" in tr.columns
            and "impressions" in tr.columns
            and (
                float(pd.to_numeric(tr["clicks"], errors="coerce").fillna(0).abs().sum()) > 1e-9
                or float(pd.to_numeric(tr["impressions"], errors="coerce").fillna(0).abs().sum()) > 1e-9
            )
        )
        # Fast path: if the aggregated truth tab already carries spend, render from it immediately.
        # Do not force connector clicks/impressions merge on every run.
        if has_cost:
            spend_pool_full = tr.copy()
            spend_sheet_master = _filter_spend_for_dashboard(tr, start_date, end_date)
            if spend_sheet_master.empty:
                spend_sheet_master = tr.copy()
            spend_sheet_for_kpis = spend_sheet_master.copy()
            for col in ("clicks", "impressions"):
                if col not in spend_sheet_for_kpis.columns:
                    spend_sheet_for_kpis[col] = 0.0
                spend_sheet_for_kpis[col] = pd.to_numeric(spend_sheet_for_kpis[col], errors="coerce").fillna(0.0)
            if has_traffic:
                return spend_sheet_for_kpis, spend_sheet_master, spend_pool_full, sheet_id, _fp_mpo
            # Even without traffic columns, keep this cheap path so cards render instead of stalling.
            return spend_sheet_for_kpis, spend_sheet_master, spend_pool_full, sheet_id, _fp_mpo

    df_spend_scope_primary = _rows_for_workbook_id(df_date, sheet_id)
    spend_blended_primary = _mpo_blend_paid_media_for_master_df(df_spend_scope_primary)
    spend_blended_all = _mpo_blend_paid_media_for_master_df(df_date)
    sum_primary = _normalized_spend_cost_sum(spend_blended_primary)
    sum_all = _normalized_spend_cost_sum(spend_blended_all)
    spend_blended = spend_blended_all.copy() if sum_all > sum_primary else spend_blended_primary.copy()
    if spend_blended.empty and not df_loaded.empty:
        _dl_primary = _rows_for_workbook_id(df_loaded, sheet_id)
        spend_blended_primary = _mpo_blend_paid_media_for_master_df(_dl_primary) if not _dl_primary.empty else pd.DataFrame()
        spend_blended_all = _mpo_blend_paid_media_for_master_df(df_loaded)
        sum_primary = _normalized_spend_cost_sum(spend_blended_primary)
        sum_all = _normalized_spend_cost_sum(spend_blended_all)
        spend_blended = spend_blended_all.copy() if sum_all > sum_primary else spend_blended_primary.copy()
        if not spend_blended.empty:
            spend_blended = _filter_by_date_range(spend_blended, start_date, end_date)
    if spend_blended.empty:
        spend_pool_full = pd.DataFrame()
        spend_sheet_master = pd.DataFrame()
    else:
        spend_pool_full = spend_blended.copy()
        spend_sheet_master = _filter_spend_for_dashboard(spend_blended, start_date, end_date)
        if spend_sheet_master.empty:
            spend_sheet_master = spend_blended.copy()

    spend_sheet_for_kpis = _mpo_merge_pool_clicks_impressions_onto_spend(
        spend_sheet_master,
        df_loaded,
        primary_sheet_id=sheet_id,
    )
    return spend_sheet_for_kpis, spend_sheet_master, spend_pool_full, sheet_id, _fp_mpo


def _apply_channel_tab_data_scope(
    spend_df: pd.DataFrame,
    *,
    key_suffix: str,
    reporting_start: date,
    reporting_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Spend-by-channel tab: **Channels** + **Month** (same layout as Marketing performance, no market slicer)."""
    if spend_df.empty:
        return spend_df.iloc[0:0].copy(), spend_df.iloc[0:0].copy()

    work = _pmc_dedupe_regional_vs_country_spend(_pmc_filter_middle_east(_pmc_frame_with_metrics(spend_df.copy())))
    if work.empty:
        work = _pmc_frame_with_metrics(spend_df.copy())
    work["_sch"] = _pmc_sheet_channel_series(work)
    work["_sch_disp"] = work["_sch"].map(_market_display_from_join_key).astype(str).str.strip()

    if "cost" in work.columns and not work.empty:
        _tot = (
            work.assign(_co=lambda d: pd.to_numeric(d["cost"], errors="coerce").fillna(0))
            .groupby("_sch_disp")["_co"]
            .sum()
            .sort_values(ascending=False)
        )
        ch_opts = [str(x) for x in _tot.index.tolist() if str(x).strip()]
    else:
        ch_opts = sorted({str(x) for x in work["_sch_disp"].tolist() if str(x).strip()}, key=str.lower)

    month_opts = _mpo_month_picker_options(work, reporting_start=reporting_start, reporting_end=reporting_end)

    _k_pmc_ch = f"{key_suffix}_channel_scope"
    _k_pmc_mo = f"{key_suffix}_month"
    if _k_pmc_ch not in st.session_state:
        st.session_state[_k_pmc_ch] = [_MPO_ALL_CHANNELS_SENTINEL]
    if _k_pmc_mo not in st.session_state:
        st.session_state[_k_pmc_mo] = [_MPO_ALL_MONTHS_SENTINEL]
    _mpo_normalize_month_multiselect_state(key_suffix)
    _mpo_normalize_channel_multiselect_state(key_suffix)

    _pmc_filter_panel = st.container()
    with _pmc_filter_panel:
        _c_ch, _c_mo = st.columns(2, gap="medium")
        with _c_ch:
            st.multiselect(
                "Channels",
                [_MPO_ALL_CHANNELS_SENTINEL] + ch_opts,
                key=_k_pmc_ch,
            )
        with _c_mo:
            st.multiselect(
                "Month",
                [_MPO_ALL_MONTHS_SENTINEL] + month_opts,
                key=_k_pmc_mo,
            )

    selected_ch = st.session_state.get(f"{key_suffix}_channel_scope", [_MPO_ALL_CHANNELS_SENTINEL])
    selected_months = st.session_state.get(f"{key_suffix}_month", [_MPO_ALL_MONTHS_SENTINEL])

    df = work.copy()
    if not _mpo_channel_multiselect_is_all(selected_ch):
        picks = set(_mpo_channel_scope_explicit(selected_ch))
        if picks:
            mch = df["_sch_disp"].isin(picks) | df["_sch"].astype(str).str.strip().isin(picks)
            df = df.loc[mch].copy()
    if not _mpo_month_multiselect_is_all(selected_months):
        _mo_pick = _mpo_month_multiselect_explicit(selected_months)
        if _mo_pick and "month" in df.columns:
            allow_k = {str(_month_norm_key(m)) for m in _mo_pick if _month_norm_key(m)}
            if allow_k:
                km = df["month"].map(_month_norm_key)
                df = df[km.isin(allow_k) | (km == "")]

    df = df.drop(columns=["_sch", "_sch_disp"], errors="ignore")
    return df, df.copy()


def _pmc_spend_scope_for_ask_ai(
    spend_df: pd.DataFrame,
    *,
    key_suffix: str,
    reporting_start: date,
    reporting_end: date,
) -> pd.DataFrame:
    """Same **Channels** / **Month** scope as Spend by channel, without rendering filter widgets (global Ask AI)."""
    if spend_df.empty:
        return spend_df.iloc[0:0].copy()
    work = _pmc_dedupe_regional_vs_country_spend(_pmc_filter_middle_east(_pmc_frame_with_metrics(spend_df.copy())))
    if work.empty:
        work = _pmc_frame_with_metrics(spend_df.copy())
    work["_sch"] = _pmc_sheet_channel_series(work)
    work["_sch_disp"] = work["_sch"].map(_market_display_from_join_key).astype(str).str.strip()
    _mpo_normalize_month_multiselect_state(key_suffix)
    _mpo_normalize_channel_multiselect_state(key_suffix)
    selected_ch = st.session_state.get(f"{key_suffix}_channel_scope", [_MPO_ALL_CHANNELS_SENTINEL])
    selected_months = st.session_state.get(f"{key_suffix}_month", [_MPO_ALL_MONTHS_SENTINEL])
    df = work.copy()
    if not _mpo_channel_multiselect_is_all(selected_ch):
        picks = set(_mpo_channel_scope_explicit(selected_ch))
        if picks:
            mch = df["_sch_disp"].isin(picks) | df["_sch"].astype(str).str.strip().isin(picks)
            df = df.loc[mch].copy()
    if not _mpo_month_multiselect_is_all(selected_months):
        _mo_pick = _mpo_month_multiselect_explicit(selected_months)
        if _mo_pick and "month" in df.columns:
            allow_k = {str(_month_norm_key(m)) for m in _mo_pick if _month_norm_key(m)}
            if allow_k:
                km = df["month"].map(_month_norm_key)
                df = df[km.isin(allow_k) | (km == "")]
    return df.drop(columns=["_sch", "_sch_disp"], errors="ignore")


def _mpo_apply_market_only(df_date: pd.DataFrame, key_suffix: str) -> pd.DataFrame:
    """Same date range as ``df_date``, Market slicer only (no Month filter) — for scorecard comparison months."""
    sm = st.session_state.get(f"{key_suffix}_market", [_MPO_ALL_GEO_SENTINEL])
    out = df_date.copy()
    picks = _mpo_market_scope_countries_only(sm)
    if picks:
        out = out[out["country"].isin(picks)]
    return out


def _mpo_shift_month_key(month_key: Optional[str], delta_months: int) -> Optional[str]:
    if not month_key:
        return None
    try:
        return str(pd.Period(str(_month_norm_key(month_key)), freq="M") + int(delta_months))
    except Exception:
        return None


def _mpo_month_value_to_date(m: Any) -> Optional[date]:
    """First day of the calendar month for a sheet ``month`` cell (for ``st.date_input`` bounds)."""
    k = _month_norm_key(m)
    if not k or str(k).strip().lower() in ("", "nan", "nat"):
        return None
    try:
        return pd.Period(str(k), freq="M").to_timestamp().date()
    except Exception:
        return None


def _mpo_date_to_month_key(d: Any) -> Optional[str]:
    """Normalize a ``date`` (or datetime) from ``st.date_input`` to a month key string."""
    if d is None:
        return None
    try:
        if isinstance(d, datetime):
            d = d.date()
        if not isinstance(d, date):
            return None
        return str(pd.Period(d, freq="M"))
    except Exception:
        return None


def _mpo_month_keys_sorted_master(master_df: pd.DataFrame) -> list[str]:
    if master_df.empty or "month" not in master_df.columns:
        return []
    raw = master_df["month"].map(_month_norm_key).dropna().astype(str).str.strip()
    raw = raw[~raw.str.lower().isin(("", "nan", "nat", "none"))]
    if raw.empty:
        return []
    return sorted({str(x) for x in raw.unique()}, key=_mpo_month_ts_for_sort)


def _mpo_headline_month_keys_for_scope(
    master_df: pd.DataFrame,
    table_df: Optional[pd.DataFrame],
    key_suffix: str,
    *,
    reporting_start: date,
    reporting_end: date,
) -> list[str]:
    """Month keys to **sum** for headline KPIs: all months in the filtered table, or only months explicitly chosen."""
    months_sel = st.session_state.get(f"{key_suffix}_month", [_MPO_ALL_MONTHS_SENTINEL])
    base = (
        table_df
        if table_df is not None and not table_df.empty and "month" in table_df.columns
        else master_df
    )
    cal_win = sorted(_month_norm_keys_in_reporting_window(reporting_start, reporting_end))
    if base.empty or "month" not in base.columns:
        if _mpo_month_multiselect_is_all(months_sel):
            return sorted(cal_win, key=_mpo_month_ts_for_sort)
        ex = _mpo_month_multiselect_explicit(months_sel)
        out = [str(_month_norm_key(m)).strip() for m in ex if _month_norm_key(m)]
        return sorted({k for k in out if k}, key=_mpo_month_ts_for_sort)
    keys = _mpo_month_keys_sorted_master(base)
    if _mpo_month_multiselect_is_all(months_sel):
        return sorted(set(cal_win) | set(keys), key=_mpo_month_ts_for_sort)
    ex = _mpo_month_multiselect_explicit(months_sel)
    out: list[str] = []
    for m in ex:
        k = _month_norm_key(m)
        sk = str(k).strip() if k is not None else ""
        if sk and sk.lower() not in ("", "nan", "nat"):
            out.append(sk)
    return sorted(set(out), key=_mpo_month_ts_for_sort)


def _mpo_compare_month_keys(
    master_df: pd.DataFrame,
    *,
    key_suffix: str,
    table_df: Optional[pd.DataFrame] = None,
) -> tuple[Optional[str], Optional[str], str]:
    """(compare_current_month_key, compare_reference_month_key, ``mom`` | ``yoy``) from expander pickers only."""
    keys_sorted = _mpo_month_keys_sorted_master(master_df)
    _cmp = str(st.session_state.get(f"{key_suffix}_scorecard_compare", "mom") or "mom")
    if _cmp not in ("mom", "yoy"):
        _cmp = "mom"
    if not keys_sorted:
        return None, None, _cmp

    if _cmp == "yoy":
        y1 = int(st.session_state.get(f"{key_suffix}_cmp_yoy_y1") or 0)
        y2 = int(st.session_state.get(f"{key_suffix}_cmp_yoy_y2") or 0)
        mnum = int(st.session_state.get(f"{key_suffix}_cmp_yoy_month") or 1)
        mnum = max(1, min(12, mnum))
        if y1 <= 0 or y2 <= 0:
            return None, None, "yoy"
        return f"{y1}-{mnum:02d}", f"{y2}-{mnum:02d}", "yoy"

    _cur = st.session_state.get(f"{key_suffix}_cmp_mom_cur")
    _ref = st.session_state.get(f"{key_suffix}_cmp_mom_ref")
    cur_k = str(_month_norm_key(_cur)).strip() if _cur is not None else ""
    ref_k = str(_month_norm_key(_ref)).strip() if _ref is not None else ""
    if not cur_k or cur_k.lower() in ("nan", "nat", "none"):
        cur_k = keys_sorted[-1]
    if not ref_k or ref_k.lower() in ("nan", "nat", "none"):
        ref_k = keys_sorted[-2] if len(keys_sorted) >= 2 else keys_sorted[-1]
    return cur_k, ref_k, "mom"


def _mpo_rows_for_norm_month(df: pd.DataFrame, month_key: Optional[str]) -> pd.DataFrame:
    if df.empty or not month_key or "month" not in df.columns:
        return df.iloc[0:0]
    nk = str(_month_norm_key(month_key)).strip()
    return df.loc[df["month"].map(_month_norm_key).astype(str).str.strip() == nk].copy()


def _normalized_post_qual_for_cw_analysis(post_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize post-lead / pipeline tab rows for **CpCW Analysis**-style LF (same sheet as B2/B3)."""
    if post_df.empty:
        return post_df
    w = _ensure_closed_won_from_text_flags(post_df.copy())
    w = _normalize(w)
    return _normalize_master_merge_frame(w)


def _post_qual_cw_analysis_mask(frame: pd.DataFrame) -> pd.Series:
    """Closed Won + Approved (``closed_won`` > 0) — no close-date cutoff; use all rows the sheet provides."""
    if frame.empty or "closed_won" not in frame.columns:
        return pd.Series(False, index=frame.index)
    return pd.to_numeric(frame["closed_won"], errors="coerce").fillna(0) > 0


def _post_qual_cw_analysis_slice(
    post_norm: pd.DataFrame,
    *,
    month_keys: Optional[list[str]],
) -> pd.DataFrame:
    """Deduped post-qual rows in **CpCW Analysis** scope (optional **close month** ∈ ``month_keys``)."""
    if post_norm.empty:
        return post_norm.iloc[0:0]
    base = _post_qual_cw_analysis_mask(post_norm)
    if month_keys:
        want = {str(_month_norm_key(m)).strip() for m in month_keys if str(_month_norm_key(m)).strip()}
        sel = pd.Series(False, index=post_norm.index)
        # Prefer **close month** (ME CpCW Analysis). Keys must match ``_month_norm_key`` (Period.astype(str) does not).
        if "cw_close_date" in post_norm.columns:
            cd = pd.to_datetime(post_norm["cw_close_date"], errors="coerce", dayfirst=True)
            close_m = cd.map(lambda t: _month_norm_key(t) if pd.notna(t) else "")
            sel_close = base & close_m.ne("") & close_m.isin(want)
            if bool(sel_close.any()):
                sel = sel_close
        # If close dates missing / mis-keyed, fall back to dashboard **month** on the post tab (same as spend merge).
        if not bool(sel.any()):
            mk = post_norm.get("month", pd.Series("", index=post_norm.index, dtype=object))
            mk = mk.map(_month_norm_key).astype(str).str.strip()
            sel = base & mk.isin(want)
    else:
        sel = base
    sub = post_norm.loc[sel].copy()
    if sub.empty:
        return sub
    return _dedupe_post_lead_rows(sub)


def _post_qual_closed_won_cw_analysis_count(
    post_norm: pd.DataFrame,
    *,
    month_keys: Optional[list[str]],
) -> int:
    """B2-style CW in the CpCW slice: ``SUM(CW)`` across rows when that exceeds unique opps (same as Q-win)."""
    d = _post_qual_cw_analysis_slice(post_norm, month_keys=month_keys)
    if d.empty or "closed_won" not in d.columns:
        return 0
    sheet = _sum_closed_won_sheet_style(d)
    uniq = _sum_closed_won_unique_opportunities(d)
    return int(sheet if sheet > uniq else uniq)


def _post_qual_first_month_lf_cw_analysis_sum(
    post_norm: pd.DataFrame,
    *,
    month_keys: Optional[list[str]],
) -> float:
    """Σ ``first_month_lf`` on the same slice as B3, aligned with how B2 counts rows (sheet vs unique).

    When ``SUM(CW)`` exceeds unique opportunities, ME / Sheets sum **LF across the same rows** (B3).
    Otherwise de-dupe LF per opportunity (``max`` per key) so pipeline duplicate rows do not inflate Σ LF.
    """
    d = _post_qual_cw_analysis_slice(post_norm, month_keys=month_keys)
    if d.empty or "first_month_lf" not in d.columns:
        return 0.0
    lf = pd.to_numeric(d["first_month_lf"], errors="coerce").fillna(0)
    sheet_cw = _sum_closed_won_sheet_style(d)
    uniq_cw = _sum_closed_won_unique_opportunities(d)
    if bool(sheet_cw > uniq_cw):
        return float(lf.sum())
    keys = _opp_key_columns_for_post_lead(d)
    if keys:
        tmp = d.loc[:, list(keys)].copy()
        tmp["_lf"] = lf
        return float(tmp.groupby(keys, dropna=False)["_lf"].max().sum())
    return float(lf.sum())


def _mpo_leads_for_norm_month(leads_df: pd.DataFrame, month_key: Optional[str]) -> pd.DataFrame:
    if leads_df.empty or not month_key:
        return leads_df.iloc[0:0]
    if "month" in leads_df.columns:
        return _mpo_rows_for_norm_month(leads_df, month_key)
    if "date" not in leads_df.columns:
        return leads_df.iloc[0:0]
    try:
        per = pd.Period(str(month_key), freq="M")
        start = per.start_time.normalize()
        end = per.end_time.normalize()
        s = pd.to_datetime(leads_df["date"], errors="coerce")
        try:
            if getattr(s.dtype, "tz", None) is not None:
                s = s.dt.tz_convert("UTC").dt.tz_localize(None)
        except Exception:
            pass
        return leads_df.loc[(s >= start) & (s <= end)].copy()
    except Exception:
        return leads_df.iloc[0:0]


def _mpo_spend_activity_for_month(spend_df: pd.DataFrame, month_key: Optional[str]) -> tuple[float, int, int]:
    if spend_df.empty or not month_key:
        return 0.0, 0, 0
    sub = _mpo_rows_for_norm_month(spend_df, month_key)
    cost = float(pd.to_numeric(sub["cost"], errors="coerce").fillna(0).sum()) if "cost" in sub.columns else 0.0
    impr = int(pd.to_numeric(sub["impressions"], errors="coerce").fillna(0).sum()) if "impressions" in sub.columns else 0
    clicks = int(pd.to_numeric(sub["clicks"], errors="coerce").fillna(0).sum()) if "clicks" in sub.columns else 0
    return cost, impr, clicks


def _mpo_rows_paid_media_from_combined(df_loaded: pd.DataFrame) -> pd.DataFrame:
    """Rows from paid-media tabs: **all non-CRM tabs** with spend/clicks/impressions (each tab = one platform)."""
    if df_loaded.empty or "source_tab" not in df_loaded.columns:
        return pd.DataFrame()
    stl = df_loaded["source_tab"].astype(str).str.lower()
    is_excl = stl.str.contains(
        r"raw\s*leads?|post\s*qual|raw\s*cw|pipeline|crm\b|opportunity|deal\s*stage|lead\s*sheet",
        na=False,
        regex=True,
    )
    is_summary = stl.str.contains(
        r"^summary$|^readme$|instructions|sheet\s*index|^_",
        na=False,
        regex=True,
    )
    sub = df_loaded.loc[~is_excl & ~is_summary].copy()
    if sub.empty:
        return sub
    # Keep the four primary paid-media platforms explicitly, even when tab naming is non-standard.
    _tl = sub["source_tab"].astype(str).str.lower()
    is_primary_four = (
        _tl.str.contains(r"\bgoogle\b", na=False, regex=True)
        | _tl.str.contains(r"\bmeta\b|facebook|instagram", na=False, regex=True)
        | _tl.str.contains(r"\bsnap(?:chat)?\b", na=False, regex=True)
        | _tl.str.contains(r"linked\s*in|linkedin", na=False, regex=True)
    )
    has_primary_four = bool(is_primary_four.any())
    st_tab = df_loaded["source_tab"].astype(str)
    has_any_ads_data_tab = bool(st_tab.map(lambda t: _tab_title_looks_like_ads_data_sheet(str(t))).any())
    if has_any_ads_data_tab:
        sub = sub.loc[~sub["source_tab"].astype(str).map(_tab_title_is_spend_rollup_tab)].copy()
    if has_primary_four:
        # When four-platform tabs exist, prioritize them for CI aggregation.
        sub = sub.loc[is_primary_four.reindex(sub.index).fillna(False)].copy()
    c = pd.to_numeric(sub.get("cost", 0), errors="coerce").fillna(0)
    cl = pd.to_numeric(sub.get("clicks", 0), errors="coerce").fillna(0)
    im = pd.to_numeric(sub.get("impressions", 0), errors="coerce").fillna(0)
    mask = (c.abs() + cl.abs() + im.abs()) > 1e-9
    return sub.loc[mask]


def _mpo_supermetrics_pool_for_clicks_impressions(
    df_loaded: pd.DataFrame,
    *,
    primary_sheet_id: str,
) -> pd.DataFrame:
    """Paid-media rows from the **Supermetrics workbook only** — used for clicks & impressions (not spend).

    Returns empty if there is no second workbook, ids match the primary, or there are no connector rows.
    """
    ads = (_optional_paid_media_sheet_id_from_secrets() or "").strip()
    prim = str(_extract_sheet_id(primary_sheet_id))
    if not ads or str(_extract_sheet_id(ads)) == prim:
        return pd.DataFrame()
    if "spreadsheet_id" not in df_loaded.columns:
        return pd.DataFrame()
    ads_only = df_loaded.loc[df_loaded["spreadsheet_id"].astype(str) == str(_extract_sheet_id(ads))].copy()
    if ads_only.empty:
        return ads_only
    return _mpo_rows_paid_media_from_combined(ads_only)


def _mpo_coalesce_str_series_with_tab_fallback(ser: pd.Series, tab_labels: pd.Series) -> pd.Series:
    """Keep per-row sheet values when present; otherwise use **tab_labels** (usually ``source_tab`` → platform)."""
    s0 = ser.astype(str).str.strip()
    mask = s0.isin(["", "Unknown", "unknown", "nan", "None", "<NA>"])
    return s0.where(~mask, tab_labels.astype(str).str.strip())


def _mpo_blend_paid_media_for_master_df(df: pd.DataFrame) -> pd.DataFrame:
    """Use stacked workbook rows for spend KPIs; fill ``platform`` / ``channel`` / ``utm_source`` from tab only when row is blank."""
    raw = _mpo_rows_paid_media_from_combined(df)
    if raw.empty:
        return raw
    out = raw.copy()
    tab = out["source_tab"].astype(str).str.strip()
    plat_from_tab = tab.map(_mpo_platform_label_from_source_tab)
    if "platform" in out.columns:
        # Do **not** overwrite row-level Platform (Search vs PMax vs YouTube on the same Google tab).
        out["platform"] = _mpo_coalesce_str_series_with_tab_fallback(out["platform"], plat_from_tab)
    else:
        out["platform"] = plat_from_tab.values
    if "channel" in out.columns:
        ch = out["channel"].astype(str).str.strip()
        mask = ch.isin(["", "Unknown", "unknown", "nan", "None", "<NA>"])
        # Empty lets ``_pmc_sheet_channel_series`` fall back to **platform** (from tab) for Spend-by-channel pivots.
        out.loc[mask, "channel"] = ""
    else:
        out["channel"] = ""
    if "utm_source" in out.columns:
        out["utm_source"] = _mpo_coalesce_str_series_with_tab_fallback(out["utm_source"], plat_from_tab)
    else:
        out["utm_source"] = plat_from_tab.values
    return out


def _mpo_allocate_monthly_metrics_by_cost_weight(spend_df: pd.DataFrame, month_totals: pd.DataFrame) -> pd.DataFrame:
    """Spread pool monthly clicks/impressions onto spend rows by cost share within each month.

    Used when Supermetrics exports have no usable **country** (all ``Unknown``) so month×country merge fails.
    """
    if spend_df.empty or month_totals.empty:
        return spend_df
    out = spend_df.copy()
    out["clicks"] = pd.to_numeric(out.get("clicks", 0), errors="coerce").fillna(0)
    out["impressions"] = pd.to_numeric(out.get("impressions", 0), errors="coerce").fillna(0)
    mo = out["month"].map(_month_norm_key)
    for _, row in month_totals.iterrows():
        mk = _month_norm_key(row.get("month"))
        if not mk or str(mk).strip().lower() in ("nan", "nat", "none"):
            continue
        Tc = float(row.get("clicks", 0) or 0)
        Ti = float(row.get("impressions", 0) or 0)
        if Tc < 1e-9 and Ti < 1e-9:
            continue
        ix = out.index[mo == mk]
        if len(ix) == 0:
            continue
        if "cost" in out.columns:
            cost = pd.to_numeric(out.loc[ix, "cost"], errors="coerce").fillna(0)
        else:
            cost = pd.Series(1.0, index=ix)
        total_c = float(cost.sum())
        if total_c > 1e-9:
            w = cost / total_c
        else:
            w = pd.Series(1.0 / max(len(ix), 1), index=ix)
        out.loc[ix, "clicks"] = (Tc * w).values
        out.loc[ix, "impressions"] = (Ti * w).values
    return out


def _mpo_merge_pool_clicks_impressions_onto_spend(
    spend_master: pd.DataFrame,
    df_loaded: pd.DataFrame,
    *,
    primary_sheet_id: Optional[str] = None,
) -> pd.DataFrame:
    """Attach **Supermetrics-only** clicks/impressions to **X-Ray spend** rows.

    Spend amounts always come from the primary workbook slice passed in; any clicks/impressions on those rows
    (from X-Ray column mapping) are cleared, then values are merged from the connector workbook.

    Tries **month × country** first, then **month-only** allocation by **X-Ray cost** weight within each month.
    """
    if spend_master.empty:
        return spend_master
    if primary_sheet_id is None:
        primary_sheet_id, _ = _workbook_id_resolution()
    prim = str(_extract_sheet_id(str(primary_sheet_id)))

    pool = _mpo_supermetrics_pool_for_clicks_impressions(df_loaded, primary_sheet_id=prim)
    # Cloud safety: if the second-workbook split is misconfigured, fall back to paid-media rows
    # from any loaded workbook so clicks/impressions are not forced to zero.
    if pool.empty:
        pool = _mpo_rows_paid_media_from_combined(df_loaded)
    else:
        _pc = float(pd.to_numeric(pool.get("clicks", 0), errors="coerce").fillna(0).sum())
        _pi = float(pd.to_numeric(pool.get("impressions", 0), errors="coerce").fillna(0).sum())
        if _pc < 1e-9 and _pi < 1e-9:
            pool_any = _mpo_rows_paid_media_from_combined(df_loaded)
            if not pool_any.empty:
                pool = pool_any
    out = _normalize_master_merge_frame(spend_master.copy())
    # Keep X-Ray clicks/impressions when Supermetrics is unavailable.
    for col in ("clicks", "impressions"):
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    if pool.empty:
        return out
    pn = _normalize_master_merge_frame(pool.copy())
    if pn.empty or "month" not in pn.columns:
        return out
    # Some connector tabs expose CTR + impressions without a clicks column.
    # Recover clicks so scorecards/charts do not stay at zero.
    if "ctr" in pn.columns:
        _cur_clicks = pd.to_numeric(pn.get("clicks", 0), errors="coerce").fillna(0)
        _cur_impr = pd.to_numeric(pn.get("impressions", 0), errors="coerce").fillna(0)
        if float(_cur_clicks.abs().sum()) < 1e-9 and float(_cur_impr.abs().sum()) > 1e-9:
            _ctr = pd.to_numeric(pn["ctr"], errors="coerce").fillna(0.0)
            _q95 = float(_ctr.abs().quantile(0.95)) if len(_ctr) else 0.0
            _ctr_ratio = _ctr if _q95 <= 1.0 else (_ctr / 100.0)
            pn["clicks"] = (_cur_impr * _ctr_ratio).clip(lower=0)
    pool_clicks = float(pd.to_numeric(pn.get("clicks", 0), errors="coerce").fillna(0).sum())
    pool_impr = float(pd.to_numeric(pn.get("impressions", 0), errors="coerce").fillna(0).sum())
    if pool_clicks < 1e-9 and pool_impr < 1e-9:
        return out
    # Supermetrics is present: reset CI and refill from connector rows.
    out["clicks"] = 0.0
    out["impressions"] = 0.0

    merged_ok = False
    if "country" in pn.columns and "country" in out.columns:
        g = pn.groupby(["month", "country"], as_index=False).agg(
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
        out = out.merge(g, on=["month", "country"], how="left", suffixes=("", "_p"))
        for col in ("clicks", "impressions"):
            cp = f"{col}_p"
            if cp in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0) + pd.to_numeric(out[cp], errors="coerce").fillna(0)
                out = out.drop(columns=[cp], errors="ignore")
        merged_ok = float(pd.to_numeric(out["clicks"], errors="coerce").fillna(0).sum()) > 1e-6 or float(
            pd.to_numeric(out["impressions"], errors="coerce").fillna(0).sum()
        ) > 1e-6

    if not merged_ok:
        g_m = pn.groupby("month", as_index=False).agg(
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
        out = _mpo_allocate_monthly_metrics_by_cost_weight(out, g_m)
    return out


def _mpo_spend_sheet_for_channel_master(df_loaded: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    """Spend-by-channel source: force ME X-Ray workbook **gid=0 Spend** rows for channel/spend truth."""
    sheet_id, _ = _workbook_id_resolution()
    _fp = _secret_fingerprint(_service_account_from_streamlit_secrets())
    spend_gid0 = load_spend_gid0_normalized(sheet_id, _fp)
    if spend_gid0.empty:
        # Fallback to already-loaded rows only if gid=0 fetch fails.
        spend_gid0 = _mpo_blend_paid_media_for_master_df(_rows_for_workbook_id(df_loaded, sheet_id))
    if spend_gid0.empty:
        return pd.DataFrame()
    spend_sheet_master = _filter_spend_for_dashboard(spend_gid0, start_date, end_date)
    if spend_sheet_master.empty:
        spend_sheet_master = spend_gid0.copy()
    return spend_sheet_master


def _mpo_render_paid_media_by_platform_summary(
    df_loaded: pd.DataFrame,
    *,
    key_suffix: str,
    primary_sheet_id: Optional[str] = None,
) -> None:
    """Per-platform table from the **Supermetrics** workbook (connector spend + CI on each network tab)."""
    if primary_sheet_id is None:
        primary_sheet_id, _ = _workbook_id_resolution()
    pool = _mpo_supermetrics_pool_for_clicks_impressions(df_loaded, primary_sheet_id=str(primary_sheet_id))
    if pool.empty or "source_tab" not in pool.columns:
        return
    rows: list[dict[str, Any]] = []
    for tab, sub in pool.groupby("source_tab", dropna=False):
        sp = float(pd.to_numeric(sub.get("cost", 0), errors="coerce").fillna(0).sum())
        cl = int(pd.to_numeric(sub.get("clicks", 0), errors="coerce").fillna(0).sum())
        im = int(pd.to_numeric(sub.get("impressions", 0), errors="coerce").fillna(0).sum())
        plat = _mpo_platform_label_from_source_tab(str(tab))
        ctr = (cl / im * 100.0) if im else None
        cpc = (sp / cl) if cl else None
        rows.append(
            {
                "Platform": plat,
                "Spend": sp,
                "Impressions": im,
                "Clicks": cl,
                "CTR %": ctr,
                "CPC": cpc,
            }
        )
    tab_df = pd.DataFrame(rows)
    if tab_df.empty:
        return
    tab_df = tab_df.sort_values("Spend", ascending=False)
    tab_df["Spend"] = tab_df["Spend"].map(
        lambda v: _format_spend_k(float(v)) if v is not None and pd.notna(v) else "—"
    )
    tab_df["CTR %"] = tab_df["CTR %"].map(lambda v: f"{v:.2f}" if v is not None and pd.notna(v) else "—")
    tab_df["CPC"] = tab_df["CPC"].map(
        lambda v: f"${v:,.2f}" if v is not None and pd.notna(v) and abs(float(v)) != float("inf") else "—"
    )
    with st.expander("Paid media KPIs by platform (Supermetrics)", expanded=False):
        st.caption(
            "Breakdown from the **Supermetrics** connector. **Headline Spend** on the scorecard uses **ME X-Ray** only; "
            "headline **Impressions / Clicks** use Supermetrics totals merged onto X-Ray spend."
        )
        st.dataframe(
            tab_df,
            width="stretch",
            hide_index=True,
            key=f"{key_suffix}_df_platform_kpi",
        )


def _debug_column_hints_for_metrics(df: pd.DataFrame) -> tuple[str, str]:
    """Short strings listing raw column names that look like clicks vs impressions (for debug)."""
    if df.empty or not len(df.columns):
        return "(none)", "(none)"
    click_cols: list[str] = []
    impr_cols: list[str] = []
    for c in df.columns:
        nk = _norm_header_key(str(c))
        if any(k in nk for k in ("click", "swipe")) and "ctr" not in nk:
            click_cols.append(str(c))
        if "impr" in nk or "impression" in nk:
            impr_cols.append(str(c))

    def _trunc(xs: list[str], n: int = 14) -> str:
        if not xs:
            return "(none detected)"
        s = ", ".join(xs[:n])
        return s + ("…" if len(xs) > n else "")

    return _trunc(click_cols), _trunc(impr_cols)


def _mpo_render_supermetrics_debug_pane(
    df_loaded: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
    key_suffix: str,
    primary_sheet_id: str,
) -> None:
    """Expandable diagnostics for Supermetrics gid tabs: parse totals vs merged frame, month coverage."""
    ads_id = (_optional_paid_media_sheet_id_from_secrets() or "").strip()
    if not ads_id or str(_extract_sheet_id(ads_id)) == str(_extract_sheet_id(primary_sheet_id)):
        return
    _fp = _secret_fingerprint(_service_account_from_streamlit_secrets())
    gid_labels: dict[int, str] = {
        0: "Google Ads",
        1802364778: "Meta Ads",
        1720904536: "Snapchat Ads",
        279936880: "LinkedIn Ads",
    }
    sheet_url_base = f"https://docs.google.com/spreadsheets/d/{_extract_sheet_id(ads_id)}/edit"
    with st.expander("Debug: Supermetrics paid-media (by gid)", expanded=False):
        st.caption(
            f"Reporting window: **{start_date:%Y-%m-%d}** → **{end_date:%Y-%m-%d}**. "
            f"Paid workbook id: `{_extract_sheet_id(ads_id)}`. Build: `{DASHBOARD_BUILD}`."
        )
        rows_out: list[dict[str, Any]] = []
        for gid in DEFAULT_PAID_MEDIA_PLATFORM_GIDS:
            label = gid_labels.get(int(gid), f"gid {gid}")
            tab_title = ""
            try:
                norm = load_worksheet_by_gid_preprocessed(ads_id, int(gid), _fp)
            except Exception as exc:
                norm = pd.DataFrame()
                tab_title = f"load error: {exc}"
            if norm is not None and not norm.empty:
                tab_title = str(norm["source_tab"].iloc[0]) if "source_tab" in norm.columns else tab_title
            n_norm = int(len(norm)) if norm is not None and not norm.empty else 0
            sc = float(pd.to_numeric(norm.get("cost", 0), errors="coerce").fillna(0).sum()) if n_norm else 0.0
            scl = int(pd.to_numeric(norm.get("clicks", 0), errors="coerce").fillna(0).sum()) if n_norm else 0
            sim = int(pd.to_numeric(norm.get("impressions", 0), errors="coerce").fillna(0).sum()) if n_norm else 0
            months_u: str = ""
            if n_norm and "month" in norm.columns:
                u = sorted(
                    {str(_month_norm_key(x)) for x in norm["month"].tolist() if str(_month_norm_key(x)).strip()},
                    key=_mpo_month_ts_for_sort,
                )
                if u:
                    months_u = f"{u[0]} → {u[-1]} ({len(u)} mo.)"
                    if len(u) > 8:
                        months_u += " · " + ", ".join(u[-8:])
                else:
                    months_u = "—"
            ch_click, ch_impr = _debug_column_hints_for_metrics(norm if n_norm else pd.DataFrame())

            n_merged = 0
            sm_sc = sm_cl = sm_im = 0
            if not df_loaded.empty and "spreadsheet_id" in df_loaded.columns and "worksheet_gid" in df_loaded.columns:
                wg = pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce")
                m = (df_loaded["spreadsheet_id"].astype(str) == str(_extract_sheet_id(ads_id))) & (wg == int(gid))
                sub = df_loaded.loc[m]
                n_merged = int(len(sub))
                if n_merged:
                    sm_sc = float(pd.to_numeric(sub.get("cost", 0), errors="coerce").fillna(0).sum())
                    sm_cl = int(pd.to_numeric(sub.get("clicks", 0), errors="coerce").fillna(0).sum())
                    sm_im = int(pd.to_numeric(sub.get("impressions", 0), errors="coerce").fillna(0).sum())

            rows_out.append(
                {
                    "Platform": label,
                    "gid": int(gid),
                    "Tab (normalized)": tab_title[:80] + ("…" if len(str(tab_title)) > 80 else ""),
                    "rows (gid load)": n_norm,
                    "rows (in df_loaded)": n_merged,
                    "Σ cost (load)": round(sc, 2),
                    "Σ clicks (load)": scl,
                    "Σ impr (load)": sim,
                    "Σ cost (merged)": round(sm_sc, 2),
                    "Σ clicks (merged)": sm_cl,
                    "Σ impr (merged)": sm_im,
                    "months (sample)": months_u or "—",
                    "cols ~clicks": ch_click[:120],
                    "cols ~impr": ch_impr[:120],
                    "open tab": f"{sheet_url_base}?gid={gid}",
                }
            )
        dbg = pd.DataFrame(rows_out)
        st.dataframe(dbg, width="stretch", hide_index=True, key=f"{key_suffix}_df_sm_debug")
        st.caption(
            "Compare **gid load** (fresh `load_worksheet_by_gid_preprocessed`) vs **merged** (rows in `df_loaded` after concat + date filter). "
            "Large gaps usually mean date/month filtering dropped rows or duplicate header parsing differed between full-workbook load and gid reload. "
            "**Row counts** are **data rows** in the tab (usually **one row per day** in time-series exports). "
            "Supermetrics **wide** layouts put **many campaigns in columns**, not extra rows — hundreds of rows with hundreds of columns is normal."
        )


def _mpo_master_month_sums(master_df: pd.DataFrame, month_key: Optional[str]) -> dict[str, float]:
    out = {"cost": 0.0, "leads": 0.0, "qualified": 0.0, "closed_won": 0.0, "tcv": 0.0, "first_month_lf": 0.0}
    if master_df.empty or not month_key:
        return out
    sub = _mpo_rows_for_norm_month(master_df, month_key)
    if sub.empty:
        return out
    for c in list(out.keys()):
        if c in sub.columns:
            out[c] = float(pd.to_numeric(sub[c], errors="coerce").fillna(0).sum())
    return out


def _mpo_pipeline_month_totals(post_sub: pd.DataFrame) -> dict[str, int]:
    def _si(col: str) -> int:
        if post_sub.empty or col not in post_sub.columns:
            return 0
        return int(pd.to_numeric(post_sub[col], errors="coerce").fillna(0).sum())

    qual = _si("qualifying")
    pitch = _si("pitching")
    nego = _si("negotiation")
    commit = _si("commitment")
    return {
        "total_live": qual + pitch + nego + commit,
        "qualifying": qual,
        "pitching": pitch,
        "negotiation": nego,
        "commitment": commit,
        "closed_lost": _si("closed_lost"),
        "cw": _sum_closed_won_unique_opportunities(_dedupe_post_lead_rows(post_sub)),
    }


def _kpi_two_month_compare_dict(
    cur_k: Optional[str],
    ref_k: Optional[str],
    *,
    spend_df: pd.DataFrame,
    leads_df: pd.DataFrame,
    post_df_kpi: pd.DataFrame,
    cw_kpi: pd.DataFrame,
) -> dict[str, Optional[float]]:
    """Scorecard delta pairs: **current** month vs **reference** month (MoM or YoY), using market-wide slices."""
    out: dict[str, Optional[float]] = {}
    if not cur_k or not ref_k:
        return out

    sc, ic, cc = _mpo_spend_activity_for_month(spend_df, cur_k)
    sp, ip, cp = _mpo_spend_activity_for_month(spend_df, ref_k)
    out["mom_spend_c"], out["mom_spend_p"] = sc, sp
    out["mom_impr_c"], out["mom_impr_p"] = float(ic), float(ip)
    out["mom_clicks_c"], out["mom_clicks_p"] = float(cc), float(cp)
    ctr_c = (cc / ic * 100.0) if ic else None
    ctr_p = (cp / ip * 100.0) if ip else None
    out["mom_ctr_c"], out["mom_ctr_p"] = ctr_c, ctr_p

    ld_c = _mpo_leads_for_norm_month(leads_df, cur_k)
    ld_p = _mpo_leads_for_norm_month(leads_df, ref_k)
    lr_c, lr_p = float(len(ld_c)), float(len(ld_p))
    qc = float(_qualified_count_from_leads(ld_c))
    qp = float(_qualified_count_from_leads(ld_p))
    out["mom_leads_rows_c"], out["mom_leads_rows_p"] = lr_c, lr_p
    out["mom_qual_status_c"], out["mom_qual_status_p"] = qc, qp
    out["mom_leads_c"], out["mom_leads_p"] = lr_c, lr_p
    out["mom_qualified_c"], out["mom_qualified_p"] = qc, qp
    out["mom_nw_c"] = float(_new_working_count_from_leads(ld_c))
    out["mom_nw_p"] = float(_new_working_count_from_leads(ld_p))

    sql_c = (qc / lr_c * 100.0) if lr_c > 0 else None
    sql_p = (qp / lr_p * 100.0) if lr_p > 0 else None
    out["mom_sql_pct_c"], out["mom_sql_pct_p"] = sql_c, sql_p

    cpl_c = (sc / lr_c) if lr_c > 0 else None
    cpl_p = (sp / lr_p) if lr_p > 0 else None
    out["mom_cpl_c"], out["mom_cpl_p"] = cpl_c, cpl_p
    cps_c = (sc / qc) if qc > 0 else None
    cps_p = (sp / qp) if qp > 0 else None
    out["mom_cpsql_c"], out["mom_cpsql_p"] = cps_c, cps_p

    post_c = _dedupe_post_lead_rows(_mpo_rows_for_norm_month(post_df_kpi, cur_k))
    post_p = _dedupe_post_lead_rows(_mpo_rows_for_norm_month(post_df_kpi, ref_k))
    pipe_c = _mpo_pipeline_month_totals(post_c)
    pipe_p = _mpo_pipeline_month_totals(post_p)
    post_norm_cmp = _normalized_post_qual_for_cw_analysis(post_df_kpi)
    cw_c = _post_qual_closed_won_cw_analysis_count(post_norm_cmp, month_keys=[cur_k] if cur_k else None)
    cw_p = _post_qual_closed_won_cw_analysis_count(post_norm_cmp, month_keys=[ref_k] if ref_k else None)
    if cw_c <= 0:
        cw_c = int(pipe_c["cw"])
    if cw_p <= 0:
        cw_p = int(pipe_p["cw"])
    out["mom_cw_c"], out["mom_cw_p"] = float(cw_c), float(cw_p)
    out["mom_live_c"], out["mom_live_p"] = float(pipe_c["total_live"]), float(pipe_p["total_live"])
    out["mom_nego_c"], out["mom_nego_p"] = float(pipe_c["negotiation"]), float(pipe_p["negotiation"])
    out["mom_commit_c"], out["mom_commit_p"] = float(pipe_c["commitment"]), float(pipe_p["commitment"])
    out["mom_clost_c"], out["mom_clost_p"] = float(pipe_c["closed_lost"]), float(pipe_p["closed_lost"])

    cw_sub_c = _mpo_rows_for_norm_month(cw_kpi, cur_k)
    cw_sub_p = _mpo_rows_for_norm_month(cw_kpi, ref_k)
    tcv_c = float(pd.to_numeric(cw_sub_c["tcv"], errors="coerce").fillna(0).sum()) if "tcv" in cw_sub_c.columns else 0.0
    tcv_p = float(pd.to_numeric(cw_sub_p["tcv"], errors="coerce").fillna(0).sum()) if "tcv" in cw_sub_p.columns else 0.0
    lf_c = _post_qual_first_month_lf_cw_analysis_sum(post_norm_cmp, month_keys=[cur_k] if cur_k else None)
    lf_p = _post_qual_first_month_lf_cw_analysis_sum(post_norm_cmp, month_keys=[ref_k] if ref_k else None)
    if lf_c <= 0:
        lf_c = (
            float(pd.to_numeric(cw_sub_c["first_month_lf"], errors="coerce").fillna(0).sum())
            if "first_month_lf" in cw_sub_c.columns
            else 0.0
        )
    if lf_p <= 0:
        lf_p = (
            float(pd.to_numeric(cw_sub_p["first_month_lf"], errors="coerce").fillna(0).sum())
            if "first_month_lf" in cw_sub_p.columns
            else 0.0
        )
    out["mom_tcv_c"], out["mom_tcv_p"] = tcv_c, tcv_p
    out["mom_lf_c"], out["mom_lf_p"] = lf_c, lf_p

    cpcw_c = (sc / float(cw_c)) if cw_c > 0 else None
    cpcw_p = (sp / float(cw_p)) if cw_p > 0 else None
    out["mom_cpcw_c"], out["mom_cpcw_p"] = cpcw_c, cpcw_p
    cpcwlf_c = (sc / lf_c) if lf_c > 0 else None
    cpcwlf_p = (sp / lf_p) if lf_p > 0 else None
    out["mom_cpcwlf_c"], out["mom_cpcwlf_p"] = cpcwlf_c, cpcwlf_p
    pct_c = (sc / tcv_c * 100.0) if tcv_c > 0 else None
    pct_p = (sp / tcv_p * 100.0) if tcv_p > 0 else None
    out["mom_spend_tcv_pct_c"], out["mom_spend_tcv_pct_p"] = pct_c, pct_p

    _cwq_c, _qq_c = _q_win_rate_inputs(post_c, leads_df)
    _cwq_p, _qq_p = _q_win_rate_inputs(post_p, leads_df)
    qw_c = (float(_cwq_c) / float(_qq_c) * 100.0) if _qq_c else None
    qw_p = (float(_cwq_p) / float(_qq_p) * 100.0) if _qq_p else None
    out["mom_qwin_c"], out["mom_qwin_p"] = qw_c, qw_p

    return out


def _mpo_scorecard_headline_totals_for_month(
    cur_k: Optional[str],
    *,
    spend_df: pd.DataFrame,
    leads_df: pd.DataFrame,
    post_df_kpi: pd.DataFrame,
    cw_kpi: pd.DataFrame,
) -> Optional[dict[str, Any]]:
    """Big KPI numbers for **one** month — same slices as ``_kpi_two_month_compare_dict`` current side."""
    if not cur_k:
        return None
    sc, ic, cc = _mpo_spend_activity_for_month(spend_df, cur_k)
    total_impr = int(ic)
    total_clicks = int(cc)
    ctr = (total_clicks / total_impr * 100.0) if total_impr else 0.0
    ld_c = _mpo_leads_for_norm_month(leads_df, cur_k)
    total_leads = int(len(ld_c))
    total_qualified = int(_qualified_count_from_leads(ld_c))
    cpc = (sc / total_clicks) if total_clicks else 0.0
    cpl = (sc / total_leads) if total_leads else 0.0
    cpsql = (sc / total_qualified) if total_qualified else 0.0
    post_c = _dedupe_post_lead_rows(_mpo_rows_for_norm_month(post_df_kpi, cur_k))
    pipe_c = _mpo_pipeline_month_totals(post_c)
    # LOCKED CW logic: scoped post-lead rows, stage Closed Won + Approved only.
    total_cw = 0
    cw_sub = _mpo_rows_for_norm_month(cw_kpi, cur_k)
    total_tcv = (
        float(pd.to_numeric(cw_sub["tcv"], errors="coerce").fillna(0).sum()) if not cw_sub.empty and "tcv" in cw_sub.columns else 0.0
    )
    total_first_month_lf = (
        float(pd.to_numeric(cw_sub["first_month_lf"], errors="coerce").fillna(0).sum())
        if not cw_sub.empty and "first_month_lf" in cw_sub.columns
        else 0.0
    )
    post_norm_cw = _normalized_post_qual_for_cw_analysis(post_df_kpi)
    lf_cw = _post_qual_first_month_lf_cw_analysis_sum(post_norm_cw, month_keys=[cur_k] if cur_k else None)
    if lf_cw > 0:
        total_first_month_lf = float(lf_cw)
    cw_cw = _post_qual_closed_won_cw_analysis_count(post_norm_cw, month_keys=[cur_k] if cur_k else None)
    total_cw = int(cw_cw)
    cw_q, qual_q = _q_win_rate_inputs(post_c, leads_df)
    return {
        "total_spend": float(sc),
        "total_impr": total_impr,
        "total_clicks": total_clicks,
        "ctr": float(ctr),
        "total_leads": total_leads,
        "total_qualified": total_qualified,
        "total_cw": total_cw,
        "total_tcv": total_tcv,
        "total_first_month_lf": total_first_month_lf,
        "cpc": float(cpc),
        "cpl": float(cpl),
        "cpsql": float(cpsql),
        "total_new_working": int(_new_working_count_from_leads(ld_c)),
        "total_pitching": int(pipe_c["pitching"]),
        "total_negotiation": int(pipe_c["negotiation"]),
        "total_commitment": int(pipe_c["commitment"]),
        "total_qualifying": int(pipe_c["qualifying"]),
        "total_total_live": int(pipe_c["total_live"]),
        "total_closed_lost": int(pipe_c["closed_lost"]),
        "cw_for_qwin": int(cw_q) if cw_q else None,
        "qual_for_qwin": int(qual_q) if qual_q else None,
    }


def _mpo_scorecard_headline_totals_for_months(
    month_keys: list[str],
    *,
    spend_df: pd.DataFrame,
    leads_df: pd.DataFrame,
    post_df_kpi: pd.DataFrame,
    cw_kpi: pd.DataFrame,
) -> Optional[dict[str, Any]]:
    """Big KPI numbers summed across **multiple** months (all months in range, or a multi-month pick)."""
    if not month_keys:
        return None
    ts, ti, tc = 0.0, 0, 0
    for mk in month_keys:
        sc, ic, cc = _mpo_spend_activity_for_month(spend_df, mk)
        ts += float(sc)
        ti += int(ic)
        tc += int(cc)
    total_leads = 0
    total_qualified = 0
    total_new_working = 0
    for mk in month_keys:
        ld = _mpo_leads_for_norm_month(leads_df, mk)
        total_leads += int(len(ld))
        total_qualified += int(_qualified_count_from_leads(ld))
        total_new_working += int(_new_working_count_from_leads(ld))
    ctr = (tc / ti * 100.0) if ti else 0.0
    cpc = (ts / tc) if tc else 0.0
    cpl = (ts / total_leads) if total_leads else 0.0
    cpsql = (ts / total_qualified) if total_qualified else 0.0
    post_frames: list[pd.DataFrame] = []
    for mk in month_keys:
        post_frames.append(_mpo_rows_for_norm_month(post_df_kpi, mk))
    post_all = pd.concat(post_frames, ignore_index=True) if post_frames else pd.DataFrame()
    post_all = _dedupe_post_lead_rows(post_all)
    pipe_c = _mpo_pipeline_month_totals(post_all)
    cw_parts: list[pd.DataFrame] = []
    for mk in month_keys:
        cw_parts.append(_mpo_rows_for_norm_month(cw_kpi, mk))
    cw_sub = pd.concat(cw_parts, ignore_index=True) if cw_parts else pd.DataFrame()
    total_tcv = (
        float(pd.to_numeric(cw_sub["tcv"], errors="coerce").fillna(0).sum())
        if not cw_sub.empty and "tcv" in cw_sub.columns
        else 0.0
    )
    total_first_month_lf = (
        float(pd.to_numeric(cw_sub["first_month_lf"], errors="coerce").fillna(0).sum())
        if not cw_sub.empty and "first_month_lf" in cw_sub.columns
        else 0.0
    )
    post_norm_cw = _normalized_post_qual_for_cw_analysis(post_df_kpi)
    # Headline B2/B3 must use the **same calendar window** as Total Marketing Spend (``month_keys``).
    # Preferring ``month_keys=None`` first paired **scoped** spend with **all-time** Σ LF and crushed CpCW:LF (e.g. ~0.007 vs ~0.92).
    cw_scoped = _post_qual_closed_won_cw_analysis_count(post_norm_cw, month_keys=month_keys)
    lf_scoped = _post_qual_first_month_lf_cw_analysis_sum(post_norm_cw, month_keys=month_keys)
    lf_from_cw_tab = float(total_first_month_lf)

    # LOCKED CW logic: scoped post-lead rows, stage Closed Won + Approved only.
    total_cw_out = int(cw_scoped)
    lf_pick = 0.0
    if total_cw_out > 0:
        if lf_scoped > 0:
            lf_pick = float(lf_scoped)
        elif lf_from_cw_tab > 0:
            lf_pick = lf_from_cw_tab
    if lf_pick > 0:
        total_first_month_lf = float(lf_pick)
    cw_q, qual_q = _q_win_rate_inputs(post_all, leads_df)
    return {
        "total_spend": float(ts),
        "total_impr": int(ti),
        "total_clicks": int(tc),
        "ctr": float(ctr),
        "total_leads": int(total_leads),
        "total_qualified": int(total_qualified),
        "total_cw": total_cw_out,
        "total_tcv": float(total_tcv),
        "total_first_month_lf": float(total_first_month_lf),
        "cpc": float(cpc),
        "cpl": float(cpl),
        "cpsql": float(cpsql),
        "total_new_working": int(total_new_working),
        "total_pitching": int(pipe_c["pitching"]),
        "total_negotiation": int(pipe_c["negotiation"]),
        "total_commitment": int(pipe_c["commitment"]),
        "total_qualifying": int(pipe_c["qualifying"]),
        "total_total_live": int(pipe_c["total_live"]),
        "total_closed_lost": int(pipe_c["closed_lost"]),
        "cw_for_qwin": int(cw_q) if cw_q else None,
        "qual_for_qwin": int(qual_q) if qual_q else None,
    }


def _kpi_funnel_delta_html(
    cur: Optional[float],
    prev: Optional[float],
    *,
    vs_label: str = "prior month",
    disabled: bool = False,
) -> str:
    """Green/red % change vs reference period; ``vs_label`` e.g. ``prior month`` or ``same month last year``."""
    if disabled:
        return '<div class="kpi-funnel-delta kpi-funnel-delta--off" aria-hidden="true"></div>'
    esc = html.escape(vs_label, quote=True)
    if cur is None or prev is None:
        return f'<div class="kpi-funnel-delta kpi-funnel-delta--na">— vs {esc}</div>'
    if prev == 0.0:
        if cur == 0.0:
            return f'<div class="kpi-funnel-delta kpi-funnel-delta--flat">→ 0% vs {esc}</div>'
        return f'<div class="kpi-funnel-delta kpi-funnel-delta--up">↑ new vs {esc}</div>'
    pct = (cur - prev) / prev * 100.0
    if pct > 0.05:
        cls, arr = "kpi-funnel-delta--up", "↑"
    elif pct < -0.05:
        cls, arr = "kpi-funnel-delta--down", "↓"
    else:
        cls, arr = "kpi-funnel-delta--flat", "→"
    return f'<div class="kpi-funnel-delta {cls}">{arr} {pct:+.1f}% vs {esc}</div>'


def _kpi_funnel_delta_pill_html(
    cur: Optional[float],
    prev: Optional[float],
    *,
    vs_label: str = "prior month",
    disabled: bool = False,
) -> str:
    """Pill badge for % change + grey ``vs …`` label (traffic hero scorecards)."""
    if disabled:
        return '<div class="kpi-funnel-delta kpi-funnel-delta--pill-wrap kpi-funnel-delta--off" aria-hidden="true"></div>'
    esc = html.escape(vs_label, quote=True)
    if cur is None or prev is None:
        return (
            f'<div class="kpi-funnel-delta kpi-funnel-delta--pill-wrap kpi-funnel-delta--na">'
            f'<span class="kpi-funnel-delta-vs">— vs {esc}</span></div>'
        )
    if prev == 0.0:
        if cur == 0.0:
            return (
                f'<div class="kpi-funnel-delta kpi-funnel-delta--pill-wrap">'
                f'<span class="kpi-funnel-delta-pill kpi-funnel-delta-pill--flat">→ 0%</span>'
                f'<span class="kpi-funnel-delta-vs">vs {esc}</span></div>'
            )
        return (
            f'<div class="kpi-funnel-delta kpi-funnel-delta--pill-wrap">'
            f'<span class="kpi-funnel-delta-pill kpi-funnel-delta-pill--up">↑ new</span>'
            f'<span class="kpi-funnel-delta-vs">vs {esc}</span></div>'
        )
    pct = (cur - prev) / prev * 100.0
    if pct > 0.05:
        cls = "kpi-funnel-delta-pill--up"
        arr = "↑"
    elif pct < -0.05:
        cls = "kpi-funnel-delta-pill--down"
        arr = "↓"
    else:
        cls = "kpi-funnel-delta-pill--flat"
        arr = "→"
    return (
        f'<div class="kpi-funnel-delta kpi-funnel-delta--pill-wrap">'
        f'<span class="kpi-funnel-delta-pill {cls}">{arr} {pct:+.1f}%</span>'
        f'<span class="kpi-funnel-delta-vs">vs {esc}</span></div>'
    )


def _kpi_period_outcome(
    cur: Optional[float],
    prev: Optional[float],
    *,
    lower_is_better: bool = False,
    disabled: bool = False,
) -> str:
    """``favorable`` | ``unfavorable`` | ``flat`` | ``na`` | ``off`` — same ±0.05% thresholds as delta text."""
    if disabled:
        return "off"
    if cur is None or prev is None:
        return "na"
    if prev == 0.0:
        if cur == 0.0:
            return "flat"
        if lower_is_better:
            return "unfavorable" if float(cur) > 0.0 else "favorable"
        return "favorable" if float(cur) > 0.0 else "flat"
    pct = (cur - prev) / prev * 100.0
    if abs(pct) <= 0.05:
        return "flat"
    if lower_is_better:
        return "favorable" if pct < -0.05 else "unfavorable"
    return "favorable" if pct > 0.05 else "unfavorable"


def _kpi_biz_signal(
    cur: Optional[float],
    prev: Optional[float],
    *,
    domain: str,
    lower_is_better: bool = False,
    disabled: bool = False,
) -> str:
    """CSS class token for scorecard border psychology: metric ``domain`` + period ``outcome``.

    **Why domains:** the same “green/red” bar feels different for *awareness* (cool blue expansion),
    *revenue* (emerald win / rose miss), *unit economics* (teal savings vs **amber** budget pressure — alert
    without the same emotional hit as revenue red), and *pipeline velocity* (violet forward motion).
    """
    o = _kpi_period_outcome(cur, prev, lower_is_better=lower_is_better, disabled=disabled)
    if o in ("off", "na"):
        return "na"
    raw = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in domain.strip().lower())
    slug = "-".join(p for p in raw.replace("_", "-").split("-") if p)
    if not slug:
        return "na"
    if o == "flat":
        return f"biz-{slug}--flat"
    return f"biz-{slug}--{'favorable' if o == 'favorable' else 'unfavorable'}"


def _kpi_funnel_sub_row(label: str, value: str) -> str:
    return (
        f'<div class="kpi-funnel-sub-row" title="{html.escape(label, quote=True)}">'
        f'<span class="kpi-funnel-sub-lbl">{html.escape(label)}</span>'
        f'<span class="kpi-funnel-sub-val">{html.escape(value)}</span></div>'
    )


# Hover text for the Marketing performance CpCW:LF tile (HTML title attribute). No spreadsheet row codes (B1…).
_CPCW_LF_CARD_TOOLTIP = (
    "CpCW:LF — cost per close won vs first-month licence fee. "
    "Main value = total marketing spend in your scope ÷ sum of first-month licence fees on the same closed-won + approved rows. "
    "Same as CpCW ÷ average first-month LF, because (Spend÷deals)÷(Σ LF÷deals)=Spend÷Σ LF. "
    "Below 1.0: spend is under the summed first-month LF for those deals. Above 1.0: spend exceeds that sum."
)


def _kpi_funnel_pastel_card_html(
    *,
    icon: str,
    title: str,
    value_s: str,
    delta_html: str,
    sub_html: str,
    delay: float,
    hue: str = "cw",
    extra_class: str = "",
    biz_signal: str = "na",
    tooltip: Optional[str] = None,
    title_sub: Optional[str] = None,
) -> str:
    """Single pastel funnel scorecard tile — same markup as ``_kpi_block`` ``_card`` (Marketing performance)."""
    h = html.escape(hue, quote=True)
    xc = html.escape(extra_class.strip(), quote=True) if extra_class.strip() else ""
    xcls = f" {xc}" if xc else ""
    b = str(biz_signal).strip().lower()
    biz_cls = f" kpi-funnel-card--{html.escape(b, quote=True)}" if b not in ("", "na") else ""
    tip = ""
    if tooltip and str(tooltip).strip():
        tip = f' title="{html.escape(str(tooltip).strip(), quote=True)}"'
    sub_title = ""
    if title_sub and str(title_sub).strip():
        sub_title = f'<div class="kpi-funnel-title-sub">{html.escape(str(title_sub).strip())}</div>'
    return (
        f'<div class="kpi-funnel-card kpi-funnel-card--pastel kpi-funnel-card--pastel-{h}{xcls}{biz_cls}"{tip} '
        f'style="animation-delay:{delay:.2f}s">'
        f'<span class="kpi-funnel-icon" aria-hidden="true">{icon}</span>'
        f'<div class="kpi-funnel-title">{html.escape(title)}</div>'
        f"{sub_title}"
        f'<div class="kpi-funnel-value">{html.escape(value_s)}</div>'
        f"{delta_html}"
        f'<div class="kpi-funnel-sub">{sub_html}</div></div>'
    )


def _mom_executive_snapshot_scorecards_html(
    *,
    scope_lbl: str,
    total_spend: float,
    total_cw: int,
    total_leads: int,
    total_qual: int,
    sql_pct: float,
    qwin_pct: float,
    spend_per_cw: float,
    mom_spend_delta: float = 0.0,
    mom_spend_current: float = 0.0,
    mom_spend_prior: float = 0.0,
    mom_spend_compare_ok: bool = False,
) -> str:
    """Market MoM headline tiles — same card components as Marketing performance; MoM spend vs prior month when ≥2 months."""
    _delta_off = _kpi_funnel_delta_html(0.0, 0.0, disabled=True)
    d = 0.0

    def _step() -> float:
        nonlocal d
        d += 0.035
        return d

    cpl = (total_spend / total_leads) if total_leads else 0.0
    cpsql = (total_spend / total_qual) if total_qual else 0.0
    cpl_s = f"${cpl:,.2f}" if total_leads and total_spend else "—"
    cpsql_s = f"${cpsql:,.2f}" if total_qual and total_spend else "—"
    cpcw_s = _format_compact_k(total_spend / total_cw) if total_cw else "—"
    spend_k = _format_spend_k(total_spend) if total_spend else "$0"

    sub_spend = _kpi_funnel_sub_row("CPL (Σ slice)", cpl_s) + _kpi_funnel_sub_row("CPSQL (Σ slice)", cpsql_s)
    sub_cw = _kpi_funnel_sub_row("CPCW", cpcw_s) + _kpi_funnel_sub_row("Spend (Σ)", spend_k)
    sub_tl = _kpi_funnel_sub_row("Qualified leads", f"{total_qual:,}") + _kpi_funnel_sub_row("SQL %", f"{sql_pct:.2f}%")
    sub_sql = _kpi_funnel_sub_row("Qualified leads", f"{total_qual:,}") + _kpi_funnel_sub_row(
        "Total leads", f"{total_leads:,}"
    )
    sub_qwin = _kpi_funnel_sub_row("Qualified leads (denom.)", f"{total_qual:,}") + _kpi_funnel_sub_row(
        "Closed won (num.)", f"{total_cw:,}"
    )

    card_spend = _kpi_funnel_pastel_card_html(
        icon="💲",
        title="Total Spend",
        value_s=spend_k,
        delta_html=_delta_off,
        sub_html=sub_spend,
        delay=_step(),
        hue="cw",
        biz_signal="na",
    )
    if mom_spend_compare_ok:
        spend_mom_delta_html = _kpi_funnel_delta_html(
            float(mom_spend_current), float(mom_spend_prior), vs_label="prior month", disabled=False
        )
        mom_val_s = f"${mom_spend_delta:+,.0f}"
        mom_sub = _kpi_funnel_sub_row("Current spend", f"${mom_spend_current:,.0f}") + _kpi_funnel_sub_row(
            "Prior spend", f"${mom_spend_prior:,.0f}"
        )
    else:
        spend_mom_delta_html = _delta_off
        mom_val_s = "—"
        mom_sub = _kpi_funnel_sub_row("MoM", "Needs 2+ calendar months in scope")
    card_mom_spend = _kpi_funnel_pastel_card_html(
        icon="↕",
        title="MoM Spend Change",
        value_s=mom_val_s,
        delta_html=spend_mom_delta_html,
        sub_html=mom_sub,
        delay=_step(),
        hue="cw",
        biz_signal="na",
    )
    card_cw = _kpi_funnel_pastel_card_html(
        icon="◎",
        title="Closed won (inc. approved)",
        value_s=f"{total_cw:,}",
        delta_html=_delta_off,
        sub_html=sub_cw,
        delay=_step(),
        hue="cw",
        biz_signal="na",
    )
    card_leads = _kpi_funnel_pastel_card_html(
        icon="👥",
        title="Total leads",
        value_s=f"{total_leads:,}",
        delta_html=_delta_off,
        sub_html=sub_tl,
        delay=_step(),
        hue="leads",
        biz_signal="na",
    )
    card_sql = _kpi_funnel_pastel_card_html(
        icon="‰",
        title="SQL %",
        value_s=f"{sql_pct:.2f}%",
        delta_html=_delta_off,
        sub_html=sub_sql,
        delay=_step(),
        hue="leads",
        biz_signal="na",
    )
    card_qwin = _kpi_funnel_pastel_card_html(
        icon="%",
        title="Q win rate %",
        value_s=f"{qwin_pct:.2f}%",
        delta_html=_delta_off,
        sub_html=sub_qwin,
        delay=_step(),
        hue="pipe",
        biz_signal="na",
    )
    card_cpcw = _kpi_funnel_pastel_card_html(
        icon="⚖",
        title="Spend per Closed Won",
        value_s=f"${spend_per_cw:,.0f}" if spend_per_cw > 0 else "—",
        delta_html=_delta_off,
        sub_html=_kpi_funnel_sub_row("Spend (Σ)", spend_k) + _kpi_funnel_sub_row("CW (Σ)", f"{total_cw:,}"),
        delay=_step(),
        hue="cw",
        biz_signal="na",
    )

    title_esc = html.escape(f"Executive snapshot — {scope_lbl}")
    return (
        f'<div class="kpi-funnel-wrap kpi-funnel-wrap--pastel-scorecard mpo-kpi-shell">'
        f'<div class="kpi-funnel-section">'
        f'<div class="kpi-funnel-section-title kpi-funnel-section-title--cw">{title_esc}</div>'
        f'<div class="kpi-funnel-grid">'
        f"{card_spend}{card_mom_spend}{card_cw}{card_leads}{card_sql}{card_qwin}{card_cpcw}"
        f"</div></div></div>"
    )


def _pmc_spend_executive_scorecards_html(
    *,
    total_spend: float,
    active_channels: int,
    top_channel: str,
    top_share_pct: float,
) -> str:
    """Spend-by-channel hero cards (ranking / concentration). MoM spend change lives on **Market MoM**."""
    d = 0.0

    def _step() -> float:
        nonlocal d
        d += 0.035
        return d

    _delta_off = _kpi_funnel_delta_html(0.0, 0.0, disabled=True)

    card_spend = _kpi_funnel_pastel_card_html(
        icon="💲",
        title="Total Spend",
        value_s=f"${total_spend:,.0f}",
        delta_html=_delta_off,
        sub_html=_kpi_funnel_sub_row("Scope", "Spend by channel") + _kpi_funnel_sub_row("MoM spend", "Market MoM tab"),
        delay=_step(),
        hue="cw",
    )
    card_active = _kpi_funnel_pastel_card_html(
        icon="◍",
        title="Active Channels",
        value_s=f"{active_channels:,}",
        delta_html=_delta_off,
        sub_html=_kpi_funnel_sub_row("Spend > 0", f"{active_channels:,}") + _kpi_funnel_sub_row("Sort", "Spend desc"),
        delay=_step(),
        hue="leads",
    )
    card_top = _kpi_funnel_pastel_card_html(
        icon="★",
        title="Top Channel",
        value_s=str(top_channel or "—"),
        delta_html=_delta_off,
        sub_html=_kpi_funnel_sub_row("Share", f"{top_share_pct:.1f}%") + _kpi_funnel_sub_row("Concentration", "Highest spend"),
        delay=_step(),
        hue="pipe",
    )
    card_share = _kpi_funnel_pastel_card_html(
        icon="◔",
        title="Top Channel Share %",
        value_s=f"{top_share_pct:.1f}%",
        delta_html=_delta_off,
        sub_html=_kpi_funnel_sub_row("Threshold", "45% warning") + _kpi_funnel_sub_row("Type", "Spend share"),
        delay=_step(),
        hue="leads",
    )
    return (
        '<div class="kpi-funnel-wrap kpi-funnel-wrap--pastel-scorecard mpo-kpi-shell">'
        '<div class="kpi-funnel-section">'
        '<div class="kpi-funnel-section-title kpi-funnel-section-title--cw">Spend snapshot — by channel</div>'
        f'<div class="kpi-funnel-grid">{card_spend}{card_active}{card_top}{card_share}</div>'
        "</div></div>"
    )


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
    total_new_working: int,
    total_total_live: int,
    total_negotiation: int,
    total_commitment: int,
    total_closed_lost: int,
    total_pitching: int = 0,
    total_qualifying: int = 0,
    q_win_cw: Optional[int] = None,
    q_win_qualified: Optional[int] = None,
    prior: Optional[dict[str, Any]] = None,
) -> None:
    """Original MPO metric groups (Closed won / Leads / Qualified leads) in funnel cards + period comparison deltas."""
    _cw_q = int(q_win_cw) if q_win_cw is not None else int(total_cw)
    _q_d = int(q_win_qualified) if q_win_qualified is not None else int(total_qualified)
    q_rate = (_cw_q / _q_d * 100) if _q_d else 0.0
    sql_rate = (total_qualified / total_leads * 100) if total_leads else 0.0
    cpcw = (total_spend / total_cw) if total_cw else 0.0
    # CpCW:LF = ME **B6** = **B1 ÷ B3** (Total Spend ÷ Σ 1st Month LF) — same as **B5 ÷ B4** when B5=B1/B2 and B4=(Σ LF)/CW.
    cpcw_lf = (total_spend / total_first_month_lf) if total_first_month_lf else 0.0
    spend_tcv_pct = (total_spend / total_tcv * 100) if total_tcv else 0.0

    pv = prior or {}
    _vs = str(pv.get("_delta_label") or "prior month")
    _no_delta = bool(pv.get("_comparison_off"))

    def _card(
        icon: str,
        title: str,
        value_s: str,
        delta_html: str,
        sub_html: str,
        delay: float,
        *,
        hue: str = "cw",
        extra_class: str = "",
        biz_signal: str = "na",
        tooltip: Optional[str] = None,
        title_sub: Optional[str] = None,
    ) -> str:
        return _kpi_funnel_pastel_card_html(
            icon=icon,
            title=title,
            value_s=value_s,
            delta_html=delta_html,
            sub_html=sub_html,
            delay=delay,
            hue=hue,
            extra_class=extra_class,
            biz_signal=biz_signal,
            tooltip=tooltip,
            title_sub=title_sub,
        )

    def _section(title: str, accent: str, cards: list[str]) -> str:
        return (
            f'<div class="kpi-funnel-section">'
            f'<div class="kpi-funnel-section-title kpi-funnel-section-title--{html.escape(accent)}">'
            f"{html.escape(title)}</div>"
            f'<div class="kpi-funnel-grid">{"".join(cards)}</div></div>'
        )

    d = 0.0

    def _d() -> float:
        nonlocal d
        d += 0.035
        return d

    sub_hero = '<div class="kpi-funnel-sub kpi-funnel-sub--hero"></div>'
    spend_sub = _kpi_funnel_sub_row("CPM", f"${(total_spend / total_impr * 1000):,.2f}" if total_impr else "—")
    _vs_traffic = "previous period"

    card_tot_impr = _card(
        "👁",
        "Total Impressions",
        f"{total_impr:,}",
        _kpi_funnel_delta_pill_html(pv.get("mom_impr_c"), pv.get("mom_impr_p"), vs_label=_vs_traffic, disabled=_no_delta),
        sub_hero,
        _d(),
        hue="leads",
        extra_class="kpi-funnel-card--hero",
        biz_signal=_kpi_biz_signal(pv.get("mom_impr_c"), pv.get("mom_impr_p"), domain="reach", disabled=_no_delta),
    )
    card_tot_clicks = _card(
        "🖱",
        "Total Clicks",
        f"{total_clicks:,}",
        _kpi_funnel_delta_pill_html(pv.get("mom_clicks_c"), pv.get("mom_clicks_p"), vs_label=_vs_traffic, disabled=_no_delta),
        sub_hero,
        _d(),
        hue="leads",
        extra_class="kpi-funnel-card--hero",
        biz_signal=_kpi_biz_signal(pv.get("mom_clicks_c"), pv.get("mom_clicks_p"), domain="reach", disabled=_no_delta),
    )
    card_tot_ctr = _card(
        "%",
        "Click-Through Rate",
        f"{ctr:.2f}%",
        _kpi_funnel_delta_pill_html(pv.get("mom_ctr_c"), pv.get("mom_ctr_p"), vs_label=_vs_traffic, disabled=_no_delta),
        sub_hero,
        _d(),
        hue="leads",
        extra_class="kpi-funnel-card--hero",
        biz_signal=_kpi_biz_signal(pv.get("mom_ctr_c"), pv.get("mom_ctr_p"), domain="engagement", disabled=_no_delta),
    )

    cpcw_s = _format_compact_k(cpcw) if total_cw and cpcw == cpcw else "—"
    tcv_s = _format_tcv_short(float(total_tcv)) if total_tcv else "—"
    cpcwlf_s = _format_ratio_cpcw_lf(float(cpcw_lf)) if total_first_month_lf else "—"
    lf_sum_s = _format_tcv_short(float(total_first_month_lf)) if total_first_month_lf else "—"
    cw_sub = _kpi_funnel_sub_row("CPCW", cpcw_s) + _kpi_funnel_sub_row("Paid media (Σ)", _format_spend_k(total_spend) if total_spend else "$0")
    cpcw_sub = _kpi_funnel_sub_row("CW + Approved (count)", f"{total_cw:,}") + _kpi_funnel_sub_row("Paid media (Σ)", _format_spend_k(total_spend) if total_spend else "$0")
    tcv_sub = _kpi_funnel_sub_row("CpCW:LF", cpcwlf_s) + _kpi_funnel_sub_row("Cost / TCV %", f"{spend_tcv_pct:.2f}%" if total_tcv else "—")
    # Sub-rows are the ratio inputs only (no TCV); labels stay free of spreadsheet row codes on this card.
    cpcwlf_sub = _kpi_funnel_sub_row("Spend", _format_spend_k(total_spend) if total_spend else "$0") + _kpi_funnel_sub_row(
        "Σ 1st month LF", lf_sum_s
    )
    pct_tcv_sub = _kpi_funnel_sub_row("Actual TCV", tcv_s) + _kpi_funnel_sub_row("Spend", _format_spend_k(total_spend) if total_spend else "$0")

    card_cw = _card(
        "◎",
        "CW (inc. approved)",
        f"{total_cw:,}",
        _kpi_funnel_delta_html(pv.get("mom_cw_c"), pv.get("mom_cw_p"), vs_label=_vs, disabled=_no_delta),
        cw_sub,
        _d(),
        biz_signal=_kpi_biz_signal(pv.get("mom_cw_c"), pv.get("mom_cw_p"), domain="growth", disabled=_no_delta),
    )
    card_spend = _card(
        "💲",
        "Total Spend",
        _format_spend_k(total_spend) if total_spend else "$0",
        _kpi_funnel_delta_html(pv.get("mom_spend_c"), pv.get("mom_spend_p"), vs_label=_vs, disabled=_no_delta),
        spend_sub,
        _d(),
        biz_signal=_kpi_biz_signal(
            pv.get("mom_spend_c"), pv.get("mom_spend_p"), domain="efficiency", lower_is_better=True, disabled=_no_delta
        ),
    )
    card_cpcw = _card(
        "$",
        "CPCW",
        cpcw_s,
        _kpi_funnel_delta_html(pv.get("mom_cpcw_c"), pv.get("mom_cpcw_p"), vs_label=_vs, disabled=_no_delta),
        cpcw_sub,
        _d(),
        biz_signal=_kpi_biz_signal(
            pv.get("mom_cpcw_c"), pv.get("mom_cpcw_p"), domain="efficiency", lower_is_better=True, disabled=_no_delta
        ),
    )
    card_tcv = _card(
        "◆",
        "Actual TCV",
        tcv_s,
        _kpi_funnel_delta_html(pv.get("mom_tcv_c"), pv.get("mom_tcv_p"), vs_label=_vs, disabled=_no_delta),
        tcv_sub,
        _d(),
        biz_signal=_kpi_biz_signal(pv.get("mom_tcv_c"), pv.get("mom_tcv_p"), domain="growth", disabled=_no_delta),
    )
    card_cpcwlf = _card(
        "≈",
        "CpCW:LF",
        cpcwlf_s,
        _kpi_funnel_delta_html(pv.get("mom_cpcwlf_c"), pv.get("mom_cpcwlf_p"), vs_label=_vs, disabled=_no_delta),
        cpcwlf_sub,
        _d(),
        biz_signal=_kpi_biz_signal(
            pv.get("mom_cpcwlf_c"), pv.get("mom_cpcwlf_p"), domain="efficiency", lower_is_better=True, disabled=_no_delta
        ),
        tooltip=_CPCW_LF_CARD_TOOLTIP,
    )
    card_ctcv = _card(
        "%",
        "Cost / TCV %",
        f"{spend_tcv_pct:.2f}%" if total_tcv else "—",
        _kpi_funnel_delta_html(pv.get("mom_spend_tcv_pct_c"), pv.get("mom_spend_tcv_pct_p"), vs_label=_vs, disabled=_no_delta),
        pct_tcv_sub,
        _d(),
        biz_signal=_kpi_biz_signal(
            pv.get("mom_spend_tcv_pct_c"),
            pv.get("mom_spend_tcv_pct_p"),
            domain="efficiency",
            lower_is_better=True,
            disabled=_no_delta,
        ),
    )

    cpl_s = f"${cpl:,.2f}" if total_leads and cpl == cpl else "—"
    cpsql_s = f"${cpsql:,.2f}" if total_qualified and cpsql == cpsql else "—"
    sub_tl = _kpi_funnel_sub_row("Qualified", f"{total_qualified:,}") + _kpi_funnel_sub_row("SQL %", f"{sql_rate:.2f}%")
    sub_qual = _kpi_funnel_sub_row("Total leads", f"{total_leads:,}") + _kpi_funnel_sub_row("CPSQL", cpsql_s)
    sub_nw = _kpi_funnel_sub_row("Total leads", f"{total_leads:,}") + _kpi_funnel_sub_row("Qualified", f"{total_qualified:,}")
    sub_sql = _kpi_funnel_sub_row("Qualified", f"{total_qualified:,}") + _kpi_funnel_sub_row("Total leads", f"{total_leads:,}")
    sub_cpl_c = _kpi_funnel_sub_row("Spend", _format_spend_k(total_spend) if total_spend else "$0") + _kpi_funnel_sub_row("Leads", f"{total_leads:,}")
    sub_cpsql_c = _kpi_funnel_sub_row("Spend", _format_spend_k(total_spend) if total_spend else "$0") + _kpi_funnel_sub_row("Qualified", f"{total_qualified:,}")

    card_tl = _card(
        "👥",
        "Total leads",
        f"{total_leads:,}",
        _kpi_funnel_delta_html(pv.get("mom_leads_rows_c"), pv.get("mom_leads_rows_p"), vs_label=_vs, disabled=_no_delta),
        sub_tl,
        _d(),
        hue="leads",
        biz_signal=_kpi_biz_signal(
            pv.get("mom_leads_rows_c"), pv.get("mom_leads_rows_p"), domain="demand", disabled=_no_delta
        ),
    )
    card_ql = _card(
        "✓",
        "Qualified",
        f"{total_qualified:,}",
        _kpi_funnel_delta_html(pv.get("mom_qual_status_c"), pv.get("mom_qual_status_p"), vs_label=_vs, disabled=_no_delta),
        sub_qual,
        _d(),
        hue="leads",
        biz_signal=_kpi_biz_signal(
            pv.get("mom_qual_status_c"), pv.get("mom_qual_status_p"), domain="demand", disabled=_no_delta
        ),
    )
    card_nw = _card(
        "◇",
        "New + working",
        f"{total_new_working:,}",
        _kpi_funnel_delta_html(pv.get("mom_nw_c"), pv.get("mom_nw_p"), vs_label=_vs, disabled=_no_delta),
        sub_nw,
        _d(),
        hue="leads",
        biz_signal=_kpi_biz_signal(pv.get("mom_nw_c"), pv.get("mom_nw_p"), domain="demand", disabled=_no_delta),
    )
    card_sql = _card(
        "‰",
        "SQL %",
        f"{sql_rate:.2f}%",
        _kpi_funnel_delta_html(pv.get("mom_sql_pct_c"), pv.get("mom_sql_pct_p"), vs_label=_vs, disabled=_no_delta),
        sub_sql,
        _d(),
        hue="leads",
        biz_signal=_kpi_biz_signal(
            pv.get("mom_sql_pct_c"), pv.get("mom_sql_pct_p"), domain="conversion", disabled=_no_delta
        ),
    )
    card_cpl = _card(
        "⬧",
        "CPL",
        cpl_s,
        _kpi_funnel_delta_html(pv.get("mom_cpl_c"), pv.get("mom_cpl_p"), vs_label=_vs, disabled=_no_delta),
        sub_cpl_c,
        _d(),
        hue="leads",
        biz_signal=_kpi_biz_signal(
            pv.get("mom_cpl_c"), pv.get("mom_cpl_p"), domain="efficiency", lower_is_better=True, disabled=_no_delta
        ),
    )
    card_cpsql = _card(
        "⬧",
        "CPSQL",
        cpsql_s,
        _kpi_funnel_delta_html(pv.get("mom_cpsql_c"), pv.get("mom_cpsql_p"), vs_label=_vs, disabled=_no_delta),
        sub_cpsql_c,
        _d(),
        hue="leads",
        biz_signal=_kpi_biz_signal(
            pv.get("mom_cpsql_c"), pv.get("mom_cpsql_p"), domain="efficiency", lower_is_better=True, disabled=_no_delta
        ),
    )

    _qual_show = int(total_qualifying)
    if _qual_show <= 0 and total_total_live > 0:
        _qual_show = max(0, int(total_total_live - total_pitching - total_negotiation - total_commitment))
    sub_live = (
        _kpi_funnel_sub_row("Qualifying", f"{_qual_show:,}")
        + _kpi_funnel_sub_row("Pitching", f"{total_pitching:,}")
        + _kpi_funnel_sub_row("Negotiation", f"{total_negotiation:,}")
        + _kpi_funnel_sub_row("Commitment", f"{total_commitment:,}")
    )
    sub_nego = _kpi_funnel_sub_row("Total live", f"{total_total_live:,}") + _kpi_funnel_sub_row("Commitment", f"{total_commitment:,}")
    sub_commit = _kpi_funnel_sub_row("Total live", f"{total_total_live:,}") + _kpi_funnel_sub_row("Negotiation", f"{total_negotiation:,}")
    sub_clost = _kpi_funnel_sub_row("CW (inc. approved)", f"{total_cw:,}") + _kpi_funnel_sub_row("Qualified (Q-win base)", f"{_q_d:,}")
    sub_qwin = _kpi_funnel_sub_row("CW (post tab)", f"{_cw_q:,}") + _kpi_funnel_sub_row("Qualified (denom.)", f"{_q_d:,}")

    card_live = _card(
        "▤",
        "Total live",
        f"{total_total_live:,}",
        _kpi_funnel_delta_html(pv.get("mom_live_c"), pv.get("mom_live_p"), vs_label=_vs, disabled=_no_delta),
        sub_live,
        _d(),
        hue="pipe",
        biz_signal=_kpi_biz_signal(pv.get("mom_live_c"), pv.get("mom_live_p"), domain="velocity", disabled=_no_delta),
    )
    card_nego = _card(
        "◆",
        "Negotiation",
        f"{total_negotiation:,}",
        _kpi_funnel_delta_html(pv.get("mom_nego_c"), pv.get("mom_nego_p"), vs_label=_vs, disabled=_no_delta),
        sub_nego,
        _d(),
        hue="pipe",
        biz_signal=_kpi_biz_signal(pv.get("mom_nego_c"), pv.get("mom_nego_p"), domain="velocity", disabled=_no_delta),
    )
    card_commit = _card(
        "◆",
        "Commitment",
        f"{total_commitment:,}",
        _kpi_funnel_delta_html(pv.get("mom_commit_c"), pv.get("mom_commit_p"), vs_label=_vs, disabled=_no_delta),
        sub_commit,
        _d(),
        hue="pipe",
        biz_signal=_kpi_biz_signal(pv.get("mom_commit_c"), pv.get("mom_commit_p"), domain="velocity", disabled=_no_delta),
    )
    card_clost = _card(
        "✕",
        "Closed lost",
        f"{total_closed_lost:,}",
        _kpi_funnel_delta_html(pv.get("mom_clost_c"), pv.get("mom_clost_p"), vs_label=_vs, disabled=_no_delta),
        sub_clost,
        _d(),
        hue="pipe",
        biz_signal=_kpi_biz_signal(
            pv.get("mom_clost_c"), pv.get("mom_clost_p"), domain="leakage", lower_is_better=True, disabled=_no_delta
        ),
    )
    card_qwin = _card(
        "%",
        "Q win rate %",
        f"{q_rate:.2f}%",
        _kpi_funnel_delta_html(pv.get("mom_qwin_c"), pv.get("mom_qwin_p"), vs_label=_vs, disabled=_no_delta),
        sub_qwin,
        _d(),
        hue="pipe",
        biz_signal=_kpi_biz_signal(pv.get("mom_qwin_c"), pv.get("mom_qwin_p"), domain="quality", disabled=_no_delta),
    )

    sec_traffic_hero = (
        f'<div class="kpi-funnel-section kpi-funnel-section--traffic-hero">'
        f'<div class="kpi-funnel-grid kpi-funnel-grid--hero-3">'
        f"{card_tot_impr}{card_tot_clicks}{card_tot_ctr}"
        f"</div></div>"
    )

    sec_cw = _section(
        "Closed won",
        "cw",
        [card_cw, card_spend, card_cpcw, card_tcv, card_cpcwlf, card_ctcv],
    )
    sec_leads = _section(
        "Leads",
        "leads",
        [card_tl, card_ql, card_nw, card_sql, card_cpl, card_cpsql],
    )
    sec_pipe = _section(
        "Qualified leads",
        "pipe",
        [card_live, card_nego, card_commit, card_clost, card_qwin],
    )

    st.markdown(
        f'<div class="kpi-funnel-wrap kpi-funnel-wrap--pastel-scorecard mpo-kpi-shell">'
        f"{sec_traffic_hero}{sec_cw}{sec_leads}{sec_pipe}</div>",
        unsafe_allow_html=True,
    )


def _collapse_duplicate_named_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Merge same-named columns by summing numeric values — duplicate ``cost`` breaks ``groupby().agg(spend=('cost','sum'))``."""
    if df.empty or not df.columns.duplicated().any():
        return df
    out: dict[str, pd.Series] = {}
    handled: set[str] = set()
    for name in df.columns:
        if name in handled:
            continue
        handled.add(name)
        block = df.loc[:, df.columns == name]
        if block.shape[1] == 1:
            out[str(name)] = block.iloc[:, 0]
        else:
            num = block.apply(lambda s: pd.to_numeric(s, errors="coerce"))
            out[str(name)] = num.sum(axis=1, min_count=1)
    return pd.DataFrame(out, index=df.index)


def _master_view_impute_month_for_spend_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Fill month only from **same-country** peers with a real calendar month.

    A global ``value_counts`` fallback (e.g. “most common month in the sheet”) dumps **all** orphan spend
    into one month (often the busiest CRM month like Sep) — wrong for Marketing Performance.
    """
    if df.empty or "month" not in df.columns or "cost" not in df.columns or "country" not in df.columns:
        return df
    out = df.copy()
    cost = pd.to_numeric(out["cost"], errors="coerce").fillna(0)
    mk = out["month"].map(_month_norm_key)
    plausible = mk.map(_dashboard_month_plausible)
    need_mask = (cost > 1e-3) & (out["month"].isna() | ~plausible | mk.eq(""))
    if not bool(need_mask.any()):
        return out
    good_mask = plausible & mk.ne("") & out["month"].notna()
    if not bool(good_mask.any()):
        return out
    for idx in out.loc[need_mask].index:
        ctry = out.at[idx, "country"]
        peer_mask = good_mask & (out["country"] == ctry)
        if not bool(peer_mask.any()):
            continue
        local_vc = out.loc[peer_mask, "month"].map(_month_norm_key).value_counts()
        if not len(local_vc):
            continue
        out.at[idx, "month"] = local_vc.index[0]
    return out


def _month_label_short(m: Any) -> str:
    k = _month_norm_key(m)
    if not k:
        return ""
    try:
        return pd.Period(k, freq="M").strftime("%b %Y")
    except Exception:
        return ""


def _mpo_month_ts_for_sort(m: Any) -> pd.Timestamp:
    k = _month_norm_key(m)
    if not k:
        return pd.Timestamp.min
    try:
        return pd.Period(k, freq="M").to_timestamp()
    except Exception:
        return pd.Timestamp.min


def _mpo_monthly_rollup_from_master(master_df: pd.DataFrame) -> pd.DataFrame:
    """Month-level sums and derived rates from the master grid (aligned with scorecard filters)."""
    if master_df.empty or "month" not in master_df.columns:
        return pd.DataFrame()
    m = master_df.copy()
    for c in ("cost", "leads", "qualified", "closed_won"):
        if c not in m.columns:
            m[c] = 0.0
        else:
            m[c] = pd.to_numeric(m[c], errors="coerce").fillna(0.0)
    agg = m.groupby("month", as_index=False).agg(
        spend=("cost", "sum"),
        leads=("leads", "sum"),
        qualified=("qualified", "sum"),
        cw=("closed_won", "sum"),
    )
    agg["_ts"] = agg["month"].map(_mpo_month_ts_for_sort)
    agg = agg.sort_values("_ts").dropna(subset=["_ts"])
    if agg.empty:
        return agg
    agg["Month"] = agg["month"].map(_month_label_short)
    agg = agg[agg["Month"].astype(str).str.len() > 0]
    agg["cpl"] = (agg["spend"] / agg["leads"]).where(agg["leads"] > 0)
    agg["cpsql"] = (agg["spend"] / agg["qualified"]).where(agg["qualified"] > 0)
    return agg


def _mpo_monthly_metric_series_from_spend(spend_df: pd.DataFrame) -> pd.DataFrame:
    """Month-level **cost / clicks / impressions** from the merged spend frame (Supermetrics CI on X-Ray spend)."""
    if spend_df.empty or "month" not in spend_df.columns:
        return pd.DataFrame()
    x = spend_df.copy()
    x["month"] = x["month"].map(_month_norm_key)
    x = x.loc[x["month"].astype(str).str.strip().ne("")]
    if x.empty:
        return pd.DataFrame()
    g = x.groupby("month", as_index=False).agg(
        cost=("cost", "sum"),
        clicks=("clicks", "sum"),
        impressions=("impressions", "sum"),
    )
    for c in ("cost", "clicks", "impressions"):
        if c not in g.columns:
            g[c] = 0.0
        else:
            g[c] = pd.to_numeric(g[c], errors="coerce").fillna(0.0)
    g["_ts"] = g["month"].map(_mpo_month_ts_for_sort)
    g = g.sort_values("_ts").dropna(subset=["_ts"])
    if g.empty:
        return g
    g["Month"] = g["month"].map(_month_label_short)
    g = g[g["Month"].astype(str).str.len() > 0]
    return g


def _mpo_segmented_or_radio(label: str, options: list[str], *, key: str) -> str:
    """Prefer ``st.segmented_control`` when the running Streamlit build supports it."""
    seg = getattr(st, "segmented_control", None)
    if callable(seg):
        try:
            return str(
                seg(
                    label,
                    options,
                    key=key,
                    label_visibility="collapsed",
                )
            )
        except TypeError:
            pass
    return str(st.radio(label, options, horizontal=True, key=key, label_visibility="collapsed"))


def _mpo_monthly_trend_fallback_from_master(master_df: pd.DataFrame) -> pd.DataFrame:
    """When spend rows are empty, still show **Cost** from the master grid rollup."""
    agg = _mpo_monthly_rollup_from_master(master_df)
    if agg.empty:
        return pd.DataFrame()
    out = pd.DataFrame(
        {
            "month": agg["month"],
            "Month": agg["Month"],
            "cost": pd.to_numeric(agg["spend"], errors="coerce").fillna(0.0),
            "clicks": 0.0,
            "impressions": 0.0,
        }
    )
    return out


def _mpo_traffic_platform_breakdown(
    df_loaded: pd.DataFrame,
    sheet_id: str,
    df_ref: pd.DataFrame,
    spend_fallback: pd.DataFrame,
) -> tuple[list[str], list[float], str]:
    """Returns (labels, values, value_label) for a donut — prefers Supermetrics **impressions** by platform."""
    pool = _mpo_supermetrics_pool_for_clicks_impressions(df_loaded, primary_sheet_id=sheet_id)
    if not pool.empty:
        p = pool.copy()
        if "platform" not in p.columns or p["platform"].astype(str).str.strip().eq("").all():
            if "source_tab" in p.columns:
                p["platform"] = p["source_tab"].astype(str).map(_mpo_platform_label_from_source_tab)
            else:
                p["platform"] = "Other"
        p = _mpo_slice_by_dashboard_ref(p, df_ref)
        p["impressions"] = pd.to_numeric(p.get("impressions", 0), errors="coerce").fillna(0)
        p["clicks"] = pd.to_numeric(p.get("clicks", 0), errors="coerce").fillna(0)
        g = p.groupby(p["platform"].astype(str).str.strip(), as_index=False).agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
        )
        g = g.loc[g["platform"].astype(str).str.len() > 0]
        use = "impressions"
        if float(g["impressions"].sum()) < 1.0 and float(g["clicks"].sum()) > 0.0:
            use = "clicks"
        g = g.sort_values(use, ascending=False)
        labels = g["platform"].tolist()
        values = pd.to_numeric(g[use], errors="coerce").fillna(0.0).tolist()
        return labels, values, "Impressions" if use == "impressions" else "Clicks"

    if spend_fallback.empty or "platform" not in spend_fallback.columns:
        return [], [], "Impressions"
    s = spend_fallback.copy()
    s = _mpo_slice_by_dashboard_ref(s, df_ref)
    s["platform"] = s["platform"].astype(str).str.strip()
    s["impressions"] = pd.to_numeric(s.get("impressions", 0), errors="coerce").fillna(0)
    s["clicks"] = pd.to_numeric(s.get("clicks", 0), errors="coerce").fillna(0)
    g = s.groupby("platform", as_index=False).agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"))
    g = g[g["platform"].astype(str).str.len() > 0]
    use = "impressions"
    if float(g["impressions"].sum()) < 1.0 and float(g["clicks"].sum()) > 0.0:
        use = "clicks"
    g = g.sort_values(use, ascending=False)
    return g["platform"].tolist(), pd.to_numeric(g[use], errors="coerce").fillna(0.0).tolist(), (
        "Impressions" if use == "impressions" else "Clicks"
    )


def _mpo_leads_conversion_slices(leads_sliced: pd.DataFrame) -> tuple[list[str], list[float]]:
    tq = int(_qualified_count_from_leads(leads_sliced))
    tl = int(_lead_rows_count(leads_sliced))
    rest = max(0, tl - tq)
    if tl <= 0 and tq <= 0 and rest <= 0:
        return [], []
    return ["Qualified", "Not qualified"], [float(tq), float(rest)]


def _mpo_funnel_stage_slices(post_sliced: pd.DataFrame) -> tuple[list[str], list[float]]:
    cols = ["qualifying", "pitching", "negotiation", "commitment"]
    present = [c for c in cols if c in post_sliced.columns]
    if not present or post_sliced.empty:
        return [], []
    labels: list[str] = []
    values: list[float] = []
    for c in present:
        v = float(pd.to_numeric(post_sliced[c], errors="coerce").fillna(0).sum())
        if v > 1e-9:
            labels.append(c.replace("_", " ").title())
            values.append(v)
    return labels, values


def _mpo_format_trend_value(ycol: str, v: float) -> str:
    if ycol == "cost":
        return f"${v:,.0f}"
    return f"{v:,.0f}"


def _mpo_mom_pct_str(cur: pd.Series) -> str:
    if cur is None or len(cur) < 2:
        return "—"
    a, b = float(cur.iloc[-2]), float(cur.iloc[-1])
    if abs(a) < 1e-9:
        return "—"
    return f"{((b - a) / a) * 100:+.1f}%"


def _render_mpo_trend_charts(
    *,
    start_date: date,
    end_date: date,
    master_df: pd.DataFrame,
    key_suffix: str,
    spend_for_charts: pd.DataFrame,
    df_loaded: pd.DataFrame,
    sheet_id: str,
    leads_df: pd.DataFrame,
    post_df_kpi: pd.DataFrame,
    df_ref_for_scope: pd.DataFrame,
) -> None:
    """One bordered panel with two columns: **Cost + Clicks + Impressions** trends and breakdown donut (same chart height)."""
    _ = start_date, end_date  # window implied by filtered monthly series / scope
    st.markdown(
        '<div class="mpo-perf-charts-wrap">'
        '<div class="looker-table-title mpo-perf-charts-page-title">Performance charts</div>'
        "</div>",
        unsafe_allow_html=True,
    )

    g = _mpo_monthly_metric_series_from_spend(spend_for_charts)
    if g.empty and not master_df.empty:
        g = _mpo_monthly_trend_fallback_from_master(master_df)

    leads_sliced = _mpo_slice_by_dashboard_ref(leads_df, df_ref_for_scope)
    post_sliced = _mpo_slice_by_dashboard_ref(post_df_kpi, df_ref_for_scope)

    # One outer frame avoids mismatched twin bordered heights. Generous margins so legends/labels are not clipped.
    _chart_h = 452
    _perf_plot_margin = dict(l=54, r=14, t=48, b=56)
    _perf_plot_margin_donut = dict(l=8, r=8, t=8, b=72)

    with st.container(border=True):
        col_left, col_right = st.columns([1, 1], gap="medium")

        with col_left:
            st.markdown('<p class="mpo-perf-chart-title">Trends</p>', unsafe_allow_html=True)
            # Spacer matches Breakdown segmented-control height so both Plotly charts start on the same baseline.
            st.markdown(
                '<div class="mpo-perf-chart-control-slot" aria-hidden="true"></div>',
                unsafe_allow_html=True,
            )

            if g.empty or len(g["Month"]) == 0:
                st.caption("Not enough monthly data in scope — widen the date range or relax filters.")
            else:
                g = g.copy()
                for c in ("cost", "clicks", "impressions"):
                    if c not in g.columns:
                        g[c] = 0.0
                xs = g["Month"]
                s_cost = pd.to_numeric(g["cost"], errors="coerce").fillna(0.0)
                s_clk = pd.to_numeric(g["clicks"], errors="coerce").fillna(0.0)
                s_imp = pd.to_numeric(g["impressions"], errors="coerce").fillna(0.0)
                fig_t = make_subplots(
                    rows=3,
                    cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.042,
                )
                fig_t.add_trace(
                    go.Scatter(
                        x=xs,
                        y=s_cost,
                        name="Cost",
                        mode="lines+markers",
                        line=dict(color="#0d9488", width=3.5),
                        marker=dict(size=8, color="#0d9488", line=dict(width=1, color="#fff")),
                        fill="tozeroy",
                        fillcolor="rgba(13, 148, 136, 0.1)",
                        hovertemplate="<b>%{x}</b><br>Cost: $%{y:,.0f}<extra></extra>",
                    ),
                    row=1,
                    col=1,
                )
                fig_t.add_trace(
                    go.Scatter(
                        x=xs,
                        y=s_clk,
                        name="Clicks",
                        mode="lines+markers",
                        line=dict(color="#2563eb", width=3.5),
                        marker=dict(size=8, color="#2563eb", line=dict(width=1, color="#fff")),
                        hovertemplate="<b>%{x}</b><br>Clicks: %{y:,.0f}<extra></extra>",
                    ),
                    row=2,
                    col=1,
                )
                fig_t.add_trace(
                    go.Scatter(
                        x=xs,
                        y=s_imp,
                        name="Impressions",
                        mode="lines+markers",
                        line=dict(color="#7c3aed", width=3.5),
                        marker=dict(size=8, color="#7c3aed", line=dict(width=1, color="#fff")),
                        hovertemplate="<b>%{x}</b><br>Impressions: %{y:,.0f}<extra></extra>",
                    ),
                    row=3,
                    col=1,
                )
                _ytitle = dict(font=dict(size=12, color="#334155"))
                _ytick = dict(size=11, color="#475569")
                fig_t.update_yaxes(
                    title=dict(text="Cost ($)", standoff=10, **_ytitle),
                    tickprefix="$",
                    tickformat=",.0f",
                    tickfont=_ytick,
                    showgrid=True,
                    gridcolor="rgba(148, 163, 184, 0.25)",
                    zeroline=False,
                    side="left",
                    automargin=True,
                    row=1,
                    col=1,
                )
                fig_t.update_yaxes(
                    title=dict(text="Clicks", standoff=10, **_ytitle),
                    tickformat=",.0f",
                    tickfont=_ytick,
                    showgrid=True,
                    gridcolor="rgba(148, 163, 184, 0.25)",
                    zeroline=False,
                    side="left",
                    automargin=True,
                    row=2,
                    col=1,
                )
                fig_t.update_yaxes(
                    title=dict(text="Impressions", standoff=10, **_ytitle),
                    tickformat=",.0f",
                    tickfont=_ytick,
                    showgrid=True,
                    gridcolor="rgba(148, 163, 184, 0.25)",
                    zeroline=False,
                    side="left",
                    automargin=True,
                    row=3,
                    col=1,
                )
                fig_t.update_xaxes(
                    title_text="Month",
                    title_font=dict(size=14, color="#0f172a"),
                    tickfont=dict(size=12, color="#334155"),
                    showgrid=False,
                    showspikes=True,
                    spikemode="across",
                    spikecolor="#cbd5e1",
                    spikesnap="cursor",
                    spikethickness=1,
                    row=3,
                    col=1,
                )
                fig_t.update_xaxes(showspikes=True, spikemode="across", spikecolor="#cbd5e1", row=1, col=1)
                fig_t.update_xaxes(showspikes=True, spikemode="across", spikecolor="#cbd5e1", row=2, col=1)
                fig_t.update_layout(
                    height=_chart_h,
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        yref="paper",
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    ),
                    hovermode="x unified",
                    hoverlabel=dict(font_size=12),
                    margin={**_perf_plot_margin},
                )
                if float(s_clk.sum()) < 1e-6 and float(s_imp.sum()) < 1e-6:
                    st.caption("Clicks/impressions are zero in this slice — check Supermetrics / filters.")
                st.plotly_chart(fig_t, width="stretch", key=f"{key_suffix}_pl_mpo_perf_trends")

        with col_right:
            st.markdown('<p class="mpo-perf-chart-title">Breakdown</p>', unsafe_allow_html=True)
            brk = _mpo_segmented_or_radio(
                "breakdown",
                ["Traffic", "Leads Conversion", "Funnel Stages"],
                key=f"{key_suffix}_mpo_breakdown_kind",
            )

            labels: list[str] = []
            values: list[float] = []
            basis_note = ""
            if brk == "Traffic":
                labels, values, _basis = _mpo_traffic_platform_breakdown(
                    df_loaded, sheet_id, df_ref_for_scope, spend_for_charts
                )
                basis_note = str(_basis)
            elif brk == "Leads Conversion":
                labels, values = _mpo_leads_conversion_slices(leads_sliced)
            else:
                labels, values = _mpo_funnel_stage_slices(post_sliced)

            if not labels or not values or float(sum(values)) < 1e-9:
                st.caption(
                    "No breakdown for this view in the current scope — adjust filters or worksheets."
                )
            else:
                pal = ["#0d9488", "#6366f1", "#f59e0b", "#ec4899", "#14b8a6", "#64748b", "#2563eb"]
                cols = [pal[i % len(pal)] for i in range(len(labels))]
                total_v = float(sum(values))
                fig_d = go.Figure(
                    data=[
                        go.Pie(
                            labels=labels,
                            values=values,
                            hole=0.48,
                            domain=dict(x=[0.04, 0.96], y=[0.06, 0.88]),
                            marker=dict(colors=cols, line=dict(color="#fff", width=1.5)),
                            textinfo="label+percent",
                            textposition="auto",
                            textfont=dict(size=12),
                            sort=True,
                            direction="clockwise",
                            insidetextorientation="horizontal",
                            hovertemplate="<b>%{label}</b><br>%{value:,.0f} · %{percent}<extra></extra>",
                        )
                    ]
                )
                _center_sub = (
                    basis_note
                    if brk == "Traffic"
                    else ("Qualified vs not" if brk == "Leads Conversion" else "Stages in scope")
                )
                fig_d.update_layout(
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.02,
                        yref="paper",
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    ),
                    margin={**_perf_plot_margin_donut},
                    height=_chart_h,
                    paper_bgcolor="white",
                    hoverlabel=dict(font_size=12),
                    annotations=[
                        dict(
                            text=f"<b>{total_v:,.0f}</b><br><span style='font-size:11px;color:#64748b'>{_center_sub}</span>",
                            xref="paper",
                            yref="paper",
                            x=0.5,
                            y=0.47,
                            xanchor="center",
                            yanchor="middle",
                            showarrow=False,
                            font=dict(size=17, color="#0f172a"),
                        ),
                    ],
                )
                st.plotly_chart(fig_d, width="stretch", key=f"{key_suffix}_pl_mpo_perf_breakdown")


def _master_view_spend_authoritative_from_grid(spend_grid: pd.DataFrame) -> pd.DataFrame:
    """``month`` × display ``Market`` spend from the spend pivot — not from ``master_df`` joins (those can show 0 in UI)."""
    if spend_grid is None or spend_grid.empty or "cost" not in spend_grid.columns or "country" not in spend_grid.columns:
        return pd.DataFrame(columns=["month", "Market", "spend"])
    x = spend_grid.copy()
    x["month"] = x["month"].map(_month_norm_key)
    x["Market"] = x["country"].map(_market_display_from_join_key)
    x["cost"] = pd.to_numeric(x["cost"], errors="coerce").fillna(0)
    return (
        x.groupby(["month", "Market"], as_index=False, dropna=False)["cost"]
        .sum()
        .rename(columns={"cost": "spend"})
    )


def _auth_spend_lookup_by_norm_keys(spend_grid: pd.DataFrame) -> pd.DataFrame:
    """Unique ``(month_norm, market_label) → spend`` for vectorized reindex onto ``gm`` rows."""
    auth = _master_view_spend_authoritative_from_grid(spend_grid)
    if auth.empty:
        return pd.DataFrame(columns=["__k1", "__k2", "spend"])
    auth["__k1"] = auth["month"].map(_month_norm_key)
    auth["__k2"] = auth["Market"].astype(str).str.strip()
    auth["spend"] = pd.to_numeric(auth["spend"], errors="coerce").fillna(0)
    return auth.groupby(["__k1", "__k2"], as_index=False)["spend"].sum()


def _overlay_spend_from_spend_grid_on_gm(gm: pd.DataFrame, spend_grid: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Prefer worksheet pivot spend per row; ``reindex`` avoids fragile ``merge`` dtype/key mismatches."""
    if spend_grid is None or spend_grid.empty or gm.empty:
        return gm
    lut = _auth_spend_lookup_by_norm_keys(spend_grid)
    if lut.empty or float(lut["spend"].sum()) < 1e-9:
        return gm
    out = gm.copy()
    k1 = out["month"].map(_month_norm_key)
    k2 = out["Market"].astype(str).str.strip()
    ser = lut.drop_duplicates(["__k1", "__k2"], keep="last").set_index(["__k1", "__k2"])["spend"]
    mi = pd.MultiIndex.from_arrays([k1, k2])
    s_auth = pd.to_numeric(ser.reindex(mi), errors="coerce")
    s_auth.index = out.index
    s_old = pd.to_numeric(out["spend"], errors="coerce").fillna(0) if "spend" in out.columns else pd.Series(0.0, index=out.index)
    out["spend"] = s_auth.where(s_auth > 1e-6, s_old)
    out["spend"] = pd.to_numeric(out["spend"], errors="coerce").fillna(0)
    return out


def _master_union_gm_with_spend_pivot(gm: pd.DataFrame, spend_grid: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Outer-join the CRM grid to the **spend worksheet pivot** (month × Market), like an Excel pivot.

    ``gm`` from ``master_df`` only includes (month, country) keys that survived the CRM merge, so real spend
    months/countries were missing from the table even when the sheet had values.
    """
    if spend_grid is None or spend_grid.empty:
        return gm
    auth = _master_view_spend_authoritative_from_grid(spend_grid)
    if auth.empty:
        return gm
    a = auth.copy()
    a["month"] = a["month"].map(_month_norm_key)
    a["Market"] = a["Market"].astype(str).str.strip()
    _bad_mo = a["month"].astype(str).str.strip().str.lower().isin(["", "nan", "nat", "none"])
    a = a.loc[~_bad_mo].copy()
    a = a.rename(columns={"spend": "_sp_sheet"})
    a = a.groupby(["month", "Market"], as_index=False)["_sp_sheet"].sum()

    b = gm.copy()
    b["month"] = b["month"].map(_month_norm_key)
    b["Market"] = b["Market"].astype(str).str.strip()
    b["_sp_crm"] = pd.to_numeric(b["spend"], errors="coerce").fillna(0)
    b = b.drop(columns=["spend"], errors="ignore")

    out = a.merge(b, on=["month", "Market"], how="outer")
    ss = pd.to_numeric(out["_sp_sheet"], errors="coerce").fillna(0)
    sc = pd.to_numeric(out["_sp_crm"], errors="coerce").fillna(0)
    out["spend"] = ss.where(ss > 1e-6, sc)
    out = out.drop(columns=["_sp_sheet", "_sp_crm"], errors="ignore")
    for col in out.columns:
        if col in ("month", "Market", "spend"):
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    return out


def _master_view_refresh_middle_east_spend_row(gm: pd.DataFrame) -> pd.DataFrame:
    """If ME countries have spend, **Middle East** = their sum; if not, keep the row as the monthly regional total (do not zero it)."""
    if gm.empty or "month" not in gm.columns or "Market" not in gm.columns or "spend" not in gm.columns:
        return gm
    out = gm.copy()
    me_label = _MIDDLE_EAST_REGION_LABEL
    for m in out["month"].dropna().unique():
        m_mask = out["month"] == m
        ix_me = out.index[m_mask & (out["Market"].astype(str).str.strip().str.lower() == me_label.lower())]
        ix_cc = out.index[
            m_mask
            & out["Market"].map(_is_middle_east_market)
            & (out["Market"].astype(str).str.strip().str.lower() != me_label.lower())
        ]
        if len(ix_me) == 1 and len(ix_cc) > 0:
            tot = float(pd.to_numeric(out.loc[ix_cc, "spend"], errors="coerce").fillna(0).sum())
            if tot > 1e-6:
                out.loc[ix_me, "spend"] = tot
    return out


def _mpo_metric_definition(metric_name: str) -> str:
    """Plain-language definition shown in drill-down (not repeated master cell copy)."""
    defs = {
        "Spend": (
            "Total marketing media cost allocated to this calendar month and market after the sheet is normalized "
            "(dates → month, country → market). It is the same additive input used for efficiency ratios."
        ),
        "CW (Inc Approved)": (
            "**Locked definition (Marketing performance headline):** count of **post-qualification / post-lead** "
            "worksheet **records** in your Market × Month scope whose **Stage** is **Closed Won** or **Approved** "
            "(same rules as ``_is_closed_won_stage_text``). Not sourced from the RAW CW truth tab or CpCW B2 math."
        ),
        "CPCW": (
            "**Cost per closed won (CpCW):** total marketing spend in the slice ÷ number of closed-won deals. "
            "It is a ratio (not summed across sheet rows)."
        ),
        "1st Month LF": (
            "Sum of first-month license fee amounts from the RAW CW / deal tab for opportunities in this month and market."
        ),
        "Actual TCV": (
            "Sum of actual total contract value (TCV) from the RAW CW tab for this month and market."
        ),
        "CPCW:LF": (
            "**Cost per close won to licence fee ratio (CpCW:LF).** It tells you how many multiples of "
            "first-month licence fee your marketing spend represents for the closed-won set in this slice. "
            "**Formula:** **CpCW ÷ (average 1st month LF per closed-won deal)** — algebraically the same as "
            "**Total Spend ÷ Σ 1st month LF** (ME **B6** = **B1 ÷ B3** on the B2/B3 row set), and the same as "
            "**B5 ÷ B4** when B5 = Spend÷CW and B4 = (Σ LF)÷CW. "
            "**CpCW** = total marketing spend ÷ number of closed-won deals. "
            "**1st month LF** = first month's licence fee from each opportunity (Salesforce), summed as Σ LF in B3. "
            "**Below 1.0** → spend is *less* than the slice's summed first-month LF (strong efficiency). "
            "**Above 1.0** → spend *exceeds* that LF sum. "
            "**Example:** **0.58** ≈ **$0.58** of marketing spend per **$1** of summed first-month LF in the slice."
        ),
        "Cost/TCV%": (
            "Spend as a percentage of actual TCV in the same month and market (spend ÷ TCV × 100)."
        ),
        "Total Leads": (
            "Lead volume for this month and market from the leads sheet / merged lead rows (row-count logic where applicable)."
        ),
    }
    return defs.get(metric_name, "Derived metric for this month and market row in the master view.")


def _mpo_calculation_trail(metric_name: str, row: pd.Series) -> list[dict[str, str]]:
    """Step table: components → how they combine → final metric (same math as the master sheet cell)."""
    spend = float(pd.to_numeric(row.get("spend", 0), errors="coerce") or 0)
    cw = int(pd.to_numeric(row.get("cw", 0), errors="coerce") or 0)
    leads = int(pd.to_numeric(row.get("leads", 0), errors="coerce") or 0)
    tcv = float(pd.to_numeric(row.get("tcv", 0), errors="coerce") or 0)
    lf = float(pd.to_numeric(row.get("lf", 0), errors="coerce") or 0)

    def _fmt_small_money(x: float) -> str:
        return _format_spend_k(x) if x and x == x else "—"

    def _fmt_tcv(x: float) -> str:
        return _format_tcv_short(x) if x and x == x else "—"

    out: list[dict[str, str]] = []
    if metric_name == "Spend":
        out = [
            {
                "Step": "1",
                "Component": "Σ marketing cost (month × market in master merge)",
                "Value": _format_spend_k(spend),
                "Combines as": "Final cell value (additive)",
            },
        ]
    elif metric_name == "CW (Inc Approved)":
        out = [
            {
                "Step": "1",
                "Component": "Σ closed won (post-qualification pipeline, month × market)",
                "Value": f"{cw:,}",
                "Combines as": "Final cell value (additive)",
            },
        ]
    elif metric_name == "Total Leads":
        out = [
            {
                "Step": "1",
                "Component": "Σ leads (master merge for this slice)",
                "Value": f"{leads:,}",
                "Combines as": "Final cell value (additive)",
            },
        ]
    elif metric_name == "1st Month LF":
        out = [
            {
                "Step": "1",
                "Component": "Σ first_month_lf (RAW CW, this slice)",
                "Value": _fmt_small_money(lf),
                "Combines as": "Final cell value (additive)",
            },
        ]
    elif metric_name == "Actual TCV":
        out = [
            {
                "Step": "1",
                "Component": "Σ tcv (RAW CW, this slice)",
                "Value": _fmt_tcv(tcv),
                "Combines as": "Final cell value (additive)",
            },
        ]
    elif metric_name == "CPCW":
        cpcw = (spend / cw) if cw else float("nan")
        out = [
            {"Step": "1", "Component": "Spend (numerator)", "Value": _format_spend_k(spend), "Combines as": "—"},
            {"Step": "2", "Component": "CW (Inc Approved) (denominator)", "Value": f"{cw:,}", "Combines as": "—"},
            {
                "Step": "3",
                "Component": "CPCW = Spend ÷ CW",
                "Value": _format_compact_k(cpcw) if cw and cpcw == cpcw else "—",
                "Combines as": "Final ratio",
            },
        ]
    elif metric_name == "CPCW:LF":
        cpcw_ratio = (spend / cw) if cw else float("nan")
        avg_lf = (lf / cw) if cw else float("nan")
        ratio = (spend / lf) if lf else float("nan")
        out = [
            {"Step": "1", "Component": "Spend", "Value": _format_spend_k(spend), "Combines as": "—"},
            {"Step": "2", "Component": "CW (Inc Approved)", "Value": f"{cw:,}", "Combines as": "—"},
            {
                "Step": "3",
                "Component": "CpCW = Spend ÷ CW",
                "Value": _format_compact_k(cpcw_ratio) if cw and cpcw_ratio == cpcw_ratio else "—",
                "Combines as": "—",
            },
            {
                "Step": "4",
                "Component": "Avg 1st Month LF per CW = Σ LF ÷ CW",
                "Value": _format_compact_k(float(avg_lf)) if cw and lf and avg_lf == avg_lf else "—",
                "Combines as": "—",
            },
            {"Step": "5", "Component": "Σ 1st Month LF (same slice)", "Value": _fmt_small_money(lf), "Combines as": "—"},
            {
                "Step": "6",
                "Component": "CpCW:LF = CpCW ÷ avg LF = B1÷B3 = Spend÷Σ LF (= B5÷B4)",
                "Value": _format_ratio_cpcw_lf(float(ratio)) if cw and lf and ratio == ratio else "—",
                "Combines as": "Final ratio (ME B6 / Looker)",
            },
        ]
    elif metric_name == "Cost/TCV%":
        pct = (spend / tcv * 100.0) if tcv else float("nan")
        out = [
            {"Step": "1", "Component": "Spend", "Value": _format_spend_k(spend), "Combines as": "—"},
            {"Step": "2", "Component": "Actual TCV", "Value": _fmt_tcv(tcv), "Combines as": "—"},
            {
                "Step": "3",
                "Component": "Cost/TCV % = (Spend ÷ TCV) × 100",
                "Value": f"{pct:.2f}%" if tcv and pct == pct else "—",
                "Combines as": "Final %",
            },
        ]
    else:
        out = [{"Step": "1", "Component": metric_name, "Value": "—", "Combines as": "See master merge"}]
    return out


def _mpo_metric_source_rows_for_metric(
    metric_name: str,
    source_rows: Optional[list[dict[str, str]]],
) -> list[dict[str, str]]:
    """Only source lines that feed the selected metric (avoid repeating unrelated pivots)."""
    if not source_rows:
        return []
    m_src = {
        "Spend": {("Spend worksheet", "Spend"), ("Spend worksheet", "Clicks"), ("Spend worksheet", "Impressions")},
        "CPCW": {
            ("Spend worksheet", "Spend"),
            ("Post qualification", "Closed won"),
        },
        "CW (Inc Approved)": {("Post qualification", "Closed won")},
        "Total Leads": {("Raw leads", "Total leads"), ("Raw leads", "Qualified")},
        "1st Month LF": {("RAW CW", "1st Month LF")},
        "Actual TCV": {("RAW CW", "Actual TCV")},
        "CPCW:LF": {
            ("Spend worksheet", "Spend"),
            ("Post qualification", "Closed won"),
            ("RAW CW", "1st Month LF"),
        },
        "Cost/TCV%": {("Spend worksheet", "Spend"), ("RAW CW", "Actual TCV")},
    }
    want = m_src.get(metric_name)
    if not want:
        return source_rows
    out = []
    for r in source_rows:
        key = (r.get("source"), r.get("metric"))
        if key in want:
            out.append(r)
    return out if out else source_rows


def _mpo_join_keys_for_market_label(market_label: str) -> set[str]:
    """Join-key set for spend/CRM rows that roll up to this Master ``Market`` row (incl. Middle East = all ME countries)."""
    ml = str(market_label).strip().lower()
    if ml in _REGION_SUBTOTAL_NAMES_LOWER:
        return set(_MIDDLE_EAST_MARKET_KEYS) | {"middle east"}
    for k, v in _MARKET_DISPLAY_FROM_KEY.items():
        if v.strip().lower() == ml:
            return {k}
    jk = _country_join_key(market_label)
    if jk and jk not in ("unknown", "nan", "<na>", ""):
        return {jk}
    nk = _norm_market_key(market_label)
    return {nk} if nk else set()


def _mpo_prep_spend_extract_for_slice(df: pd.DataFrame) -> pd.DataFrame:
    """Align month/country with the same path as spend pivots (date → month, country join keys)."""
    if df.empty:
        return df
    x = df.copy()
    if "cost" in x.columns:
        x = _canonicalize_spend_month_column(x)
    return _normalize_master_merge_frame(x)


def _mpo_df_slice_month_market(
    df: Optional[pd.DataFrame],
    month_key: str,
    market_label: str,
) -> pd.DataFrame:
    """Rows from a source tab filtered to normalized month key and market (join-key aware; ME row = all ME countries)."""
    if df is None or df.empty or not month_key:
        return pd.DataFrame()
    x = _mpo_prep_spend_extract_for_slice(df.copy())
    if "month" not in x.columns or "country" not in x.columns:
        return pd.DataFrame()
    x["_mk"] = x["month"].map(_month_norm_key)
    keys = _mpo_join_keys_for_market_label(market_label)
    if not keys:
        return pd.DataFrame()
    jk_series = x["country"].map(_country_join_key)
    m = x["_mk"].eq(month_key) & jk_series.isin(keys)
    out = x.loc[m].copy()
    if not out.empty:
        return out
    # Fallback: match by display label (legacy rows where join key differs)
    x["_ml"] = x["country"].map(_market_display_from_join_key).astype(str).str.strip()
    m2 = x["_mk"].eq(month_key) & x["_ml"].eq(str(market_label).strip())
    return x.loc[m2].copy()


def _mpo_df_slice_month_channel(
    df: Optional[pd.DataFrame],
    month_key: str,
    channel_display: str,
) -> pd.DataFrame:
    """Spend rows for normalized **month** × **sheet channel** (display label matches master **Channel** column)."""
    if df is None or df.empty or not month_key:
        return pd.DataFrame()
    x = _mpo_prep_spend_extract_for_slice(df.copy())
    if "month" not in x.columns:
        return pd.DataFrame()
    x["_mk"] = x["month"].map(_month_norm_key)
    x["_sch"] = _pmc_sheet_channel_series(x)
    x["_disp"] = x["_sch"].map(_market_display_from_join_key).astype(str).str.strip()
    ch = str(channel_display).strip()
    if ch in ("", "—", "-"):
        return pd.DataFrame()
    mk = str(_month_norm_key(month_key)).strip()
    m = x["_mk"].eq(mk) & (x["_disp"].eq(ch) | x["_sch"].astype(str).str.strip().eq(ch))
    return x.loc[m].copy()


def _mpo_df_slice_month_market_fallback_month_only(
    df: Optional[pd.DataFrame],
    month_key: str,
) -> pd.DataFrame:
    """All rows for this month (used when market slice is empty but master still shows spend)."""
    if df is None or df.empty or not month_key:
        return pd.DataFrame()
    x = _mpo_prep_spend_extract_for_slice(df.copy())
    if "month" not in x.columns:
        return pd.DataFrame()
    x["_mk"] = x["month"].map(_month_norm_key)
    return x.loc[x["_mk"].eq(month_key)].copy()


def _mpo_country_section_label(ckey: Any) -> str:
    """Stable label for a spend row ``country`` group key (groupby key)."""
    try:
        if ckey is None or (isinstance(ckey, float) and pd.isna(ckey)):
            return "(blank country)"
        s = str(ckey).strip()
        if not s or s.lower() in ("nan", "none", "<na>"):
            return "(blank country)"
        return _market_display_from_join_key(s)
    except Exception:
        return "Unknown"


def _mpo_spend_records_display_table(
    sp: pd.DataFrame,
    *,
    include_market_column: bool = True,
    max_rows: int = 500,
) -> pd.DataFrame:
    """Line-level spend rows for the modal (readable column order, capped row count)."""
    if sp.empty:
        return pd.DataFrame()
    d = sp.copy()
    drop_internal = [c for c in d.columns if str(c).startswith("_")]
    d = d.drop(columns=[c for c in drop_internal if c in d.columns], errors="ignore")
    pref = [
        "cost",
        "country",
        "month",
        "date",
        "campaign_name",
        "campaign",
        "utm_campaign",
        "channel",
        "platform",
        "utm_source",
        "clicks",
        "impressions",
    ]
    cols = [c for c in pref if c in d.columns]
    if not cols:
        cols = [c for c in d.columns if c not in ("_mk", "_ml")][:18]
    out = d[cols].head(max_rows).copy()
    if "cost" in out.columns:
        out = out.rename(columns={"cost": "Spend"})
        out["Spend"] = pd.to_numeric(out["Spend"], errors="coerce")
    if "country" in out.columns:
        if include_market_column:
            out["Market"] = out["country"].map(_market_display_from_join_key).fillna(out["country"].astype(str))
        out = out.drop(columns=["country"])
    if "month" in out.columns:
        out["Month"] = out["month"].astype(str)
        out = out.drop(columns=["month"])
    first = [c for c in ("Spend", "Market", "Month", "date") if c in out.columns]
    rest = [c for c in out.columns if c not in first]
    return out[first + rest]


def _mpo_iter_spend_record_tables_by_country(
    sp: pd.DataFrame,
    *,
    max_total_rows: int = 500,
) -> list[tuple[str, float, int, pd.DataFrame]]:
    """Split spend rows into sorted per-country tables; caps total rows across sections."""
    if sp.empty:
        return []
    if "country" not in sp.columns:
        tbl = _mpo_spend_records_display_table(
            sp,
            include_market_column=False,
            max_rows=max_total_rows,
        )
        if tbl.empty:
            return []
        subtotal = float(pd.to_numeric(tbl.get("Spend", 0), errors="coerce").fillna(0).sum())
        return [("All rows (no country column)", subtotal, len(tbl), tbl)]

    remaining = max_total_rows
    sections: list[tuple[str, float, int, pd.DataFrame]] = []
    grouped = sorted(
        sp.groupby("country", dropna=False),
        key=lambda kv: _mpo_country_section_label(kv[0]).lower(),
    )
    for ckey, sub in grouped:
        if remaining <= 0:
            break
        label = _mpo_country_section_label(ckey)
        tbl = _mpo_spend_records_display_table(
            sub,
            include_market_column=False,
            max_rows=remaining,
        )
        if tbl.empty:
            continue
        subtotal = float(pd.to_numeric(tbl["Spend"], errors="coerce").fillna(0).sum()) if "Spend" in tbl.columns else 0.0
        n = int(len(tbl))
        sections.append((label, subtotal, n, tbl))
        remaining -= n
    return sections


def _mpo_month_period_range_caption(month_key: str) -> str:
    try:
        p = pd.Period(str(month_key), freq="M")
        start = p.to_timestamp().strftime("%B %d, %Y")
        end = (p + 1).to_timestamp().strftime("%B %d, %Y")
        return f"{start} - {end}"
    except Exception:
        return ""


def _mpo_build_spend_drilldown_table(sp: pd.DataFrame) -> pd.DataFrame:
    """Campaign / line-item style pivot for spend rows (matches reference Marketing Performance table)."""
    if sp.empty or "cost" not in sp.columns:
        return pd.DataFrame(
            columns=["Campaign name", "Total spend", "Platform", "Countries", "Records"]
        )
    sp = sp.copy()
    sp["_cost"] = pd.to_numeric(sp["cost"], errors="coerce").fillna(0.0)
    group_col: Optional[str] = None
    for c in ("campaign_name", "campaign", "utm_campaign", "channel", "utm_source", "platform"):
        if c in sp.columns and sp[c].notna().any():
            group_col = c
            break
    out_rows: list[dict[str, Any]] = []
    if group_col is None:
        plat = "—"
        if "platform" in sp.columns and sp["platform"].notna().any():
            plat = str(sp["platform"].dropna().iloc[0])
        ctry = "—"
        if "country" in sp.columns:
            ctry = ", ".join(sp["country"].dropna().astype(str).str.strip().unique().tolist()[:12])
        out_rows.append(
            {
                "Campaign name": "All spend rows (no campaign column)",
                "Total spend": float(sp["_cost"].sum()),
                "Platform": plat,
                "Countries": ctry,
                "Records": int(len(sp)),
            }
        )
    else:
        for key, sub in sp.groupby(group_col, dropna=False):
            nm = str(key).strip()
            if not nm or nm.lower() in ("nan", "none"):
                nm = "(blank)"
            if len(nm) > 72:
                nm = nm[:69] + "..."
            plat = "—"
            if "platform" in sub.columns and sub["platform"].notna().any():
                plat = str(sub["platform"].dropna().iloc[0])
            ctry = "—"
            if "country" in sub.columns:
                ctry = ", ".join(sorted(set(sub["country"].dropna().astype(str).str.strip().unique().tolist()))[:12])
            out_rows.append(
                {
                    "Campaign name": nm,
                    "Total spend": float(pd.to_numeric(sub["cost"], errors="coerce").fillna(0).sum()),
                    "Platform": plat,
                    "Countries": ctry,
                    "Records": int(len(sub)),
                }
            )
    tab = pd.DataFrame(out_rows)
    if not tab.empty:
        tab["Total spend"] = tab["Total spend"].map(lambda v: f"${v:,.2f}" if pd.notna(v) else "—")
    return tab


def _mpo_source_pivot_rows(
    *,
    month_key: str,
    market_label: str,
    detail_sources: Optional[dict[str, pd.DataFrame]],
    pivot_dimension: Literal["market", "channel"] = "market",
) -> list[dict[str, str]]:
    if not detail_sources:
        return []

    def _slice(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if pivot_dimension == "channel":
            return _mpo_df_slice_month_channel(df, month_key, market_label)
        return _mpo_df_slice_month_market(df, month_key, market_label)

    rows: list[dict[str, str]] = []

    sp = _slice(detail_sources.get("spend"))
    if not sp.empty:
        spend = float(pd.to_numeric(sp.get("cost", 0), errors="coerce").fillna(0).sum())
        clicks = int(pd.to_numeric(sp.get("clicks", 0), errors="coerce").fillna(0).sum())
        impr = int(pd.to_numeric(sp.get("impressions", 0), errors="coerce").fillna(0).sum())
        rows += [
            {"source": "Spend worksheet", "metric": "Spend", "value": _format_spend_k(spend)},
            {"source": "Spend worksheet", "metric": "Clicks", "value": f"{clicks:,}"},
            {"source": "Spend worksheet", "metric": "Impressions", "value": f"{impr:,}"},
        ]
    if pivot_dimension == "channel":
        return rows

    ld = _slice(detail_sources.get("leads"))
    if not ld.empty:
        leads_n = _lead_rows_count(ld)
        qual_n = _qualified_count_from_leads(ld)
        rows += [
            {"source": "Raw leads", "metric": "Total leads", "value": f"{int(leads_n):,}"},
            {"source": "Raw leads", "metric": "Qualified", "value": f"{int(qual_n):,}"},
        ]

    pq = _slice(detail_sources.get("post"))
    if not pq.empty:
        cw = int(pd.to_numeric(pq.get("closed_won", 0), errors="coerce").fillna(0).sum())
        pitching = int(pd.to_numeric(pq.get("pitching", 0), errors="coerce").fillna(0).sum())
        negotiation = int(pd.to_numeric(pq.get("negotiation", 0), errors="coerce").fillna(0).sum())
        commitment = int(pd.to_numeric(pq.get("commitment", 0), errors="coerce").fillna(0).sum())
        rows += [
            {"source": "Post qualification", "metric": "Closed won", "value": f"{cw:,}"},
            {"source": "Post qualification", "metric": "Pitching", "value": f"{pitching:,}"},
            {"source": "Post qualification", "metric": "Negotiation", "value": f"{negotiation:,}"},
            {"source": "Post qualification", "metric": "Commitment", "value": f"{commitment:,}"},
        ]

    cwf = _slice(detail_sources.get("cw"))
    if not cwf.empty:
        tcv = float(pd.to_numeric(cwf.get("tcv", 0), errors="coerce").fillna(0).sum())
        lf = float(pd.to_numeric(cwf.get("first_month_lf", 0), errors="coerce").fillna(0).sum())
        rows += [
            {"source": "RAW CW", "metric": "Actual TCV", "value": _format_tcv_short(tcv) if tcv else "—"},
            {"source": "RAW CW", "metric": "1st Month LF", "value": _format_spend_k(lf) if lf else "—"},
        ]

    return rows


def _render_mpo_master_metric_detail_card(
    *,
    row: pd.Series,
    metric_name: str,
    month_label: str,
    market_label: str,
    value_text: str,
    source_rows: Optional[list[dict[str, str]]] = None,
    detail_sources: Optional[dict[str, pd.DataFrame]] = None,
    month_key: str = "",
    impr: int = 0,
    clk: int = 0,
) -> None:
    period_caption = _mpo_month_period_range_caption(month_key) if month_key else ""

    if metric_name == "Spend":
        spend_src = detail_sources.get("spend") if detail_sources else None
        sp = _mpo_df_slice_month_market(spend_src, month_key, market_label)
        used_month_fallback = False
        if sp.empty and month_key:
            sp_fb = _mpo_df_slice_month_market_fallback_month_only(spend_src, month_key)
            if not sp_fb.empty:
                sp = sp_fb
                used_month_fallback = True
        spend_sum = float(pd.to_numeric(sp.get("cost", 0), errors="coerce").fillna(0).sum()) if not sp.empty else float(
            pd.to_numeric(row.get("spend", 0), errors="coerce") or 0
        )
        n_rows = int(len(sp)) if not sp.empty else 0
        drill = _mpo_build_spend_drilldown_table(sp)

        st.markdown(
            f'<div class="mpo-modal-hero">'
            f'<div class="mpo-modal-hero-title">Spend Details for {html.escape(month_label)}</div>'
            f'<div class="mpo-modal-hero-sub">{html.escape(period_caption)}</div>'
            f'<div class="mpo-modal-hero-market">{html.escape(market_label)}</div>'
            "</div>",
            unsafe_allow_html=True,
        )
        if used_month_fallback:
            st.warning(
                "Spend rows could not be matched to this **market** using country keys (see Middle East / naming). "
                "Showing **all spend rows for this month** in the extract so you still see line items."
            )
        k1, k2, k3 = st.columns(3)
        with k1:
            st.markdown(
                f'<div class="mpo-modal-card mpo-modal-card--primary">'
                f'<div class="mpo-modal-card-label">Spend</div>'
                f'<div class="mpo-modal-card-value">{html.escape(_format_spend_k(spend_sum))}</div>'
                "</div>",
                unsafe_allow_html=True,
            )
        with k2:
            st.markdown(
                f'<div class="mpo-modal-card">'
                f'<div class="mpo-modal-card-label">Period</div>'
                f'<div class="mpo-modal-card-value-sm">{html.escape(month_label)}</div>'
                "</div>",
                unsafe_allow_html=True,
            )
        with k3:
            st.markdown(
                f'<div class="mpo-modal-card">'
                f'<div class="mpo-modal-card-label">Count</div>'
                f'<div class="mpo-modal-card-value-sm">{n_rows:,} rows</div>'
                "</div>",
                unsafe_allow_html=True,
            )
        st.caption(f"Master sheet cell (formatted): {value_text}")
        sp_by_country = _mpo_iter_spend_record_tables_by_country(sp)
        st.markdown("##### Spend records (by country)")
        if not sp_by_country:
            st.info(
                "No line-level spend rows in the extract matched this month (and market). "
                "The master cell can still show spend from **merged / allocated** totals. "
                "Check that the Spend tab has **month** and **country** populated for detail rows."
            )
        else:
            shown = sum(len(t[3]) for t in sp_by_country)
            st.caption(
                f"Showing {shown:,} line-level row(s) across {len(sp_by_country)} country group(s) "
                f"(matched slice {n_rows:,} rows; up to 500 lines total per view)."
            )
            for _ci, (_clabel, _csub, _cn, sp_rec) in enumerate(sp_by_country):
                if _ci:
                    st.divider()
                st.markdown(
                    f"**{html.escape(_clabel)}** — {_format_spend_k(_csub)} · {_cn:,} row(s)"
                )
                st.dataframe(
                    sp_rec,
                    width="stretch",
                    hide_index=True,
                    height=min(420, max(140, 56 + _cn * 28)),
                    key=f"mpo_spend_rec_{month_key}_{market_label}_{_ci}",
                )
        st.markdown(f"##### By campaign ({len(drill)})")
        if drill.empty:
            st.info(
                "No campaign-level roll-up could be built from the matched rows (missing campaign column or zero cost)."
            )
        else:
            st.dataframe(
                drill,
                width="stretch",
                hide_index=True,
                key=f"mpo_spend_drill_{month_key}_{market_label}",
            )
        with st.expander("Metric definition & formula", expanded=False):
            st.write(_mpo_metric_definition(metric_name))
        return

    st.markdown(
        f'<div class="mpo-modal-hero">'
        f'<div class="mpo-modal-hero-title">{html.escape(metric_name)} details for {html.escape(month_label)}</div>'
        f'<div class="mpo-modal-hero-sub">{html.escape(period_caption)}</div>'
        f'<div class="mpo-modal-hero-market">{html.escape(market_label)}</div>'
        "</div>",
        unsafe_allow_html=True,
    )
    st.caption(f"Master sheet cell value: {value_text}")
    st.markdown("##### What this metric means")
    st.write(_mpo_metric_definition(metric_name))
    trail = _mpo_calculation_trail(metric_name, row)
    trail_df = pd.DataFrame(trail)
    st.markdown("##### Calculation trail (inputs → final value)")
    st.dataframe(
        trail_df,
        width="stretch",
        hide_index=True,
        key=f"mpo_trail_{metric_name}_{market_label}_{month_label}",
    )
    filtered = _mpo_metric_source_rows_for_metric(metric_name, source_rows)
    if filtered:
        src_df = pd.DataFrame(filtered)
        src_df = src_df.rename(columns={"source": "Source tab / roll-up", "metric": "Field", "value": "Value"})
        st.markdown("##### Source check (only rows that feed this metric)")
        st.dataframe(
            src_df,
            width="stretch",
            hide_index=True,
            key=f"mpo_src_metric_{metric_name}_{market_label}_{month_label}",
        )


def _t3b3_quarter_tuple_from_month(m: Any) -> Optional[tuple[int, int]]:
    """T3B3 quarter with **one-month negative offset** from calendar quarters (trailing-close buffer).

    **Q1** = Dec, Jan, Feb **Q2** = Mar, Apr, May **Q3** = Jun, Jul, Aug **Q4** = Sep, Oct, Nov

    The label **year** is the calendar year that owns **Jan–Nov**; **December** maps to **Q1 of the following
    T3B3 year** (e.g. Dec-2025 → ``(2026, 1)`` with Jan-2026 and Feb-2026).
    """
    k = _month_norm_key(m)
    if not k:
        return None
    try:
        per = pd.Period(k, freq="M")
        y, mo = int(per.year), int(per.month)
    except Exception:
        return None
    if mo == 12:
        return (y + 1, 1)
    if mo in (1, 2):
        return (y, 1)
    if mo in (3, 4, 5):
        return (y, 2)
    if mo in (6, 7, 8):
        return (y, 3)
    if mo in (9, 10, 11):
        return (y, 4)
    return None


def _t3b3_quarter_label_from_tuple(yq: tuple[int, int]) -> str:
    y, q = yq
    return f"{y} - T3B3 Q{q}"


def _t3b3_goal_cpcw_lf_rows_from_gm(gm: pd.DataFrame) -> pd.DataFrame:
    """CPCW:LF **goal** lines: (1) UAE, (2) Saudi Arabia, (3) Kuwait + Bahrain combined — same T3B3 quarters."""
    cols = [
        "T3B3 Quarter",
        "CPCW:LF goal scope",
        "Spend",
        "CW (Inc Approved)",
        "CPCW",
        "1st Month LF",
        "Actual TCV",
        "CPCW:LF",
        "Cost/TCV%",
        "Total Leads",
        "Qualified",
        "SQL%",
    ]
    if gm.empty or "month" not in gm.columns or "Market" not in gm.columns:
        return pd.DataFrame(columns=cols)
    reg_lower = _REGION_SUBTOTAL_NAMES_LOWER
    src = gm.loc[~gm["Market"].astype(str).str.strip().str.lower().isin(reg_lower)].copy()
    if src.empty:
        return pd.DataFrame(columns=cols)
    src = src.copy()
    src["_jk"] = src["Market"].map(lambda m: _country_join_key(str(m)))
    src["_yq"] = src["month"].map(_t3b3_quarter_tuple_from_month)
    src = src[src["_yq"].notna()].copy()
    for c in ("spend", "cw", "tcv", "lf", "leads", "qualified"):
        if c not in src.columns:
            src[c] = 0.0
        src[c] = pd.to_numeric(src[c], errors="coerce").fillna(0.0)

    yq_list = sorted(
        {t for t in src["_yq"].dropna().unique().tolist() if t},
        key=lambda t: (int(t[0]), int(t[1])),
        reverse=True,
    )
    rows_out: list[dict[str, Any]] = []
    for yq in yq_list:
        sub = src.loc[src["_yq"] == yq]
        if sub.empty:
            continue
        qlab = _t3b3_quarter_label_from_tuple((int(yq[0]), int(yq[1])))
        uae = sub.loc[sub["_jk"].eq("united arab emirates")]
        saudi = sub.loc[sub["_jk"].eq("saudi arabia")]
        kw_bh = sub.loc[sub["_jk"].isin({"kuwait", "bahrain"})]
        for label, chunk in (
            ("UAE", uae),
            ("Saudi Arabia", saudi),
            ("Kuwait + Bahrain", kw_bh),
        ):
            if chunk.empty:
                continue
            d = _t3b3_metric_dict_sum_frame(chunk)
            raw: dict[str, Any] = {
                "spend": d.get("spend", 0.0),
                "cw": d.get("cw", 0.0),
                "tcv": d.get("tcv", 0.0),
                "lf": d.get("lf", 0.0),
                "leads": d.get("leads", 0.0),
                "qualified": d.get("qualified", 0.0),
            }
            _t3b3_add_derived_from_sums(raw)
            rows_out.append(
                {
                    "T3B3 Quarter": qlab,
                    "CPCW:LF goal scope": label,
                    "Spend": raw.get("Spend"),
                    "CW (Inc Approved)": raw.get("CW (Inc Approved)"),
                    "CPCW": raw.get("CPCW"),
                    "1st Month LF": raw.get("1st Month LF"),
                    "Actual TCV": raw.get("Actual TCV"),
                    "CPCW:LF": raw.get("CPCW:LF"),
                    "Cost/TCV%": raw.get("Cost/TCV%"),
                    "Total Leads": raw.get("Total Leads"),
                    "Qualified": raw.get("Qualified"),
                    "SQL%": raw.get("SQL%"),
                }
            )

    if not rows_out:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows_out)[cols]


def _t3b3_quarter_display_and_edges(
    df: pd.DataFrame,
    *,
    qcol: str = "T3B3 Quarter",
) -> tuple[pd.DataFrame, list[bool], list[bool], list[bool]]:
    """Show quarter label on **Middle East** subtotal only (detailed table); goals table last scope per quarter; else sensible fallbacks."""
    out = df.copy()
    n = len(out)
    if n == 0 or qcol not in out.columns:
        return out, [], [], []
    labs_orig = [str(out[qcol].iloc[i]).strip() for i in range(n)]
    label_col = "Market" if "Market" in out.columns else ("CPCW:LF goal scope" if "CPCW:LF goal scope" in out.columns else None)
    mlabels: Optional[list[str]] = None
    if label_col:
        mlabels = [str(out[label_col].iloc[i]).strip().lower() for i in range(n)]
    me_lbl = _MIDDLE_EAST_REGION_LABEL.strip().lower()
    is_goals = "CPCW:LF goal scope" in out.columns

    def _is_me_row(idx: int) -> bool:
        if not mlabels:
            return False
        t = mlabels[idx]
        return t == me_lbl or t == "middle east (subtotal)"

    is_first = [False] * n
    is_last = [False] * n
    shade_alt = [False] * n
    gid = 0
    for pos in range(n):
        cur = labs_orig[pos]
        cl = cur.lower()
        if cl == "grand total":
            is_first[pos] = pos == 0 or str(labs_orig[pos - 1]).lower() != "grand total"
            is_last[pos] = True
            shade_alt[pos] = False
            continue
        if pos == 0:
            is_first[pos] = True
        else:
            is_first[pos] = labs_orig[pos] != labs_orig[pos - 1]
        if pos == n - 1:
            is_last[pos] = True
        else:
            is_last[pos] = labs_orig[pos] != labs_orig[pos + 1]
        if pos == 0 or labs_orig[pos] != labs_orig[pos - 1]:
            gid += 1
        shade_alt[pos] = gid % 2 == 0

    qi = out.columns.get_loc(qcol)
    i = 0
    while i < n:
        lab = labs_orig[i]
        if lab.lower() == "grand total":
            out.iat[i, qi] = lab
            i += 1
            continue
        j = i
        while j + 1 < n and labs_orig[j + 1] == lab:
            j += 1
        span = list(range(i, j + 1))
        me_idxs = [p for p in span if _is_me_row(p)]
        if me_idxs:
            show_p = me_idxs[0]
        elif len(span) == 1:
            show_p = span[0]
        else:
            show_p = span[-1] if is_goals else span[0]
        for p in span:
            out.iat[p, qi] = lab if p == show_p else ""
        i = j + 1

    return out, is_first, is_last, shade_alt


def _t3b3_view_style_css(
    df: pd.DataFrame,
    *,
    quarter_first: Optional[list[bool]] = None,
    quarter_last: Optional[list[bool]] = None,
    shade_alt: Optional[list[bool]] = None,
) -> pd.DataFrame:
    """Same palette as master view; ``T3B3 Quarter`` column + bold subtotals / grand total."""
    _align_c = "text-align: center; vertical-align: middle;"
    _align_l = "text-align: left; vertical-align: middle; padding-left: 8px;"

    def _cell(base: str, *, center: bool = True) -> str:
        a = _align_c if center else _align_l
        b = base.strip().rstrip(";")
        return f"{b}; {a}" if b else a

    qcol = "T3B3 Quarter"
    css = pd.DataFrame("", index=df.index, columns=df.columns)
    _label_col = "Market" if "Market" in df.columns else ("CPCW:LF goal scope" if "CPCW:LF goal scope" in df.columns else None)
    if _label_col:
        mkt_lower = df[_label_col].astype(str).str.strip().str.lower()
    else:
        mkt_lower = pd.Series("", index=df.index)
    me_lbl = _MIDDLE_EAST_REGION_LABEL.strip().lower()
    is_region = mkt_lower.eq(me_lbl) | mkt_lower.eq("middle east (subtotal)")
    is_grand = df[qcol].astype(str).str.strip().str.lower().eq("grand total")
    is_subtotal = is_region | is_grand
    non_me = ~is_region & ~is_grand
    cyan = "background-color: #e8f4f8; color: #0f172a;"
    white = "background-color: #ffffff; color: #0f172a;"
    sub_bold = "font-weight: 700;"
    ratio_sub = "background-color: #ffffff; font-weight: 700; color: #0f172a;"
    empty_cell = "background-color: #fafafa; color: #94a3b8;"

    def _rgy(val: Any, lo: float, hi: float) -> str:
        try:
            v = float(val)
        except (TypeError, ValueError):
            return _cell("background-color: #fee2e2; color: #991b1b;")
        if pd.isna(v) or v == 0.0:
            return _cell("background-color: #fee2e2; color: #991b1b;")
        if v <= lo:
            return _cell("background-color: #dcfce7; color: #166534;")
        if v <= hi:
            return _cell("background-color: #fef9c3; color: #854d0e;")
        return _cell("background-color: #fee2e2; color: #b91c1c;")

    lf_lo = lf_hi = 1.0
    ct_lo = ct_hi = 5.0
    if "CPCW:LF" in df.columns:
        s_lf = pd.to_numeric(df.loc[non_me, "CPCW:LF"], errors="coerce").dropna()
        if len(s_lf) >= 2:
            lf_lo, lf_hi = float(s_lf.quantile(0.33)), float(s_lf.quantile(0.66))
        else:
            lf_lo, lf_hi = 1.0, 2.5
    if "Cost/TCV%" in df.columns:
        s_ct = pd.to_numeric(df.loc[non_me, "Cost/TCV%"], errors="coerce").dropna()
        if len(s_ct) >= 2:
            ct_lo, ct_hi = float(s_ct.quantile(0.33)), float(s_ct.quantile(0.66))
        else:
            ct_lo, ct_hi = 5.0, 12.0
    sql_lo = sql_hi = 20.0
    if "SQL%" in df.columns:
        s_sq = pd.to_numeric(df.loc[non_me, "SQL%"], errors="coerce").dropna()
        if len(s_sq) >= 2:
            sql_lo, sql_hi = float(s_sq.quantile(0.33)), float(s_sq.quantile(0.66))
        else:
            sql_lo, sql_hi = 15.0, 30.0

    cyan_cols = {"Spend", "CW (Inc Approved)", "CPCW", "1st Month LF", "Actual TCV"}
    idx_list = list(df.index)
    for pos, i in enumerate(idx_list):
        sub = bool(is_subtotal.loc[i])
        gr = bool(is_grand.loc[i])
        sh = bool(shade_alt[pos]) if shade_alt and pos < len(shade_alt) else False
        _bg_plain = "#f8fafc" if sh and not sub else "#ffffff"
        if sub:
            bg_mkt = sub_bold + f" background-color: {_bg_plain}; color: #0f172a;"
        else:
            bg_mkt = f"background-color: {_bg_plain}; color: #0f172a;"
        c_hex = "#d8eaf1" if sh else "#e8f4f8"
        bg_cyan = f"background-color: {c_hex}; color: #0f172a;" + (sub_bold if sub else "")
        bg_lf = "#f8fafc" if sh and not sub else "#ffffff"
        row_edge = ""
        if gr:
            row_edge = "border-top: 4px solid #334155"
        elif quarter_first and pos < len(quarter_first) and quarter_first[pos] and pos > 0:
            row_edge = "border-top: 4px solid #64748b"
        if quarter_last and pos < len(quarter_last) and quarter_last[pos]:
            row_edge = (row_edge + "; " if row_edge else "") + "border-bottom: 4px solid #64748b"

        def _rx(s: str) -> str:
            if not row_edge:
                return s
            return f"{s.rstrip().rstrip(';')}; {row_edge}"

        def _ew(st: str) -> str:
            if not row_edge:
                return st
            return f"{st.rstrip().rstrip(';')}; {row_edge}"

        for col in df.columns:
            if col == qcol:
                v = df.loc[i, col]
                qcell_bg = "#eef2f7" if sh and str(v).strip() == "" else None
                if v == "" or (isinstance(v, float) and pd.isna(v)):
                    eb = f"background-color: {qcell_bg or '#fafafa'}; color: #94a3b8"
                    css.loc[i, col] = _cell(_rx(eb))
                elif gr or sub:
                    css.loc[i, col] = _cell(
                        _rx(
                            "background-color: #f1f5f9; font-weight: 700; color: #334155; border-bottom: 1px solid #e2e8f0"
                        )
                    )
                else:
                    css.loc[i, col] = _cell(
                        _rx(
                            "background-color: #f1f5f9; font-weight: 600; color: #334155; border-bottom: 1px solid #e2e8f0"
                        )
                    )
            elif col in {"Market", "CPCW:LF goal scope"}:
                css.loc[i, col] = _cell(_rx(bg_mkt), center=False)
            elif col in cyan_cols:
                css.loc[i, col] = _cell(_rx(bg_cyan))
            elif col in {"Total Leads", "Qualified"}:
                base = (f"background-color: {bg_lf}; color: #0f172a;" + sub_bold) if sub else f"background-color: {bg_lf}; color: #0f172a;"
                css.loc[i, col] = _cell(_rx(base))
            elif col == "SQL%":
                if sub:
                    css.loc[i, col] = _cell(_rx(ratio_sub))
                else:
                    css.loc[i, col] = _ew(_rgy(df.loc[i, col], sql_lo, sql_hi))
            elif col == "CPCW:LF":
                if sub:
                    css.loc[i, col] = _cell(_rx(ratio_sub))
                else:
                    css.loc[i, col] = _ew(_rgy(df.loc[i, col], lf_lo, lf_hi))
            elif col == "Cost/TCV%":
                if sub:
                    css.loc[i, col] = _cell(_rx(ratio_sub))
                else:
                    css.loc[i, col] = _ew(_rgy(df.loc[i, col], ct_lo, ct_hi))
            else:
                css.loc[i, col] = _cell(_rx(white))
    return css


def _master_build_gm_with_metrics(
    df: pd.DataFrame,
    spend_grid: Optional[pd.DataFrame],
    *,
    pivot_dimension: Literal["market", "channel"] = "market",
) -> pd.DataFrame:
    """Month × Market (or **Channel**) grid with derived KPIs — single builder for master pivot + T3B3."""
    df = _normalize_master_merge_frame(df)
    if not df.empty and "month" in df.columns:
        _mpl = df["month"].map(lambda x: _dashboard_month_plausible(_month_norm_key(x)))
        if "cost" in df.columns:
            _has_spend = pd.to_numeric(df["cost"], errors="coerce").fillna(0) > 1e-3
            _keep = _mpl | _has_spend
        else:
            _keep = _mpl
        _df_f = df.loc[_keep].copy()
        if not _df_f.empty:
            df = _df_f
    df = _collapse_duplicate_named_columns(df)
    if "cost" in df.columns:
        df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)
    if "spend" in df.columns:
        s_alt = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
        if "cost" in df.columns:
            df["cost"] = df["cost"].where(df["cost"] > 1e-6, s_alt)
        else:
            df["cost"] = s_alt
    df = _master_view_impute_month_for_spend_rows(df)

    agg: dict[str, tuple[str, str]] = {}
    for _out, _src in (
        ("spend", "cost"),
        ("cw", "closed_won"),
        ("clicks", "clicks"),
        ("leads", "leads"),
        ("qualified", "qualified"),
    ):
        if _src in df.columns:
            agg[_out] = (_src, "sum")
    if "tcv" in df.columns:
        agg["tcv"] = ("tcv", "sum")
    if "first_month_lf" in df.columns:
        agg["lf"] = ("first_month_lf", "sum")
    if "impressions" in df.columns:
        agg["impressions"] = ("impressions", "sum")
    for _src, _dst in (
        ("pitching", "pitching"),
        ("new", "new"),
        ("working", "working"),
        ("qualifying", "qualifying"),
        ("negotiation", "negotiation"),
        ("commitment", "commitment"),
        ("closed_lost", "closed_lost"),
    ):
        if _src in df.columns:
            agg[_dst] = (_src, "sum")

    g = df.groupby(["month", "country"], as_index=False, dropna=False).agg(**agg).sort_values(
        ["month", "country"], ascending=[False, True]
    )
    for _required in ("spend", "cw", "clicks", "leads", "qualified"):
        if _required not in g.columns:
            g[_required] = 0.0
    g["Market"] = g["country"].map(_market_display_from_join_key)
    sum_map: dict[str, str] = {}
    for c in (
        "spend",
        "cw",
        "tcv",
        "lf",
        "leads",
        "qualified",
        "clicks",
        "impressions",
        "pitching",
        "new",
        "working",
        "qualifying",
        "negotiation",
        "commitment",
        "closed_lost",
    ):
        if c in g.columns:
            sum_map[c] = "sum"
    gm = g.groupby(["month", "Market"], as_index=False, dropna=False).agg(sum_map)
    if pivot_dimension == "channel":
        gm = gm.rename(columns={"Market": "Channel"})
        gm = _master_view_drop_empty_months(gm)
    else:
        gm = _master_union_gm_with_spend_pivot(gm, spend_grid)
        gm = _master_view_drop_empty_months(gm)
        gm = _master_view_append_middle_east_first(gm)
        gm = _overlay_spend_from_spend_grid_on_gm(gm, spend_grid)
        gm = _master_view_refresh_middle_east_spend_row(gm)
    gm["Spend"] = gm["spend"]
    gm["CW (Inc Approved)"] = _to_int_series_safe(gm["cw"])
    if "lf" in gm.columns:
        gm["1st Month LF"] = gm["lf"]
    if "tcv" in gm.columns:
        gm["Actual TCV"] = gm["tcv"]
    gm["Total Leads"] = gm["leads"]
    gm["CPCW"] = gm.apply(
        lambda r: (r["spend"] / r["cw"]) if r["cw"] and r["cw"] > 0 else float("nan"),
        axis=1,
    )
    if "lf" in gm.columns:
        gm["CPCW:LF"] = gm.apply(
            lambda r: (r["spend"] / r["lf"]) if r["cw"] and r["cw"] > 0 and r["lf"] and r["lf"] > 0 else float("nan"),
            axis=1,
        )
    if "tcv" in gm.columns:
        gm["Cost/TCV%"] = gm.apply(
            lambda r: (r["spend"] / r["tcv"] * 100) if r["tcv"] and r["tcv"] > 0 else float("nan"),
            axis=1,
        )
    gm["CPL"] = gm.apply(
        lambda r: (r["spend"] / r["leads"]) if r["leads"] and r["leads"] > 0 else float("nan"),
        axis=1,
    )
    gm["SQL %"] = gm.apply(
        lambda r: (r["qualified"] / r["leads"] * 100) if r["leads"] and r["leads"] > 0 else float("nan"),
        axis=1,
    )

    metrics = [
        "Spend",
        "CW (Inc Approved)",
        "CPCW",
        "1st Month LF",
        "Actual TCV",
        "CPCW:LF",
        "Cost/TCV%",
        "Total Leads",
    ]
    for m in metrics:
        if m not in gm.columns:
            gm[m] = float("nan")
    return gm


def _t3b3_add_derived_from_sums(row: dict[str, Any]) -> None:
    sp = float(row.get("spend", 0) or 0)
    cw = float(row.get("cw", 0) or 0)
    lf = float(row.get("lf", 0) or 0)
    tcv = float(row.get("tcv", 0) or 0)
    leads = float(row.get("leads", 0) or 0)
    qual = float(row.get("qualified", 0) or 0)
    row["CPCW"] = (sp / cw) if cw > 0 else float("nan")
    row["CPCW:LF"] = (sp / lf) if cw > 0 and lf > 0 else float("nan")
    row["Cost/TCV%"] = (sp / tcv * 100) if tcv > 0 else float("nan")
    row["Spend"] = sp
    row["CW (Inc Approved)"] = round(cw, 2)
    row["1st Month LF"] = lf
    row["Actual TCV"] = tcv
    row["Total Leads"] = round(leads, 2)
    row["Qualified"] = round(qual, 2)
    row["SQL%"] = (qual / leads * 100) if leads > 0 else float("nan")


def _t3b3_metric_dict_sum_frame(
    frame: pd.DataFrame,
    cols: tuple[str, ...] = ("spend", "cw", "tcv", "lf", "leads", "qualified"),
) -> dict[str, float]:
    out: dict[str, float] = {}
    for c in cols:
        if frame.empty or c not in frame.columns:
            out[c] = 0.0
        else:
            out[c] = float(pd.to_numeric(frame[c], errors="coerce").fillna(0).sum())
    return out


def _t3b3_dict_has_activity(d: dict[str, float]) -> bool:
    return bool(
        d.get("spend", 0) > 1e-6
        or d.get("cw", 0) > 0
        or d.get("leads", 0) > 0
        or d.get("qualified", 0) > 0
        or d.get("tcv", 0) > 1e-6
        or d.get("lf", 0) > 1e-6
    )


def _t3b3_me_master_row_sum_for_quarter(gm: pd.DataFrame, yq: tuple[int, int]) -> Optional[dict[str, float]]:
    """Sum the **Middle East** aggregate rows from the master grid for months in ``yq`` (same roll-up as month × market)."""
    if gm.empty or "Market" not in gm.columns or "month" not in gm.columns:
        return None
    lab = _MIDDLE_EAST_REGION_LABEL.strip().lower()
    m_m = gm["Market"].astype(str).str.strip().str.lower() == lab
    m_q = gm["month"].map(lambda m: _t3b3_quarter_tuple_from_month(m) == yq)
    chunk = gm.loc[m_m & m_q]
    if chunk.empty:
        return None
    return _t3b3_metric_dict_sum_frame(chunk)


def _t3b3_detail_rows_from_gm(gm: pd.DataFrame) -> pd.DataFrame:
    """Quarter × market grid; **Middle East** roll-up matches master; includes **Qualified** / **SQL%**."""
    cols = [
        "T3B3 Quarter",
        "Market",
        "Spend",
        "CW (Inc Approved)",
        "CPCW",
        "1st Month LF",
        "Actual TCV",
        "CPCW:LF",
        "Cost/TCV%",
        "Total Leads",
        "Qualified",
        "SQL%",
    ]
    if gm.empty or "month" not in gm.columns or "Market" not in gm.columns:
        return pd.DataFrame(columns=cols)
    gm_q = gm.copy()
    gm_q["_yq"] = gm_q["month"].map(_t3b3_quarter_tuple_from_month)
    gm_q = gm_q[gm_q["_yq"].notna()].copy()
    if gm_q.empty:
        return pd.DataFrame(columns=cols)
    reg_lower = _REGION_SUBTOTAL_NAMES_LOWER
    src = gm_q.loc[~gm_q["Market"].astype(str).str.strip().str.lower().isin(reg_lower)].copy()

    for c in ("spend", "cw", "tcv", "lf", "leads", "qualified"):
        if c not in src.columns:
            src[c] = 0.0
        src[c] = pd.to_numeric(src[c], errors="coerce").fillna(0.0)

    gq = src.groupby(["_yq", "Market"], as_index=False).agg(
        spend=("spend", "sum"),
        cw=("cw", "sum"),
        tcv=("tcv", "sum"),
        lf=("lf", "sum"),
        leads=("leads", "sum"),
        qualified=("qualified", "sum"),
    )

    priority = ["Bahrain", "Kuwait", "Saudi Arabia", "UAE"]

    def _mkt_sort(mkt: str) -> tuple[int, str]:
        try:
            return (priority.index(str(mkt)), str(mkt))
        except ValueError:
            return (50, str(mkt))

    q_candidates = sorted({t for t in gm_q["_yq"].tolist() if t}, key=lambda t: (t[0], t[1]), reverse=True)
    activity_q: list[tuple[int, int]] = []
    for yq in q_candidates:
        probe = _t3b3_metric_dict_sum_frame(gm_q.loc[gm_q["_yq"] == yq])
        if _t3b3_dict_has_activity(probe):
            activity_q.append(yq)
    # Source-of-truth only: quarters that have real activity in ``gm`` (e.g. April rows aggregate into Q2).
    quarters = sorted(activity_q, key=lambda t: (t[0], t[1]), reverse=True)
    if not quarters:
        return pd.DataFrame(columns=cols)
    out_rows: list[dict[str, Any]] = []
    grand = {"spend": 0.0, "cw": 0.0, "tcv": 0.0, "lf": 0.0, "leads": 0.0, "qualified": 0.0}

    for yq in quarters:
        sub = gq[gq["_yq"] == yq].copy()
        qlab = _t3b3_quarter_label_from_tuple(yq)
        markets = sorted(sub["Market"].unique().tolist(), key=_mkt_sort)

        me_roll = _t3b3_me_master_row_sum_for_quarter(gm, yq)
        me_from_countries = _t3b3_metric_dict_sum_frame(sub.loc[sub["Market"].map(_is_middle_east_market)])
        if me_roll is not None and _t3b3_dict_has_activity(me_roll):
            me_tot = me_roll
        else:
            me_tot = me_from_countries

        non_me = sub.loc[~sub["Market"].map(_is_middle_east_market)]
        nm = _t3b3_metric_dict_sum_frame(non_me)
        for k in grand:
            grand[k] += nm.get(k, 0.0) + me_tot.get(k, 0.0)

        for mkt in markets:
            r = sub.loc[sub["Market"] == mkt].iloc[0]
            sp = float(r["spend"])
            cw = float(r["cw"])
            lf = float(r["lf"])
            tcv_v = float(r["tcv"])
            ld = float(r["leads"])
            qf = float(r["qualified"])
            row: dict[str, Any] = {
                "T3B3 Quarter": qlab,
                "Market": mkt,
                "spend": sp,
                "cw": cw,
                "lf": lf,
                "tcv": tcv_v,
                "leads": ld,
                "qualified": qf,
            }
            _t3b3_add_derived_from_sums(row)
            out_rows.append(row)

        if _t3b3_dict_has_activity(me_tot):
            row_me: dict[str, Any] = {"T3B3 Quarter": qlab, "Market": _MIDDLE_EAST_REGION_LABEL}
            row_me.update(me_tot)
            _t3b3_add_derived_from_sums(row_me)
            out_rows.append(row_me)

    row_gt: dict[str, Any] = {"T3B3 Quarter": "Grand Total", "Market": "", **grand}
    _t3b3_add_derived_from_sums(row_gt)
    out_rows.append(row_gt)

    return pd.DataFrame(out_rows)[cols]


def _t3b3_kw_bh_rows_from_gm(gm: pd.DataFrame) -> pd.DataFrame:
    """Quarter rollup for Kuwait + Bahrain only."""
    reg_lower = _REGION_SUBTOTAL_NAMES_LOWER
    src = gm.loc[~gm["Market"].astype(str).str.strip().str.lower().isin(reg_lower)].copy()
    src = src.loc[src["Market"].astype(str).str.strip().isin({"Kuwait", "Bahrain"})].copy()
    cols = [
        "T3B3 Quarter",
        "Market",
        "Spend",
        "CW (Inc Approved)",
        "CPCW",
        "1st Month LF",
        "Actual TCV",
        "CPCW:LF",
        "Cost/TCV%",
        "Total Leads",
        "Qualified",
        "SQL%",
    ]
    if src.empty:
        return pd.DataFrame(columns=cols)
    src["_yq"] = src["month"].map(_t3b3_quarter_tuple_from_month)
    src = src[src["_yq"].notna()].copy()
    for c in ("spend", "cw", "tcv", "lf", "leads", "qualified"):
        if c not in src.columns:
            src[c] = 0.0
        src[c] = pd.to_numeric(src[c], errors="coerce").fillna(0.0)
    gq = src.groupby("_yq", as_index=False).agg(
        spend=("spend", "sum"),
        cw=("cw", "sum"),
        tcv=("tcv", "sum"),
        lf=("lf", "sum"),
        leads=("leads", "sum"),
        qualified=("qualified", "sum"),
    )
    if gq.empty:
        return pd.DataFrame(columns=cols)
    from_data = sorted(
        {t for t in gq["_yq"].dropna().unique().tolist() if t},
        key=lambda t: (int(t[0]), int(t[1])),
        reverse=True,
    )
    rows: list[dict[str, Any]] = []
    for yq in from_data:
        r = gq.loc[gq["_yq"] == yq].iloc[0]
        sp, cw_v, tcv_v, lf_v, ld_v, qf_v = (
            float(r["spend"]),
            float(r["cw"]),
            float(r["tcv"]),
            float(r["lf"]),
            float(r["leads"]),
            float(r["qualified"]),
        )
        row = {
            "T3B3 Quarter": _t3b3_quarter_label_from_tuple((int(yq[0]), int(yq[1]))),
            "Market": "Kuwait + Bahrain",
            "spend": sp,
            "cw": cw_v,
            "tcv": tcv_v,
            "lf": lf_v,
            "leads": ld_v,
            "qualified": qf_v,
        }
        _t3b3_add_derived_from_sums(row)
        rows.append(row)
    return pd.DataFrame(rows)[cols]


def _render_t3b3_quarter_sections(
    gm: pd.DataFrame,
    *,
    key_suffix: str,
) -> None:
    """T3B3 quarterly tables (offset quarter definition + CPCW:LF goal scopes)."""
    st.markdown('<div class="looker-table-title">T3B3 view</div>', unsafe_allow_html=True)
    detail = _t3b3_detail_rows_from_gm(gm)
    kb = _t3b3_kw_bh_rows_from_gm(gm)
    cpcw_goals = (
        _t3b3_goal_cpcw_lf_rows_from_gm(gm) if _SHOW_T3B3_CPCW_LF_GOALS_TABLE else pd.DataFrame()
    )

    def _fmt_t3b3(pvt: pd.DataFrame) -> Any:
        def _fmt_for_metric(metric_name: str) -> Any:
            if metric_name == "Spend":
                return (
                    lambda x: _format_spend_k(float(x)) if pd.notna(x) and not isinstance(x, str) else (
                        x if isinstance(x, str) else "—"
                    )
                )
            if metric_name == "CPCW":
                return lambda x: _format_compact_k(float(x)) if pd.notna(x) else "—"
            if metric_name in {"Actual TCV", "1st Month LF"}:
                if metric_name == "Actual TCV":
                    return lambda x: _format_tcv_short(float(x)) if pd.notna(x) else "—"
                return lambda x: _format_spend_k(float(x)) if pd.notna(x) else "—"
            if metric_name == "CPCW:LF":
                return lambda x: _format_ratio_cpcw_lf(float(x)) if pd.notna(x) else "—"
            if metric_name == "Cost/TCV%":
                return lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
            if metric_name in {"CW (Inc Approved)", "Total Leads", "Qualified"}:
                return lambda x: f"{float(x):,.2f}" if pd.notna(x) else "—"
            if metric_name == "SQL%":
                return lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
            return lambda x: f"{x}" if pd.notna(x) else "—"

        _lbl = lambda x: str(x) if pd.notna(x) and str(x).strip() else ""
        fmt_map: dict[str, Any] = {
            "T3B3 Quarter": lambda x: "" if x == "" or (isinstance(x, float) and pd.isna(x)) else str(x),
            "Market": _lbl,
            "CPCW:LF goal scope": _lbl,
        }
        for c in pvt.columns:
            if c in {"T3B3 Quarter", "Market", "CPCW:LF goal scope"}:
                continue
            fmt_map[c] = _fmt_for_metric(c)
        pvt_disp, q_first, q_last, q_shade = _t3b3_quarter_display_and_edges(pvt)
        css_matrix = _t3b3_view_style_css(
            pvt_disp,
            quarter_first=q_first or None,
            quarter_last=q_last or None,
            shade_alt=q_shade or None,
        )
        styler = pvt_disp.style.format(fmt_map, na_rep="—")
        for col in css_matrix.columns:
            styler = styler.apply(
                lambda s, c=col: css_matrix.loc[s.index, c],
                axis=0,
                subset=[col],
            )
        return styler

    if detail.empty:
        st.info("No quarter-level rows to show for T3B3 (check months in range and filters).")
    else:
        st.markdown("##### Detailed market performance")
        st.dataframe(_fmt_t3b3(detail), width="stretch", hide_index=True, key=f"{key_suffix}_t3b3_detail")

    if not kb.empty:
        st.markdown("##### Kuwait + Bahrain")
        st.dataframe(_fmt_t3b3(kb), width="stretch", hide_index=True, key=f"{key_suffix}_t3b3_kb")

    if _SHOW_T3B3_CPCW_LF_GOALS_TABLE and not cpcw_goals.empty:
        st.markdown("##### CPCW:LF — goal markets (UAE · Saudi · Kuwait + Bahrain)")
        st.dataframe(_fmt_t3b3(cpcw_goals), width="stretch", hide_index=True, key=f"{key_suffix}_t3b3_cpcw_goals")


def _render_master_view_pivot_from_gm(
    gm: pd.DataFrame,
    *,
    key_suffix: str,
    section_title: Optional[str],
    detail_sources: Optional[dict[str, pd.DataFrame]],
    pivot_dimension: Literal["market", "channel"],
    table_mode: Literal["full", "spend_only"],
) -> None:
    """Styled master pivot + metric drill; ``gm`` must be from ``_master_build_gm_with_metrics`` (numeric ``Spend`` etc.)."""
    row_heading = "Channel" if pivot_dimension == "channel" else "Market"
    gm = gm.copy()
    if section_title:
        st.markdown(f'<div class="looker-table-title">{section_title}</div>', unsafe_allow_html=True)

    metrics = (
        ["Spend"]
        if table_mode == "spend_only" and pivot_dimension == "channel"
        else [
            "Spend",
            "CW (Inc Approved)",
            "CPCW",
            "1st Month LF",
            "Actual TCV",
            "CPCW:LF",
            "Cost/TCV%",
            "Total Leads",
        ]
    )
    for m in metrics:
        if m not in gm.columns:
            gm[m] = float("nan")

    pvt = gm.copy()
    pvt["_month_sort_lbl"] = pvt["month"].map(_month_label_short)
    cols = ["month", "_month_sort_lbl", row_heading] + [m for m in metrics if m in pvt.columns]
    pvt = pvt[[c for c in cols if c in pvt.columns]]
    pvt["Month"] = ""

    def _month_label_sort_key(m: Any) -> Any:
        try:
            return pd.Period(str(m), freq="M")
        except Exception:
            return str(m)

    # Enforce **newest calendar month first** (Spend-by-channel + master); **Middle East** subtotal last within each month.
    pvt["_sort_ts"] = pvt["month"].map(_mpo_month_ts_for_sort)
    _me_low = _MIDDLE_EAST_REGION_LABEL.strip().lower()
    pvt["_me_last"] = pvt[row_heading].map(lambda x: 1 if str(x).strip().lower() == _me_low else 0)
    pvt = pvt.sort_values(
        ["_sort_ts", "_me_last", row_heading],
        ascending=[False, True, True],
        kind="mergesort",
        na_position="last",
    )
    # Month label in the **Month** column only on the **Middle East** row (same idea as T3B3 quarter beside ME).
    for ml in sorted(pvt["_month_sort_lbl"].dropna().unique(), key=_month_label_sort_key, reverse=True):
        ix = pvt.index[pvt["_month_sort_lbl"] == ml].tolist()
        if not ix:
            continue
        raw_m = pvt.loc[ix[0], "month"]
        lbl = _month_label_short(raw_m)
        me_idx: Optional[Any] = None
        for i in ix:
            rn = str(pvt.loc[i, row_heading]).strip().lower()
            if rn == _me_low or rn == "middle east (subtotal)":
                me_idx = i
                break
        if me_idx is not None:
            pvt.loc[me_idx, "Month"] = lbl
        else:
            pvt.loc[ix[0], "Month"] = lbl
    idx_order = list(pvt.index)
    _mk = [_month_norm_key(pvt.loc[i, "month"]) for i in idx_order]
    month_block_first = [pos == 0 or _mk[pos] != _mk[pos - 1] for pos in range(len(idx_order))]
    month_block_last = [
        pos == len(idx_order) - 1 or _mk[pos] != _mk[pos + 1] for pos in range(len(idx_order))
    ]
    pvt = pvt.drop(columns=["_sort_ts", "_me_last", "month", "_month_sort_lbl"], errors="ignore")
    out_cols = ["Month", row_heading] + [m for m in metrics if m in pvt.columns]
    pvt = pvt[out_cols]
    cell_metric_allowlist = list(metrics)
    if table_mode == "spend_only" and pivot_dimension == "channel":
        _keep = [c for c in ["Month", row_heading, "Spend"] if c in pvt.columns]
        pvt = pvt[_keep]
        cell_metric_allowlist = [c for c in ["Spend"] if c in pvt.columns]
    # Streamlit's Arrow table path often ignores ``Styler.format`` for numeric cells — use strings for Spend.
    if "Spend" in pvt.columns:
        pvt["Spend"] = pvt["Spend"].apply(
            lambda v: _format_spend_k(float(v)) if v is not None and not pd.isna(v) else "—"
        )

    def _fmt_for_metric(metric_name: str) -> Any:
        if metric_name == "Spend":
            return lambda x: x if isinstance(x, str) else (_format_spend_k(float(x)) if pd.notna(x) else "—")
        if metric_name == "CPCW":
            return lambda x: _format_compact_k(float(x)) if pd.notna(x) else "—"
        if metric_name in {"CPL", "1st Month LF", "Actual TCV"}:
            if metric_name == "CPL":
                return lambda x: f"${x:,.2f}" if pd.notna(x) else "—"
            if metric_name == "1st Month LF":
                return lambda x: _format_spend_k(float(x)) if pd.notna(x) else "—"
            return lambda x: _format_tcv_short(float(x)) if pd.notna(x) else "—"
        if metric_name == "CPCW:LF":
            return lambda x: _format_ratio_cpcw_lf(float(x)) if pd.notna(x) else "—"
        if metric_name in {"SQL %", "Cost/TCV%"}:
            return lambda x: f"{x:.2f}%" if pd.notna(x) else "—"
        if metric_name in {"CW (Inc Approved)", "Total Leads"}:
            return lambda x: f"{x:,.2f}" if pd.notna(x) else "—"
        return lambda x: f"{x:,.2f}" if pd.notna(x) else "—"

    fmt_map: dict[str, Any] = {
        "Month": lambda x: "" if x == "" or (isinstance(x, float) and pd.isna(x)) else str(x),
        row_heading: lambda x: str(x) if pd.notna(x) else "—",
    }
    for c in pvt.columns:
        if c in {"Month", "Market", "Channel"}:
            continue
        fmt_map[c] = _fmt_for_metric(c)

    css_matrix = _master_view_style_css(
        pvt,
        month_block_first=month_block_first,
        month_block_last=month_block_last,
    )
    styler = pvt.style.format(fmt_map, na_rep="—")
    for col in css_matrix.columns:
        styler = styler.apply(
            lambda s, c=col: css_matrix.loc[s.index, c],
            axis=0,
            subset=[col],
        )
    detail_state = None
    try:
        detail_state = st.dataframe(
            styler,
            width="stretch",
            hide_index=True,
            key=f"{key_suffix}_df_master_pivot",
            on_select="rerun",
            selection_mode="single-cell",
        )
    except TypeError:
        st.dataframe(styler, width="stretch", hide_index=True, key=f"{key_suffix}_df_master_pivot")

    _detail_base = f"{key_suffix}_master_metric_detail"
    _payload_k = f"{_detail_base}_payload"
    _open_k = f"{_detail_base}_open"
    _last_sig_k = f"{_detail_base}_last_sig"

    if detail_state is not None:
        sel = getattr(detail_state, "selection", None)
        sel_dict: dict[str, Any] = {}
        if sel is not None:
            if hasattr(sel, "to_dict"):
                try:
                    sel_dict = dict(sel.to_dict())
                except Exception:
                    sel_dict = {}
            elif isinstance(sel, dict):
                sel_dict = dict(sel)

        rix: Optional[int] = None
        col_raw: Any = None
        cells = list(sel_dict.get("cells", []) or [])
        if cells:
            c0 = cells[0] or {}
            if isinstance(c0, dict):
                try:
                    rix = int(c0.get("row"))
                except Exception:
                    rix = None
                col_raw = c0.get("column")
            elif isinstance(c0, (list, tuple)):
                # Some Streamlit builds return cells like (row_idx, col_idx_or_name).
                try:
                    if len(c0) >= 1:
                        rix = int(c0[0])
                except Exception:
                    rix = None
                try:
                    if len(c0) >= 2:
                        col_raw = c0[1]
                except Exception:
                    col_raw = None
            else:
                try:
                    rix = int(getattr(c0, "row"))
                except Exception:
                    rix = None
                col_raw = getattr(c0, "column", None)
        else:
            rows = list(getattr(sel, "rows", []) or sel_dict.get("rows", []) or []) if sel else []
            cols = list(getattr(sel, "columns", []) or sel_dict.get("columns", []) or []) if sel else []
            if rows and cols:
                try:
                    rix = int(rows[0])
                except Exception:
                    rix = None
                col_raw = cols[0]

        col_name: Optional[str] = None
        if isinstance(col_raw, int):
            if 0 <= int(col_raw) < len(pvt.columns):
                col_name = str(pvt.columns[int(col_raw)])
        elif col_raw is not None:
            col_name = str(col_raw)

        if rix is not None and col_name and 0 <= rix < len(gm) and col_name in cell_metric_allowlist:
            row = gm.reset_index(drop=True).iloc[rix]
            month_label = _month_label_short(row.get("month")) or str(row.get("month") or "Selected period")
            market_label = str(row.get(row_heading) or row.get("Market") or row.get("Channel") or "Selected market")
            value_fn = fmt_map.get(col_name, lambda x: str(x))
            try:
                value_text = str(value_fn(row.get(col_name)))
            except Exception:
                value_text = str(row.get(col_name))
            sig = f"{month_label}|{market_label}|{col_name}|{rix}"
            if st.session_state.get(_last_sig_k) != sig:
                month_key = _month_norm_key(row.get("month")) or ""
                st.session_state[_last_sig_k] = sig
                st.session_state[_open_k] = True
                st.session_state[_payload_k] = {
                    "metric_name": col_name,
                    "month_label": month_label,
                    "market_label": market_label,
                    "month_key": month_key,
                    "value_text": value_text,
                    "spend": float(pd.to_numeric(row.get("spend", 0), errors="coerce") or 0),
                    "cw": int(pd.to_numeric(row.get("cw", 0), errors="coerce") or 0),
                    "leads": int(pd.to_numeric(row.get("leads", 0), errors="coerce") or 0),
                    "tcv": float(pd.to_numeric(row.get("tcv", 0), errors="coerce") or 0),
                    "lf": float(pd.to_numeric(row.get("lf", 0), errors="coerce") or 0),
                    "impr": int(float(pd.to_numeric(row.get("impressions", 0), errors="coerce") or 0)),
                    "clk": int(float(pd.to_numeric(row.get("clicks", 0), errors="coerce") or 0)),
                    "source_rows": _mpo_source_pivot_rows(
                        month_key=month_key,
                        market_label=market_label,
                        detail_sources=detail_sources,
                        pivot_dimension=pivot_dimension,
                    ),
                }

    payload = st.session_state.get(_payload_k)

    is_open = bool(st.session_state.get(_open_k)) and isinstance(payload, dict)
    if not is_open:
        return

    payload_row = pd.Series(
        {
            "spend": payload.get("spend", 0),
            "cw": payload.get("cw", 0),
            "leads": payload.get("leads", 0),
            "tcv": payload.get("tcv", 0),
            "lf": payload.get("lf", 0),
        }
    )
    _dialog = getattr(st, "dialog", None)
    if callable(_dialog):

        def _dismiss_master_metric_dialog() -> None:
            st.session_state[_open_k] = False

        try:
            _dialog_decorator = _dialog("Details", width="large", on_dismiss=_dismiss_master_metric_dialog)
        except TypeError:
            _dialog_decorator = _dialog("Details", width="large")

        @_dialog_decorator
        def _show_master_metric_dialog() -> None:
            _render_mpo_master_metric_detail_card(
                row=payload_row,
                metric_name=str(payload.get("metric_name") or "Metric"),
                month_label=str(payload.get("month_label") or "Selected period"),
                market_label=str(payload.get("market_label") or "Selected market"),
                value_text=str(payload.get("value_text") or "—"),
                source_rows=payload.get("source_rows"),
                detail_sources=detail_sources,
                month_key=str(payload.get("month_key") or ""),
                impr=int(payload.get("impr") or 0),
                clk=int(payload.get("clk") or 0),
            )

        _show_master_metric_dialog()
    else:
        _render_mpo_master_metric_detail_card(
            row=payload_row,
            metric_name=str(payload.get("metric_name") or "Metric"),
            month_label=str(payload.get("month_label") or "Selected period"),
            market_label=str(payload.get("market_label") or "Selected market"),
            value_text=str(payload.get("value_text") or "—"),
            source_rows=payload.get("source_rows"),
            detail_sources=detail_sources,
            month_key=str(payload.get("month_key") or ""),
            impr=int(payload.get("impr") or 0),
            clk=int(payload.get("clk") or 0),
        )


def _master_performance_table(
    df: pd.DataFrame,
    *,
    key_suffix: str,
    section_title: Optional[str] = "Master view",
    spend_grid: Optional[pd.DataFrame] = None,
    detail_sources: Optional[dict[str, pd.DataFrame]] = None,
    pivot_dimension: Literal["market", "channel"] = "market",
    table_mode: Literal["full", "spend_only"] = "full",
    month_not_before: Optional[date] = None,
) -> None:
    """Month × Market/Channel master pivot; uses the same ``gm`` builder as quarterly T3B3 when wired that way."""
    _ = month_not_before
    gm = _master_build_gm_with_metrics(df, spend_grid, pivot_dimension=pivot_dimension)
    _render_master_view_pivot_from_gm(
        gm,
        key_suffix=key_suffix,
        section_title=section_title,
        detail_sources=detail_sources,
        pivot_dimension=pivot_dimension,
        table_mode=table_mode,
    )


def render_page_marketing_performance(
    df_loaded: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> None:
    key_suffix = "mpo"
    # Marketing performance: use **entire merged workbook** — ignore sidebar date pickers for row scope.
    _rng_lo, _rng_hi = date(1990, 1, 1), date(2100, 12, 31)
    _ = start_date, end_date  # kept for signature compatibility with the tab router
    df_date = df_loaded
    if df_date.empty:
        st.info("No rows loaded.")
        return

    _dashboard_tab_page_header()
    st.caption("Ads Cost Snapshot — last updated on April 6, 2026")
    df, _ = _apply_marketing_performance_filters(
        df_date,
        key_suffix=key_suffix,
        reporting_start=_rng_lo,
        reporting_end=_rng_hi,
    )

    spend_sheet_for_kpis, spend_sheet_master, spend_pool_full, sheet_id, _fp_mpo = _mpo_load_spend_sheet_for_kpis(
        df_loaded,
        df_date,
        _rng_lo,
        _rng_hi,
    )
    spend_df = _spend_slice_for_dashboard_filters(spend_sheet_for_kpis, df)

    def _tab_subset(frame: pd.DataFrame, tab_keywords: list[str]) -> pd.DataFrame:
        if "source_tab" not in frame.columns:
            return frame
        s = frame["source_tab"].astype(str).str.lower()
        mask = pd.Series(False, index=frame.index)
        for k in tab_keywords:
            mask = mask | s.str.contains(k.lower(), na=False, regex=True)
        return frame[mask].copy()

    def _strict_gid_source(gid: Optional[int], *, prefer_full_rows: bool = False) -> pd.DataFrame:
        """Resolve one worksheet from merged in-memory data (avoid runtime network calls in the render hot path)."""
        if gid is None:
            return pd.DataFrame()
        by_gid_all = _rows_by_worksheet_id(df_loaded, int(gid), sheet_id)
        if prefer_full_rows and not by_gid_all.empty:
            return by_gid_all
        by_gid_date = _rows_by_worksheet_id(df_date, int(gid), sheet_id)
        if not by_gid_date.empty:
            return by_gid_date
        if not by_gid_all.empty:
            return by_gid_all
        return pd.DataFrame()

    # Prefer the canonical truth tab for ALL non-spend marketing metrics when present.
    truth_gid = _default_truth_gid_from_secrets()
    truth_df = _strict_gid_source(truth_gid)
    truth_metric_cols = {
        "leads",
        "qualified",
        "pitching",
        "new",
        "working",
        "negotiation",
        "commitment",
        "qualifying",
        "closed_lost",
        "closed_won",
        "tcv",
        "first_month_lf",
    }
    use_truth_for_nonspend = (not truth_df.empty) and bool(truth_metric_cols.intersection(set(truth_df.columns)))

    # Business mapping by tab (strict gid sources unless truth tab already carries the metric).
    # - Leads / Qualified: Leads worksheet gid
    # - Post-lead pipeline stages: Raw Post Qualification worksheet gid
    # - TCV / 1st Month LF: RAW CW worksheet gid
    leads_gid = _default_leads_gid_from_secrets()
    if use_truth_for_nonspend:
        leads_df = truth_df.copy()
    else:
        leads_df = _strict_gid_source(leads_gid)
        if leads_df.empty:
            leads_df = _tab_subset(df_date, list(_MPO_LEAD_TAB_PATTERNS))
    leads_df = _ensure_closed_won_from_text_flags(leads_df)
    # Cards, CPL/CPSQL, Q-win, Master overlay — use **Leads worksheet** rows in the same Market × Month scope as ``df``.
    # Prefer rows already in the merged workbook (same tab) to avoid an extra Sheets round-trip per run.
    _lw_tab = _rows_by_worksheet_id(df_date, int(leads_gid), sheet_id)
    if _lw_tab.empty:
        _lw_tab = _rows_by_worksheet_id(df_loaded, int(leads_gid), sheet_id)
    if not _lw_tab.empty:
        _lw_scoped = _mpo_slice_by_dashboard_ref(_lw_tab, df) if not df.empty else _lw_tab.copy()
        leads_df = _ensure_closed_won_from_text_flags(_lw_scoped)

    pq_gid = _optional_post_qual_gid_from_secrets()
    if use_truth_for_nonspend:
        post_df_kpi = truth_df.copy()
    else:
        post_df_kpi = _strict_gid_source(pq_gid)
        if post_df_kpi.empty:
            post_df_kpi = _tab_subset(df_date, list(_POST_LEAD_SOURCE_TAB_PATTERNS))
    post_df_kpi = _ensure_closed_won_from_text_flags(post_df_kpi)

    # **CpCW Analysis** (B2/B3) must read the dedicated Post Qual tab. When ``use_truth_for_nonspend`` maps
    # ``post_df_kpi`` to the truth sheet, LF/close-date columns often do not match ME Post Lead — LF would fall
    # back to RAW ``cw_kpi`` and crush CpCW:LF vs the spreadsheet.
    post_df_cpcw_analysis = post_df_kpi
    if use_truth_for_nonspend and pq_gid is not None:
        _pq_cw = _strict_gid_source(pq_gid, prefer_full_rows=True)
        if _pq_cw.empty:
            _pq_cw = _strict_gid_source(pq_gid)
        if _pq_cw.empty and "source_tab" in df_date.columns:
            _pq_cw = _tab_subset(df_date, list(_POST_LEAD_SOURCE_TAB_PATTERNS))
        if not _pq_cw.empty:
            post_df_cpcw_analysis = _ensure_closed_won_from_text_flags(_pq_cw)

    # CW (inc. approved) — LOCKED source:
    # post-qual/post-lead rows in the same Market × Month scope only.
    total_cw = _mpo_cw_kpi_post_lead_record_count(post_df_cpcw_analysis, df)

    # Pipeline / “Qualified leads” cards: same **Market × Month** scope as the spend table.
    post_df_kpi_scoped = (
        _mpo_slice_by_dashboard_ref(post_df_kpi, df)
        if (not post_df_kpi.empty and not df.empty)
        else post_df_kpi.copy()
    )
    post_df_cpcw_scoped = (
        _mpo_slice_by_dashboard_ref(post_df_cpcw_analysis, df)
        if (not post_df_cpcw_analysis.empty and not df.empty)
        else post_df_cpcw_analysis.copy()
    )

    def _crm_stage_activity_sum(_fr: pd.DataFrame) -> float:
        if _fr.empty:
            return 0.0
        _acc = 0.0
        for _col in ("qualifying", "pitching", "negotiation", "commitment"):
            if _col in _fr.columns:
                _acc += float(pd.to_numeric(_fr[_col], errors="coerce").fillna(0).sum())
        return _acc

    # Source-of-truth tabs usually lack per-row stage columns (all zeros); post-qual carries Qualifying…Commitment.
    post_df_pipe_scoped = (
        post_df_cpcw_scoped
        if _crm_stage_activity_sum(post_df_cpcw_scoped) > _crm_stage_activity_sum(post_df_kpi_scoped)
        else post_df_kpi_scoped
    )
    # Closed-won KPI should stay deduped to avoid cross-tab opportunity duplication.
    post_df = _dedupe_post_lead_rows(post_df_pipe_scoped)

    cw_truth_gid = _optional_cw_source_truth_gid_from_secrets()
    raw_cw_gid = _optional_raw_cw_gid_from_secrets()
    # TCV source of truth: prefer CW source-truth sheet (gid 1871946442, column T: TCV converted),
    # then fall back to RAW CW gid.
    tcv_source_gid = cw_truth_gid if cw_truth_gid is not None else raw_cw_gid
    cw_df = _strict_gid_source(tcv_source_gid, prefer_full_rows=True)
    if cw_df.empty and raw_cw_gid is not None and (tcv_source_gid is None or int(raw_cw_gid) != int(tcv_source_gid)):
        cw_df = _strict_gid_source(raw_cw_gid, prefer_full_rows=True)
    if cw_df.empty:
        cw_df = _resolve_cw_tcv_dataframe(df_loaded, df)
    if cw_df.empty and use_truth_for_nonspend:
        cw_df = truth_df.copy()
    _mk_sel = st.session_state.get(f"{key_suffix}_market", [_MPO_ALL_GEO_SENTINEL])
    _mo_sel = st.session_state.get(f"{key_suffix}_month", [_MPO_ALL_MONTHS_SENTINEL])
    _is_all_markets = _mpo_market_scope_is_all(_mk_sel)
    _is_all_months = _mpo_month_multiselect_is_all(_mo_sel)
    # For "All markets + All months", TCV should reflect full CW source-truth stage scope.
    # Otherwise, keep the same dashboard Month x Country window.
    _cw_scope_df = pd.DataFrame() if (_is_all_markets and _is_all_months) else df
    cw_kpi = _cw_dataframe_for_kpis(cw_df, _cw_scope_df)
    _tcv_sum_override: Optional[float] = None
    _lf_sum_override: Optional[float] = None
    if _is_all_markets and _is_all_months and cw_truth_gid is not None:
        _r_ov, _s_ov, _lf_ov = _closed_won_tcv_lf_sums_from_source_truth_gid(sheet_id, int(cw_truth_gid))
        if _r_ov > 0:
            _tcv_sum_override = float(_s_ov)
            if _lf_ov and float(_lf_ov) > 0:
                _lf_sum_override = float(_lf_ov)

    total_spend = float(spend_df["cost"].sum()) if "cost" in spend_df.columns else 0.0
    if total_spend <= 0.0 and _normalized_spend_cost_sum(spend_sheet_master) > 0.0:
        total_spend = float(pd.to_numeric(spend_sheet_master["cost"], errors="coerce").fillna(0).sum())
    if total_spend <= 0.0 and "cost" in cw_kpi.columns:
        _cw_s = float(pd.to_numeric(cw_kpi["cost"], errors="coerce").fillna(0).sum())
        if _cw_s > 1e-6:
            total_spend = _cw_s
    if total_spend <= 0.0 and "cpcw" in cw_kpi.columns and "closed_won" in cw_kpi.columns:
        _px_s = float(
            (
                pd.to_numeric(cw_kpi["cpcw"], errors="coerce").fillna(0)
                * pd.to_numeric(cw_kpi["closed_won"], errors="coerce").fillna(0)
            ).sum()
        )
        if _px_s > 1e-6:
            total_spend = _px_s
    total_impr = int(spend_df["impressions"].sum()) if "impressions" in spend_df.columns else 0
    total_clicks = int(spend_df["clicks"].sum()) if "clicks" in spend_df.columns else 0
    total_leads = _lead_rows_count(leads_df)
    total_qualified = _qualified_count_from_leads(leads_df)
    if total_leads == 0:
        by_g2 = _rows_by_worksheet_id(df_loaded, int(leads_gid), sheet_id)
        if not by_g2.empty:
            leads_df = by_g2
            total_leads = _lead_rows_count(leads_df)
            total_qualified = _qualified_count_from_leads(leads_df)
        elif "source_tab" in df_loaded.columns:
            st_key = df_loaded["source_tab"].astype(str).str.strip().str.casefold()
            exact2 = df_loaded.loc[st_key.isin({"lead", "leads"})].copy()
            if not exact2.empty:
                leads_df = exact2
                total_leads = _lead_rows_count(leads_df)
                total_qualified = _qualified_count_from_leads(leads_df)
    total_pitching = int(post_df_pipe_scoped["pitching"].sum()) if "pitching" in post_df_pipe_scoped.columns else 0
    total_new = int(post_df["new"].sum()) if "new" in post_df.columns else 0
    total_working = int(post_df["working"].sum()) if "working" in post_df.columns else 0
    total_negotiation = int(post_df_pipe_scoped["negotiation"].sum()) if "negotiation" in post_df_pipe_scoped.columns else 0
    total_commitment = int(post_df_pipe_scoped["commitment"].sum()) if "commitment" in post_df_pipe_scoped.columns else 0
    total_qualifying = int(post_df_pipe_scoped["qualifying"].sum()) if "qualifying" in post_df_pipe_scoped.columns else 0
    # Total Live (CRM): Qualifying + Pitching + Negotiation + Commitment — not the broader sheet `total_live` flag.
    total_total_live = total_qualifying + total_pitching + total_negotiation + total_commitment
    total_closed_lost = int(post_df_pipe_scoped["closed_lost"].sum()) if "closed_lost" in post_df_pipe_scoped.columns else 0
    total_tcv = float(cw_kpi["tcv"].sum()) if "tcv" in cw_kpi.columns else 0.0
    if _tcv_sum_override is not None:
        total_tcv = float(_tcv_sum_override)
    total_first_month_lf = float(cw_kpi["first_month_lf"].sum()) if "first_month_lf" in cw_kpi.columns else 0.0
    total_new_working = _new_working_count_from_leads(leads_df)
    if int(total_qualified) <= 0:
        _qualified_from_truth = 0
        if not truth_df.empty and "qualified" in truth_df.columns:
            _truth_scoped_q = _mpo_slice_by_dashboard_ref(truth_df, df) if not df.empty else truth_df.copy()
            _qualified_from_truth = int(pd.to_numeric(_truth_scoped_q["qualified"], errors="coerce").fillna(0).sum())
        _qualified_from_df = int(pd.to_numeric(df.get("qualified", 0), errors="coerce").fillna(0).sum()) if "qualified" in df.columns else 0
        total_qualified = int(max(int(total_qualified), _qualified_from_truth, _qualified_from_df))
    if int(total_qualifying) <= 0:
        _qualifying_from_kpi = (
            int(pd.to_numeric(post_df_kpi_scoped["qualifying"], errors="coerce").fillna(0).sum())
            if (not post_df_kpi_scoped.empty and "qualifying" in post_df_kpi_scoped.columns)
            else 0
        )
        _qualifying_from_truth = 0
        if not truth_df.empty and "qualifying" in truth_df.columns:
            _truth_scoped_pq = _mpo_slice_by_dashboard_ref(truth_df, df) if not df.empty else truth_df.copy()
            _qualifying_from_truth = int(pd.to_numeric(_truth_scoped_pq["qualifying"], errors="coerce").fillna(0).sum())
        total_qualifying = int(max(int(total_qualifying), _qualifying_from_kpi, _qualifying_from_truth))
        total_total_live = int(total_qualifying + total_pitching + total_negotiation + total_commitment)

    # Per-metric safety fallbacks.
    if total_spend == 0.0 and "cost" in df.columns:
        total_spend = float(df["cost"].sum())
    gid0_spend_sum = float(st.session_state.get("_gid0_spend_sum", 0.0) or 0.0)
    if total_spend == 0.0 and gid0_spend_sum > 0.0:
        total_spend = gid0_spend_sum
    # Cloud safety net: if mapped sources still resolve to zeros, fall back to full filtered frame.
    if (
        total_spend == 0.0
        and total_leads == 0
        and total_qualified == 0
        and total_cw == 0
        and total_tcv == 0.0
        and total_first_month_lf == 0.0
    ):
        total_spend = float(df["cost"].sum()) if "cost" in df.columns else 0.0
        total_impr = int(df["impressions"].sum()) if "impressions" in df.columns else 0
        total_clicks = int(df["clicks"].sum()) if "clicks" in df.columns else 0
        total_leads = _lead_rows_count(leads_df if not leads_df.empty else df)
        total_qualified = _qualified_count_from_leads(leads_df if not leads_df.empty else df)
        _pk = _tab_subset(df_loaded, list(_POST_LEAD_SOURCE_TAB_PATTERNS))
        _gpq = _optional_post_qual_gid_from_secrets()
        if _gpq is not None and "worksheet_gid" in df_loaded.columns:
            wg2 = pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce")
            by_pq2 = df_loaded.loc[wg2 == int(_gpq)].copy()
            if not by_pq2.empty:
                _pk = by_pq2
        _pk = _pk if not _pk.empty else df
        total_pitching = int(_pk["pitching"].sum()) if "pitching" in _pk.columns else 0
        total_cw = _mpo_cw_kpi_post_lead_record_count(post_df_cpcw_analysis, df)
        total_new = int(df["new"].sum()) if "new" in df.columns else 0
        total_working = int(df["working"].sum()) if "working" in df.columns else 0
        total_negotiation = int(_pk["negotiation"].sum()) if "negotiation" in _pk.columns else 0
        total_commitment = int(_pk["commitment"].sum()) if "commitment" in _pk.columns else 0
        total_qualifying = int(_pk["qualifying"].sum()) if "qualifying" in _pk.columns else 0
        total_total_live = total_qualifying + total_pitching + total_negotiation + total_commitment
        total_closed_lost = int(_pk["closed_lost"].sum()) if "closed_lost" in _pk.columns else 0
        _cw_fb = _cw_dataframe_for_kpis(_resolve_cw_tcv_dataframe(df_loaded, df), df)
        total_tcv = float(_cw_fb["tcv"].sum()) if "tcv" in _cw_fb.columns else 0.0
        total_first_month_lf = float(_cw_fb["first_month_lf"].sum()) if "first_month_lf" in _cw_fb.columns else 0.0
        cw_kpi = _cw_fb
        total_new_working = _new_working_count_from_leads(leads_df if not leads_df.empty else df)
    ctr = (total_clicks / total_impr * 100) if total_impr else 0
    cpc = (total_spend / total_clicks) if total_clicks else 0.0
    cpl = (total_spend / total_leads) if total_leads else 0.0
    cpsql = (total_spend / total_qualified) if total_qualified else 0.0
    # Progressive render: paint KPI cards early so the page is not blank while heavy tables/charts build.
    _kpi_slot = st.empty()
    with _kpi_slot.container():
        _kpi_block(
            total_spend=total_spend,
            total_impr=total_impr,
            total_clicks=total_clicks,
            ctr=ctr,
            total_leads=total_leads,
            total_qualified=total_qualified,
            total_cw=total_cw,
            q_win_cw=int(total_cw),
            q_win_qualified=int(total_qualified),
            total_tcv=total_tcv,
            total_first_month_lf=total_first_month_lf,
            cpc=cpc,
            cpl=cpl,
            cpsql=cpsql,
            total_new_working=total_new_working,
            total_total_live=total_total_live,
            total_negotiation=total_negotiation,
            total_commitment=total_commitment,
            total_closed_lost=total_closed_lost,
            total_pitching=total_pitching,
            total_qualifying=total_qualifying,
            prior={"_comparison_off": True},
        )
    # Optional emergency mode: only cards (skip master/trends). Default OFF to keep full dashboard visible.
    _fast_kpi_mode = str(os.environ.get("XRAY_FAST_KPI_MODE", "0")).strip().lower() not in ("0", "false", "off", "no")
    if _fast_kpi_mode and use_truth_for_nonspend:
        _kpi_block(
            total_spend=total_spend,
            total_impr=total_impr,
            total_clicks=total_clicks,
            ctr=ctr,
            total_leads=total_leads,
            total_qualified=total_qualified,
            total_cw=total_cw,
            q_win_cw=int(total_cw),
            q_win_qualified=int(total_qualified),
            total_tcv=total_tcv,
            total_first_month_lf=total_first_month_lf,
            cpc=cpc,
            cpl=cpl,
            cpsql=cpsql,
            total_new_working=total_new_working,
            total_total_live=total_total_live,
            total_negotiation=total_negotiation,
            total_commitment=total_commitment,
            total_closed_lost=total_closed_lost,
            total_pitching=total_pitching,
            total_qualifying=total_qualifying,
            prior={"_comparison_off": True},
        )
        st.caption("Fast mode active: detailed master/trend blocks are skipped to keep dashboard rendering responsive.")
        return

    def _agg_for_master(frame: pd.DataFrame, metrics: list[str]) -> pd.DataFrame:
        if frame.empty or "month" not in frame.columns or "country" not in frame.columns:
            return pd.DataFrame(columns=["month", "country"] + metrics)
        cols = [c for c in metrics if c in frame.columns]
        if not cols:
            return pd.DataFrame(columns=["month", "country"] + metrics)
        out = frame.groupby(["month", "country"], as_index=False)[cols].sum()
        for c in metrics:
            if c not in out.columns:
                out[c] = 0.0
        return out[["month", "country"] + metrics]

    _sp_for_g = spend_sheet_master
    if _normalized_spend_cost_sum(_sp_for_g) <= 0.0 and _normalized_spend_cost_sum(spend_pool_full) > 0.0:
        _sp_for_g = spend_pool_full
    spend_g = _spend_sheet_pivot_by_month_country(_sp_for_g)
    if spend_g.empty or _normalized_spend_cost_sum(spend_g) <= 0.0:
        spend_g = _spend_sheet_pivot_by_month_country(spend_pool_full)
    leads_norm = _normalize_master_merge_frame(leads_df)
    leads_for_master = _ensure_leads_metric_for_master(leads_norm)
    leads_g = _agg_for_master(leads_for_master, ["leads", "qualified"])
    if int(total_leads) > 0:
        lg_sum = float(pd.to_numeric(leads_g["leads"], errors="coerce").fillna(0).sum()) if not leads_g.empty else 0.0
        if lg_sum < 0.85 * float(total_leads):
            lc = _leads_pivot_rowcount_by_month_country(leads_df)
            if not lc.empty:
                qg = _agg_for_master(leads_for_master, ["qualified"]) if "qualified" in leads_for_master.columns else pd.DataFrame()
                leads_g = lc if qg.empty else lc.merge(qg, on=["month", "country"], how="outer")
                if "qualified" not in leads_g.columns:
                    leads_g["qualified"] = 0.0
                leads_g["qualified"] = pd.to_numeric(leads_g["qualified"], errors="coerce").fillna(0.0)
                leads_g["leads"] = _to_int_series_safe(leads_g["leads"])
    post_g = _agg_for_master(
        _normalize_master_merge_frame(post_df),
        ["closed_won", "pitching", "new", "working", "qualifying", "negotiation", "commitment", "closed_lost"],
    )
    if not post_g.empty:
        post_g = post_g.copy()
        _q = pd.to_numeric(post_g["qualifying"], errors="coerce").fillna(0) if "qualifying" in post_g.columns else 0
        _p = pd.to_numeric(post_g["pitching"], errors="coerce").fillna(0) if "pitching" in post_g.columns else 0
        _n = pd.to_numeric(post_g["negotiation"], errors="coerce").fillna(0) if "negotiation" in post_g.columns else 0
        _c = pd.to_numeric(post_g["commitment"], errors="coerce").fillna(0) if "commitment" in post_g.columns else 0
        post_g["total_live"] = _q + _p + _n + _c
    _cw_base = _normalize_master_merge_frame(cw_kpi)
    if "cost" in _cw_base.columns:
        _cw_base = _cw_base.rename(columns={"cost": "cw_tab_cost"})
    _cw_metrics = ["tcv", "first_month_lf"] + (["cw_tab_cost"] if "cw_tab_cost" in _cw_base.columns else [])
    cw_g = _agg_for_master(_cw_base, _cw_metrics)

    spend_proxy_g = pd.DataFrame()
    _cwk = _normalize_master_merge_frame(cw_kpi)
    if not _cwk.empty and "cpcw" in _cwk.columns and "closed_won" in _cwk.columns:
        _px = _cwk.copy()
        _px["_cost_cw_px"] = pd.to_numeric(_px["cpcw"], errors="coerce").fillna(0) * pd.to_numeric(
            _px["closed_won"], errors="coerce"
        ).fillna(0)
        if float(_px["_cost_cw_px"].abs().sum()) > 1e-6:
            spend_proxy_g = (
                _px.groupby(["month", "country"], as_index=False)["_cost_cw_px"]
                .sum()
                .rename(columns={"_cost_cw_px": "cost_cpcw_proxy"})
            )

    # Master-view fallbacks (never use full ``df`` for spend — it would pull cost from non-Spend tabs).
    if post_g.empty or ("closed_won" in post_g.columns and float(post_g["closed_won"].sum()) == 0.0):
        post_g = _agg_for_master(
            _normalize_master_merge_frame(
                _dedupe_post_lead_rows(_tab_subset(df, list(_POST_LEAD_SOURCE_TAB_PATTERNS)))
            ),
            ["closed_won", "pitching", "new", "working", "qualifying", "negotiation", "commitment", "closed_lost"],
        )
        if not post_g.empty:
            post_g = post_g.copy()
            _q = pd.to_numeric(post_g["qualifying"], errors="coerce").fillna(0) if "qualifying" in post_g.columns else 0
            _p = pd.to_numeric(post_g["pitching"], errors="coerce").fillna(0) if "pitching" in post_g.columns else 0
            _n = pd.to_numeric(post_g["negotiation"], errors="coerce").fillna(0) if "negotiation" in post_g.columns else 0
            _c = pd.to_numeric(post_g["commitment"], errors="coerce").fillna(0) if "commitment" in post_g.columns else 0
            post_g["total_live"] = _q + _p + _n + _c

    master_df = spend_g.merge(leads_g, on=["month", "country"], how="outer")
    master_df = master_df.merge(post_g, on=["month", "country"], how="outer")
    master_df = master_df.merge(cw_g, on=["month", "country"], how="outer")
    master_df = master_df.fillna(0)
    master_df = _master_df_coalesce_month_country(master_df)
    if "cw_tab_cost" in master_df.columns:
        _c_sp = pd.to_numeric(master_df["cost"], errors="coerce").fillna(0)
        _c_cw = pd.to_numeric(master_df["cw_tab_cost"], errors="coerce").fillna(0)
        master_df["cost"] = _c_sp.where(_c_sp > 1e-6, _c_cw)
        master_df = master_df.drop(columns=["cw_tab_cost"], errors="ignore")
    if not spend_proxy_g.empty and "cost_cpcw_proxy" in spend_proxy_g.columns:
        master_df = master_df.merge(spend_proxy_g, on=["month", "country"], how="left")
        master_df["cost_cpcw_proxy"] = pd.to_numeric(master_df["cost_cpcw_proxy"], errors="coerce").fillna(0)
        _c_sp = pd.to_numeric(master_df["cost"], errors="coerce").fillna(0)
        master_df["cost"] = _c_sp.where(_c_sp > 1e-6, master_df["cost_cpcw_proxy"])
        master_df = master_df.drop(columns=["cost_cpcw_proxy"], errors="ignore")
    if not spend_pool_full.empty:
        master_df = _coalesce_master_cost_from_spend_pivot(master_df, spend_pool_full)
        master_df = _allocate_spend_pool_by_country_and_cw(master_df, spend_pool_full)
    if master_df.empty:
        master_df = df.copy()
    else:
        metric_probe = [c for c in ("cost", "leads", "qualified", "closed_won", "tcv", "first_month_lf") if c in master_df.columns]
        probe_total = float(master_df[metric_probe].sum(numeric_only=True).sum()) if metric_probe else 1.0
        if probe_total == 0.0:
            master_df = df.copy()
        elif not spend_pool_full.empty:
            master_df = _impute_master_df_cost_from_spend_pool(
                master_df,
                spend_pool_full,
                start_date=_rng_lo,
                end_date=_rng_hi,
            )
    _spend_for_master_ui = spend_g
    if _normalized_spend_cost_sum(_spend_for_master_ui) < 1e-6 and _normalized_spend_cost_sum(spend_pool_full) > 1e-6:
        _spend_for_master_ui = _spend_sheet_pivot_by_month_country(spend_pool_full)

    _headline_keys = _mpo_headline_month_keys_for_scope(
        master_df,
        df,
        key_suffix,
        reporting_start=_rng_lo,
        reporting_end=_rng_hi,
    )
    if not _headline_keys:
        _fb = _mpo_month_keys_sorted_master(master_df)
        _headline_keys = _fb[-1:] if _fb else []

    _kpi_prior: dict[str, Any] = {"_comparison_off": True}

    # B2/B3 must use the **same Market × Month** scope as ``spend_df`` (``df``). Using the full post-qual tab here
    # summed every geography's LF for the headline months while spend was ME-only → Σ LF ~ $17M vs ~$132k in Excel.
    post_df_cpcw_headline = _mpo_slice_by_dashboard_ref(post_df_cpcw_analysis, df)

    # Headline KPIs: **sum** across months in scope — same **Data scope** as the multiselects (``spend_df``, ``cw_kpi``).
    _hm = (
        _mpo_scorecard_headline_totals_for_months(
            _headline_keys,
            spend_df=spend_df,
            leads_df=leads_df,
            post_df_kpi=(
                post_df_cpcw_headline
                if not post_df_cpcw_headline.empty
                else (post_df_pipe_scoped if not post_df_pipe_scoped.empty else post_df_cpcw_analysis)
            ),
            cw_kpi=cw_kpi,
        )
        if _headline_keys
        else None
    )
    if _hm:
        total_spend = float(_hm["total_spend"])
        total_impr = int(_hm["total_impr"])
        total_clicks = int(_hm["total_clicks"])
        ctr = float(_hm["ctr"])
        total_leads = int(_hm["total_leads"])
        total_qualified = int(_hm["total_qualified"])
        total_tcv = float(_hm["total_tcv"])
        total_first_month_lf = float(_hm["total_first_month_lf"])
        cpc = float(_hm["cpc"])
        cpl = float(_hm["cpl"])
        cpsql = float(_hm["cpsql"])
        total_new_working = int(_hm["total_new_working"])
        total_pitching = int(_hm["total_pitching"])
        total_negotiation = int(_hm["total_negotiation"])
        total_commitment = int(_hm["total_commitment"])
        total_qualifying = int(_hm["total_qualifying"])
        total_total_live = int(_hm["total_total_live"])
        total_closed_lost = int(_hm["total_closed_lost"])
        cw_for_qwin = _hm.get("cw_for_qwin")
        qual_for_qwin = _hm.get("qual_for_qwin")

    # Σ LF for CpCW:LF = sum of first-month licence fee / **rent** for the **same deals** as the CW card
    # (post-qual closed won + approved rows in the tab's Market × Month scope); join RAW ``cw_kpi`` when LF is not on post-qual.
    _cw_only_lf = _mpo_post_qual_closed_won_rows_for_kpis(post_df_cpcw_analysis, df)
    _lf_same_deals = _mpo_first_month_lf_sum_same_deals_as_post_qual_cw_rows(_cw_only_lf, cw_kpi)
    if _lf_same_deals > 0:
        total_first_month_lf = float(_lf_same_deals)

    # Keep Actual TCV card aligned with the CW KPI slice (post-qual / merged workbook scope).
    if _tcv_sum_override is not None:
        total_tcv = float(_tcv_sum_override)
    elif isinstance(cw_kpi, pd.DataFrame) and "tcv" in cw_kpi.columns:
        total_tcv = float(pd.to_numeric(cw_kpi["tcv"], errors="coerce").fillna(0).sum())
    if _lf_sum_override is not None and float(_lf_sum_override) > 0 and float(total_first_month_lf) <= 0:
        total_first_month_lf = float(_lf_sum_override)
    # Top-row lead counts: use full scoped **Leads** row set (same as expander). Headline month-sums can disagree.
    # Pipeline + Q-win: scoped **post** rows (Q-win CW from post-qual slice with stage flags, not truth rollups).
    _pqw = (
        _mpo_slice_by_dashboard_ref(post_df_cpcw_analysis, df)
        if (not post_df_cpcw_analysis.empty and not df.empty)
        else post_df_cpcw_analysis.copy()
    )
    if _pqw.empty:
        _pqw = post_df
    if not leads_df.empty:
        total_leads = int(_lead_rows_count(leads_df))
        total_qualified = int(_qualified_count_from_leads(leads_df))
        total_new_working = int(_new_working_count_from_leads(leads_df))
    if not post_df_pipe_scoped.empty:
        total_pitching = int(pd.to_numeric(post_df_pipe_scoped["pitching"], errors="coerce").fillna(0).sum()) if "pitching" in post_df_pipe_scoped.columns else 0
        total_negotiation = int(pd.to_numeric(post_df_pipe_scoped["negotiation"], errors="coerce").fillna(0).sum()) if "negotiation" in post_df_pipe_scoped.columns else 0
        total_commitment = int(pd.to_numeric(post_df_pipe_scoped["commitment"], errors="coerce").fillna(0).sum()) if "commitment" in post_df_pipe_scoped.columns else 0
        total_qualifying = int(pd.to_numeric(post_df_pipe_scoped["qualifying"], errors="coerce").fillna(0).sum()) if "qualifying" in post_df_pipe_scoped.columns else 0
        total_total_live = total_qualifying + total_pitching + total_negotiation + total_commitment
        total_closed_lost = int(pd.to_numeric(post_df_pipe_scoped["closed_lost"], errors="coerce").fillna(0).sum()) if "closed_lost" in post_df_pipe_scoped.columns else 0
    cpl = (total_spend / float(total_leads)) if total_leads else 0.0
    cpsql = (total_spend / float(total_qualified)) if total_qualified else 0.0
    cw_for_qwin, qual_for_qwin = _q_win_rate_inputs(_pqw, leads_df)
    _enable_sm_traffic = str(os.environ.get("XRAY_ENABLE_SM_TRAFFIC_OVERRIDE", "0")).strip().lower() in ("1", "true", "yes", "on")
    if _enable_sm_traffic:
        _sm_traffic = _mpo_traffic_totals_from_sm_pool(
            df_loaded,
            df,
            primary_sheet_id=sheet_id,
            start_date=_rng_lo,
            end_date=_rng_hi,
            headline_month_keys=_headline_keys,
            key_suffix=key_suffix,
        )
        if _sm_traffic is not None:
            total_impr, total_clicks, ctr = _sm_traffic
            cpc = (total_spend / float(total_clicks)) if total_clicks else 0.0
            cpl = (total_spend / float(total_leads)) if total_leads else 0.0
            cpsql = (total_spend / float(total_qualified)) if total_qualified else 0.0

    with _kpi_slot.container():
        _kpi_block(
            total_spend=total_spend,
            total_impr=total_impr,
            total_clicks=total_clicks,
            ctr=ctr,
            total_leads=total_leads,
            total_qualified=total_qualified,
            total_cw=total_cw,
            q_win_cw=cw_for_qwin,
            q_win_qualified=qual_for_qwin,
            total_tcv=total_tcv,
            total_first_month_lf=total_first_month_lf,
            cpc=cpc,
            cpl=cpl,
            cpsql=cpsql,
            total_new_working=total_new_working,
            total_total_live=total_total_live,
            total_negotiation=total_negotiation,
            total_commitment=total_commitment,
            total_closed_lost=total_closed_lost,
            total_pitching=total_pitching,
            total_qualifying=total_qualifying,
            prior=_kpi_prior,
        )

    gm_mpo = _master_build_gm_with_metrics(master_df, _spend_for_master_ui, pivot_dimension="market")
    gm_mpo = _overlay_gm_leads_qualified_from_raw_leads(gm_mpo, leads_df)
    _render_master_view_pivot_from_gm(
        gm_mpo,
        key_suffix=key_suffix,
        section_title="Master view",
        detail_sources={
            "spend": spend_sheet_for_kpis,
            "leads": leads_df,
            "post": post_df_pipe_scoped,
            "cw": cw_kpi,
        },
        pivot_dimension="market",
        table_mode="full",
    )
    _render_t3b3_quarter_sections(gm_mpo, key_suffix=f"{key_suffix}_t3b3")

    _render_mpo_trend_charts(
        start_date=_rng_lo,
        end_date=_rng_hi,
        master_df=master_df,
        key_suffix=key_suffix,
        spend_for_charts=spend_df,
        df_loaded=df_loaded,
        sheet_id=sheet_id,
        leads_df=leads_df,
        post_df_kpi=post_df_pipe_scoped,
        df_ref_for_scope=df,
    )


def _mom_monthly_series(df: pd.DataFrame, spend_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Month-level aggregates for Market MoM charts (spend, funnel counts, rates).

    When ``spend_df`` is set (sliced paid-media spend, same path as Marketing performance), monthly **spend**
    uses ``_mpo_spend_activity_for_month`` so totals match the main tab; otherwise spend comes from ``df['cost']``.
    """
    if df.empty or "month" not in df.columns:
        return pd.DataFrame()
    d = df.copy()
    if "closed_won" not in d.columns:
        d["closed_won"] = 0
    if "qualified" not in d.columns:
        d["qualified"] = 0
    monthly = (
        d.groupby("month", as_index=False)
        .agg(cw=("closed_won", "sum"), qualified=("qualified", "sum"))
        .sort_values("month")
    )
    if spend_df is not None and not spend_df.empty and "cost" in spend_df.columns:
        monthly["spend"] = monthly["month"].map(
            lambda m: float(_mpo_spend_activity_for_month(spend_df, str(_month_norm_key(m)))[0])
        )
    elif "cost" in d.columns:
        _sp = d.groupby("month", as_index=False).agg(spend=("cost", "sum"))
        monthly = monthly.merge(_sp, on="month", how="left")
        monthly["spend"] = pd.to_numeric(monthly["spend"], errors="coerce").fillna(0.0)
    else:
        monthly["spend"] = 0.0
    monthly["cw"] = _to_int_series_safe(monthly["cw"])
    monthly["qualified"] = pd.to_numeric(monthly["qualified"], errors="coerce").fillna(0.0)
    month_leads: list[int] = []
    for m in monthly["month"].tolist():
        gm = d[d["month"] == m]
        month_leads.append(_lead_rows_count(gm))
    monthly["leads"] = month_leads
    monthly["sql_pct"] = monthly.apply(
        lambda r: (float(r["qualified"]) / float(r["leads"]) * 100.0) if r["leads"] else 0.0,
        axis=1,
    )
    monthly["q_win_pct"] = monthly.apply(
        lambda r: (float(r["cw"]) / float(r["qualified"]) * 100.0) if r["qualified"] else 0.0,
        axis=1,
    )
    monthly["cpl"] = monthly.apply(
        lambda r: (float(r["spend"]) / float(r["leads"])) if r["leads"] else float("nan"),
        axis=1,
    )
    monthly["cpsql"] = monthly.apply(
        lambda r: (float(r["spend"]) / float(r["qualified"])) if r["qualified"] else float("nan"),
        axis=1,
    )
    monthly["month_lbl"] = monthly["month"].map(lambda x: _month_norm_key(x))
    # Normalized month key for sorting / watchouts (same basis as charts).
    monthly["month_key"] = monthly["month"].map(lambda m: str(_month_norm_key(m) or "").strip())
    return monthly


def _mom_market_month_delta_table(df: pd.DataFrame, spend_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """MoM table by market/month with delta columns vs previous month per market."""
    if df.empty or "month" not in df.columns or "country" not in df.columns:
        return pd.DataFrame()

    base = df.copy()
    if "closed_won" not in base.columns:
        base["closed_won"] = 0
    if "qualified" not in base.columns:
        base["qualified"] = 0
    grp = (
        base.groupby(["month", "country"], as_index=False)
        .agg(
            cw=("closed_won", "sum"),
            qualified=("qualified", "sum"),
        )
        .rename(columns={"country": "market"})
    )
    _mk_clean = grp["market"].astype(str).str.strip().str.lower()
    grp = grp.loc[~_mk_clean.isin({"", "unknown", "nan", "none", "<na>"})].copy()
    if grp.empty:
        return pd.DataFrame()
    grp["leads"] = (
        base.groupby(["month", "country"]).size().reindex(list(zip(grp["month"], grp["market"])), fill_value=0).to_numpy()
    )
    grp["cw"] = pd.to_numeric(grp["cw"], errors="coerce").fillna(0)
    grp["qualified"] = pd.to_numeric(grp["qualified"], errors="coerce").fillna(0)
    grp["sql_pct"] = grp.apply(
        lambda r: (float(r["qualified"]) / float(r["leads"]) * 100.0) if r["leads"] else 0.0,
        axis=1,
    )
    grp["q_win_pct"] = grp.apply(
        lambda r: (float(r["cw"]) / float(r["qualified"]) * 100.0) if r["qualified"] else 0.0,
        axis=1,
    )

    if spend_df is not None and not spend_df.empty and "cost" in spend_df.columns:
        sp = spend_df.copy()
        if "country" in sp.columns and "month" in sp.columns:
            sp["_mk"] = sp["country"].map(_country_join_key)
            sp["_mm"] = sp["month"].map(_month_norm_key)
            spg = sp.groupby(["_mm", "_mk"], as_index=False)["cost"].sum().rename(columns={"cost": "spend"})
            grp["_mk"] = grp["market"].map(_country_join_key)
            grp["_mm"] = grp["month"].map(_month_norm_key)
            grp = grp.merge(spg, on=["_mm", "_mk"], how="left")
            grp["spend"] = pd.to_numeric(grp["spend"], errors="coerce").fillna(0.0)
            grp = grp.drop(columns=["_mk", "_mm"], errors="ignore")
        else:
            grp["spend"] = 0.0
    else:
        grp["spend"] = (
            base.groupby(["month", "country"], as_index=False)["cost"].sum()["cost"].to_numpy()
            if "cost" in base.columns
            else 0.0
        )

    _jk_g = grp["market"].map(_country_join_key)
    _month_norm_series = grp["month"].map(_month_norm_key)
    _month_keys_sorted = sorted(
        {str(x) for x in _month_norm_series.dropna().unique() if x},
        key=_mpo_month_ts_for_sort,
    )
    _me_rows: list[dict[str, Any]] = []
    for _mk in _month_keys_sorted:
        _mm = _month_norm_series.astype(str) == str(_mk)
        _chunk = grp.loc[_mm]
        if _chunk.empty:
            continue
        _jk_c = _chunk["market"].map(_country_join_key)
        _c_only = _chunk.loc[_jk_c.isin(_MIDDLE_EAST_MARKET_KEYS)]
        _r_only = _chunk.loc[_jk_c.eq("middle east")]
        if not _c_only.empty:
            _sp = float(pd.to_numeric(_c_only["spend"], errors="coerce").fillna(0).sum())
            _cw = int(pd.to_numeric(_c_only["cw"], errors="coerce").fillna(0).sum())
            _qual = float(pd.to_numeric(_c_only["qualified"], errors="coerce").fillna(0).sum())
            _ld = int(pd.to_numeric(_c_only["leads"], errors="coerce").fillna(0).sum())
            _mo = _c_only["month"].iloc[0]
        elif not _r_only.empty:
            _sp = float(pd.to_numeric(_r_only["spend"], errors="coerce").fillna(0).sum())
            _cw = int(pd.to_numeric(_r_only["cw"], errors="coerce").fillna(0).sum())
            _qual = float(pd.to_numeric(_r_only["qualified"], errors="coerce").fillna(0).sum())
            _ld = int(pd.to_numeric(_r_only["leads"], errors="coerce").fillna(0).sum())
            _mo = _r_only["month"].iloc[0]
        else:
            continue
        _sql = (float(_qual) / float(_ld) * 100.0) if _ld else 0.0
        _qw = (float(_cw) / float(_qual) * 100.0) if _qual else 0.0
        _me_rows.append(
            {
                "month": _mo,
                "market": _MIDDLE_EAST_REGION_LABEL,
                "cw": _cw,
                "qualified": _qual,
                "leads": _ld,
                "spend": _sp,
                "sql_pct": _sql,
                "q_win_pct": _qw,
            }
        )
    grp = grp.loc[~_jk_g.eq("middle east")].copy()
    if _me_rows:
        grp = pd.concat([grp, pd.DataFrame(_me_rows)], ignore_index=True)

    grp["month_key"] = grp["month"].map(_month_norm_key).astype(str)
    grp = grp.sort_values(["market", "month_key"], key=lambda c: c.map(_mpo_month_ts_for_sort) if c.name == "month_key" else c)

    for col in ("spend", "cw", "leads", "qualified", "sql_pct", "q_win_pct"):
        grp[f"delta_{col}"] = grp.groupby("market")[col].diff().fillna(0.0)

    out = grp.rename(
        columns={
            "month_key": "Month",
            "market": "Market",
            "spend": "Spend",
            "delta_spend": "Δ Spend",
            "cw": "CW",
            "delta_cw": "Δ CW",
            "leads": "Leads",
            "delta_leads": "Δ Leads",
            "qualified": "Qualified",
            "delta_qualified": "Δ Qualified",
            "sql_pct": "SQL %",
            "delta_sql_pct": "Δ SQL pp",
            "q_win_pct": "Q win %",
            "delta_q_win_pct": "Δ Q win pp",
        }
    )
    out = out[
        [
            "Month",
            "Market",
            "Spend",
            "Δ Spend",
            "Leads",
            "Δ Leads",
            "Qualified",
            "Δ Qualified",
            "CW",
            "Δ CW",
            "SQL %",
            "Δ SQL pp",
            "Q win %",
            "Δ Q win pp",
        ]
    ].copy()
    out["Spend"] = pd.to_numeric(out["Spend"], errors="coerce").fillna(0.0)
    out["Δ Spend"] = pd.to_numeric(out["Δ Spend"], errors="coerce").fillna(0.0)
    for c in ("CW", "Δ CW", "Leads", "Δ Leads", "Qualified", "Δ Qualified"):
        out[c] = _to_int_series_safe(out[c])
    for c in ("SQL %", "Δ SQL pp", "Q win %", "Δ Q win pp"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0).round(2)
    out["_me_sort"] = out["Market"].astype(str).eq(_MIDDLE_EAST_REGION_LABEL)
    out = out.sort_values(["Month", "_me_sort", "Market"], ascending=[False, True, True]).drop(columns=["_me_sort"])
    out = out.reset_index(drop=True)
    return out


def render_page_market_mom(
    df_loaded: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> None:
    """Month-over-month view framed on **Closed Won** — the outcome marketing spend is meant to produce; funnel metrics are leading signals."""
    key_suffix = "mom"
    _mpo_filter_key = "mpo"
    df_filtered = _filter_by_date_range(df_loaded, start_date, end_date)
    df_date = df_loaded if df_filtered.empty else df_filtered
    if df_date.empty:
        st.info("No rows in the selected date range.")
        return

    _dashboard_tab_page_header()

    spend_sheet_for_kpis, _, _, _, _ = _mpo_load_spend_sheet_for_kpis(
        df_loaded,
        df_date,
        start_date,
        end_date,
    )
    df = _mpo_dataframe_from_session_filters(df_date, key_suffix=_mpo_filter_key)
    if "country" in df.columns:
        _mk = df["country"].astype(str).str.strip().str.lower()
        df = df.loc[~_mk.isin({"", "unknown", "nan", "none", "<na>"})].copy()
    spend_df_mpo = _spend_slice_for_dashboard_filters(spend_sheet_for_kpis, df)

    if df.empty:
        st.warning("No rows match the current filters — widen the date range or clear sheet filters.")
        return

    st.markdown('<div class="mom-page-wrap">', unsafe_allow_html=True)

    _sel_mk = st.session_state.get(f"{_mpo_filter_key}_market", [_MPO_ALL_GEO_SENTINEL])
    _mk_only = _mpo_market_scope_countries_only(_sel_mk)
    if len(_mk_only) == 1:
        scope_lbl = _mk_only[0]
    elif _mk_only:
        scope_lbl = f"{len(_mk_only)} markets"
    else:
        scope_lbl = "All markets (portfolio)"
    if _mk_only:
        st.caption(f"Active market filter: {', '.join(_mk_only)}")
    _hk_mom = _mpo_headline_month_keys_for_scope(
        pd.DataFrame(),
        df,
        _mpo_filter_key,
        reporting_start=start_date,
        reporting_end=end_date,
    )
    if not _hk_mom:
        _fb_m = _mpo_month_keys_sorted_master(df)
        _hk_mom = _fb_m[-1:] if _fb_m else []
    if _hk_mom:
        total_spend = float(sum(_mpo_spend_activity_for_month(spend_df_mpo, mk)[0] for mk in _hk_mom))
    else:
        total_spend = (
            float(pd.to_numeric(spend_df_mpo["cost"], errors="coerce").fillna(0).sum())
            if (not spend_df_mpo.empty and "cost" in spend_df_mpo.columns)
            else 0.0
        )
    total_cw = int(pd.to_numeric(df["closed_won"], errors="coerce").fillna(0).sum()) if "closed_won" in df.columns else 0
    total_leads = _lead_rows_count(df)
    total_qual = int(pd.to_numeric(df["qualified"], errors="coerce").fillna(0).sum()) if "qualified" in df.columns else 0
    sql_all = (total_qual / total_leads * 100.0) if total_leads else 0.0
    qwin_all = (total_cw / total_qual * 100.0) if total_qual else 0.0
    cpcw_all = (total_spend / total_cw) if total_cw else 0.0

    monthly = _mom_monthly_series(df, spend_df=spend_df_mpo)
    _mom_d = 0.0
    _mom_cur = 0.0
    _mom_prev = 0.0
    _mom_ok = False
    if not monthly.empty and len(monthly) >= 2:
        _ms = monthly.sort_values("month_key")
        _mom_cur = float(_ms.iloc[-1]["spend"])
        _mom_prev = float(_ms.iloc[-2]["spend"])
        _mom_d = _mom_cur - _mom_prev
        _mom_ok = True

    st.markdown(
        _mom_executive_snapshot_scorecards_html(
            scope_lbl=scope_lbl,
            total_spend=total_spend,
            total_cw=total_cw,
            total_leads=total_leads,
            total_qual=total_qual,
            sql_pct=sql_all,
            qwin_pct=qwin_all,
            spend_per_cw=cpcw_all,
            mom_spend_delta=_mom_d,
            mom_spend_current=_mom_cur,
            mom_spend_prior=_mom_prev,
            mom_spend_compare_ok=_mom_ok,
        ),
        unsafe_allow_html=True,
    )
    st.markdown(
        (
            '<div style="padding:10px 12px;border:1px solid #e2e8f0;border-radius:10px;background:#f8fafc;'
            'font-size:13px;color:#334155;margin:8px 0 10px 0;">'
            f"MoM narrative: Spend is <b>${total_spend:,.0f}</b>, generating <b>{total_leads:,}</b> leads, "
            f"<b>{total_qual:,}</b> qualified leads (<b>{sql_all:.1f}% SQL</b>), and <b>{total_cw:,}</b> closed won "
            f"(<b>{qwin_all:.1f}% Q win</b>) — prioritize actions where spend growth is not translating downstream."
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    st.markdown(
        (
            '<div style="padding:9px 12px;border:1px dashed #cbd5e1;border-radius:10px;background:#ffffff;'
            'font-size:13px;color:#334155;margin:4px 0 12px 0;">'
            "<b>Funnel journey:</b> Spend → Leads → Qualified → Closed Won"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    st.caption(
        "Spend uses the **same paid-media source** as **Marketing performance**. "
        "Set **Market** and **Month** on that tab — this view reuses them (no duplicate filters here). "
        "Charts tie spend to the funnel that should feed **Closed Won**."
    )

    mom_delta_tbl = _mom_market_month_delta_table(df, spend_df=spend_df_mpo)
    if monthly.empty:
        st.info("No calendar months in this slice — check filters and month columns.")
        if not mom_delta_tbl.empty:
            st.markdown('<div class="looker-table-title">Month × market — momentum toward Closed Won</div>', unsafe_allow_html=True)
            st.dataframe(
                mom_delta_tbl,
                width="stretch",
                hide_index=True,
                height=360,
                key=f"{key_suffix}_df_mom_delta_empty",
            )
        else:
            _master_performance_table(df, key_suffix=f"{key_suffix}_mom", section_title="Month × market detail")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown('<div class="looker-table-title">Month × market — momentum toward Closed Won</div>', unsafe_allow_html=True)
    st.caption(
        "Key question: **where is Closed Won momentum strongest or weakest vs last month?** "
        "Default rank is **Δ CW**; use **Δ Spend** next to **Closed won** in each row to see whether spend and wins are aligned — "
        "or switch ranking to conversion rates when spend looks fine but wins do not."
    )
    if not mom_delta_tbl.empty:
        ctl_m, ctl_s = st.columns((1, 1.2), gap="small")
        month_opts = sorted(mom_delta_tbl["Month"].dropna().astype(str).unique().tolist(), key=_mpo_month_ts_for_sort, reverse=True)
        with ctl_m:
            month_focus = st.selectbox(
                "Focus month",
                ["All months"] + month_opts,
                index=0,
                key=f"{key_suffix}_mom_tbl_month_focus",
            )
        with ctl_s:
            sort_label = st.selectbox(
                "Rank by (Closed Won first)",
                ["Δ CW", "Q win %", "SQL %", "Δ Spend"],
                index=0,
                key=f"{key_suffix}_mom_tbl_rank_metric",
            )
        tbl_view = mom_delta_tbl.copy()
        if month_focus != "All months":
            tbl_view = tbl_view.loc[tbl_view["Month"].astype(str) == str(month_focus)].copy()
            _me_sort = tbl_view["Market"].astype(str).eq(_MIDDLE_EAST_REGION_LABEL)
            tbl_view = tbl_view.assign(_me_sort=_me_sort).sort_values(["_me_sort", sort_label], ascending=[True, False]).drop(
                columns=["_me_sort"]
            )
        else:
            _month_rank = tbl_view["Month"].map(_mpo_month_ts_for_sort)
            _me_sort = tbl_view["Market"].astype(str).eq(_MIDDLE_EAST_REGION_LABEL)
            tbl_view = (
                tbl_view.assign(_month_rank=_month_rank, _me_sort=_me_sort)
                .sort_values(["_month_rank", "_me_sort", sort_label], ascending=[False, True, False])
                .drop(columns=["_month_rank", "_me_sort"])
            )
        # Hide rows with no conversion signal (spend-only rows with zero CW/SQL/Q-win) to keep the table focused.
        _cw0 = pd.to_numeric(tbl_view["CW"], errors="coerce").fillna(0).eq(0)
        _sql0 = pd.to_numeric(tbl_view["SQL %"], errors="coerce").fillna(0).eq(0)
        _qw0 = pd.to_numeric(tbl_view["Q win %"], errors="coerce").fillna(0).eq(0)
        tbl_view = tbl_view.loc[~(_cw0 & _sql0 & _qw0)].copy()
        def _decision_flag(_r: pd.Series) -> str:
            _dsp = float(pd.to_numeric(_r.get("Δ Spend"), errors="coerce") or 0.0)
            _dcw = float(pd.to_numeric(_r.get("Δ CW"), errors="coerce") or 0.0)
            _sqlv = float(pd.to_numeric(_r.get("SQL %"), errors="coerce") or 0.0)
            _qw = float(pd.to_numeric(_r.get("Q win %"), errors="coerce") or 0.0)
            if _dsp > 0 and _dcw <= 0:
                return "Spend up / CW flat"
            if _sqlv >= 28.0 and _qw >= 4.0:
                return "Healthy conversion"
            if _sqlv >= 28.0 and _qw < 4.0:
                return "Downstream weak"
            return "Monitor"

        tbl_view["Decision Flag"] = tbl_view.apply(_decision_flag, axis=1)
        compact_cols = ["Month", "Market", "Spend", "Leads", "Qualified", "CW", "SQL %", "Q win %", "Δ Spend", "Δ CW", "Decision Flag"]
        compact_tbl = tbl_view[compact_cols].copy()

        def _delta_cell(v: float, money: bool) -> str:
            if v > 0:
                bg, fg, arrow = "#ecfdf5", "#166534", "▲"
            elif v < 0:
                bg, fg, arrow = "#fef2f2", "#991b1b", "▼"
            else:
                bg, fg, arrow = "#f8fafc", "#475569", "→"
            txt = f"${abs(v):,.0f}" if money else f"{int(abs(v)):,}"
            return (
                f'<span style="display:inline-block;padding:2px 8px;border-radius:999px;'
                f'background:{bg};color:{fg};font-weight:700;">{arrow} {txt}</span>'
            )

        rows_html: list[str] = []
        for _, r in compact_tbl.head(120).iterrows():
            spend = float(pd.to_numeric(r["Spend"], errors="coerce") or 0.0)
            d_sp = float(pd.to_numeric(r["Δ Spend"], errors="coerce") or 0.0)
            cw = int(pd.to_numeric(r["CW"], errors="coerce") or 0)
            leads_v = int(pd.to_numeric(r["Leads"], errors="coerce") or 0)
            qual_v = int(pd.to_numeric(r["Qualified"], errors="coerce") or 0)
            sql = float(pd.to_numeric(r["SQL %"], errors="coerce") or 0.0)
            qwin = float(pd.to_numeric(r["Q win %"], errors="coerce") or 0.0)
            d_cw = float(pd.to_numeric(r["Δ CW"], errors="coerce") or 0.0)
            _is_me = str(r["Market"]) == _MIDDLE_EAST_REGION_LABEL
            _row_style = (
                ' style="background:#f0fdfa;font-weight:700;border-top:1px solid #99f6e4;"'
                if _is_me
                else ""
            )
            rows_html.append(
                f"<tr{_row_style}>"
                f"<td>{html.escape(str(r['Month']))}</td>"
                f"<td>{html.escape(str(r['Market']))}</td>"
                f"<td>${spend:,.0f}</td>"
                f"<td>{leads_v:,}</td>"
                f"<td>{qual_v:,}</td>"
                f"<td>{cw:,}</td>"
                f"<td>{sql:.1f}%</td>"
                f"<td>{qwin:.1f}%</td>"
                f"<td>{_delta_cell(d_sp, money=True)}</td>"
                f"<td>{_delta_cell(d_cw, money=False)}</td>"
                f"<td>{html.escape(str(r['Decision Flag']))}</td>"
                "</tr>"
            )
        table_html = (
            '<div style="max-height:360px;overflow:auto;border:1px solid #e2e8f0;border-radius:12px;background:#fff;">'
            '<table style="width:100%;border-collapse:collapse;font-size:12px;">'
            '<thead style="position:sticky;top:0;background:#f8fafc;z-index:1;">'
            '<tr>'
            '<th style="text-align:left;padding:8px;border-bottom:1px solid #e2e8f0;">Month</th>'
            '<th style="text-align:left;padding:8px;border-bottom:1px solid #e2e8f0;">Market</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid #e2e8f0;">Spend</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid #e2e8f0;">Leads</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid #e2e8f0;">Qualified</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid #e2e8f0;">Closed won</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid #e2e8f0;">SQL %</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid #e2e8f0;">Q win %</th>'
            '<th style="text-align:left;padding:8px;border-bottom:1px solid #e2e8f0;">Δ Spend</th>'
            '<th style="text-align:left;padding:8px;border-bottom:1px solid #e2e8f0;">Δ CW</th>'
            '<th style="text-align:left;padding:8px;border-bottom:1px solid #e2e8f0;">Decision Flag</th>'
            "</tr></thead>"
            f"<tbody>{''.join(rows_html)}</tbody>"
            "</table></div>"
        )
        st.markdown(table_html, unsafe_allow_html=True)
    else:
        _master_performance_table(df, key_suffix=f"{key_suffix}_mom", section_title="")

    _plot_mom = dict(template="plotly_white", paper_bgcolor="white", plot_bgcolor="white", font=dict(size=12))
    _xaxis = dict(showgrid=True, gridcolor="rgba(148,163,184,0.25)", title="")
    # Legend below plot keeps the in-chart title clear of the series and matches MoM / spend-channel charts.
    _mom_chart_legend = dict(
        orientation="h",
        yanchor="top",
        y=-0.22,
        xanchor="center",
        x=0.5,
        font=dict(size=11),
    )
    _mom_chart_margin = dict(l=12, r=12, t=58, b=92)
    c_vol, c_qlt = st.columns(2)
    with c_vol:
        st.markdown('<div class="looker-table-title" style="margin-top:0;">Funnel volume</div>', unsafe_allow_html=True)
        st.caption("**Closed won** is the outcome; leads and qualified show whether the funnel can support it.")
        st.markdown('<div style="height:14px" aria-hidden="true"></div>', unsafe_allow_html=True)
        fig_v = make_subplots(specs=[[{"secondary_y": True}]])
        _cw_max = float(pd.to_numeric(monthly["cw"], errors="coerce").fillna(0).max()) if "cw" in monthly.columns else 0.0
        _lq_max = float(
            max(
                pd.to_numeric(monthly["leads"], errors="coerce").fillna(0).max() if "leads" in monthly.columns else 0.0,
                pd.to_numeric(monthly["qualified"], errors="coerce").fillna(0).max() if "qualified" in monthly.columns else 0.0,
            )
        )
        fig_v.add_trace(
            go.Bar(
                x=monthly["month_lbl"],
                y=monthly["cw"],
                name="Closed won",
                marker_color="#0f766e",
            ),
            secondary_y=True,
        )
        fig_v.add_trace(
            go.Bar(
                x=monthly["month_lbl"],
                y=monthly["leads"],
                name="Lead rows",
                marker_color="#93c5fd",
            ),
            secondary_y=False,
        )
        fig_v.add_trace(
            go.Bar(
                x=monthly["month_lbl"],
                y=monthly["qualified"],
                name="Qualified leads",
                marker_color="#a78bfa",
            ),
            secondary_y=False,
        )
        fig_v.update_layout(
            title=dict(
                text="Closed won, lead rows, and qualified leads by month",
                font=dict(size=14),
                pad=dict(t=4, b=14),
            ),
            barmode="group",
            height=420,
            **_plot_mom,
            margin=_mom_chart_margin,
            legend=_mom_chart_legend,
            yaxis=dict(
                title="Leads / Qualified leads",
                showgrid=True,
                gridcolor="rgba(148,163,184,0.2)",
                range=[0, _lq_max + 200],
            ),
            yaxis2=dict(title="Closed won", showgrid=False, range=[0, _cw_max + 200]),
            xaxis=_xaxis,
        )
        st.plotly_chart(fig_v, width="stretch", key=f"{key_suffix}_pl_volume")
    with c_qlt:
        st.markdown('<div class="looker-table-title" style="margin-top:0;">Conversion quality</div>', unsafe_allow_html=True)
        st.caption("SQL % and Q win % explain how efficiently qualified leads turn into **Closed won**.")
        st.markdown('<div style="height:14px" aria-hidden="true"></div>', unsafe_allow_html=True)
        fig_q = go.Figure()
        fig_q.add_trace(
            go.Scatter(
                x=monthly["month_lbl"],
                y=monthly["sql_pct"],
                name="SQL %",
                mode="lines+markers",
                line=dict(color="#0f766e", width=2.5),
                marker=dict(size=8),
            )
        )
        fig_q.add_trace(
            go.Scatter(
                x=monthly["month_lbl"],
                y=monthly["q_win_pct"],
                name="Q win % (CW ÷ qualified leads)",
                mode="lines+markers",
                line=dict(color="#6366f1", width=2.5),
                marker=dict(size=8),
            )
        )
        fig_q.add_hline(
            y=30.0,
            line_dash="dot",
            line_color="#0f766e",
            opacity=0.65,
            annotation_text="SQL target 30%",
            annotation_position="top left",
        )
        fig_q.add_hline(
            y=4.0,
            line_dash="dot",
            line_color="#6366f1",
            opacity=0.65,
            annotation_text="Q win ref 4%",
            annotation_position="top right",
        )
        fig_q.update_layout(
            title=dict(text="SQL % and Q win % over time", font=dict(size=14), pad=dict(t=4, b=14)),
            height=420,
            **_plot_mom,
            margin=_mom_chart_margin,
            legend=_mom_chart_legend,
            yaxis=dict(title="Percent", ticksuffix="%", showgrid=True, gridcolor="rgba(148,163,184,0.2)"),
            xaxis=_xaxis,
        )
        st.plotly_chart(fig_q, width="stretch", key=f"{key_suffix}_pl_quality")
        if not monthly.empty and "month_key" in monthly.columns:
            _latest = monthly.sort_values("month_key", key=lambda s: s.map(_mpo_month_ts_for_sort)).iloc[-1]
            _lv = float(pd.to_numeric(_latest.get("leads"), errors="coerce") or 0.0)
            _qv = float(pd.to_numeric(_latest.get("qualified"), errors="coerce") or 0.0)
            _cv = float(pd.to_numeric(_latest.get("cw"), errors="coerce") or 0.0)
            leak_1 = (_lv - _qv) if _lv > 0 else 0.0
            leak_2 = (_qv - _cv) if _qv > 0 else 0.0
            if leak_1 >= leak_2:
                _msg = (
                    f"Biggest leakage is **Leads → Qualified** in {str(_latest.get('month_lbl') or 'current period')}: "
                    f"{int(leak_1):,} leads did not qualify. Action: tighten lead quality and targeting."
                )
            else:
                _msg = (
                    f"Biggest leakage is **Qualified → Closed Won** in {str(_latest.get('month_lbl') or 'current period')}: "
                    f"{int(leak_2):,} qualified leads did not close. Action: improve downstream sales conversion."
                )
            st.markdown('<div class="looker-table-title" style="margin-top:10px;">Funnel watchout</div>', unsafe_allow_html=True)
            st.markdown(f"- {_msg}")

    st.markdown("</div>", unsafe_allow_html=True)


def _pmc_normalize_channel_label(raw: str) -> str:
    """Map raw platform/channel strings toward ME X-Ray **Unified Channel** names (paid + organic)."""
    s = str(raw or "").strip()
    if not s or s.lower() in ("unknown", "nan", "none", "nat"):
        return "Other"
    tl = s.lower()
    if "performance max" in tl or re.search(r"\bpmax\b", tl):
        return "PMax"
    if "google" in tl and "organic" not in tl and "seo" not in tl:
        if "ads" in tl or "adwords" in tl or tl in ("google", "google ads", "google_ads"):
            return "Google Search"
    if "meta" in tl or "facebook" in tl or "instagram" in tl:
        return "Meta"
    if "linked" in tl:
        return "LinkedIn"
    if "snap" in tl:
        return "Snapchat"
    return s


def _pmc_unified_channel_series(df: pd.DataFrame) -> pd.Series:
    """Prefer **Unified Channel** (sheet), then **platform**, **channel**, **source_tab** — same grain as the pivot table."""
    if df.empty:
        return pd.Series(dtype=object)
    n = len(df)
    empty = pd.Series([""] * n, index=df.index, dtype=object)
    ucx = (
        df["Unified Channel"].astype(str).str.strip()
        if "Unified Channel" in df.columns
        else empty
    )
    plat = (
        df["platform"].astype(str).str.strip()
        if "platform" in df.columns
        else empty
    )
    ch = (
        df["channel"].astype(str).str.strip()
        if "channel" in df.columns
        else empty
    )
    stb = df["source_tab"].astype(str) if "source_tab" in df.columns else empty

    def _pick(i: int) -> str:
        u = str(ucx.iloc[i])
        if u and u.lower() not in ("unknown", "nan", "none", "nat", ""):
            return _pmc_normalize_channel_label(u)
        p = str(plat.iloc[i])
        c = str(ch.iloc[i])
        t = str(stb.iloc[i])
        if p and p.lower() not in ("unknown", "nan", "none", "nat"):
            return _pmc_normalize_channel_label(p)
        if c and c.lower() not in ("unknown", "nan", "none", "nat", ""):
            return _pmc_normalize_channel_label(c)
        return _pmc_normalize_channel_label(_mpo_platform_label_from_source_tab(t))

    return pd.Series([_pick(i) for i in range(n)], index=df.index, dtype=object)


def _pmc_align_channel_label_for_xray_pivot(lab: str) -> str:
    """Map Ads / Supermetrics platform strings toward ME X-Ray **Unified Channel**–style labels where needed.

    Rows often have **Channel** = ``Google Search`` in the sheet while **Platform** = ``Google Ads``; without this,
    spend lands on **Google Ads** and the **Google Search** pivot row shows ``$0``.

    Sheet-native unified labels (Organic, Instagram Organic, Meta, …) pass through unchanged.
    """
    s = str(lab or "").strip()
    if not s or s == "(blank)":
        return s
    tl = s.lower()
    if "performance max" in tl or re.search(r"\bpmax\b", tl):
        return "PMax"
    # Only fold generic **Google Ads** naming into **Google Search**; do not remap organic / brand strings.
    if "google" in tl and "organic" not in tl and "seo" not in tl:
        if "ads" in tl or "adwords" in tl or tl in ("google", "google ads", "google_ads"):
            return "Google Search"
    return s


def _pmc_sheet_channel_series(df: pd.DataFrame) -> pd.Series:
    """Pivot-style channel: prefer **Unified Channel** (ME X-Ray), then **channel**, else **platform**, else tab."""
    if df.empty:
        return pd.Series(dtype=object)
    n = len(df)
    empty = pd.Series([""] * n, index=df.index, dtype=object)
    ucx = (
        df["Unified Channel"].astype(str).str.strip()
        if "Unified Channel" in df.columns
        else empty
    )
    plat = (
        df["platform"].astype(str).str.strip()
        if "platform" in df.columns
        else empty
    )
    ch = (
        df["channel"].astype(str).str.strip()
        if "channel" in df.columns
        else empty
    )
    stb = df["source_tab"].astype(str) if "source_tab" in df.columns else empty

    def _bad(s: str) -> bool:
        t = str(s).strip().lower()
        # ``_mpo_blend_paid_media_for_master_df`` used to fill blanks with "Paid media", which blocked platform fallback.
        return not t or t in ("unknown", "nan", "none", "nat") or t in ("paid media", "paid_media")

    def _pick(i: int) -> str:
        u = str(ucx.iloc[i])
        if not _bad(u):
            return _pmc_align_channel_label_for_xray_pivot(u.strip())
        c = str(ch.iloc[i])
        if not _bad(c):
            return _pmc_align_channel_label_for_xray_pivot(c.strip())
        p = str(plat.iloc[i])
        if not _bad(p):
            return _pmc_align_channel_label_for_xray_pivot(p.strip())
        tab_lab = _mpo_platform_label_from_source_tab(str(stb.iloc[i])).strip()
        if not _bad(tab_lab):
            return _pmc_align_channel_label_for_xray_pivot(tab_lab)
        return "(blank)"

    return pd.Series([_pick(i) for i in range(n)], index=df.index, dtype=object)


def _pmc_frame_with_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Align ``country`` / ``month`` join keys with the main Master view (UAE vs Uae, regional aliases)."""
    out = df.copy()
    if not out.empty:
        out = _normalize_master_merge_frame(out)
    out["unified_channel"] = _pmc_unified_channel_series(out)
    if "closed_won" not in out.columns:
        out["closed_won"] = 0
    else:
        out["closed_won"] = pd.to_numeric(out["closed_won"], errors="coerce").fillna(0)
    if "qualified" not in out.columns:
        out["qualified"] = 0
    else:
        out["qualified"] = pd.to_numeric(out["qualified"], errors="coerce").fillna(0)
    if "tcv" not in out.columns:
        out["tcv"] = 0.0
    else:
        out["tcv"] = pd.to_numeric(out["tcv"], errors="coerce").fillna(0.0)
    if "first_month_lf" not in out.columns:
        out["first_month_lf"] = 0.0
    else:
        out["first_month_lf"] = pd.to_numeric(out["first_month_lf"], errors="coerce").fillna(0.0)
    if "cost" not in out.columns:
        out["cost"] = 0.0
    else:
        out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0.0)
    return out


def _pmc_floor_march_or_later(w: date) -> date:
    """Spend-by-channel: include data from **1 March** of ``w.year`` when the reporting window starts earlier."""
    m1 = date(w.year, 3, 1)
    return m1 if w < m1 else w


def _pmc_filter_month_not_before(df: pd.DataFrame, not_before: date) -> pd.DataFrame:
    """Drop rows whose ``month`` is strictly before ``not_before`` (calendar month)."""
    if df.empty or "month" not in df.columns:
        return df
    try:
        floor = pd.Period(year=not_before.year, month=not_before.month, freq="M")
    except Exception:
        return df

    def _ge(m: Any) -> bool:
        try:
            mk = _month_norm_key(m)
            if not mk or str(mk).strip().lower() in ("nan", "nat", "none", ""):
                return True
            return pd.Period(str(mk), freq="M") >= floor
        except Exception:
            return True

    return df.loc[df["month"].map(_ge)].copy()


_PMC_CHANNEL_ORDER: tuple[str, ...] = (
    "Google Search",
    "LinkedIn",
    "Meta",
    "PMax",
    "Snapchat",
    "Other",
)


def _pmc_order_channels_df(by_ch: pd.DataFrame) -> pd.DataFrame:
    """Match Looker/PDF axis order (not sort by spend)."""
    if by_ch.empty:
        return by_ch
    d = by_ch.copy()
    ord_map = {c: i for i, c in enumerate(_PMC_CHANNEL_ORDER)}
    d["_sort"] = d["unified_channel"].map(lambda x: ord_map.get(str(x).strip(), 99))
    d = d.sort_values(["_sort", "unified_channel"]).drop(columns=["_sort"])
    return d


def _pmc_scoped_leads_rows_for_channel_metrics(df_loaded: pd.DataFrame, df_scope: pd.DataFrame) -> pd.DataFrame:
    """Leads-sheet rows scoped to the same **market + month** slice as spend (not filtered to spend channel names)."""
    if df_loaded.empty or "source_tab" not in df_loaded.columns:
        return pd.DataFrame()
    s = df_loaded["source_tab"].astype(str).str.lower()
    st_exact = df_loaded["source_tab"].astype(str).str.strip()
    mask = pd.Series(False, index=df_loaded.index)
    for pat in _MPO_LEAD_TAB_PATTERNS:
        mask = mask | s.str.contains(pat.lower(), na=False, regex=True)
    # Include whole worksheets that carry ``Is_Qualifying`` (or similar), even if tab title does not match patterns.
    _iq = _leads_is_qualifying_column_name(df_loaded.columns)
    if _iq is not None:
        v = df_loaded[_iq]
        ve = v.astype(str).str.strip().str.lower()
        populated = v.notna() & ~ve.isin(["", "nan", "nat", "none"])
        if bool(populated.any()):
            tabs_with_flag = st_exact.loc[populated].unique().tolist()
            mask = mask | st_exact.isin(tabs_with_flag)
    leads = df_loaded.loc[mask].copy()
    if leads.empty:
        return leads
    leads = _mpo_slice_by_dashboard_ref(leads, df_scope)
    if leads.empty:
        return leads
    uc = leads.get("Unified Channel", pd.Series(index=leads.index, dtype=object)).astype(str).str.strip()
    usd = leads.get("UTM Source Detail", pd.Series(index=leads.index, dtype=object)).astype(str).str.strip()
    ch = uc.where(~uc.str.lower().isin(["", "unknown", "nan", "none", "nat"]), usd)
    ch = ch.where(~ch.str.lower().isin(["", "unknown", "nan", "none", "nat"]), "Other")
    leads["_pmc_ch"] = ch.map(_pmc_align_channel_label_for_xray_pivot)
    leads["_is_qualified"] = _to_int_series_safe(_leads_is_qualified_mask(leads))
    if "month" in leads.columns:
        leads["_month_key"] = leads["month"].map(_month_norm_key).astype(str).str.strip()
        leads = leads.loc[
            leads["_month_key"].ne("")
            & ~leads["_month_key"].str.lower().isin(["nan", "nat", "none"])
        ].copy()
    else:
        leads["_month_key"] = ""
    return leads


def _pmc_leads_channel_lut_from_leads_sheet(df_loaded: pd.DataFrame, df_scope: pd.DataFrame) -> pd.DataFrame:
    """Qualified / lead **counts** by Unified Channel from the Leads sheet (Qualified status and/or ``Is_Qualifying``)."""
    leads = _pmc_scoped_leads_rows_for_channel_metrics(df_loaded, df_scope)
    if leads.empty:
        return pd.DataFrame(columns=["unified_channel", "leads_from_leads", "qualified_from_leads"])
    lut = (
        leads.groupby("_pmc_ch", as_index=False)
        .agg(leads_from_leads=("_pmc_ch", "count"), qualified_from_leads=("_is_qualified", "sum"))
        .rename(columns={"_pmc_ch": "unified_channel"})
    )
    return _pmc_order_channels_df(lut)


def _pmc_by_channel_summary(u: pd.DataFrame, leads_lut: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    g = (
        u.groupby("unified_channel", as_index=False)
        .agg(
            spend=("cost", "sum"),
            cw=("closed_won", "sum"),
            qualified_spend=("qualified", "sum"),
            tcv=("tcv", "sum"),
            spend_rows=("cost", "count"),
        )
    )
    if leads_lut is not None and not leads_lut.empty:
        g = g.merge(leads_lut, on="unified_channel", how="left")
        _lv = pd.to_numeric(g["leads_from_leads"], errors="coerce").fillna(0)
        _qv = pd.to_numeric(g["qualified_from_leads"], errors="coerce").fillna(0)
        _lr = pd.to_numeric(g["spend_rows"], errors="coerce").fillna(0)
        _qs = pd.to_numeric(g["qualified_spend"], errors="coerce").fillna(0)
        g["leads"] = _to_int_series_safe(_lv.where(_lv > 0, _lr))
        g["qualified"] = _to_int_series_safe(_qv.where(_qv > 0, _qs))
        g = g.drop(columns=["leads_from_leads", "qualified_from_leads"], errors="ignore")
    else:
        g["leads"] = _to_int_series_safe(g["spend_rows"])
        g["qualified"] = _to_int_series_safe(g["qualified_spend"])
    g = g.drop(columns=["qualified_spend"], errors="ignore")
    g["CPL"] = g.apply(lambda r: (r["spend"] / r["leads"]) if r["leads"] else float("nan"), axis=1)
    g["SQL%"] = g.apply(
        lambda r: (r["qualified"] / r["leads"] * 100.0) if r["leads"] else float("nan"),
        axis=1,
    )
    return _pmc_order_channels_df(g.drop(columns=["spend_rows"], errors="ignore"))


def _pmc_delta_chip(v: float, *, money: bool = False) -> str:
    n = float(pd.to_numeric(pd.Series([v]), errors="coerce").fillna(0).iloc[0])
    if n > 0:
        arr, bg, fg = "▲", "#ecfdf3", "#166534"
    elif n < 0:
        arr, bg, fg = "▼", "#fef2f2", "#991b1b"
    else:
        arr, bg, fg = "→", "#f8fafc", "#475569"
    fmt = f"${abs(n):,.2f}" if money else f"{abs(n):,.2f}"
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:999px;'
        f'background:{bg};color:{fg};font-weight:700;">{arr} {fmt}</span>'
    )


def _pmc_insights_fmt_money(v: Any) -> str:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return "—"
    return f"${float(x):,.2f}"


def _pmc_insights_fmt_int(v: Any) -> str:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return "—"
    return f"{float(x):,.2f}"


def _pmc_insights_fmt_pct(v: Any) -> str:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return "—"
    return f"{float(x):.1f}%"


def _pmc_insights_mom_sql_chip(v: Any) -> str:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return '<span style="color:#64748b;font-weight:600;">—</span>'
    return _pmc_delta_chip(float(x), money=False)


def _pmc_blended_channel_insights(
    df_loaded: pd.DataFrame,
    df_scope: pd.DataFrame,
    u: pd.DataFrame,
    by_ch: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """Leadership-ready channel insight table + narrative bullets, blending Spend + Leads + Post Qual + CW tabs."""
    if by_ch.empty:
        return by_ch, []
    base = by_ch.copy()
    base["channel"] = base["unified_channel"].astype(str).str.strip()

    # Blend Post-Qualification signals by channel when available.
    post = _tab_subset_by_patterns(df_loaded, list(_POST_LEAD_SOURCE_TAB_PATTERNS))
    post = _mpo_slice_by_dashboard_ref(post, df_scope) if not post.empty else post
    if not post.empty:
        post = _pmc_frame_with_metrics(post)
        post["channel"] = post["unified_channel"].astype(str).str.strip()
        pcol = "qualifying" if "qualifying" in post.columns else "qualified"
        p = (
            post.groupby("channel", as_index=False)
            .agg(post_qual=(pcol, "sum"))
            .assign(post_qual=lambda d: pd.to_numeric(d["post_qual"], errors="coerce").fillna(0))
        )
        base = base.merge(p, on="channel", how="left")
    else:
        base["post_qual"] = 0.0

    # Optional RAW CW overlay by channel (fallback only where base CW is zero).
    cw_tab = _tab_subset_by_patterns(df_loaded, list(_RAW_CW_TAB_PATTERNS))
    cw_tab = _mpo_slice_by_dashboard_ref(cw_tab, df_scope) if not cw_tab.empty else cw_tab
    if not cw_tab.empty:
        cw_tab = _pmc_frame_with_metrics(cw_tab)
        cw_tab["channel"] = cw_tab["unified_channel"].astype(str).str.strip()
        c = cw_tab.groupby("channel", as_index=False).agg(cw_from_cw=("closed_won", "sum"))
        c["cw_from_cw"] = pd.to_numeric(c["cw_from_cw"], errors="coerce").fillna(0)
        base = base.merge(c, on="channel", how="left")
    else:
        base["cw_from_cw"] = 0.0

    for col in ("spend", "cw", "qualified", "leads", "tcv", "post_qual", "cw_from_cw"):
        base[col] = pd.to_numeric(base.get(col, 0), errors="coerce").fillna(0.0)
    base["cw"] = pd.concat([base["cw"], base["cw_from_cw"]], axis=1).max(axis=1)
    base["sql_pct"] = base.apply(
        lambda r: (r["qualified"] / r["leads"] * 100.0) if r["leads"] else float("nan"),
        axis=1,
    )
    base["qwin_pct"] = base.apply(
        lambda r: (r["cw"] / r["qualified"] * 100.0) if r["qualified"] else float("nan"),
        axis=1,
    )
    base["cpcw"] = base.apply(lambda r: (r["spend"] / r["cw"]) if r["cw"] else float("nan"), axis=1)
    base["cpsql"] = base.apply(lambda r: (r["spend"] / r["qualified"]) if r["qualified"] else float("nan"), axis=1)

    tot_spend = float(base["spend"].sum())
    tot_cw = float(base["cw"].sum())
    tot_qual = float(base["qualified"].sum())
    base["spend_share_pct"] = (base["spend"] / tot_spend * 100.0) if tot_spend else 0.0
    base["cw_share_pct"] = (base["cw"] / tot_cw * 100.0) if tot_cw else 0.0
    base["qual_share_pct"] = (base["qualified"] / tot_qual * 100.0) if tot_qual else 0.0

    # MoM: spend + CW from spend rows; SQL pp from **Leads sheet** (latest vs prior month in scope).
    months = sorted({str(_month_norm_key(m)) for m in u.get("month", pd.Series(dtype=object)).tolist() if _month_norm_key(m)})
    cur_m = months[-1] if months else None
    prev_m = months[-2] if len(months) > 1 else None
    if cur_m and prev_m:
        um = u.copy()
        um["month_key"] = um["month"].map(_month_norm_key)
        mm = (
            um.loc[um["month_key"].isin([cur_m, prev_m])]
            .groupby(["month_key", "unified_channel"], as_index=False)
            .agg(spend=("cost", "sum"), cw=("closed_won", "sum"))
        )
        cur = mm.loc[mm["month_key"].eq(cur_m)].rename(
            columns={"unified_channel": "channel", "spend": "_sp_cur", "cw": "_cw_cur"}
        )
        prv = mm.loc[mm["month_key"].eq(prev_m)].rename(
            columns={"unified_channel": "channel", "spend": "_sp_prev", "cw": "_cw_prev"}
        )
        base = base.merge(cur.drop(columns=["month_key"], errors="ignore"), on="channel", how="left")
        base = base.merge(prv.drop(columns=["month_key"], errors="ignore"), on="channel", how="left")
        for col in ("_sp_cur", "_cw_cur", "_sp_prev", "_cw_prev"):
            base[col] = pd.to_numeric(base.get(col, 0), errors="coerce").fillna(0.0)
        base["mom_spend_delta"] = base["_sp_cur"] - base["_sp_prev"]
        base["mom_cw_delta"] = base["_cw_cur"] - base["_cw_prev"]

        lr_mom = _pmc_scoped_leads_rows_for_channel_metrics(df_loaded, df_scope)

        def _sql_pct_for_month(mk: str) -> pd.DataFrame:
            if lr_mom.empty or "_month_key" not in lr_mom.columns:
                return pd.DataFrame(columns=["channel", "_sql"])
            sub = lr_mom.loc[lr_mom["_month_key"].eq(str(mk))]
            if sub.empty:
                return pd.DataFrame(columns=["channel", "_sql"])
            gg = sub.groupby("_pmc_ch", as_index=False).agg(_n=("_pmc_ch", "count"), _q=("_is_qualified", "sum"))
            gg["_sql"] = gg.apply(
                lambda r: (r["_q"] / r["_n"] * 100.0) if r["_n"] else float("nan"),
                axis=1,
            )
            return gg.rename(columns={"_pmc_ch": "channel"})[["channel", "_sql"]]

        cur_sql = _sql_pct_for_month(str(cur_m))
        prev_sql = _sql_pct_for_month(str(prev_m))
        msql = cur_sql.merge(prev_sql, on="channel", how="outer", suffixes=("_c", "_p"))
        if not msql.empty:
            msql["mom_sql_delta_pp"] = pd.to_numeric(msql["_sql_c"], errors="coerce") - pd.to_numeric(
                msql["_sql_p"], errors="coerce"
            )
            base = base.merge(msql[["channel", "mom_sql_delta_pp"]], on="channel", how="left")
        else:
            base["mom_sql_delta_pp"] = float("nan")
    else:
        base["mom_spend_delta"] = 0.0
        base["mom_cw_delta"] = 0.0
        base["mom_sql_delta_pp"] = float("nan")

    _sql_eff = pd.to_numeric(base["sql_pct"], errors="coerce").fillna(0.0)
    _qw_eff = pd.to_numeric(base["qwin_pct"], errors="coerce").fillna(0.0)
    base["eff_score"] = (base["cw_share_pct"] - base["spend_share_pct"]) + (_qw_eff * 0.20) + (_sql_eff * 0.10)
    base = base.sort_values(["eff_score", "cw", "spend"], ascending=[False, False, False])

    bullets: list[str] = []
    if not base.empty:
        top = base.iloc[0]
        if tot_cw > 1e-9:
            bullets.append(
                f"{top['channel']}: {top['cw_share_pct']:.1f}% of CW on {top['spend_share_pct']:.1f}% of spend (efficiency read)."
            )
        else:
            bullets.append(
                "No **closed won** in this channel scope — confirm RAW CW / month filters, or that deals map to **Unified Channel**."
            )
        cw_jump = base.sort_values("mom_cw_delta", ascending=False).iloc[0]
        if float(cw_jump["mom_cw_delta"]) > 0:
            bullets.append(
                f"Fastest CW momentum: {cw_jump['channel']} ({cw_jump['mom_cw_delta']:+.0f} MoM) while spend moved {cw_jump['mom_spend_delta']:+.0f}."
            )
        spend_drag = base.sort_values("eff_score", ascending=True).iloc[0]
        bullets.append(
            f"Watchlist: {spend_drag['channel']} (spend share {spend_drag['spend_share_pct']:.1f}% vs CW share {spend_drag['cw_share_pct']:.1f}%)."
        )
    return base, bullets


def _pmc_render_magic_insights(df_loaded: pd.DataFrame, df_scope: pd.DataFrame, u: pd.DataFrame, by_ch: pd.DataFrame) -> None:
    if by_ch.empty:
        return
    table, bullets = _pmc_blended_channel_insights(df_loaded, df_scope, u, by_ch)
    if table.empty:
        return
    st.markdown('<div class="dash-master-surface">', unsafe_allow_html=True)
    st.markdown('<div class="looker-table-title">Channel intelligence snapshot</div>', unsafe_allow_html=True)
    st.caption(
        "**Spend** = Σ cost (ME X-Ray scope). **Leads** / **Qualified** = Leads-sheet rows; **Qualified** uses Lead Status. "
        "**Post-qual** = Σ qualifying (post-qual tab). **Closed won** = higher of spend-row CW and RAW CW tab, by channel. "
        "**SQL %** = Qualified / Leads; **Q win %** = Closed won / Qualified. **MoM SQL pp** = Leads-sheet SQL % (latest vs prior month)."
    )
    for b in bullets[:5]:
        st.markdown(f"- {b}")

    view = table.copy()
    view["MoM Spend"] = view["mom_spend_delta"].map(lambda v: _pmc_delta_chip(v, money=True))
    view["MoM CW"] = view["mom_cw_delta"].map(lambda v: _pmc_delta_chip(v, money=False))
    view["MoM SQL pp"] = view["mom_sql_delta_pp"].map(_pmc_insights_mom_sql_chip)
    view["spend"] = view["spend"].map(_pmc_insights_fmt_money)
    view["leads"] = view["leads"].map(_pmc_insights_fmt_int)
    view["qualified"] = view["qualified"].map(_pmc_insights_fmt_int)
    view["post_qual"] = view["post_qual"].map(_pmc_insights_fmt_int)
    view["cw"] = view["cw"].map(_pmc_insights_fmt_int)
    view["sql_pct"] = view["sql_pct"].map(_pmc_insights_fmt_pct)
    view["qwin_pct"] = view["qwin_pct"].map(_pmc_insights_fmt_pct)
    view["cpsql"] = view["cpsql"].map(_pmc_insights_fmt_money)
    view["cpcw"] = view["cpcw"].map(_pmc_insights_fmt_money)
    view["spend_share_pct"] = view["spend_share_pct"].map(_pmc_insights_fmt_pct)
    view["cw_share_pct"] = view["cw_share_pct"].map(_pmc_insights_fmt_pct)
    out = view[
        [
            "channel",
            "spend",
            "leads",
            "qualified",
            "post_qual",
            "cw",
            "sql_pct",
            "qwin_pct",
            "cpsql",
            "cpcw",
            "spend_share_pct",
            "cw_share_pct",
            "MoM Spend",
            "MoM CW",
            "MoM SQL pp",
        ]
    ].rename(
        columns={
            "channel": "Channel",
            "spend": "Spend",
            "leads": "Leads (sheet)",
            "qualified": "Qualified",
            "post_qual": "Post-qual (Σ)",
            "cw": "Closed won",
            "sql_pct": "SQL %",
            "qwin_pct": "Q win %",
            "cpsql": "CPSQL",
            "cpcw": "CPCW",
            "spend_share_pct": "Spend share %",
            "cw_share_pct": "CW share %",
        }
    )
    st.markdown(
        out.to_html(index=False, escape=False, classes=["mpo-detail-table"]),
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _ai_openai_key_and_source() -> tuple[str, str]:
    """Read OpenAI key from common secret/env names and report source for debugging."""
    try:
        s = st.secrets
        for k in (
            "OPENAI_API_KEY",
            "openai_api_key",
            "OPENAI_KEY",
            "openai_key",
            "OPEN_AI_API_KEY",
            "OPENAI_APIKEY",
            "openai_apikey",
            "OPENAI_TOKEN",
            "openai_token",
        ):
            v = (s.get(k) or "").strip()
            if v:
                return v, f"secrets.{k}"
        # Also support nested secret blocks like [openai] api_key="..."
        for parent in ("openai", "OPENAI"):
            try:
                blk = s.get(parent)  # type: ignore[assignment]
                if hasattr(blk, "get"):
                    v = (blk.get("api_key") or blk.get("OPENAI_API_KEY") or "").strip()
                    if v:
                        return v, f"secrets.{parent}.api_key"
            except Exception:
                pass
    except Exception:
        pass
    for k in (
        "OPENAI_API_KEY",
        "openai_api_key",
        "OPENAI_KEY",
        "OPEN_AI_API_KEY",
        "OPENAI_APIKEY",
        "openai_apikey",
        "OPENAI_TOKEN",
        "openai_token",
    ):
        v = (os.environ.get(k) or "").strip()
        if v:
            return v, f"env.{k}"
    return "", "none"


def _ai_openai_key_from_secrets_or_env() -> str:
    """Backward-compatible getter used by existing call sites."""
    return _ai_openai_key_and_source()[0]


def _ai_openai_model_from_secrets_or_env() -> str:
    """Allow model override without code edits."""
    try:
        s = st.secrets
        for k in ("OPENAI_MODEL", "openai_model"):
            v = (s.get(k) or "").strip()
            if v:
                return v
    except Exception:
        pass
    return (os.environ.get("OPENAI_MODEL") or "").strip() or "gpt-4o-mini"


def _ai_channel_scope_payload(by_ch_blended: pd.DataFrame, u_scope: pd.DataFrame) -> dict[str, Any]:
    """Compact numeric payload for LLM answers bound to current dashboard scope."""
    if by_ch_blended.empty:
        return {
            "totals": {"spend": 0.0, "leads": 0.0, "qualified": 0.0, "closed_won": 0.0, "tcv": 0.0},
            "channels": [],
            "months": [],
        }
    d = by_ch_blended.copy()
    for c in ("spend", "leads", "qualified", "cw", "tcv", "sql_pct", "qwin_pct", "mom_spend_delta", "mom_cw_delta"):
        d[c] = pd.to_numeric(d.get(c, 0), errors="coerce").fillna(0.0)
    d = d.sort_values("spend", ascending=False)
    ch_rows: list[dict[str, Any]] = []
    for _, r in d.head(10).iterrows():
        ch_rows.append(
            {
                "channel": str(r.get("channel") or r.get("unified_channel") or ""),
                "spend": float(r.get("spend") or 0.0),
                "leads": float(r.get("leads") or 0.0),
                "qualified": float(r.get("qualified") or 0.0),
                "closed_won": float(r.get("cw") or 0.0),
                "tcv": float(r.get("tcv") or 0.0),
                "sql_pct": float(r.get("sql_pct") or 0.0),
                "qwin_pct": float(r.get("qwin_pct") or 0.0),
                "mom_spend_delta": float(r.get("mom_spend_delta") or 0.0),
                "mom_cw_delta": float(r.get("mom_cw_delta") or 0.0),
            }
        )
    totals = {
        "spend": float(d["spend"].sum()),
        "leads": float(d["leads"].sum()),
        "qualified": float(d["qualified"].sum()),
        "closed_won": float(d["cw"].sum()),
        "tcv": float(d["tcv"].sum()),
    }
    month_keys = []
    if not u_scope.empty and "month" in u_scope.columns:
        month_keys = sorted(
            {str(_month_norm_key(m)) for m in u_scope["month"].tolist() if _month_norm_key(m)},
            key=_mpo_month_ts_for_sort,
        )
    return {"totals": totals, "channels": ch_rows, "months": month_keys[-12:]}


def _ai_workbook_fallback_payload(df_loaded: pd.DataFrame, start_date: date, end_date: date) -> dict[str, Any]:
    """When channel blend cannot run, still give the LLM workbook-level totals for the reporting window."""
    df = _filter_spend_for_dashboard(df_loaded, start_date, end_date) if not df_loaded.empty else df_loaded
    spend = float(pd.to_numeric(df["cost"], errors="coerce").fillna(0).sum()) if not df.empty and "cost" in df.columns else 0.0
    cw = float(pd.to_numeric(df["closed_won"], errors="coerce").fillna(0).sum()) if not df.empty and "closed_won" in df.columns else 0.0
    leads = float(_lead_rows_count(df)) if not df.empty else 0.0
    qual = float(pd.to_numeric(df["qualified"], errors="coerce").fillna(0).sum()) if not df.empty and "qualified" in df.columns else 0.0
    tcv = float(pd.to_numeric(df["tcv"], errors="coerce").fillna(0).sum()) if not df.empty and "tcv" in df.columns else 0.0
    months: list[str] = []
    if not df.empty and "month" in df.columns:
        months = sorted(
            {str(_month_norm_key(m)) for m in df["month"].tolist() if _month_norm_key(m)},
            key=_mpo_month_ts_for_sort,
        )
    return {
        "totals": {
            "spend": spend,
            "leads": leads,
            "qualified": qual,
            "closed_won": cw,
            "tcv": tcv,
        },
        "channels": [],
        "months": months[-12:],
    }


def _build_global_ask_ai_payload(df_loaded: pd.DataFrame, start_date: date, end_date: date) -> tuple[dict[str, Any], str]:
    """Blended channel metrics when possible; otherwise workbook totals. Second value is a short scope caption."""
    key_suffix = "pmc"
    try:
        spend_base = _mpo_spend_sheet_for_channel_master(df_loaded, start_date, end_date)
        if spend_base.empty:
            raise ValueError("no spend base")
        df_scope = _pmc_spend_scope_for_ask_ai(
            spend_base,
            key_suffix=key_suffix,
            reporting_start=start_date,
            reporting_end=end_date,
        )
        _m0 = _pmc_floor_march_or_later(start_date)
        df_scope = _pmc_filter_month_not_before(df_scope, _m0)
        if df_scope.empty:
            raise ValueError("empty after march floor")
        u = _pmc_frame_with_metrics(df_scope.copy())
        u["unified_channel"] = _pmc_sheet_channel_series(u)
        leads_lut = _pmc_leads_channel_lut_from_leads_sheet(df_loaded, df_scope)
        by_ch = _pmc_by_channel_summary(u, leads_lut=leads_lut)
        chart_base, _ = _pmc_blended_channel_insights(df_loaded, df_scope, u, by_ch)
        payload = _ai_channel_scope_payload(chart_base if not chart_base.empty else pd.DataFrame(), u)
        payload["blend"] = "channel_master"
        note = (
            "Uses **Spend by channel** month/channel filters (that tab) and the same blended metrics as Channel intelligence."
        )
        return payload, note
    except Exception:
        pl = _ai_workbook_fallback_payload(df_loaded, start_date, end_date)
        pl["blend"] = "workbook_fallback"
        note = (
            "Channel blend unavailable for this window — using workbook-level totals only. "
            "Visit **Spend by channel** once if filters were never initialized."
        )
        return pl, note


def _ai_dashboard_snapshot(df_loaded: pd.DataFrame, start_date: date, end_date: date) -> dict[str, Any]:
    """Richer dashboard-wide context so the assistant answers like an on-page analyst."""
    df = _filter_spend_for_dashboard(df_loaded, start_date, end_date) if not df_loaded.empty else df_loaded
    if df.empty:
        return {"window": {"start": str(start_date), "end": str(end_date)}, "totals": {}, "markets": [], "monthly": []}
    spend = pd.to_numeric(df["cost"], errors="coerce").fillna(0) if "cost" in df.columns else pd.Series([0] * len(df))
    cw = pd.to_numeric(df["closed_won"], errors="coerce").fillna(0) if "closed_won" in df.columns else pd.Series([0] * len(df))
    q = pd.to_numeric(df["qualified"], errors="coerce").fillna(0) if "qualified" in df.columns else pd.Series([0] * len(df))
    leads = float(_lead_rows_count(df))
    totals = {
        "spend": float(spend.sum()),
        "closed_won": float(cw.sum()),
        "qualified": float(q.sum()),
        "leads": leads,
        "sql_pct": float((float(q.sum()) / leads * 100.0) if leads > 0 else 0.0),
        "qwin_pct": float((float(cw.sum()) / float(q.sum()) * 100.0) if float(q.sum()) > 0 else 0.0),
    }
    ccol = "country" if "country" in df.columns else ("market" if "market" in df.columns else None)
    markets: list[dict[str, Any]] = []
    if ccol:
        g = (
            df.assign(
                _sp=pd.to_numeric(df.get("cost", 0), errors="coerce").fillna(0.0),
                _cw=pd.to_numeric(df.get("closed_won", 0), errors="coerce").fillna(0.0),
                _q=pd.to_numeric(df.get("qualified", 0), errors="coerce").fillna(0.0),
            )
            .groupby(ccol, as_index=False)
            .agg(spend=("_sp", "sum"), closed_won=("_cw", "sum"), qualified=("_q", "sum"))
            .sort_values("spend", ascending=False)
            .head(8)
        )
        for _, r in g.iterrows():
            markets.append(
                {
                    "market": str(r.get(ccol) or ""),
                    "spend": float(r.get("spend") or 0.0),
                    "closed_won": float(r.get("closed_won") or 0.0),
                    "qualified": float(r.get("qualified") or 0.0),
                }
            )
    monthly_rows: list[dict[str, Any]] = []
    if "month" in df.columns:
        m = (
            df.assign(
                _mk=df["month"].map(_month_norm_key),
                _sp=pd.to_numeric(df.get("cost", 0), errors="coerce").fillna(0.0),
                _cw=pd.to_numeric(df.get("closed_won", 0), errors="coerce").fillna(0.0),
                _q=pd.to_numeric(df.get("qualified", 0), errors="coerce").fillna(0.0),
            )
            .dropna(subset=["_mk"])
            .groupby("_mk", as_index=False)
            .agg(spend=("_sp", "sum"), closed_won=("_cw", "sum"), qualified=("_q", "sum"))
            .sort_values("_mk", key=lambda s: s.map(_mpo_month_ts_for_sort))
            .tail(12)
        )
        for _, r in m.iterrows():
            monthly_rows.append(
                {
                    "month": str(r.get("_mk") or ""),
                    "spend": float(r.get("spend") or 0.0),
                    "closed_won": float(r.get("closed_won") or 0.0),
                    "qualified": float(r.get("qualified") or 0.0),
                }
            )
    return {
        "window": {"start": str(start_date), "end": str(end_date)},
        "totals": totals,
        "markets": markets,
        "monthly": monthly_rows,
    }


def _ai_rule_based_channel_insights(payload: dict[str, Any]) -> list[str]:
    """Deterministic backup insights when no LLM key is configured."""
    totals = payload.get("totals", {})
    ch = payload.get("channels", []) or []
    if not ch:
        sp = float(totals.get("spend") or 0)
        if sp <= 0 and float(totals.get("closed_won") or 0) <= 0:
            return ["No channel rows in this filter scope."]
        return [
            f"Workbook-scope totals: ${sp:,.0f} spend, {float(totals.get('leads') or 0):,.0f} leads, "
            f"{float(totals.get('qualified') or 0):,.0f} qualified, {float(totals.get('closed_won') or 0):,.0f} closed won "
            f"(channel breakdown unavailable — check filters or open **Spend by channel** once)."
        ]
    out: list[str] = []
    top_spend = ch[0]
    out.append(
        f"Top spend channel is {top_spend['channel']} (${top_spend['spend']:,.0f}) with {top_spend['closed_won']:,.0f} closed won."
    )
    best_qw = max(ch, key=lambda r: float(r.get("qwin_pct") or 0.0))
    out.append(
        f"Best Q win % is {best_qw['channel']} at {best_qw['qwin_pct']:.1f}% (CW / qualified)."
    )
    worst_eff = min(ch, key=lambda r: (float(r.get("closed_won") or 0.0) / max(float(r.get("spend") or 0.0), 1.0)))
    out.append(
        f"Efficiency watchlist: {worst_eff['channel']} has low CW per spend (CW {worst_eff['closed_won']:,.0f} on ${worst_eff['spend']:,.0f})."
    )
    if float(totals.get("qualified") or 0) > 0:
        out.append(
            f"Scope totals: ${float(totals.get('spend') or 0):,.0f} spend, {float(totals.get('leads') or 0):,.0f} leads, "
            f"{float(totals.get('qualified') or 0):,.0f} qualified, {float(totals.get('closed_won') or 0):,.0f} closed won."
        )
    return out


def _ai_openai_answer(
    question: str,
    payload: dict[str, Any],
    *,
    model: str,
    api_key: str,
    mode: str,
    history: Optional[list[dict[str, str]]] = None,
) -> str:
    """Call OpenAI Chat Completions via HTTPS without extra package dependency."""
    mission_brief = (
        "Primary mission: develop and operate a specialized ChatGPT analyst for customer data analysis and conversion optimization. "
        "It must diagnose why users do not complete purchases, compare successful vs unsuccessful journeys, and provide actionable recommendations. "
        "Always cover: behavioral drop-off analysis, UX/checkout friction, offer/personalization opportunities, loyalty/retention levers, "
        "and implementation guidance for API integration, dashboards/reporting, and continuous optimization."
    )
    system_prompt = (
        "You are a senior performance marketing strategist and RevOps analyst. "
        "Use only the provided JSON metrics and never invent values. "
        f"Analyzer mode: {mode}. "
        f"{mission_brief} "
        "Treat the payload as the live dashboard state for this user, and answer like a professional analyst reviewing the page. "
        "Respond with practical, decision-ready guidance: diagnosis, likely causes, prioritized actions, and risks. "
        "Tie every recommendation to a numeric signal from payload."
    )
    user_prompt = (
        "Current user question:\n"
        f"{question.strip()}\n\n"
        "Metrics payload (current dashboard filter scope):\n"
        f"{json.dumps(payload, ensure_ascii=True)}\n\n"
        "Return:\n"
        "- concise markdown answer (not JSON)\n"
        "- include sections: Diagnosis, Likely causes, Actions\n"
        "- include exact numbers when referenced\n"
        "- include a short confidence line at the end."
    )
    msgs: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    if history:
        for m in history[-8:]:
            role = str(m.get("role") or "").strip().lower()
            content = str(m.get("content") or "").strip()
            if role in ("user", "assistant") and content:
                msgs.append({"role": role, "content": content})
    msgs.append({"role": "user", "content": user_prompt})
    body = {"model": model, "temperature": 0.2, "messages": msgs}
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8")
        parsed = json.loads(raw)
        choices = parsed.get("choices") or []
        if not choices:
            return "No model response was returned."
        msg = choices[0].get("message") or {}
        txt = str(msg.get("content") or "").strip()
        return txt or "Model returned an empty response."
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = str(e)
        return f"AI request failed (HTTP {e.code}). {detail[:400]}"
    except Exception as e:
        return f"AI request failed. {str(e)}"


def _xray_render_ai_panel() -> None:
    """Right-side persistent chat panel styled like an assistant widget."""
    panel = st.container(key="xray_ai_panel")
    with panel:
        payload = st.session_state.get("_xray_ai_payload") or {}
        note = str(st.session_state.get("_xray_ai_scope_note") or "")
        _k, _k_src = _ai_openai_key_and_source()
        if "xray_ai_messages" not in st.session_state:
            st.session_state["xray_ai_messages"] = []
        if "xray_ai_input" not in st.session_state:
            st.session_state["xray_ai_input"] = ""
        if "xray_ai_clear_input" not in st.session_state:
            st.session_state["xray_ai_clear_input"] = False
        if "xray_ai_mode" not in st.session_state:
            st.session_state["xray_ai_mode"] = "General"
        if "xray_ai_model_pick" not in st.session_state:
            st.session_state["xray_ai_model_pick"] = "o3"
        # Clear input only before the widget is rendered (Streamlit restriction).
        if bool(st.session_state.get("xray_ai_clear_input")):
            st.session_state["xray_ai_input"] = ""
            st.session_state["xray_ai_clear_input"] = False
        # Clean up legacy greeting from older builds.
        _msgs = st.session_state.get("xray_ai_messages", [])
        if _msgs and isinstance(_msgs, list):
            first_txt = str((_msgs[0] or {}).get("content") or "")
            if "Ollie" in first_txt:
                st.session_state["xray_ai_messages"] = []

        st.markdown('<div class="xray-ai-panel-title">Ask KitchenPark AI</div>', unsafe_allow_html=True)
        st.caption("KitchenPark AI widget")
        h1, h2, h3, h4 = st.columns([2.5, 1.2, 0.4, 0.4], gap="small")
        with h2:
            st.selectbox("Model", ["o3", "GPT-4o", "GPT-5", "GPT-5.2"], key="xray_ai_model_pick", label_visibility="collapsed")
        with h3:
            st.markdown('<div class="xray-ai-icon-btn">⋮</div>', unsafe_allow_html=True)
        with h4:
            if st.button("✕", key="xray_ai_close_btn"):
                st.session_state["xray_ai_open"] = False
                st.rerun()

        if not st.session_state["xray_ai_messages"]:
            intro = (
                "Hi! I'm **KitchenPark AI**.\n\n"
                f"Current mode: **{st.session_state.get('xray_ai_mode','General')}**.\n\n"
                "I analyze marketing performance, conversion drop-offs, and growth actions from your current dashboard context."
            )
            st.session_state["xray_ai_messages"].append({"role": "assistant", "content": intro})

        chat_box = st.container(border=True, key="xray_ai_chat_box", height=460)
        with chat_box:
            for m in st.session_state.get("xray_ai_messages", []):
                role = "assistant" if m.get("role") == "assistant" else "user"
                with st.chat_message(role):
                    st.markdown(str(m.get("content") or ""))

        t1, t2 = st.columns([1.3, 4], gap="small")
        with t1:
            if st.button("Test OpenAI", key="xray_ai_test_btn"):
                api_key_test = _k or _ai_openai_key_from_secrets_or_env()
                if not api_key_test:
                    st.session_state["xray_ai_conn_status"] = f"Missing API key ({_k_src})."
                else:
                    probe = _ai_openai_answer(
                        "Reply with exactly OPENAI_OK",
                        {"totals": {}, "channels": [], "months": []},
                        model="gpt-4o",
                        api_key=api_key_test,
                        mode="General",
                        history=[],
                    )
                    st.session_state["xray_ai_conn_status"] = (
                        "Connected (OPENAI_OK)"
                        if "OPENAI_OK" in str(probe)
                        else f"Connected but unexpected reply: {str(probe)[:120]}"
                    )
        with t2:
            st.caption(str(st.session_state.get("xray_ai_conn_status") or ""))

        with st.form("xray_ai_send_form", clear_on_submit=True):
            in1, in2, in3 = st.columns([4.4, 1.7, 0.8], gap="small")
            with in1:
                q = st.text_input(
                    "Ask",
                    key="xray_ai_input_form",
                    label_visibility="collapsed",
                    placeholder="Ask anything about your dashboard...",
                )
            with in2:
                analyzer_mode = st.selectbox(
                    "Mode",
                    ["General", "Paid media optimizer", "Funnel doctor", "CMO brief"],
                    key="xray_ai_mode",
                    label_visibility="collapsed",
                )
            with in3:
                send_clicked = st.form_submit_button("➤", type="primary", use_container_width=True)

        q = str(q or "").strip()
        if send_clicked and q:
            st.session_state["xray_ai_messages"].append({"role": "user", "content": q})
            api_key = _k or _ai_openai_key_from_secrets_or_env()
            selected_model = str(st.session_state.get("xray_ai_model_pick") or "o3")
            model_map = {"o3": "o3", "GPT-4o": "gpt-4o", "GPT-5": "gpt-5", "GPT-5.2": "gpt-5.2"}
            model = model_map.get(selected_model, _ai_openai_model_from_secrets_or_env())
            if not api_key:
                err = (
                    "OpenAI key not detected in this deployed app. "
                    "Expected keys: `OPENAI_API_KEY`, `OPENAI_KEY`, `OPENAI_APIKEY`, "
                    "or nested `[openai] api_key` in Streamlit secrets."
                )
                st.session_state["xray_ai_messages"].append({"role": "assistant", "content": err})
                st.rerun()
            with st.spinner("Thinking..."):
                answer = _ai_openai_answer(
                    q,
                    payload,
                    model=model,
                    api_key=api_key,
                    mode=analyzer_mode,
                    history=st.session_state.get("xray_ai_messages", [])[:-1],
                )
            if str(answer).lower().startswith("ai request failed"):
                answer = f"Connection issue while calling OpenAI.\n\n{answer}"
            footer = (
                f"\n\n_Model: `{model}` · mode: `{analyzer_mode}` · blend: `{payload.get('blend', 'n/a')}` "
                f"· months: {', '.join(payload.get('months') or ['n/a'])}_"
            )
            st.session_state["xray_ai_messages"].append({"role": "assistant", "content": f"{answer}{footer}"})
            st.rerun()


def _render_xray_floating_ask_ai(df_loaded: pd.DataFrame, start_date: date, end_date: date) -> None:
    """Fixed-position Ask AI control — available on every tab (same scope logic as blended channel metrics)."""
    if "xray_ai_open" not in st.session_state:
        st.session_state["xray_ai_open"] = False
    if "xray_ai_payload_ready" not in st.session_state:
        st.session_state["xray_ai_payload_ready"] = False
    if st.button(
        "Ask KitchenPark AI",
        key="xray_ask_ai_fab",
        type="secondary",
        help="Insights for the current data scope (blended metrics when available)",
    ):
        st.session_state["xray_ai_open"] = True
        st.session_state["xray_ai_payload_ready"] = False
    if st.session_state.get("xray_ai_open"):
        if not st.session_state.get("xray_ai_payload_ready"):
            with st.spinner("Preparing AI context..."):
                payload, note = _build_global_ask_ai_payload(df_loaded, start_date, end_date)
                payload["dashboard_snapshot"] = _ai_dashboard_snapshot(df_loaded, start_date, end_date)
            st.session_state["_xray_ai_payload"] = payload
            st.session_state["_xray_ai_scope_note"] = note
            st.session_state["xray_ai_payload_ready"] = True
        _xray_render_ai_panel()


def _pmc_by_ch_top_n_for_charts(by_ch: pd.DataFrame, *, max_channels: int = 6) -> pd.DataFrame:
    """Keep charts readable: top channels by spend, roll the rest into **Other** (table above stays full)."""
    if by_ch.empty or len(by_ch) <= max_channels:
        return by_ch.copy()
    d = by_ch.copy()
    d["_sp"] = pd.to_numeric(d["spend"], errors="coerce").fillna(0.0)
    d = d.sort_values("_sp", ascending=False, kind="mergesort")
    top = d.head(max_channels - 1).drop(columns=["_sp"], errors="ignore")
    rest = d.iloc[max_channels - 1 :].drop(columns=["_sp"], errors="ignore")
    if rest.empty:
        return top
    num_cols = [c for c in ("spend", "leads", "qualified", "cw", "tcv") if c in rest.columns]
    other: dict[str, Any] = {"unified_channel": "Other"}
    for c in num_cols:
        other[c] = float(pd.to_numeric(rest[c], errors="coerce").fillna(0.0).sum())
    for c in rest.columns:
        if c in other or c == "unified_channel":
            continue
        if c in ("CPL", "SQL%"):
            continue
        if pd.api.types.is_numeric_dtype(rest[c]):
            other[c] = float(pd.to_numeric(rest[c], errors="coerce").fillna(0.0).sum())
    out = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
    if "CPL" in out.columns:
        out["CPL"] = out.apply(
            lambda r: (float(r["spend"]) / float(r["leads"])) if float(r.get("leads") or 0) > 0 else float("nan"),
            axis=1,
        )
    if "SQL%" in out.columns:
        out["SQL%"] = out.apply(
            lambda r: (float(r["qualified"]) / float(r["leads"]) * 100.0) if float(r.get("leads") or 0) > 0 else 0.0,
            axis=1,
        )
    return out


def _pmc_spend_exec_frame(by_ch: pd.DataFrame) -> pd.DataFrame:
    """Spend-first channel view with rank/share/MoM/flags used by Spend-by-Channel executive UI."""
    if by_ch.empty:
        return by_ch.copy()
    d = by_ch.copy()
    if "channel" not in d.columns and "unified_channel" in d.columns:
        _uc = d.loc[:, "unified_channel"]
        if isinstance(_uc, pd.DataFrame):
            _uc = _uc.iloc[:, 0]
        d["channel"] = _uc.astype(str)
    if "channel" in d.columns:
        _ch = d.loc[:, "channel"]
        if isinstance(_ch, pd.DataFrame):
            _ch = _ch.iloc[:, 0]
        d["channel"] = _ch.astype(str).str.strip()
    else:
        d["channel"] = "Other"
    for c in ("spend", "leads", "qualified", "cw", "mom_spend_delta"):
        d[c] = pd.to_numeric(d.get(c, 0), errors="coerce").fillna(0.0)
    d["share_pct"] = (d["spend"] / float(d["spend"].sum()) * 100.0) if float(d["spend"].sum()) > 0 else 0.0
    d["CPL"] = d.apply(lambda r: (float(r["spend"]) / float(r["leads"])) if float(r["leads"]) > 0 else float("nan"), axis=1)
    d = d.sort_values(["spend", "qualified", "cw"], ascending=[False, False, False], kind="mergesort").reset_index(drop=True)
    d["rank"] = d.index + 1

    def _flag(r: pd.Series) -> str:
        sp = float(r.get("spend") or 0.0)
        q = float(r.get("qualified") or 0.0)
        cw = float(r.get("cw") or 0.0)
        if sp > 0 and q <= 0 and cw <= 0:
            return "No visible output"
        if sp > 0 and q <= 0:
            return "Low quality"
        if pd.notna(r.get("CPL")) and float(r.get("CPL") or 0.0) > 0 and float(r.get("CPL") or 0.0) > d["CPL"].replace([float("inf")], pd.NA).dropna().median() * 1.35:
            return "High CPL"
        return "Healthy"

    d["flag"] = d.apply(_flag, axis=1)
    return d


def _pmc_render_charts(by_ch: pd.DataFrame, key_suffix: str) -> None:
    """Spend-by-channel executive charts: horizontal spend ranking + spend vs qualified."""
    if by_ch.empty:
        st.caption("Not enough channel-level rows for charts.")
        return
    d = _pmc_spend_exec_frame(by_ch)
    if d.empty:
        st.caption("No spend rows for charts.")
        return
    _plot = dict(template="plotly_white", paper_bgcolor="white", plot_bgcolor="white", font=dict(size=12))
    _margin = dict(l=12, r=12, t=54, b=68)
    _legend = dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, font=dict(size=11))

    # 1) Horizontal spend bar ranking (descending).
    fig_sp = go.Figure()
    fig_sp.add_trace(
        go.Bar(
            y=d["channel"],
            x=d["spend"],
            orientation="h",
            marker=dict(color="#0d9488"),
            text=[f"${v:,.0f}" for v in d["spend"]],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>Spend: $%{x:,.0f}<extra></extra>",
            name="Spend",
        )
    )
    fig_sp.update_layout(
        title=dict(text="Spend ranking by channel", font=dict(size=14)),
        height=max(360, 60 + 42 * len(d)),
        margin=_margin,
        showlegend=False,
        **_plot,
    )
    fig_sp.update_xaxes(title="Spend ($)", tickprefix="$", tickformat=",.0f", showgrid=True, gridcolor="rgba(148,163,184,0.2)")
    fig_sp.update_yaxes(title="", autorange="reversed")
    st.plotly_chart(fig_sp, width="stretch", key=f"{key_suffix}_spend_rank_barh")

    # 2) Spend vs qualified leads (dual axis).
    fig_sq = make_subplots(specs=[[{"secondary_y": True}]])
    fig_sq.add_trace(
        go.Bar(
            x=d["channel"],
            y=d["spend"],
            name="Spend",
            marker_color="#0d9488",
            hovertemplate="<b>%{x}</b><br>Spend: $%{y:,.0f}<extra></extra>",
        ),
        secondary_y=False,
    )
    fig_sq.add_trace(
        go.Scatter(
            x=d["channel"],
            y=d["qualified"],
            mode="lines+markers",
            name="Qualified leads",
            line=dict(color="#6366f1", width=2.5),
            marker=dict(size=8),
            hovertemplate="<b>%{x}</b><br>Qualified: %{y:,.0f}<extra></extra>",
        ),
        secondary_y=True,
    )
    fig_sq.update_layout(
        title=dict(text="Spend vs qualified leads by channel", font=dict(size=14)),
        height=420,
        margin=_margin,
        legend=_legend,
        hovermode="x unified",
        **_plot,
    )
    fig_sq.update_xaxes(title="Channel", tickangle=-18, automargin=True, showgrid=True, gridcolor="rgba(148,163,184,0.25)")
    fig_sq.update_yaxes(title="Spend ($)", tickprefix="$", tickformat=",.0f", showgrid=True, gridcolor="rgba(148,163,184,0.2)", secondary_y=False)
    _q_hi = float(pd.to_numeric(d["qualified"], errors="coerce").fillna(0.0).max())
    fig_sq.update_yaxes(
        title="Qualified leads",
        tickformat=",.0f",
        showgrid=False,
        rangemode="tozero",
        range=[0, max(1.0, _q_hi * 1.08)] if _q_hi > 0 else [0, 1],
        secondary_y=True,
    )
    st.plotly_chart(fig_sq, width="stretch", key=f"{key_suffix}_spend_vs_qualified")


def _render_page_performance_marketing_channels(
    df_loaded: pd.DataFrame,
    df_scope: pd.DataFrame,
    start_date: date,
    end_date: date,
    key_suffix: str,
) -> None:
    """Channel spend from **March** of the reporting-start year onward; main grid is **Spend** only (+ charts)."""
    if df_scope.empty or "cost" not in df_scope.columns:
        st.info("No spend rows for the selected channels and months.")
        return
    _pmc_m0 = _pmc_floor_march_or_later(start_date)
    df_scope = _pmc_filter_month_not_before(df_scope, _pmc_m0)
    if df_scope.empty:
        st.info("No spend rows on or after March for the selected window.")
        return
    u = _pmc_frame_with_metrics(df_scope.copy())
    # Keep chart/intelligence channel bins identical to the month×channel master table.
    u["unified_channel"] = _pmc_sheet_channel_series(u)
    spend_g = _spend_sheet_pivot_by_month_channel(u)
    if spend_g.empty:
        st.info("No spend to aggregate for this scope (check **month** and **channel** filters).")
        return
    leads_lut = _pmc_leads_channel_lut_from_leads_sheet(df_loaded, df_scope)
    by_ch = _pmc_by_channel_summary(u, leads_lut=leads_lut)
    chart_base, _magic_bullets = _pmc_blended_channel_insights(df_loaded, df_scope, u, by_ch)
    d = _pmc_spend_exec_frame(chart_base if not chart_base.empty else by_ch)
    if d.empty:
        st.info("No channel-level spend rows to display.")
        return

    # KPI cards.
    total_spend = float(d["spend"].sum())
    active_channels = int((d["spend"] > 0).sum())
    top_row = d.iloc[0]
    top_channel = str(top_row["channel"])
    top_share = float(top_row["share_pct"])
    st.markdown(
        _pmc_spend_executive_scorecards_html(
            total_spend=total_spend,
            active_channels=active_channels,
            top_channel=top_channel,
            top_share_pct=top_share,
        ),
        unsafe_allow_html=True,
    )

    # Insight strip.
    _warn_conc = top_share >= 45.0
    _warn_no_out = bool((d["flag"] == "No visible output").any())
    _insight = (
        f"Top spend concentration: **{top_channel}** holds **{top_share:.1f}%** of channel spend."
        + (" Concentration risk is elevated." if _warn_conc else " Concentration is within a manageable range.")
        + (" Spend with no visible output exists; prioritize immediate review." if _warn_no_out else " No major zero-output spend pockets detected.")
    )
    st.markdown(
        f'<div style="padding:10px 12px;border:1px solid #e2e8f0;border-radius:10px;background:#f8fafc;'
        f'font-size:13px;color:#334155;margin:8px 0 12px 0;">{_insight}</div>',
        unsafe_allow_html=True,
    )

    # Ranked spend table.
    ranked = d[["rank", "channel", "spend", "share_pct", "mom_spend_delta", "flag"]].copy()
    ranked = ranked.rename(
        columns={
            "rank": "Rank",
            "channel": "Channel",
            "spend": "Spend",
            "share_pct": "Share %",
            "mom_spend_delta": "MoM delta",
            "flag": "Flag",
        }
    )
    ranked["Spend"] = ranked["Spend"].map(lambda x: f"${float(x):,.0f}")
    ranked["Share %"] = ranked["Share %"].map(lambda x: f"{float(x):.1f}%")
    ranked["MoM delta"] = ranked["MoM delta"].map(lambda x: f"${float(x):+,.0f}")
    st.markdown('<div class="looker-table-title">Ranked spend table</div>', unsafe_allow_html=True)
    st.dataframe(ranked, width="stretch", hide_index=True, key=f"{key_suffix}_spend_rank_tbl")

    # Simplified main table + advanced metrics in expander.
    st.markdown('<div class="looker-table-title" style="margin-top:10px;">Spend efficiency by channel</div>', unsafe_allow_html=True)
    show_cols = ["channel", "spend", "share_pct", "leads", "qualified", "cw", "CPL"]
    simple = d[show_cols].copy().rename(
        columns={
            "channel": "Channel",
            "spend": "Spend",
            "share_pct": "Share %",
            "leads": "Leads",
            "qualified": "Qualified",
            "cw": "Closed Won",
            "CPL": "CPL",
        }
    )
    simple["Spend"] = simple["Spend"].map(lambda x: f"${float(x):,.0f}")
    simple["Share %"] = simple["Share %"].map(lambda x: f"{float(x):.1f}%")
    simple["Leads"] = pd.to_numeric(simple["Leads"], errors="coerce").fillna(0).map(lambda x: f"{int(x):,}")
    simple["Qualified"] = pd.to_numeric(simple["Qualified"], errors="coerce").fillna(0).map(lambda x: f"{int(x):,}")
    simple["Closed Won"] = pd.to_numeric(simple["Closed Won"], errors="coerce").fillna(0).map(lambda x: f"{int(x):,}")
    simple["CPL"] = pd.to_numeric(simple["CPL"], errors="coerce").map(lambda x: f"${float(x):,.0f}" if pd.notna(x) else "—")

    major_n = min(8, len(simple))
    st.dataframe(simple.head(major_n), width="stretch", hide_index=True, key=f"{key_suffix}_spend_simple_tbl")
    if len(simple) > major_n:
        with st.expander("Minor channels (expand)", expanded=False):
            st.dataframe(simple.iloc[major_n:], width="stretch", hide_index=True, key=f"{key_suffix}_spend_minor_tbl")

    with st.expander("Advanced metrics", expanded=False):
        adv_cols = [c for c in ("channel", "spend", "share_pct", "mom_spend_delta", "flag", "qualified", "cw", "CPL") if c in d.columns]
        adv = d[adv_cols].copy().rename(
            columns={
                "channel": "Channel",
                "spend": "Spend",
                "share_pct": "Share %",
                "mom_spend_delta": "MoM delta",
                "flag": "Flag",
                "qualified": "Qualified",
                "cw": "Closed Won",
                "CPL": "CPL",
            }
        )
        st.dataframe(adv, width="stretch", hide_index=True, key=f"{key_suffix}_spend_adv_tbl")

    st.markdown('<div class="dash-chart-stack">', unsafe_allow_html=True)
    st.markdown('<div class="looker-table-title">Spend charts</div>', unsafe_allow_html=True)
    _pmc_render_charts(d, key_suffix=key_suffix)
    st.markdown('<div class="looker-table-title" style="margin-top:10px;">Spend watchouts</div>', unsafe_allow_html=True)
    _w: list[str] = []
    _w.append(f"{top_channel} carries {top_share:.1f}% of spend.")
    _drag = d.loc[d["flag"].eq("No visible output")]
    if not _drag.empty:
        _w.append(f"Output risk: {', '.join(_drag['channel'].astype(str).head(2).tolist())} show spend with no visible output.")
    _mom_down = d.sort_values("mom_spend_delta").iloc[0]
    _w.append(f"Biggest MoM spend pullback: {_mom_down['channel']} ({float(_mom_down['mom_spend_delta']):+,.0f}).")
    for w in _w[:3]:
        st.markdown(f"- {w}")
    st.markdown("</div>", unsafe_allow_html=True)


def render_page_channels(df_loaded: pd.DataFrame, start_date: date, end_date: date, *, inbound: bool) -> None:
    key_suffix = "inb" if inbound else "pmc"

    _dashboard_tab_page_header("Inbound channels" if inbound else None)
    if inbound:
        # Keep inbound source aligned with the same ME X-Ray spend source used by Spend-by-channel.
        inbound_base = _mpo_spend_sheet_for_channel_master(df_loaded, start_date, end_date)
        if inbound_base.empty:
            st.warning("No ME X-Ray spend rows found for inbound channels in the selected date range.")
            return
        st.caption("Spend & efficiency by source — filters below apply to this tab.")
        df, _ = _apply_sheet_filters(inbound_base, key_suffix=key_suffix, filters_in_row=True)
    else:
        # Spend pivots are by **month**; ``date`` alone often drops sheet rows (same fix as ``_filter_spend_for_dashboard`` elsewhere).
        df_date = _filter_spend_for_dashboard(df_loaded, start_date, end_date)
        if df_date.empty:
            st.info("No rows in the selected date range.")
            return
        spend_base = _mpo_spend_sheet_for_channel_master(df_loaded, start_date, end_date)
        if spend_base.empty:
            st.warning(
                "Could not resolve spend rows from the primary workbook — check sheet load and **spreadsheet_id** tagging."
            )
            return
        df_scope, _ = _apply_channel_tab_data_scope(
            spend_base,
            key_suffix=key_suffix,
            reporting_start=start_date,
            reporting_end=end_date,
        )
        _render_page_performance_marketing_channels(df_loaded, df_scope, start_date, end_date, key_suffix)
        return

    group_col = "utm_source"
    if group_col not in df.columns:
        st.warning(f"Column `{group_col}` missing; showing channel breakdown instead.")
        group_col = "channel"
    agg = (
        df.groupby(group_col, as_index=False)
        .agg(spend=("cost", "sum"), clicks=("clicks", "sum"), cw=("closed_won", "sum"))
        .sort_values("spend", ascending=False)
    )
    grp_leads: list[int] = []
    for k in agg[group_col].tolist():
        gk = df[df[group_col] == k]
        grp_leads.append(_lead_rows_count(gk))
    agg["leads"] = grp_leads
    agg["CPL"] = agg.apply(lambda r: (r["spend"] / r["leads"]) if r["leads"] else float("nan"), axis=1)

    st.markdown('<div class="dash-master-surface">', unsafe_allow_html=True)
    st.markdown('<div class="looker-table-title">Channel master view</div>', unsafe_allow_html=True)
    st.dataframe(agg, width="stretch", hide_index=True, key=f"{key_suffix}_df_ch")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="dash-chart-stack">', unsafe_allow_html=True)
    st.markdown('<div class="looker-table-title">Channel trends</div>', unsafe_allow_html=True)
    m1, m2 = st.columns(2)
    with m1:
        fig = px.bar(agg.head(20), x=group_col, y="spend", title="Spend by source")
        fig.update_traces(marker_color="#4f8483")
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
        st.plotly_chart(fig, width="stretch", key=f"{key_suffix}_pl_spend")
    with m2:
        trend = (
            df.groupby(["month", group_col], as_index=False)
            .agg(spend=("cost", "sum"))
            .sort_values(["month", group_col])
        )
        top = trend.groupby(group_col)["spend"].sum().nlargest(5).index.tolist()
        trend = trend[trend[group_col].isin(top)]
        fig2 = px.line(trend, x="month", y="spend", color=group_col, markers=True, title="Spend trend (top groups)")
        fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=8, r=8, t=45, b=8))
        st.plotly_chart(fig2, width="stretch", key=f"{key_suffix}_pl_trend")
    st.markdown("</div>", unsafe_allow_html=True)


def _extras_skip_tabs_already_loaded(df_loaded: pd.DataFrame, extras: list[pd.DataFrame]) -> list[pd.DataFrame]:
    """Avoid duplicate rows when `load_all_worksheets_combined` already contains the same tab titles."""
    if df_loaded.empty or "source_tab" not in df_loaded.columns:
        return extras
    existing = set(df_loaded["source_tab"].dropna().astype(str).str.strip().unique())
    out: list[pd.DataFrame] = []
    for extra in extras:
        if extra.empty:
            continue
        if "source_tab" not in extra.columns:
            out.append(extra)
            continue
        tabs = set(extra["source_tab"].dropna().astype(str).str.strip().unique())
        if tabs & existing:
            continue
        out.append(extra)
    return out


_DASH_NAV_OPTIONS = [
    "Marketing performance",
    "Market MoM",
    "Spend by channel",
]


def render_main_dashboard(
    start_date: date,
    end_date: date,
) -> None:
    """Load Google Sheets workbook (all tabs), then route to report pages."""
    # Keep the old top-tab UX while preserving lazy execution (only one page branch runs).
    _nav = st.radio(
        "Dashboard section",
        _DASH_NAV_OPTIONS,
        horizontal=True,
        key="dash_main_nav",
        label_visibility="collapsed",
    )
    sheet_id, _ = _workbook_id_resolution()
    _fp = _secret_fingerprint(_service_account_from_streamlit_secrets())
    truth_gid = _default_truth_gid_from_secrets()
    ads_id = _optional_paid_media_sheet_id_from_secrets()
    include_ads_workbook = _nav == "Spend by channel"
    _load_banner = st.empty()
    _load_banner.info("**Loading…**")
    load_error: Optional[str] = None
    df_loaded = pd.DataFrame()
    gid0_sum = 0.0
    try:
        try:
            df_loaded = load_source_of_truth_tab(sheet_id, int(truth_gid), _fp)
        except Exception:
            df_loaded = pd.DataFrame()
        if df_loaded.empty:
            df_loaded = load_all_worksheets_combined(sheet_id, _fp)
        # If truth tab loaded first, still ensure core tabs are present for accurate leads/pipeline cards.
        if not df_loaded.empty and "worksheet_gid" in df_loaded.columns:
            _core_gids: list[int] = [_default_leads_gid_from_secrets()]
            for _g in (_optional_post_qual_gid_from_secrets(), _optional_raw_cw_gid_from_secrets(), _optional_cw_source_truth_gid_from_secrets()):
                if _g is not None:
                    _core_gids.append(int(_g))
            _core_gids = list(dict.fromkeys([int(x) for x in _core_gids if x is not None]))
            _wg = pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce")
            _missing_gids = [g for g in _core_gids if not bool((_wg == int(g)).any())]
            if _missing_gids:
                _parts: list[pd.DataFrame] = []
                for _gid in _missing_gids:
                    try:
                        _sub = load_worksheet_by_gid_preprocessed(sheet_id, int(_gid), _fp)
                    except Exception:
                        _sub = pd.DataFrame()
                    if not _sub.empty:
                        _parts.append(_sub)
                if _parts:
                    df_loaded = pd.concat([df_loaded] + _parts, ignore_index=True)
        needs_spend_inject = True
        if not df_loaded.empty and "source_tab" in df_loaded.columns and "cost" in df_loaded.columns:
            sl = df_loaded["source_tab"].astype(str).str.strip().str.lower()
            spend_like = sl.str.contains(
                r"^(?:raw\s*)?spend$|raw\s*spend|sum\s*spend|media\s*spend", na=False, regex=True
            )
            spend_rows = df_loaded.loc[spend_like]
            if not spend_rows.empty and float(pd.to_numeric(spend_rows["cost"], errors="coerce").fillna(0).sum()) > 0:
                needs_spend_inject = False
        if needs_spend_inject:
            spend_norm = pd.DataFrame()
            try:
                spend_norm = load_named_worksheet_normalized(sheet_id, "Spend", _fp)
            except Exception:
                spend_norm = pd.DataFrame()
            if spend_norm.empty:
                spend_norm = load_spend_worksheet_fallback(sheet_id, _fp)
            if not spend_norm.empty:
                if df_loaded.empty:
                    df_loaded = spend_norm
                else:
                    df_loaded = pd.concat([df_loaded, spend_norm], ignore_index=True)
        spend_gid0 = load_spend_gid0_normalized(sheet_id, _fp)
        gid0_sum = float(load_spend_gid0_raw_sum(sheet_id, _fp))
        if not spend_gid0.empty:
            if not df_loaded.empty and "source_tab" in df_loaded.columns:
                _rm_syn = df_loaded["source_tab"].astype(str).str.match(r"^gid:\d+_spend$", na=False)
                df_loaded = df_loaded.loc[~_rm_syn].copy()
            if df_loaded.empty:
                df_loaded = spend_gid0
            else:
                df_loaded = pd.concat([df_loaded, spend_gid0], ignore_index=True)

        _ws_meta = list_worksheet_meta(sheet_id, _fp)
        spend_named = _load_first_matching_worksheet_from_meta(
            sheet_id, (r"^spend$", r"raw\s*spend", r"sum\s*spend"), _fp, _ws_meta
        )
        leads_named = _load_first_matching_worksheet_from_meta(
            sheet_id, (r"^leads?$", r"raw\s*leads?"), _fp, _ws_meta
        )
        post_named = _load_first_matching_worksheet_from_meta(
            sheet_id,
            (r"post\s*leads?", r"raw.*post.*qual", r"post\s+qual", r"post.*qualif"),
            _fp,
            _ws_meta,
        )
        cw_named = _load_first_matching_worksheet_from_meta(
            sheet_id,
            tuple(_RAW_CW_TAB_PATTERNS),
            _fp,
            _ws_meta,
        )
        extras = [x for x in (spend_named, leads_named, post_named, cw_named) if not x.empty]
        extras = _extras_skip_tabs_already_loaded(df_loaded, extras)
        if extras:
            if df_loaded.empty:
                df_loaded = pd.concat(extras, ignore_index=True)
            else:
                df_loaded = pd.concat([df_loaded] + extras, ignore_index=True)
        if not df_loaded.empty:
            df_loaded = _dataframe_with_spreadsheet_id(df_loaded, sheet_id)
        if include_ads_workbook and ads_id and ads_id != sheet_id:
            df_ads = load_all_worksheets_combined(ads_id, _fp)
            df_ads_gid = _load_paid_media_platform_tabs_by_gid(ads_id, _fp)
            if not df_ads_gid.empty:
                if df_ads.empty:
                    df_ads = df_ads_gid
                elif "worksheet_gid" in df_ads.columns:
                    wg_ads = pd.to_numeric(df_ads["worksheet_gid"], errors="coerce")
                    keep = ~wg_ads.isin(list(DEFAULT_PAID_MEDIA_PLATFORM_GIDS))
                    df_ads = pd.concat([df_ads.loc[keep].copy(), df_ads_gid], ignore_index=True)
                else:
                    df_ads = pd.concat([df_ads, df_ads_gid], ignore_index=True)
            df_ads = _dataframe_with_spreadsheet_id(df_ads, ads_id)
            if not df_ads.empty:
                if df_loaded.empty:
                    df_loaded = df_ads
                else:
                    df_loaded = pd.concat([df_loaded, df_ads], ignore_index=True)
    except Exception as exc:
        load_error = str(exc)
    finally:
        try:
            _load_banner.empty()
        except Exception:
            pass

    st.session_state["_gid0_spend_sum"] = float(gid0_sum or 0.0)

    if not load_error and not df_loaded.empty:
        df_loaded = _enforce_global_reporting_floor(df_loaded)
        df_loaded = _apply_sep2025_all_sheets_except_leads_postlead(df_loaded)

    _no_data_msg = (
        "No data rows were returned. Check tabs and column headers against the ME X-Ray template."
    )

    if load_error:
        st.error(f"Failed to load spreadsheet: {load_error}")
        with st.expander("Load error details", expanded=False):
            st.exception(RuntimeError(load_error))
        return
    if df_loaded.empty:
        st.warning(_no_data_msg)
        return

    if _nav == "Marketing performance":
        render_page_marketing_performance(df_loaded, start_date, end_date)
    elif _nav == "Market MoM":
        render_page_market_mom(df_loaded, start_date, end_date)
    else:
        render_page_channels(df_loaded, start_date, end_date, inbound=False)

    # AI widget temporarily disabled per request.
    # _render_xray_floating_ask_ai(df_loaded, start_date, end_date)


def main() -> None:
    st.set_page_config(
        page_title="KitchenPark Marketing Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(
        """
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
    /* Do not set a tiny font-size on .stApp — it made almost all UI unreadable (~8px). */
    .stApp {
        background: linear-gradient(165deg, #f1f5f9 0%, #eef2f7 45%, #f8fafc 100%);
        font-family: 'Inter', ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif;
    }
    section[data-testid="stSidebar"] { display: none !important; }
    [data-testid="collapsedControl"] { display: none !important; }
    header[data-testid="stHeader"] { background: #FFFFFF !important; border-bottom: 1px solid #E2E8F0; }
    header[data-testid="stHeader"] * { color: #1E293B !important; }
    .looker-header {
        background: transparent;
        border: none;
        border-radius: 0;
        padding: 4px 8px;
        margin: -1rem -1rem 6px -1rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 6px;
        color: #1f2937;
    }
    .looker-header-title {
        font-size: 1.28rem;
        font-weight: 700;
        letter-spacing: -0.03em;
        color: #0f172a;
        margin: 0;
        line-height: 1.15;
    }
    .looker-header-logo {
        width: auto; height: 40px; object-fit: contain;
        margin-right: 6px; vertical-align: middle; border: none;
        background: transparent; border-radius: 0;
    }
    .live-pill {
        background: #ecfdf3;
        color: #16a34a;
        border: 1px solid #bbf7d0;
        border-radius: 999px;
        padding: 2px 8px;
        font-size: 10px;
        font-weight: 600;
        line-height: 1;
    }
    .refresh-note { color: #6b7280; font-size: 10px; }
    .deploy-build {
        display: inline-block;
        flex-shrink: 0;
        white-space: nowrap;
        font-size: 12px;
        font-weight: 600;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        color: #0f172a;
        background: linear-gradient(180deg, #f1f5f9 0%, #e2e8f0 100%);
        padding: 4px 10px;
        border-radius: 8px;
        margin-left: 4px;
        border: 2px solid #4f8483;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06);
    }
    .stButton > button {
        border: 1px solid #b7d9d5;
        border-radius: 999px;
        background: #eef8f7;
        color: #19766f;
        font-size: 12px;
        width: 28px;
        min-width: 28px;
        height: 28px;
        padding: 0;
        line-height: 1;
    }
    .stButton > button:hover { border-color: #4f8483; color: #0f766e; }
    .looker-page-h1 {
        font-size: 1.5rem;
        font-weight: 700;
        letter-spacing: -0.035em;
        color: #0f172a;
        margin: 4px 0 14px 0;
        line-height: 1.2;
    }
    .looker-table-title { font-size: 1.0rem; font-weight: 700; color: #202124; margin: 20px 0 8px 0; }
    .dash-tab-head-cluster {
        margin: 0 0 2px 0;
        padding: 0;
    }
    .dash-tab-kicker-wrap {
        margin: 0 0 4px 0;
        line-height: 1.2;
    }
    /* Compact “rubber stamp” kicker */
    .dash-tab-kicker-stamp {
        display: inline-block;
        font-size: 0.5rem;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        color: #5c6b82;
        padding: 3px 9px 4px;
        border: 1px dashed rgba(71, 85, 105, 0.42);
        border-radius: 3px;
        background: linear-gradient(165deg, #f8fafc 0%, #eef2f7 55%, #e8edf3 100%);
        box-shadow:
            0 1px 0 rgba(255, 255, 255, 0.85) inset,
            0 1px 2px rgba(15, 23, 42, 0.06);
        transform: rotate(-0.35deg);
        user-select: none;
    }
    .dash-tab-kicker-sep {
        opacity: 0.65;
        font-weight: 600;
    }
    .dash-tab-kicker-revops {
        font-size: 0.85em;
        font-weight: 600;
        letter-spacing: 0.14em;
        color: #6b7b92;
    }
    .dash-tab-heading {
        font-size: 1.22rem;
        font-weight: 800;
        letter-spacing: -0.035em;
        color: #0f172a;
        margin: 0 0 2px 0;
        line-height: 1.2;
    }
    .dash-master-surface {
        margin: 0 0 14px 0;
        padding: 12px 14px 14px;
        border-radius: 14px;
        border: 1px solid rgba(15, 23, 42, 0.08);
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.98) 0%, rgba(248, 250, 252, 0.95) 100%);
        box-shadow: 0 4px 20px rgba(15, 23, 42, 0.06);
    }
    .dash-master-surface .looker-table-title:first-child { margin-top: 0 !important; }
    .dash-kpi-band { padding-bottom: 16px; }
    .dash-chart-stack {
        margin: 0 0 8px 0;
        padding: 4px 0 0 0;
    }
    .dash-chart-stack .looker-table-title { margin-top: 4px !important; }
    .mom-page-wrap {
        margin: 2px 0 0 0;
    }
    /* KPI scorecards: glass panels, staggered entrance, hover lift */
    .kpi-section {
        min-width: 0;
        overflow: visible;
        background: linear-gradient(155deg, rgba(255,255,255,0.92) 0%, rgba(248,250,252,0.88) 100%);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border-radius: 16px;
        padding: 14px 14px 16px;
        border: 1px solid rgba(226, 232, 240, 0.95);
        box-shadow: 0 4px 24px rgba(15, 23, 42, 0.06);
        transition: box-shadow 0.35s ease, transform 0.35s ease;
    }
    .kpi-section:hover {
        box-shadow: 0 14px 44px rgba(79, 132, 131, 0.11);
        transform: translateY(-1px);
    }
    .kpi-section-head {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 12px;
        padding-bottom: 10px;
        border-bottom: 1px solid rgba(226, 232, 240, 0.95);
    }
    .kpi-section-marker {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        flex-shrink: 0;
        box-shadow: 0 0 0 3px rgba(79, 132, 131, 0.18);
        animation: kpi-marker-pulse 2.4s ease-in-out infinite;
    }
    .kpi-section--cw .kpi-section-marker { background: linear-gradient(135deg, #0d9488, #4f8483); }
    .kpi-section--leads .kpi-section-marker { background: linear-gradient(135deg, #2563eb, #38bdf8); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.2); }
    .kpi-section--pipe .kpi-section-marker { background: linear-gradient(135deg, #7c3aed, #a78bfa); box-shadow: 0 0 0 3px rgba(124, 58, 237, 0.2); }
    .kpi-section-head h3 {
        margin: 0;
        font-size: 0.92rem;
        font-weight: 700;
        color: #0f172a;
        letter-spacing: -0.02em;
    }
    .kpi-card-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
        overflow: visible;
    }
    @media (max-width: 640px) {
        .kpi-card-grid { grid-template-columns: 1fr; }
    }
    .kpi-card {
        position: relative;
        overflow: visible;
        border-radius: 12px;
        padding: 11px 12px 13px;
        background: linear-gradient(160deg, #ffffff 0%, #f8fafc 55%, #f1f5f9 100%);
        border: 1px solid #e2e8f0;
        box-shadow: 0 2px 10px rgba(15, 23, 42, 0.045);
        transition: transform 0.28s cubic-bezier(0.22, 1, 0.36, 1), box-shadow 0.28s ease, border-color 0.28s ease;
        animation: kpi-card-enter 0.6s cubic-bezier(0.22, 1, 0.36, 1) backwards;
        isolation: isolate;
        cursor: default;
    }
    .kpi-card::before {
        content: "";
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        border-radius: 12px 12px 0 0;
        opacity: 0.9;
    }
    .kpi-section--cw .kpi-card::before { background: linear-gradient(90deg, #0d9488, #4f8483); }
    .kpi-section--leads .kpi-card::before { background: linear-gradient(90deg, #2563eb, #38bdf8); }
    .kpi-section--pipe .kpi-card::before { background: linear-gradient(90deg, #7c3aed, #c4b5fd); }
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 32px rgba(15, 23, 42, 0.12);
        border-color: #cbd5e1;
    }
    /* Crisp hover popover (CSS only; text from data-kpi-tip) */
    .kpi-card[data-kpi-tip]::after {
        content: attr(data-kpi-tip);
        position: absolute;
        left: 50%;
        bottom: calc(100% + 10px);
        transform: translate3d(-50%, 6px, 0);
        min-width: 168px;
        max-width: min(280px, 78vw);
        padding: 10px 12px 11px;
        background: #ffffff;
        color: #1e293b;
        font-size: 11px;
        font-weight: 500;
        line-height: 1.5;
        text-align: left;
        text-transform: none;
        letter-spacing: 0.01em;
        white-space: normal;
        word-wrap: break-word;
        border-radius: 8px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 12px 40px rgba(15, 23, 42, 0.16), 0 0 0 1px rgba(15, 23, 42, 0.04);
        opacity: 0;
        visibility: hidden;
        transition: opacity 0.12s ease, transform 0.12s ease, visibility 0.12s;
        transition-delay: 0.02s;
        z-index: 1000;
        pointer-events: none;
    }
    .kpi-section--cw .kpi-card[data-kpi-tip]::after {
        border-top: 3px solid #4f8483;
    }
    .kpi-section--leads .kpi-card[data-kpi-tip]::after {
        border-top: 3px solid #2563eb;
    }
    .kpi-section--pipe .kpi-card[data-kpi-tip]::after {
        border-top: 3px solid #7c3aed;
    }
    .kpi-card[data-kpi-tip]:hover::after {
        opacity: 1;
        visibility: visible;
        transform: translate3d(-50%, 0, 0);
        transition-delay: 0s;
    }
    @media (max-width: 520px) {
        .kpi-card[data-kpi-tip]::after {
            left: 0;
            right: 0;
            transform: translate3d(0, 6px, 0);
            max-width: none;
            width: 100%;
        }
        .kpi-card[data-kpi-tip]:hover::after {
            transform: translate3d(0, 0, 0);
        }
    }
    .kpi-card-label {
        font-size: 10px;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.055em;
        line-height: 1.35;
        margin-bottom: 6px;
    }
    .kpi-card-value {
        font-size: clamp(1.02rem, 2.2vw, 1.42rem);
        font-weight: 700;
        color: #0f172a;
        letter-spacing: -0.03em;
        line-height: 1.2;
        font-variant-numeric: tabular-nums;
    }
    /* MPO filter panel — executive controls shell */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%) !important;
        border: 1px solid rgba(15, 23, 42, 0.09) !important;
        border-radius: 14px !important;
        box-shadow:
            0 1px 0 rgba(255, 255, 255, 0.9) inset,
            0 10px 30px -18px rgba(15, 23, 42, 0.16) !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlock"] {
        gap: 0.7rem !important;
        padding: 6px 4px 8px 4px !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] label,
    div[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stWidgetLabel"] p {
        font-size: 0.72rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.04em !important;
        text-transform: uppercase !important;
        color: #64748b !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] .stRadio [role="radiogroup"] {
        display: flex !important;
        flex-wrap: wrap !important;
        align-items: center !important;
        gap: 8px !important;
        padding: 6px !important;
        background: #f1f5f9 !important;
        border-radius: 12px !important;
        border: 1px solid rgba(15, 23, 42, 0.05) !important;
    }
    /* MPO comparison — single compact bar (replaces large dual cards + footer) */
    .mpo-cmp-wrap { margin: 0 0 10px 0; }
    .mpo-cmp-bar {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        justify-content: space-between;
        gap: 8px 14px;
        padding: 8px 12px;
        background: #ffffff;
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 10px;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
    }
    .mpo-cmp-dates {
        font-size: 0.9375rem;
        color: #0f172a;
        letter-spacing: -0.02em;
        font-variant-numeric: tabular-nums;
    }
    .mpo-cmp-dates strong { font-weight: 700; }
    .mpo-cmp-vs {
        margin: 0 8px;
        font-size: 0.7rem;
        font-weight: 600;
        color: #94a3b8;
        text-transform: lowercase;
    }
    .mpo-cmp-trail {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 8px;
        margin-left: auto;
    }
    .mpo-cmp-pill {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .mpo-cmp-pill--mom { background: #ecfdf5; color: #0f766e; border: 1px solid #99f6e4; }
    .mpo-cmp-pill--yoy { background: #ede9fe; color: #5b21b6; border: 1px solid #c4b5fd; }
    .mpo-cmp-panel-intro {
        font-size: 12px;
        color: #475569;
        line-height: 1.45;
        margin: 0 0 10px 0;
        padding: 9px 11px;
        background: #f8fafc;
        border-radius: 8px;
        border: 1px solid #e2e8f0;
    }
    .mpo-cmp-vs--muted { font-size: 0.75rem; font-weight: 500; color: #94a3b8; text-transform: none; margin-left: 6px; }
    .mpo-cmp-mkt { font-size: 11px; font-weight: 500; color: #94a3b8; }
    .mpo-cmp-empty {
        padding: 8px 12px;
        color: #64748b;
        font-size: 12px;
        line-height: 1.4;
        background: #f8fafc;
        border-radius: 8px;
        border: 1px dashed #e2e8f0;
        margin-bottom: 6px;
    }
    .mpo-cmp-bar--surface {
        background: linear-gradient(135deg, #ffffff 0%, #f8fafc 55%, #f1f5f9 100%) !important;
        border: 1px solid rgba(15, 23, 42, 0.09) !important;
        border-radius: 14px !important;
        box-shadow:
            0 1px 0 rgba(255, 255, 255, 0.9) inset,
            0 10px 28px -14px rgba(15, 23, 42, 0.18) !important;
        padding: 12px 16px 12px 22px !important;
        position: relative;
        overflow: hidden;
    }
    .mpo-cmp-bar--surface::before {
        content: "";
        position: absolute;
        left: 0;
        top: 0;
        bottom: 0;
        width: 4px;
        border-radius: 14px 0 0 14px;
        background: linear-gradient(180deg, #0d9488 0%, #4f8483 100%);
        opacity: 0.95;
    }
    .mpo-cmp-bar--surface > * { position: relative; z-index: 1; }
    .mpo-sec-head {
        display: flex;
        gap: 14px;
        align-items: stretch;
        margin: 6px 0 14px 0;
        padding: 14px 16px 16px;
        border-radius: 16px;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.95) 0%, rgba(248, 250, 252, 0.65) 100%);
        border: 1px solid rgba(15, 23, 42, 0.07);
        box-shadow: 0 4px 20px rgba(15, 23, 42, 0.05);
    }
    .mpo-sec-head-accent {
        width: 5px;
        border-radius: 999px;
        background: linear-gradient(180deg, #0d9488, #6366f1);
        flex-shrink: 0;
    }
    .mpo-sec-head-body { min-width: 0; }
    .mpo-sec-head-title {
        margin: 0 0 6px 0;
        font-size: 1.125rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        color: #0f172a;
    }
    .mpo-sec-head-desc {
        margin: 0;
        font-size: 0.8125rem;
        line-height: 1.55;
        color: #64748b;
        font-weight: 500;
    }
    .mpo-kpi-shell {
        margin-top: 0;
        padding: 4px 6px 3px;
        border-radius: 14px;
        background: linear-gradient(165deg, rgba(255, 255, 255, 0.65) 0%, rgba(248, 250, 252, 0.4) 100%);
        border: 1px solid rgba(15, 23, 42, 0.06);
        box-shadow: 0 16px 48px -28px rgba(15, 23, 42, 0.14);
    }
    [data-testid="column"] {
        min-width: 0 !important;
    }
    .mpo-perf-charts-wrap {
        margin: 6px 0 4px 0;
    }
    .mpo-perf-charts-page-title {
        margin-bottom: 2px !important;
    }
    .mpo-perf-chart-title {
        margin: 0 0 6px 0;
        font-size: 1.08rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        color: #0f172a;
        min-height: 1.75rem;
    }
    .mpo-perf-chart-control-slot {
        min-height: 44px;
        margin: 0 0 4px 0;
    }
    .streamlit-expanderHeader {
        border-radius: 10px !important;
        border-left: 3px solid #4f8483 !important;
        background: #f8fafc !important;
    }
    [data-testid="stExpanderDetails"] {
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 10px;
        background: rgba(255, 255, 255, 0.82);
        padding: 10px 10px 6px 10px;
    }
    .mpo-top-toolbar {
        margin: 0;
    }
    .mpo-toolbar-divider {
        width: 1px;
        height: 42px;
        margin: 4px auto 0 auto;
        background: linear-gradient(180deg, rgba(148, 163, 184, 0.08), rgba(71, 85, 105, 0.35), rgba(148, 163, 184, 0.08));
        border-radius: 999px;
    }
    .mpo-expander-anchor {
        display: flex;
        align-items: center;
        gap: 12px;
        margin: 10px 0 6px 0;
    }
    .mpo-expander-anchor-line {
        flex: 1 1 auto;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(15, 23, 42, 0.12), transparent);
    }
    .mpo-expander-anchor-txt {
        flex: 0 0 auto;
        font-size: 9px;
        font-weight: 800;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: #64748b;
    }
    /* MPO filter toolbar — compact bar, same glass feel as funnel scorecards */
    .mpo-toolbar-summary {
        display: flex;
        flex-wrap: wrap;
        align-items: baseline;
        gap: 6px 12px;
        margin: 6px 0 14px 0;
        padding: 10px 14px 11px;
        background: linear-gradient(160deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        box-shadow: 0 2px 12px rgba(15, 23, 42, 0.045);
        line-height: 1.45;
    }
    .mpo-toolbar-summary--muted {
        color: #64748b;
        font-size: 13px;
        font-weight: 500;
        line-height: 1.5;
    }
    .mpo-toolbar-summary--oneline {
        font-size: 13px;
        font-weight: 500;
        color: #334155;
        line-height: 1.55;
    }
    .mpo-toolbar-summary--oneline strong {
        color: #0f172a;
        font-weight: 600;
    }
    .mpo-toolbar-chip {
        flex: 0 0 auto;
        font-size: 9px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #64748b;
    }
    .mpo-toolbar-pair {
        font-size: 13px;
        font-weight: 700;
        color: #0f172a;
        font-variant-numeric: tabular-nums;
        letter-spacing: -0.02em;
    }
    .mpo-toolbar-vs {
        font-weight: 600;
        color: #94a3b8;
        padding: 0 4px;
    }
    .mpo-toolbar-rule {
        flex: 1 1 auto;
        font-size: 11px;
        font-weight: 600;
        color: #4f8483;
        text-align: right;
        min-width: 100px;
    }
    @media (max-width: 640px) {
        .mpo-toolbar-rule { text-align: left; width: 100%; }
    }
    /* Marketing funnel scorecards (Looker-style deltas + sub-metrics) */
    .kpi-funnel-wrap { display: flex; flex-direction: column; gap: 2px; }
    .kpi-funnel-section--traffic-hero { margin-bottom: 4px; }
    .kpi-funnel-grid--hero-3 {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 6px;
        margin: 0 0 2px 0;
    }
    @media (max-width: 900px) {
        .kpi-funnel-grid--hero-3 { grid-template-columns: 1fr; }
    }
    .kpi-funnel-card--hero {
        min-height: 102px;
        background: linear-gradient(180deg, #ffffff 0%, #eef1f5 100%) !important;
        border: 1px solid #e2e8f0 !important;
    }
    .kpi-funnel-card--hero .kpi-funnel-title {
        font-size: 11px;
        font-weight: 600;
        color: #718096 !important;
        letter-spacing: 0.01em;
        margin-right: 0;
    }
    .kpi-funnel-card--hero .kpi-funnel-icon {
        display: inline-block;
        font-size: 1.2rem;
        line-height: 1;
        margin-bottom: 3px;
    }
    .kpi-funnel-delta--pill-wrap {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 6px 8px;
        margin-bottom: 3px;
    }
    .kpi-funnel-delta-pill {
        display: inline-block;
        font-size: 11px;
        font-weight: 700;
        padding: 3px 8px;
        border-radius: 999px;
        line-height: 1.35;
        font-variant-numeric: tabular-nums;
    }
    .kpi-funnel-delta-pill--up { color: #137333; background: #e6f4ea; }
    .kpi-funnel-delta-pill--down { color: #c5221f; background: #fce8e6; }
    .kpi-funnel-delta-pill--flat { color: #64748b; background: #f1f5f9; }
    .kpi-funnel-delta-vs {
        font-size: 11px;
        font-weight: 500;
        color: #94a3b8;
    }
    .kpi-funnel-sub--hero {
        border-top: none !important;
        padding-top: 0 !important;
        margin-top: 0 !important;
        min-height: 0;
    }
    .kpi-funnel-section { margin-bottom: 3px; }
    .kpi-funnel-section-title {
        font-size: 0.88rem;
        font-weight: 700;
        margin: 8px 0 5px 0;
        padding-bottom: 4px;
        letter-spacing: -0.02em;
        border-bottom: 2px solid #cbd5e1;
    }
    .kpi-funnel-section-title--cw { color: #4a5568; border-bottom-color: #cbd5e1; }
    .kpi-funnel-section-title--leads { color: #4a5568; border-bottom-color: #cbd5e1; }
    .kpi-funnel-section-title--pipe { color: #4a5568; border-bottom-color: #cbd5e1; }
    .kpi-funnel-section:first-child .kpi-funnel-section-title { margin-top: 0; }
    .kpi-funnel-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 6px;
        margin: 0 0 3px 0;
    }
    @media (min-width: 1500px) {
        .kpi-funnel-grid { grid-template-columns: repeat(5, minmax(0, 1fr)); }
    }
    @media (max-width: 1200px) {
        .kpi-funnel-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 560px) {
        .kpi-funnel-grid { grid-template-columns: 1fr; }
    }
    .kpi-funnel-card {
        position: relative;
        border-radius: 14px;
        padding: 7px 9px 6px;
        min-height: 98px;
        transition: box-shadow 0.25s ease, transform 0.25s ease, border-color 0.25s ease;
        animation: kpi-funnel-enter 0.55s cubic-bezier(0.22, 1, 0.36, 1) backwards;
        background: linear-gradient(165deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e8edf2;
        box-shadow: 0 2px 14px rgba(15, 23, 42, 0.05);
    }
    /* Pastel metric tiles — default (overridden inside ``.kpi-funnel-wrap--pastel-scorecard``). */
    .kpi-funnel-card--pastel {
        border: none;
        box-shadow:
            0 1px 2px rgba(15, 23, 42, 0.04),
            0 8px 24px rgba(15, 23, 42, 0.06);
    }
    /*
     * Reference metric card (attached UI): one neutral surface, slate rail, soft lift — same for every tile.
     * ``biz-*`` classes remain on nodes for logic; visuals stay unified (no per-domain color).
     */
    .kpi-funnel-wrap--pastel-scorecard {
        --kpi-ref-bg: linear-gradient(180deg, #ffffff 0%, #eef1f5 100%);
        --kpi-ref-rail: #4a5568;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel {
        background: var(--kpi-ref-bg);
        border: 1px solid #e2e8f0;
        border-left: 6px solid var(--kpi-ref-rail);
        border-radius: 14px;
        box-shadow: 0 2px 8px rgba(15, 23, 42, 0.07), 0 1px 2px rgba(15, 23, 42, 0.04);
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel:hover {
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.1);
        transform: translateY(-1px);
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--hero {
        background: var(--kpi-ref-bg) !important;
        border: 1px solid #e2e8f0 !important;
        border-left: 6px solid var(--kpi-ref-rail) !important;
        border-radius: 14px !important;
        box-shadow: 0 2px 8px rgba(15, 23, 42, 0.07), 0 1px 2px rgba(15, 23, 42, 0.04) !important;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--hero:hover {
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.1) !important;
    }
    .kpi-funnel-card:hover {
        box-shadow: 0 10px 28px rgba(15, 23, 42, 0.1);
        border-color: #cbd5e1;
        transform: translateY(-2px);
    }
    .kpi-funnel-icon {
        display: inline-block;
        line-height: 1;
        margin-bottom: 2px;
        font-size: 1.1rem;
    }
    .kpi-funnel-card--pastel .kpi-funnel-icon {
        opacity: 0.88;
        font-size: 1.05rem;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel .kpi-funnel-icon {
        display: inline-block;
        opacity: 0.9;
    }
    .kpi-funnel-title {
        font-size: 9.5px;
        font-weight: 700;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.055em;
        margin: 0 0 3px 0;
        line-height: 1.25;
    }
    .kpi-funnel-card--pastel-cw .kpi-funnel-title { color: #0f5d4a; }
    .kpi-funnel-card--pastel-leads .kpi-funnel-title { color: #1e40af; }
    .kpi-funnel-card--pastel-pipe .kpi-funnel-title { color: #5b21b6; }
    .kpi-funnel-value {
        font-size: clamp(1.05rem, 2vw, 1.38rem);
        font-weight: 700;
        color: #0f172a;
        line-height: 1.12;
        margin-bottom: 2px;
        font-variant-numeric: tabular-nums;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-title {
        text-transform: none;
        letter-spacing: 0.01em;
        font-size: 11px;
        font-weight: 600;
        color: #718096 !important;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-cw .kpi-funnel-title,
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-leads .kpi-funnel-title,
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-pipe .kpi-funnel-title {
        color: #718096 !important;
    }
    .kpi-funnel-title-sub {
        font-size: 9.5px;
        font-weight: 500;
        color: #64748b;
        line-height: 1.35;
        margin: 0 0 5px 0;
        text-transform: none;
        letter-spacing: 0.01em;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-title-sub {
        font-size: 10px;
        color: #475569;
        margin-top: -1px;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-value {
        font-size: clamp(1.12rem, 2.15vw, 1.52rem);
        font-weight: 800;
        color: #0a0a0a;
        letter-spacing: -0.035em;
    }
    .kpi-funnel-delta { font-size: 11px; font-weight: 600; margin-bottom: 4px; line-height: 1.35; }
    .kpi-funnel-delta--up { color: #15803d; }
    .kpi-funnel-delta--down { color: #dc2626; }
    .kpi-funnel-delta--flat { color: #64748b; }
    .kpi-funnel-delta--na { color: #94a3b8; font-weight: 500; }
    .kpi-funnel-delta--off { display: none !important; height: 0 !important; margin: 0 !important; padding: 0 !important; overflow: hidden !important; }
    .kpi-funnel-sub {
        border-top: 1px solid #f1f5f9;
        padding-top: 5px;
        margin-top: 1px;
    }
    .kpi-funnel-card--pastel-cw .kpi-funnel-sub { border-top-color: rgba(15, 93, 74, 0.16); }
    .kpi-funnel-card--pastel-leads .kpi-funnel-sub { border-top-color: rgba(30, 64, 175, 0.14); }
    .kpi-funnel-card--pastel-pipe .kpi-funnel-sub { border-top-color: rgba(91, 33, 182, 0.14); }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel .kpi-funnel-sub { border-top-color: #e2e8f0; }
    .kpi-funnel-sub-row {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        gap: 6px;
        margin-top: 2px;
        font-size: 9.5px;
        line-height: 1.35;
    }
    .kpi-funnel-sub-lbl { color: #64748b; font-weight: 500; }
    .kpi-funnel-sub-val { font-weight: 600; color: #1e293b; font-variant-numeric: tabular-nums; }
    .kpi-funnel-card--pastel-cw .kpi-funnel-sub-lbl { color: #0d8060; }
    .kpi-funnel-card--pastel-cw .kpi-funnel-sub-val { color: #064e3b; }
    .kpi-funnel-card--pastel-leads .kpi-funnel-sub-lbl { color: #2563eb; }
    .kpi-funnel-card--pastel-leads .kpi-funnel-sub-val { color: #172554; }
    .kpi-funnel-card--pastel-pipe .kpi-funnel-sub-lbl { color: #6d28d9; }
    .kpi-funnel-card--pastel-pipe .kpi-funnel-sub-val { color: #4c1d95; }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-cw .kpi-funnel-sub-lbl,
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-leads .kpi-funnel-sub-lbl,
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-pipe .kpi-funnel-sub-lbl {
        color: #718096 !important;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-cw .kpi-funnel-sub-val,
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-leads .kpi-funnel-sub-val,
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-card--pastel-pipe .kpi-funnel-sub-val {
        color: #2d3748 !important;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-sub-row { font-size: 10px; }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-section-title {
        margin: 10px 0 6px 0;
        padding-bottom: 4px;
        border-bottom-width: 1px;
        font-size: 0.86rem;
    }
    .kpi-funnel-wrap--pastel-scorecard .kpi-funnel-section:first-child .kpi-funnel-section-title { margin-top: 0; }
    @keyframes kpi-funnel-enter {
        from { opacity: 0; transform: translateY(12px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @media (prefers-reduced-motion: reduce) {
        .kpi-funnel-card { animation: none; }
        .kpi-funnel-card:hover { transform: none; }
    }
    @keyframes kpi-card-enter {
        from { opacity: 0; transform: translateY(14px) scale(0.98); }
        to { opacity: 1; transform: translateY(0) scale(1); }
    }
    @keyframes kpi-marker-pulse {
        0%, 100% { transform: scale(1); opacity: 1; }
        50% { transform: scale(1.12); opacity: 0.88; }
    }
    @media (prefers-reduced-motion: reduce) {
        .kpi-card { animation: none; }
        .kpi-section-marker { animation: none; }
        .kpi-card:hover, .kpi-section:hover { transform: none; }
    }
    .looker-kpi-big {
        background: linear-gradient(180deg, #5c9090 0%, #4f8483 100%);
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
        border: 1px solid #4f8483;
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
        border-left: 4px solid #4f8483;
        box-shadow: 0 1px 3px rgba(0,0,0,.08);
        min-height: 92px;
    }
    .metric-title { font-size: 12px; color: #374151; margin-bottom: 8px; font-weight: 600; }
    .metric-value { font-size: 28px; font-weight: 700; line-height: 1.1; color: #111827; }
    .block-title { font-size: 18px; font-weight: 700; color: #0f172a; margin: 8px 0 4px 0; }
    .block-subtitle { font-size: 12px; color: #64748b; margin-bottom: 12px; }
    /* Tab strip: baseweb (older Streamlit) + native tablist (newer builds / cloud) */
    .stTabs [data-baseweb="tab-list"],
    [data-testid="stTabs"] [role="tablist"] {
        gap: 6px;
        background: rgba(255, 255, 255, 0.72);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        padding: 6px;
        border-radius: 999px;
        border: 1px solid rgba(15, 23, 42, 0.06);
        box-shadow: 0 2px 12px rgba(15, 23, 42, 0.04);
        overflow-x: auto !important;
        overflow-y: hidden !important;
        flex-wrap: nowrap !important;
    }
    [data-testid="stTabs"] {
        margin-top: 2px;
        margin-bottom: 4px;
    }
    /* Less air above the first block inside a tab (title + filters sit higher). */
    div[role="tabpanel"] > div {
        padding-top: 0.2rem !important;
    }
    div[role="tabpanel"] > div > [data-testid="stVerticalBlock"] {
        gap: 0.35rem !important;
    }
    section[data-testid="stMain"] .block-container {
        padding-top: 0.75rem !important;
        padding-bottom: 1.1rem !important;
    }
    .stTabs [data-baseweb="tab"],
    [data-testid="stTabs"] button[role="tab"] {
        padding: 9px 18px;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.8125rem;
        color: #64748b;
        flex-shrink: 0;
        letter-spacing: -0.01em;
    }
    .stTabs [aria-selected="true"],
    [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, #0d9488 0%, #0f766e 100%) !important;
        color: white !important;
        box-shadow: 0 2px 10px rgba(13, 148, 136, 0.35);
    }
    [data-testid="stMetric"] {
        background: #e9f3f8;
        border: 1px solid #d5e4ec;
        border-left: 3px solid #4f8483;
        border-radius: 8px;
        padding: 6px 10px;
    }
    [data-testid="stMetricLabel"] { font-size: 11px !important; color: #4b5563 !important; }
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; color: #1f2937 !important; }
    .stRadio [role="radiogroup"] { gap: 10px; }
    .stSelectbox > label, .stRadio > label, .stTextInput > label {
        font-size: 0.72rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.04em !important;
        text-transform: uppercase !important;
        color: #64748b !important;
    }
    .stTabs [aria-selected="true"] span { color: white !important; }
    /* Main section nav (replaces ``st.tabs``): pill strip aligned with tab styling above */
    .st-key-dash_main_nav [role="radiogroup"] {
        gap: 6px;
        background: rgba(255, 255, 255, 0.72);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        padding: 8px 10px;
        border-radius: 999px;
        border: 1px solid rgba(15, 23, 42, 0.06);
        box-shadow: 0 2px 12px rgba(15, 23, 42, 0.04);
        flex-wrap: nowrap !important;
        overflow-x: auto !important;
        overflow-y: hidden !important;
        width: 100%;
        margin: 4px 0 12px 0;
    }
    .st-key-dash_main_nav label {
        margin: 0 !important;
        padding: 11px 22px !important;
        border-radius: 999px !important;
        font-weight: 600 !important;
        font-size: 0.86rem !important;
        color: #64748b !important;
        letter-spacing: -0.01em;
        border: 1px solid transparent !important;
        cursor: pointer;
        min-height: 42px;
    }
    .st-key-dash_main_nav label > div:first-child {
        display: none !important;  /* hide radio circle so it looks like old tabs */
    }
    .st-key-dash_main_nav label > div:last-child {
        margin-left: 0 !important;
    }
    .st-key-dash_main_nav [data-testid="stMarkdownContainer"] p {
        margin: 0 !important;
        line-height: 1.15 !important;
    }
    .st-key-dash_main_nav label:hover {
        background: rgba(148, 163, 184, 0.14) !important;
    }
    .st-key-dash_main_nav label:has(input:checked) {
        background: linear-gradient(135deg, #0d9488 0%, #0f766e 100%) !important;
        color: #ffffff !important;
        box-shadow: 0 2px 10px rgba(13, 148, 136, 0.35);
        border-color: rgba(13, 148, 136, 0.35) !important;
    }
    .st-key-dash_main_nav label:has(input:checked) p,
    .st-key-dash_main_nav label:has(input:checked) span {
        color: #ffffff !important;
    }
    .streamlit-expanderHeader { background: #F8FAFC; border-radius: 8px; border-left: 4px solid #4f8483; }
    .stTextInput input, .stSelectbox > div, .stDateInput input {
        border-radius: 10px !important;
        background: #ffffff !important;
        border: 1px solid rgba(15, 23, 42, 0.1) !important;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04) !important;
    }
    .stDateInput { max-width: 240px; }
    .stDateInput label { font-size: 11px !important; color: #6b7280 !important; }
    /* Multiselect chips — calm teal */
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"],
    .stMultiSelect [data-baseweb="tag"] {
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%) !important;
        color: #111827 !important;
        border: 1px solid #cbd5e1 !important;
        border-radius: 999px !important;
        font-weight: 600 !important;
    }
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"] *,
    .stMultiSelect [data-baseweb="tag"] * {
        color: #111827 !important;
    }
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"] [role="button"] svg,
    .stMultiSelect [data-baseweb="tag"] [role="button"] svg {
        fill: #374151 !important;
        color: #374151 !important;
    }
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"] [role="button"] svg path,
    .stMultiSelect [data-baseweb="tag"] [role="button"] svg path {
        fill: #374151 !important;
        stroke: #374151 !important;
    }
    [data-testid="stWidgetLabelHelp"] { display: none !important; }
    .stDataFrame {
        border-radius: 12px;
        box-shadow: 0 2px 12px rgba(15, 23, 42, 0.07);
        border: 2px solid rgba(71, 85, 105, 0.42);
    }
    .mpo-modal-hero {
        margin: 0 0 14px 0;
        padding-bottom: 12px;
        border-bottom: 1px solid #e2e8f0;
    }
    .mpo-modal-hero-title {
        font-size: 1.2rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        color: #0f172a;
        margin: 0 0 4px 0;
    }
    .mpo-modal-hero-sub {
        font-size: 0.8125rem;
        color: #64748b;
        font-weight: 500;
        margin: 0 0 4px 0;
    }
    .mpo-modal-hero-market {
        font-size: 0.75rem;
        font-weight: 600;
        color: #0f766e;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .mpo-modal-card {
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 12px 14px;
        background: #fafafa;
        min-height: 88px;
    }
    .mpo-modal-card--primary {
        background: linear-gradient(165deg, #eff6ff 0%, #dbeafe 55%, #f8fafc 100%);
        border-color: #bfdbfe;
        box-shadow: 0 4px 14px rgba(37, 99, 235, 0.08);
    }
    .mpo-modal-card-label {
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #64748b;
        margin-bottom: 6px;
    }
    .mpo-modal-card-value {
        font-size: 1.45rem;
        font-weight: 800;
        color: #0f172a;
        font-variant-numeric: tabular-nums;
    }
    .mpo-modal-card-value-sm {
        font-size: 1.05rem;
        font-weight: 700;
        color: #0f172a;
    }
    .mpo-modal-related-title {
        font-size: 0.9rem;
        font-weight: 700;
        color: #334155;
        margin: 16px 0 8px 0;
    }
    .mpo-modal-related-tile {
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 10px 12px;
        background: #ffffff;
    }
    .mpo-modal-related-lbl {
        font-size: 10px;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
    }
    .mpo-modal-related-val {
        font-size: 1.1rem;
        font-weight: 700;
        color: #0f172a;
        font-variant-numeric: tabular-nums;
    }
    /* Dialog header dismiss (X) */
    [data-testid="stDialog"] [data-testid="stHeader"] button[aria-label="Close"],
    [data-testid="stDialog"] [data-testid="stHeader"] button {
        min-width: 44px !important;
        min-height: 44px !important;
        border-radius: 999px !important;
    }
    .mpo-detail-card {
        margin: 12px 0 4px 0;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        background: #ffffff;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
        padding: 14px;
    }
    .mpo-detail-title {
        font-size: 1.02rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        color: #0f172a;
        margin: 0 0 2px 0;
    }
    .mpo-detail-sub {
        font-size: 12px;
        font-weight: 600;
        color: #64748b;
        margin: 0 0 10px 0;
    }
    .mpo-detail-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
        margin-bottom: 10px;
    }
    .mpo-detail-kpi {
        background: linear-gradient(145deg, #f8fafc 0%, #f1f5f9 100%);
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 9px 10px;
    }
    .mpo-detail-kpi-lbl {
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #64748b;
    }
    .mpo-detail-kpi-val {
        margin-top: 4px;
        font-size: 1.12rem;
        font-weight: 800;
        color: #0f172a;
        font-variant-numeric: tabular-nums;
    }
    .mpo-detail-table-wrap {
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        overflow: hidden;
        margin-bottom: 10px;
    }
    .mpo-detail-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
    }
    .mpo-detail-table th, .mpo-detail-table td {
        padding: 8px 10px;
        border-bottom: 1px solid #f1f5f9;
        text-align: left;
    }
    .mpo-detail-table thead th {
        background: #f8fafc;
        color: #334155;
        font-weight: 700;
    }
    .mpo-detail-note {
        font-size: 12px;
        line-height: 1.5;
        color: #92400e;
        background: #fffbeb;
        border: 1px solid #fde68a;
        border-radius: 10px;
        padding: 10px 11px;
    }
    @media (max-width: 780px) {
        .mpo-detail-grid { grid-template-columns: 1fr; }
    }
    [data-testid="stMetricValue"] { color: #1E293B !important; }
    [data-testid="stMetricLabel"] { color: #64748B !important; }
    .stCaption { color: #64748B !important; }
    .stAlert { border-radius: 8px; border-left: 4px solid #4f8483; }
    /* Replace red-like status accents with app green palette */
    [data-testid="stAlert"] svg, [data-testid="stNotification"] svg { color: #4f8483 !important; fill: #4f8483 !important; }
    [data-baseweb="tag"][class*="danger"], [class*="danger"], [class*="error"] { color: #19766f !important; }
    /* Global Ask AI — fixed “ghost” FAB (all tabs); key matches st.button key=xray_ask_ai_fab */
    .st-key-xray_ask_ai_fab {
        position: fixed !important;
        bottom: 1.25rem !important;
        right: 1.25rem !important;
        z-index: 99999 !important;
        width: auto !important;
        pointer-events: auto !important;
    }
    .st-key-xray_ask_ai_fab button {
        border-radius: 999px !important;
        background: rgba(255,255,255,0.94) !important;
        border: 1px solid #e2e8f0 !important;
        box-shadow: 0 8px 28px rgba(15,23,42,0.12) !important;
        color: #334155 !important;
        font-weight: 600 !important;
        padding: 0.55rem 1.15rem !important;
        backdrop-filter: blur(6px);
    }
    .st-key-xray_ask_ai_fab button:hover {
        border-color: #cbd5e1 !important;
        box-shadow: 0 10px 32px rgba(15,23,42,0.16) !important;
    }
    .st-key-xray_ai_panel {
        position: fixed !important;
        right: 1rem !important;
        bottom: 1rem !important;
        width: min(44vw, 700px) !important;
        max-width: 700px !important;
        min-width: 420px !important;
        height: 86vh !important;
        z-index: 100000 !important;
        background: #ffffff !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 14px !important;
        box-shadow: 0 18px 44px rgba(15,23,42,0.24) !important;
        padding: 12px !important;
        overflow: hidden !important;
    }
    .xray-ai-panel-title {
        margin: -12px -12px 10px -12px;
        padding: 12px 14px;
        border-radius: 14px 14px 0 0;
        color: #ffffff;
        font-weight: 700;
        font-size: 28px;
        background: linear-gradient(90deg, #6366f1 0%, #7c3aed 55%, #8b5cf6 100%);
    }
    .xray-ai-icon-btn {
        text-align: center;
        font-size: 20px;
        color: #ffffff;
        background: rgba(124,58,237,0.92);
        border-radius: 8px;
        padding: 4px 0;
        line-height: 1.2;
    }
    .st-key-xray_ai_chat_box {
        background: #f8fafc !important;
        border: 1px solid #e2e8f0 !important;
    }
    @media (max-width: 980px) {
        .st-key-xray_ai_panel {
            right: 0.4rem !important;
            bottom: 0.4rem !important;
            left: 0.4rem !important;
            width: auto !important;
            min-width: 0 !important;
            height: 86vh !important;
        }
    }
    section.main .block-container {
        padding-bottom: 6rem !important;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    logo_uri = _logo_data_uri(_default_logo_path_from_secrets())
    logo_html = (
        f'<img class="looker-header-logo" src="{logo_uri}" alt="Logo" />'
        if logo_uri
        else '<span style="display:inline-block;width:38px;height:38px;border-radius:6px;background:#4f8483;margin-right:10px;"></span>'
    )
    refreshed_text = datetime.now().strftime("Refreshed %I:%M %p").lstrip("0")
    st.markdown('<div class="looker-header">', unsafe_allow_html=True)
    st.markdown(
        f"""
    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;row-gap:6px;">
      {logo_html}
      <h1 class="looker-header-title">KitchenPark Marketing Dashboard</h1>
      <span class="live-pill">● Live</span>
      <span class="refresh-note">{refreshed_text}</span>
      <span class="deploy-build" title="Deploy / cache bust string. If this does not match the latest commit, Streamlit Cloud may not have pulled GitHub yet — redeploy or reboot the app.">Build: {html.escape(DASHBOARD_BUILD)}</span>
    </div>
    """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    _start = date(2025, 1, 1)
    _end = date(2026, 12, 31)

    render_main_dashboard(_start, _end)


if __name__ == "__main__":
    main()

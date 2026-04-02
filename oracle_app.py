"""
Oracle-style ME dashboard scaffold backed by Google Sheets.

Run:
    streamlit run oracle_app.py
"""

from __future__ import annotations

import hashlib
import html
import io
import json
import os
import re
import base64
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd
import plotly.express as px
import streamlit as st

DEFAULT_SHEET_ID = "1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8"
DEFAULT_SOURCE_TRUTH_GID = 8109573
DEFAULT_LEADS_WORKSHEET_GID = 743065354
# Default empty on Streamlit Cloud; set `XRAY_EXCEL_PATH` in secrets or `XRAY_EXCEL_PATH_DEFAULT` locally.
DEFAULT_LOCAL_EXCEL_PATH = (os.environ.get("XRAY_EXCEL_PATH_DEFAULT") or "").strip()
DEFAULT_LOGO_PATH = (
    os.environ.get("XRAY_LOGO_PATH_DEFAULT")
    or str((Path(__file__).resolve().parent / "assets" / "logo.png"))
    or ""
).strip()


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


def _default_leads_gid_from_secrets() -> int:
    """Optional Streamlit secret XRAY_LEADS_GID overrides default leads tab gid."""
    try:
        s = st.secrets
        v = (s.get("XRAY_LEADS_GID") or s.get("xray_leads_gid") or "").strip()
        return int(v) if v else DEFAULT_LEADS_WORKSHEET_GID
    except Exception:
        return DEFAULT_LEADS_WORKSHEET_GID


def _optional_post_qual_gid_from_secrets() -> Optional[int]:
    """Optional Streamlit secret XRAY_POST_QUAL_GID: load this tab alone for pipeline KPIs (Total Live)."""
    try:
        s = st.secrets
        v = (s.get("XRAY_POST_QUAL_GID") or s.get("xray_post_qual_gid") or "").strip()
        return int(v) if v else None
    except Exception:
        return None


def _optional_raw_cw_gid_from_secrets() -> Optional[int]:
    """Optional Streamlit secret XRAY_RAW_CW_GID: TCV / 1st Month LF tab (same as Looker Actual TCV & CPCW:LF)."""
    try:
        s = st.secrets
        v = (s.get("XRAY_RAW_CW_GID") or s.get("xray_raw_cw_gid") or "").strip()
        return int(v) if v else None
    except Exception:
        return None


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


def _read_sheet_auth_loose(
    sheet_id: str,
    service_account_data: Union[bytes, dict, str],
    *,
    worksheet_gid: int,
) -> pd.DataFrame:
    """Fallback reader for tabs where get_all_records() fails due unusual header rows."""
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
    ws = sh.get_worksheet_by_id(int(worksheet_gid))
    grid = ws.get_all_values()
    if not grid:
        return pd.DataFrame()
    # Choose the first non-empty row as header.
    header_idx = None
    for i, row in enumerate(grid):
        if any(str(cell).strip() for cell in row):
            header_idx = i
            break
    if header_idx is None:
        return pd.DataFrame()
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
) -> list[list[str]]:
    """Raw cell grid from a worksheet (for header-row detection when ``get_all_records`` is wrong)."""
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
    ws = sh.get_worksheet_by_id(int(worksheet_gid))
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
    for c in df.columns:
        if _norm_header_key(c) == "post_lead_stage":
            return c
    best: list[tuple[int, str]] = []
    for c in df.columns:
        nk = _norm_header_key(c)
        if "post_lead" in nk and "stage" in nk:
            best.append((0, c))
        elif nk in ("stagename", "stage_name", "opportunity_stage"):
            best.append((1, c))
    if best:
        best.sort(key=lambda x: (x[0], x[1]))
        return best[0][1]
    for c in df.columns:
        if _norm_header_key(c) == "stage":
            return c
    return next((c for c in df.columns if "stage" in _norm_header_key(c)), None)


def _is_closed_won_stage_text(val: Any) -> bool:
    """Count rows in Closed Won, including formally approved; exclude Not Approved / Closed Lost."""
    t = str(val).lower().strip()
    if not t or t in ("nan", "none"):
        return False
    if "closed lost" in t:
        return False
    if "closed won" in t:
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

# RAW CW / TCV tab — Actual TCV, 1st Month LF, CPCW:LF (Looker: SUM(Spend)/SUM(1st Month LF) at scorecard level).
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
# Sort order within a month (normalized keys; UAE aliases unified for ordering).
_ME_MARKET_ORDER: tuple[str, ...] = (
    "bahrain",
    "kuwait",
    "saudi arabia",
    "united arab emirates",
    "oman",
    "qatar",
    "jordan",
    "lebanon",
    "iraq",
)


def _norm_market_key(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().lower())


_COUNTRY_JOIN_ALIASES: dict[str, str] = {
    "uae": "united arab emirates",
    "u.a.e": "united arab emirates",
    "u.a.e.": "united arab emirates",
    "the uae": "united arab emirates",
    "ksa": "saudi arabia",
    "kingdom of saudi arabia": "saudi arabia",
    "kingdom of saudi": "saudi arabia",
}


def _country_join_key(name: str) -> str:
    """Align Spend vs CRM market labels (e.g. UAE ↔ United Arab Emirates) for filtering."""
    k = _norm_market_key(name)
    if k in ("", "unknown", "nan", "<na>"):
        return k
    if k in _COUNTRY_JOIN_ALIASES:
        return _COUNTRY_JOIN_ALIASES[k]
    return k


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
    if k in _MARKET_DISPLAY_FROM_KEY:
        return _MARKET_DISPLAY_FROM_KEY[k]
    if not k or k in ("unknown", "nan", "<na>"):
        return "Unknown"
    return " ".join(w.capitalize() for w in k.split())


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
                d = pd.to_datetime(out["date"], errors="coerce")
                fill = d.dt.to_period("M").astype(str)
                mk = mk.where(~bad, fill)
        out["month"] = mk
    return out


def _month_norm_key(m: Any) -> str:
    """Canonical ``YYYY-MM`` period string for matching Spend rows to dashboard ``month``."""
    if m is None or (isinstance(m, float) and pd.isna(m)):
        return ""
    ms = str(m).strip()
    if not ms or ms.lower() in ("nat", "none", "nan"):
        return ""
    try:
        return str(pd.Period(str(m), freq="M"))
    except Exception:
        try:
            ts = pd.to_datetime(m, errors="coerce")
            if pd.isna(ts):
                return ms
            return str(ts.to_period("M"))
        except Exception:
            return ms


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


def _is_middle_east_market(name: str) -> bool:
    k = _norm_market_key(name)
    if k == "uae":
        k = "united arab emirates"
    return k in _MIDDLE_EAST_MARKET_KEYS


# Master View regional roll-up (first row under each month for ME markets).
_MIDDLE_EAST_REGION_LABEL = "Middle East"
_REGION_SUBTOTAL_NAMES = frozenset(
    {_MIDDLE_EAST_REGION_LABEL, "middle east", "mena"}
)


def _market_row_sort_key_mena(market: str) -> tuple:
    """Country rows only: Middle East countries in fixed order, then other markets A–Z."""
    m = str(market).strip()
    k = _norm_market_key(m)
    if k == "uae":
        k = "united arab emirates"
    if k in _MIDDLE_EAST_MARKET_KEYS:
        try:
            pos = _ME_MARKET_ORDER.index(k)
        except ValueError:
            pos = 40
        return (0, f"{pos:02d}", m)
    return (1, m.lower(), m)


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
    if (
        not df_dashboard.empty
        and not out.empty
        and "month" in out.columns
        and "country" in out.columns
        and "month" in df_dashboard.columns
        and "country" in df_dashboard.columns
    ):
        pairs = df_dashboard[["month", "country"]].drop_duplicates()
        if not pairs.empty:
            merged = out.merge(pairs, on=["month", "country"], how="inner")
            if not merged.empty:
                out = merged
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
    cw_bin = (cw > 0).astype(int)
    keys = _opp_key_columns_for_post_lead(df)
    if keys:
        tmp = df.loc[:, keys].copy()
        tmp["_cw"] = cw_bin
        return int(tmp.groupby(keys, dropna=False)["_cw"].max().sum())
    return int(cw_bin.sum())


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
    # Post-lead X-Ray / Salesforce export (binary 0/1 column)
    "is_cw": "closed_won",
    "utm_source_gp": "utm_source",
    "utm_source": "utm_source",
    "utm_source_l": "utm_source_l",
    "utm_source_o": "utm_source_o",
    "month": "report_month",
    "tcv": "tcv",
    "tcv_usd": "tcv",
    "tcv_converted": "tcv",
    "actual_tcv": "tcv",
    "actual_tcv_usd": "tcv",
    "1st_month_lf": "first_month_lf",
    "monthly_lf_usd": "first_month_lf",
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
    if ("raw" in t and "lead" in t and "post" not in t):
        # Convert lead rows into additive metrics so they can be combined with spend.
        if "Leads" not in df.columns:
            df["Leads"] = 1
        status = df.get("Lead Status", pd.Series(index=df.index, dtype=str)).astype(str).str.lower()
        if "Qualified" not in df.columns:
            df["Qualified"] = status.str.contains("qualified", na=False).astype(int)
        if "Date Formatted" in df.columns and "Date" not in df.columns:
            df["Date"] = pd.to_datetime(df["Date Formatted"], errors="coerce")
    if _is_post_lead_pipeline_tab(t):
        # Stage rows become pipeline counters (post-lead funnel). Prefer Post Lead Stage column.
        stage_col = _resolve_post_lead_stage_column(df)
        raw_stage = df.get(stage_col or "Stage", pd.Series(index=df.index, dtype=str))
        stage = raw_stage.astype(str).str.lower().str.strip()
        if "Qualified" not in df.columns:
            df["Qualified"] = 1
        if "Pitching" not in df.columns:
            df["Pitching"] = stage.str.contains("pitch", na=False).astype(int)
        if "Qualifying" not in df.columns:
            # Distinct from Qualified SQL / Disqualified — match Qualifying + Qualification (Salesforce labels).
            _qual_core = stage.str.contains("qualifying", na=False) | stage.str.contains(
                "qualification", na=False
            )
            df["Qualifying"] = (
                _qual_core
                & ~stage.str.contains("qualified", na=False)
                & ~stage.str.contains("disqualif", na=False)
            ).astype(int)
        _has_is_cw_col = any(_norm_header_key(c) == "is_cw" for c in df.columns)
        if "Closed Won" not in df.columns and not _has_is_cw_col:
            df["Closed Won"] = raw_stage.map(_is_closed_won_stage_text).astype(int)
        if "Negotiation" not in df.columns:
            df["Negotiation"] = stage.str.contains("negotiation", na=False).astype(int)
        if "Commitment" not in df.columns:
            df["Commitment"] = stage.str.contains("commitment", na=False).astype(int)
        if "Closed Lost" not in df.columns:
            df["Closed Lost"] = stage.str.contains("closed lost", na=False).astype(int)
        if "Total Live" not in df.columns:
            df["Total Live"] = stage.str.contains("new|working|qualifying|pitch|negotiation|commitment", na=False).astype(int)
        if "Date" not in df.columns:
            date_col = next(
                (
                    c
                    for c in df.columns
                    if _norm_header_key(c) in {"formatted_date", "created_date", "create_date", "date", "date_formatted"}
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

    # Global fallback: if any sheet has a Stage-like column, derive Closed Won (inc approved).
    # This prevents CW from dropping to zero when tab naming differs in source files.
    stage_col_any = _resolve_post_lead_stage_column(df)
    if stage_col_any is None:
        stage_col_any = next((c for c in df.columns if "stage" in _norm_header_key(c)), None)
    _has_is_cw_g = any(_norm_header_key(c) == "is_cw" for c in df.columns)
    if stage_col_any and "Closed Won" not in df.columns and not _has_is_cw_g:
        df["Closed Won"] = df[stage_col_any].map(_is_closed_won_stage_text).astype(int)
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
    # These often appear as two headers for the same measure (e.g. Actual TCV + TCV USD); summing doubles row TCV/LF.
    _DEDUPE_NUMERIC_MAX_FIELDS = frozenset({"tcv", "first_month_lf"})
    for field, srcs in field_to_sources.items():
        if len(srcs) == 1:
            out[field] = df[srcs[0]]
        elif field in _NUM_FIELDS:
            acc = _to_number_series(df[srcs[0]])
            for c in srcs[1:]:
                nxt = _to_number_series(df[c])
                if field == "closed_won":
                    acc = acc.combine(nxt, max)
                elif field in _DEDUPE_NUMERIC_MAX_FIELDS:
                    acc = acc.combine(nxt, max)
                else:
                    acc = acc + nxt
            out[field] = acc
        else:
            out[field] = df[srcs[0]]

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
                or nk in ("investment", "budget", "media_spend")
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
    # Keep undated rows so stage-based metrics (e.g., post-qualification CW) don't get zeroed.
    mask = ((s >= start_ts) & (s <= end_ts)) | s.isna()
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
            if raw.empty or len(raw.columns) == 0:
                raw = _read_sheet_auth_loose(
                    sheet_id,
                    secret_creds,
                    worksheet_gid=int(ws_gid),
                )
        except Exception:
            # If strict read fails, retry with loose grid reader before skipping the tab.
            try:
                raw = _read_sheet_auth_loose(
                    sheet_id,
                    secret_creds,
                    worksheet_gid=int(ws_gid),
                )
            except Exception:
                tab_stats.append((title, -1))
                continue
        raw = _preprocess_excel_sheet(raw, title)
        df = _normalize(raw)
        if df.empty:
            tab_stats.append((title, 0))
            continue
        df = df.copy()
        df["source_tab"] = (title.strip() if title.strip() else "Sheet")
        df["worksheet_gid"] = int(ws_gid)
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


def _tab_title_for_worksheet_gid(sheet_id: str, worksheet_gid: int, _secret_fp: str) -> str:
    try:
        for title, gid in list_worksheet_meta(sheet_id, _secret_fp):
            if int(gid) == int(worksheet_gid):
                return (title or "").strip() or "sheet"
    except Exception:
        pass
    return "post_qual"


@st.cache_data(ttl=300)
def load_worksheet_by_gid_preprocessed(sheet_id: str, worksheet_gid: int, _secret_fp: str) -> pd.DataFrame:
    """Read one tab by gid with the same preprocess + normalize path as ``load_all_worksheets_combined``."""
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
    title = _tab_title_for_worksheet_gid(sheet_id, worksheet_gid, _secret_fp)
    raw = _preprocess_excel_sheet(raw, title)
    out = _normalize(raw)
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
    out = _normalize(raw)
    out["source_tab"] = f"gid:{worksheet_gid}"
    return out


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
            grid = ws.get_all_values()
            raw = _dataframe_from_grid_with_keyword_header(grid, "spend")
        except Exception:
            return pd.DataFrame()
    raw = _preprocess_excel_sheet(raw, worksheet_name)
    out = _normalize(raw)
    if (out.empty or "cost" not in out.columns or float(out["cost"].sum()) == 0.0) and worksheet_name.strip().lower() == "spend":
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
            grid = ws.get_all_values()
            raw2 = _dataframe_from_grid_with_keyword_header(grid, "spend")
            if not raw2.empty:
                out = _normalize(_preprocess_excel_sheet(raw2, worksheet_name))
        except Exception:
            pass
    if out.empty:
        return out
    out["source_tab"] = worksheet_name
    return out


@st.cache_data(ttl=300)
def load_spend_worksheet_fallback(sheet_id: str, _secret_fp: str) -> pd.DataFrame:
    """Find a spend-like worksheet by title, load by gid, preprocess + normalize."""
    meta = list_worksheet_meta(sheet_id, _secret_fp)
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return pd.DataFrame()
    spend_candidates = [(t, gid) for t, gid in meta if "spend" in str(t).strip().lower()]
    if not spend_candidates:
        return pd.DataFrame()
    title, ws_gid = spend_candidates[0]
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
    raw = _preprocess_excel_sheet(raw, str(title))
    out = _normalize(raw)
    if out.empty:
        return out
    out["source_tab"] = str(title)
    return out


@st.cache_data(ttl=300)
def load_spend_gid0_normalized(sheet_id: str, _secret_fp: str) -> pd.DataFrame:
    """Spend from worksheet ``gid=0`` (first tab URL). Tries several parses and keeps the one with real ``cost``."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return pd.DataFrame()

    raws: list[pd.DataFrame] = []
    try:
        raw = _read_sheet_auth(
            sheet_id,
            secret_creds,
            worksheet_name=None,
            worksheet_gid=0,
        )
        if raw.empty or len(raw.columns) == 0:
            raw = pd.DataFrame()
        if not raw.empty:
            raws.append(raw)
    except Exception:
        pass
    try:
        loose = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=0)
        if not loose.empty:
            raws.append(loose)
    except Exception:
        pass
    try:
        grid = _read_sheet_grid_values(sheet_id, secret_creds, 0)
        for kw in ("spend", "cost_usd", "cost", "amount", "investment", "budget", "media"):
            gdf = _dataframe_from_grid_with_keyword_header(grid, kw)
            if not gdf.empty:
                raws.append(gdf)
    except Exception:
        pass

    best = pd.DataFrame()
    best_sum = 0.0
    for raw in raws:
        if raw.empty or len(raw.columns) == 0:
            continue
        try:
            cand = _normalize(_preprocess_excel_sheet(raw.copy(), "spend"))
            s = _normalized_spend_cost_sum(cand)
            if s > best_sum:
                best_sum = s
                best = cand
        except Exception:
            continue

    if best.empty:
        return pd.DataFrame()
    best["source_tab"] = "gid:0_spend"
    return best


@st.cache_data(ttl=300)
def load_spend_gid0_raw_sum(sheet_id: str, _secret_fp: str) -> float:
    """Direct raw Spend sum from gid=0: keyword-based grids first, then loose first-row header."""
    secret_creds = _service_account_from_streamlit_secrets()
    if not secret_creds:
        return 0.0

    def _scan_frame_for_spend_sum(raw: pd.DataFrame) -> float:
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

    best = 0.0
    try:
        grid = _read_sheet_grid_values(sheet_id, secret_creds, 0)
        for kw in ("spend", "cost_usd", "cost", "amount", "investment", "budget", "media"):
            df = _dataframe_from_grid_with_keyword_header(grid, kw)
            sm = _scan_frame_for_spend_sum(df)
            if abs(sm) > abs(best):
                best = sm
    except Exception:
        pass
    if best != 0.0:
        return best
    try:
        raw = _read_sheet_auth_loose(sheet_id, secret_creds, worksheet_gid=0)
    except Exception:
        return 0.0
    return _scan_frame_for_spend_sum(raw)


@st.cache_data(ttl=300)
def load_first_matching_worksheet_normalized(
    sheet_id: str,
    name_patterns: tuple[str, ...],
    _secret_fp: str,
) -> pd.DataFrame:
    """Load first worksheet whose title matches any regex pattern."""
    meta = list_worksheet_meta(sheet_id, _secret_fp)
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
    out["source_tab"] = str(title)
    out["worksheet_gid"] = int(ws_gid)
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


def _format_spend_k(v: float) -> str:
    """Spend in thousands with K (and M for very large totals)."""
    if v == 0:
        return "$0"
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    k = v / 1_000
    if abs(k) >= 1:
        return f"${k:.1f}K"
    return f"${k:.2f}K"


def _lead_rows_count(frame: pd.DataFrame) -> int:
    """Lead count = data-row count of the resolved leads slice (sheet rows, header excluded)."""
    return int(len(frame)) if isinstance(frame, pd.DataFrame) else 0


def _new_working_count_from_leads(frame: pd.DataFrame) -> int:
    """Count leads where Lead Status is exactly New or Working."""
    if frame.empty or "lead_status_text" not in frame.columns:
        return 0
    s = frame["lead_status_text"].astype(str).str.strip().str.lower()
    return int(s.isin({"new", "working"}).sum())


def _qualified_count_from_leads(frame: pd.DataFrame) -> int:
    """Count leads where Lead Status is exactly Qualified."""
    if frame.empty or "lead_status_text" not in frame.columns:
        return 0
    s = frame["lead_status_text"].astype(str).str.strip().str.lower()
    return int(s.eq("qualified").sum())


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
    """Per month: **Middle East** aggregate row first (ME countries only), then country rows."""
    if gm.empty:
        return gm
    parts: list[pd.DataFrame] = []

    def _month_sort_key(m: Any) -> Any:
        try:
            return pd.Period(str(m), freq="M")
        except Exception:
            return str(m)

    months_sorted = sorted(gm["month"].dropna().unique(), key=_month_sort_key, reverse=True)
    for month in months_sorted:
        grp = gm[gm["month"] == month].copy()
        grp = grp[~grp["Market"].astype(str).str.strip().isin(_REGION_SUBTOTAL_NAMES)]
        if grp.empty:
            continue
        grp["_sk"] = grp["Market"].map(_market_row_sort_key_mena)
        country_block = grp.sort_values("_sk").drop(columns="_sk")
        me_mask = country_block["Market"].map(_is_middle_east_market)
        me_slice = country_block[me_mask]
        blocks: list[pd.DataFrame] = []
        if not me_slice.empty:
            # ``spend`` is month×market cost from the Spend sheet only; Middle East = sum across ME markets.
            row: dict[str, Any] = {"month": month, "Market": _MIDDLE_EAST_REGION_LABEL}
            for c in gm.columns:
                if c in ("month", "Market"):
                    continue
                if c in me_slice.columns:
                    row[c] = float(pd.to_numeric(me_slice[c], errors="coerce").fillna(0).sum())
            mena_df = pd.DataFrame([row])
            for c in gm.columns:
                if c not in mena_df.columns:
                    mena_df[c] = float("nan")
            mena_df = mena_df[gm.columns]
            blocks.append(mena_df)
        blocks.append(country_block)
        parts.append(pd.concat(blocks, ignore_index=True))
    if not parts:
        return gm
    return pd.concat(parts, ignore_index=True)


def _master_view_style_css(df: pd.DataFrame) -> pd.DataFrame:
    """Looker-like fills: cyan inputs, white leads, R/G/Y ratios; bold Middle East region row."""
    css = pd.DataFrame("", index=df.index, columns=df.columns)
    is_region = df["Market"].astype(str).str.strip().str.lower().isin({"middle east", "mena"})
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
            return "background-color: #fee2e2; color: #991b1b;"
        if pd.isna(v) or v == 0.0:
            return "background-color: #fee2e2; color: #991b1b;"
        if v <= lo:
            return "background-color: #dcfce7; color: #166534;"
        if v <= hi:
            return "background-color: #fef9c3; color: #854d0e;"
        return "background-color: #fee2e2; color: #b91c1c;"

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
    for i in df.index:
        me = bool(is_region.loc[i])
        for col in df.columns:
            if col == "Unified Date":
                v = df.loc[i, col]
                if v == "" or (isinstance(v, str) and not str(v).strip()):
                    css.loc[i, col] = empty_cell
                else:
                    css.loc[i, col] = "background-color: #f1f5f9; font-weight: 600; color: #334155; border-bottom: 1px solid #e2e8f0;"
            elif col == "Market":
                css.loc[i, col] = (me_bold + " background-color: #ffffff; color: #0f172a;") if me else white
            elif col in cyan_cols:
                css.loc[i, col] = (cyan + me_bold) if me else cyan
            elif col == "Total Leads":
                css.loc[i, col] = (white + me_bold) if me else white
            elif col == "CPCW:LF":
                if me:
                    css.loc[i, col] = ratio_me
                else:
                    css.loc[i, col] = _rgy(df.loc[i, col], lf_lo, lf_hi)
            elif col == "Cost/TCV%":
                if me:
                    css.loc[i, col] = ratio_me
                else:
                    css.loc[i, col] = _rgy(df.loc[i, col], ct_lo, ct_hi)
            else:
                css.loc[i, col] = white
    return css


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


def _apply_marketing_performance_filters(
    df_date: pd.DataFrame,
    *,
    key_suffix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Performance-tab filters with Market + Month side-by-side first."""
    c1, c2 = st.columns(2)

    with c1:
        market_opts = sorted([x for x in df_date["country"].dropna().unique().tolist() if x and x != "Unknown"])
        selected_markets = st.multiselect(
            "Market",
            ["All Markets"] + market_opts,
            default=["All Markets"],
            key=f"{key_suffix}_market",
        )

    with c2:
        month_opts = sorted([x for x in df_date["month"].dropna().unique().tolist() if x and x != "NaT"])
        selected_months = st.multiselect(
            "Month",
            ["All Months"] + month_opts,
            default=["All Months"],
            key=f"{key_suffix}_month",
        )

    df = df_date.copy()
    if "All Markets" not in selected_markets and selected_markets:
        df = df[df["country"].isin(selected_markets)]
    if "All Months" not in selected_months and selected_months:
        df = df[df["month"].isin(selected_months)]

    return df, df.copy()


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
    q_win_cw: Optional[int] = None,
    q_win_qualified: Optional[int] = None,
) -> None:
    """Three section columns with custom HTML/CSS KPI cards (hover + staggered animation)."""
    # Q Win Rate% — Looker / X-Ray: SUM(CW) / SUM(Qualified) on the same model (post tab SUM + post Qualified when present).
    _cw_q = int(q_win_cw) if q_win_cw is not None else int(total_cw)
    _q_d = int(q_win_qualified) if q_win_qualified is not None else int(total_qualified)
    q_rate = (_cw_q / _q_d * 100) if _q_d else 0.0
    sql_rate = (total_qualified / total_leads * 100) if total_leads else 0.0
    cpcw = (total_spend / total_cw) if total_cw else 0.0
    # Training deck formulas:
    # CpCW:LF = CpCW / 1st Month LF(avg) == Marketing Spend / total 1st Month LF
    cpcw_lf = (total_spend / total_first_month_lf) if total_first_month_lf else 0.0
    spend_tcv_pct = (total_spend / total_tcv * 100) if total_tcv else 0.0

    _cw_help = (
        "Deals that are in a Closed Won status, including any deals that have been formally approved."
    )
    _q_win_help = (
        "Matches X-Ray / Looker: SUM(CW) ÷ SUM(Qualified) on the post-qual tab (sheet-style row sums). "
        "Qualified falls back to the leads tab when the post tab has no real Qualified column. "
        "The CW card above may still use Market/Month filters."
    )
    _cost_tcv_help = "Looker / X-Ray: SUM(Spend) ÷ SUM(Actual TCV), shown as a percent (same as Cost/TCV%)."
    _actual_tcv_help = (
        "Sum of Actual TCV on the RAW CW tab for closed-won deals only (stage or Is_CW), "
        "using the same Market and Month filters as Spend."
    )
    _spend_help = "Total media spend from the spend worksheet, after your Market and Month filters."
    _cpcw_help = "Cost per closed won: total spend ÷ count of closed-won deals (unique opportunities where applicable)."
    _cpcw_lf_help = (
        "Spend ÷ sum of 1st Month LF for closed-won rows on RAW CW (same filters as Actual TCV). "
        "Lower means more LF per dollar of marketing."
    )
    _leads_total_help = "Data rows on the canonical Raw Leads tab (or configured leads worksheet), scoped to your filters."
    _qualified_help = "Leads whose status is Qualified on the leads tab."
    _new_work_help = "Count of leads in New or Working status on the leads tab."
    _sql_pct_help = "SQL % = Qualified ÷ Total Leads × 100 (same lead slice as the cards above)."
    _cpl_help = "CPL = total spend ÷ total leads (after filters)."
    _cpsql_help = "CPSQL = total spend ÷ qualified count (after filters)."
    _total_live_help = (
        "Pipeline headcount: Qualifying + Pitching + Negotiation + Commitment on the post-qualification tab "
        "(full tab totals unless you narrow by filters on rows that carry those stages)."
    )
    _nego_help = "Opportunities in Negotiation stage on the post-qualification export."
    _commit_help = "Opportunities in Commitment stage on the post-qualification export."
    _closed_lost_help = "Opportunities in Closed Lost stage on the post-qualification export."
    sections: list[tuple[str, list[tuple[str, str, str]]]] = [
        (
            "Closed Won",
            [
                ("CW (Inc Approved)", f"{total_cw:,}", _cw_help),
                ("Spend", _format_spend_k(total_spend), _spend_help),
                ("CPCW", f"${cpcw:,.2f}" if total_cw else "—", _cpcw_help),
                ("Actual TCV", _format_currency(total_tcv) if total_tcv else "—", _actual_tcv_help),
                ("CpCW:LF", f"{cpcw_lf:.2f}" if total_first_month_lf else "—", _cpcw_lf_help),
                ("Cost/TCV%", f"{spend_tcv_pct:.2f}%" if total_tcv else "—", _cost_tcv_help),
            ],
        ),
        (
            "Leads",
            [
                ("Total Leads", f"{total_leads:,}", _leads_total_help),
                ("Qualified", f"{total_qualified:,}", _qualified_help),
                ("New + Working", f"{total_new_working:,}", _new_work_help),
                ("SQL %", f"{sql_rate:.2f}%", _sql_pct_help),
                ("CPL", f"${cpl:,.2f}" if total_leads else "—", _cpl_help),
                ("CPSQL", f"${cpsql:,.2f}" if total_qualified else "—", _cpsql_help),
            ],
        ),
        (
            "Qualified Leads",
            [
                ("Total Live", f"{total_total_live:,}", _total_live_help),
                ("Negotiation", f"{total_negotiation:,}", _nego_help),
                ("Commitment", f"{total_commitment:,}", _commit_help),
                ("Closed Lost", f"{total_closed_lost:,}", _closed_lost_help),
                ("Q Win Rate%", f"{q_rate:.2f}%", _q_win_help),
            ],
        ),
    ]

    def _kpi_data_tip_attr(help_text: str) -> str:
        t = " ".join(help_text.split())
        return f' data-kpi-tip="{html.escape(t, quote=True)}"'

    accent_map = {"Closed Won": "cw", "Leads": "leads", "Qualified Leads": "pipe"}
    sec_cols = st.columns(3)
    for i, (sec_title, cards) in enumerate(sections):
        accent = accent_map.get(sec_title, "cw")
        with sec_cols[i]:
            parts: list[str] = [
                f'<div class="kpi-section kpi-section--{accent}">',
                '<div class="kpi-section-head"><span class="kpi-section-marker" aria-hidden="true"></span>',
                f"<h3>{html.escape(sec_title)}</h3></div>",
                '<div class="kpi-card-grid">',
            ]
            for j, card in enumerate(cards):
                label, value, tip = card[0], card[1], card[2]
                parts.append(
                    f'<div class="kpi-card" style="animation-delay:{j * 0.055:.3f}s"{_kpi_data_tip_attr(tip)}>'
                    f'<div class="kpi-card-label">{html.escape(label)}</div>'
                    f'<div class="kpi-card-value">{html.escape(value)}</div></div>'
                )
            parts.append("</div></div>")
            st.markdown("".join(parts), unsafe_allow_html=True)


def _master_performance_table(
    df: pd.DataFrame,
    *,
    key_suffix: str,
    section_title: Optional[str] = "Marketing Performance Master View",
) -> None:
    """Unified Date column (first row per month only), Middle East subtotal, cyan input metrics, R/G/Y on ratios."""
    df = _normalize_master_merge_frame(df)
    if section_title:
        st.markdown(f'<div class="looker-table-title">{section_title}</div>', unsafe_allow_html=True)
    agg: dict[str, tuple[str, str]] = {
        "spend": ("cost", "sum"),
        "cw": ("closed_won", "sum"),
        "clicks": ("clicks", "sum"),
        "leads": ("leads", "sum"),
        "qualified": ("qualified", "sum"),
    }
    if "tcv" in df.columns:
        agg["tcv"] = ("tcv", "sum")
    if "first_month_lf" in df.columns:
        agg["lf"] = ("first_month_lf", "sum")
    if "impressions" in df.columns:
        agg["impressions"] = ("impressions", "sum")
    # Preserve post-tab pipeline fields from the merged master frame (were dropped before).
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

    g = df.groupby(["month", "country"], as_index=False).agg(**agg).sort_values(
        ["month", "country"], ascending=[False, True]
    )
    g["Market"] = g["country"].map(_market_display_from_join_key)
    # Sum additive fields per month × market, then recompute ratios (Looker: do not SUM(CPCW:LF)).
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
    gm = g.groupby(["month", "Market"], as_index=False).agg(sum_map)
    gm = _master_view_drop_empty_months(gm)
    gm = _master_view_append_middle_east_first(gm)
    gm["Spend"] = gm["spend"]
    gm["CW (Inc Approved)"] = gm["cw"].astype(int)
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
            lambda r: (r["spend"] / r["lf"]) if r["lf"] and r["lf"] > 0 else float("nan"),
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

    pvt = gm.copy()
    pvt["Month"] = pvt["month"].apply(lambda m: pd.Period(m, freq="M").strftime("%b %Y") if pd.notna(m) else "")
    pvt = pvt.drop(columns=["month"], errors="ignore")
    cols = ["Month", "Market"] + [m for m in metrics if m in pvt.columns]
    pvt = pvt[cols]
    pvt["Unified Date"] = ""

    def _month_label_sort_key(m: Any) -> Any:
        try:
            return pd.Period(str(m), freq="M")
        except Exception:
            return str(m)

    for m in sorted(pvt["Month"].dropna().unique(), key=_month_label_sort_key, reverse=True):
        ix = pvt.index[pvt["Month"] == m].tolist()
        if ix:
            pvt.loc[ix[0], "Unified Date"] = m
    pvt = pvt.drop(columns=["Month"], errors="ignore")
    out_cols = ["Unified Date", "Market"] + [m for m in metrics if m in pvt.columns]
    pvt = pvt[out_cols]

    def _fmt_for_metric(metric_name: str) -> Any:
        if metric_name == "Spend":
            return lambda x: _format_spend_k(float(x)) if pd.notna(x) else "—"
        if metric_name in {"CPCW", "CPL", "Actual TCV", "1st Month LF"}:
            return lambda x: f"${x:,.2f}" if pd.notna(x) else "—"
        if metric_name == "CPCW:LF":
            return lambda x: f"{x:,.2f}" if pd.notna(x) else "—"
        if metric_name in {"SQL %", "Cost/TCV%"}:
            return lambda x: f"{x:.2f}%" if pd.notna(x) else "—"
        if metric_name in {"CW (Inc Approved)", "Total Leads"}:
            return lambda x: f"{x:,.0f}" if pd.notna(x) else "—"
        return lambda x: f"{x:,.2f}" if pd.notna(x) else "—"

    fmt_map: dict[str, Any] = {
        "Unified Date": lambda x: "" if x == "" or (isinstance(x, float) and pd.isna(x)) else str(x),
        "Market": lambda x: str(x) if pd.notna(x) else "—",
    }
    for c in pvt.columns:
        if c in {"Unified Date", "Market"}:
            continue
        fmt_map[c] = _fmt_for_metric(c)

    css_matrix = _master_view_style_css(pvt)
    styler = pvt.style.format(fmt_map, na_rep="—")
    for col in css_matrix.columns:
        styler = styler.apply(
            lambda s, c=col: css_matrix.loc[s.index, c],
            axis=0,
            subset=[col],
        )
    st.dataframe(styler, use_container_width=True, hide_index=True, key=f"{key_suffix}_df_master_pivot")


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
    df, _ = _apply_marketing_performance_filters(df_date, key_suffix=key_suffix)

    st.caption("Filters apply to scorecards, master table, and charts below.")

    sheet_id = _extract_sheet_id(_default_sheet_id_from_secrets())
    _fp_mpo = _secret_fingerprint(_service_account_from_streamlit_secrets())
    # Spend only from the canonical Spend worksheet (gid=0) on the ME X-Ray workbook:
    # https://docs.google.com/spreadsheets/d/1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8/edit?gid=0
    spend_gid0_wks = load_spend_gid0_normalized(sheet_id, _fp_mpo)
    spend_sheet_master = _filter_by_date_range(spend_gid0_wks, start_date, end_date)
    if spend_sheet_master.empty and "worksheet_gid" in df_loaded.columns:
        _g0 = df_loaded.loc[pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce") == 0].copy()
        if not _g0.empty:
            spend_sheet_master = _filter_by_date_range(_g0, start_date, end_date)
    # If gid=0 is not the Spend layout (or dates filter everything out) but another tab is named Spend / Raw Spend, use it.
    if _normalized_spend_cost_sum(spend_sheet_master) == 0.0:
        alt_spend = load_first_matching_worksheet_normalized(
            sheet_id,
            (r"^spend$", r"raw\s*spend", r"sum\s*spend"),
            _fp_mpo,
        )
        alt_spend = _filter_by_date_range(alt_spend, start_date, end_date)
        if _normalized_spend_cost_sum(alt_spend) > 0.0:
            spend_sheet_master = alt_spend
    spend_df = _spend_slice_for_dashboard_filters(spend_sheet_master, df)

    def _tab_subset(frame: pd.DataFrame, tab_keywords: list[str]) -> pd.DataFrame:
        if "source_tab" not in frame.columns:
            return frame
        s = frame["source_tab"].astype(str).str.lower()
        mask = pd.Series(False, index=frame.index)
        for k in tab_keywords:
            mask = mask | s.str.contains(k.lower(), na=False, regex=True)
        return frame[mask].copy()

    def _pick_source(frame: pd.DataFrame, patterns: list[str], metric_cols: list[str]) -> pd.DataFrame:
        """Prefer mapped tabs, but fall back to full frame if mapped slice is empty/zero."""
        subset = _tab_subset(frame, patterns)
        if subset.empty:
            return frame
        present_cols = [c for c in metric_cols if c in subset.columns]
        if not present_cols:
            return frame
        subset_total = 0.0
        frame_total = 0.0
        for c in present_cols:
            subset_total += float(pd.to_numeric(subset[c], errors="coerce").fillna(0).sum())
            frame_total += float(pd.to_numeric(frame.get(c, 0), errors="coerce").fillna(0).sum())
        if subset_total == 0.0 and frame_total > 0.0:
            return frame
        return subset

    # Business mapping by tab:
    # - Spend: worksheet gid=0 on the sheet above (see ``spend_sheet_master`` / ``spend_df``).
    # - Leads / Qualified: Raw Leads
    # - CW (inc approved) + pipeline stages: Raw Post Qualification
    # - TCV / 1st Month LF: RAW CW
    leads_df = _pick_source(df_loaded, [r"raw\s*leads?"], ["leads", "qualified"])
    leads_gid = _default_leads_gid_from_secrets()
    # Strict source of truth for Total Leads: read the canonical leads worksheet by gid.
    try:
        leads_by_gid = load_marketing_data(sheet_id, int(leads_gid), _fp_mpo)
        if not leads_by_gid.empty:
            leads_df = leads_by_gid
    except Exception:
        pass
    if "worksheet_gid" in df_loaded.columns:
        wg = pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce")
        by_gid = df_loaded.loc[wg == int(leads_gid)].copy()
        if not by_gid.empty:
            leads_df = by_gid
    # Never fall back to the full workbook here — _pick_source would mix RAW CW into post-lead totals.
    post_df = _dedupe_post_lead_rows(_tab_subset(df, list(_POST_LEAD_SOURCE_TAB_PATTERNS)))
    # Pipeline KPIs (Total Live = Q+P+N+C): full workbook tab(s), no market/month slice.
    # Do NOT _dedupe_post_lead_rows here — cross-tab dedupe dropped ~10 rows vs Sheets SUM() when the same opp
    # appeared on two post-qual tabs; Sheets totals still sum both tabs.
    post_df_kpi = _tab_subset(df_loaded, list(_POST_LEAD_SOURCE_TAB_PATTERNS))
    pq_gid = _optional_post_qual_gid_from_secrets()
    if pq_gid is not None and "worksheet_gid" in df_loaded.columns:
        wg = pd.to_numeric(df_loaded["worksheet_gid"], errors="coerce")
        by_pq = df_loaded.loc[wg == int(pq_gid)].copy()
        if not by_pq.empty:
            post_df_kpi = by_pq
    elif pq_gid is not None:
        try:
            _sid = _extract_sheet_id(_default_sheet_id_from_secrets())
            _fp2 = _secret_fingerprint(_service_account_from_streamlit_secrets())
            _direct = load_worksheet_by_gid_preprocessed(_sid, int(pq_gid), _fp2)
            if not _direct.empty:
                post_df_kpi = _direct
        except Exception:
            pass
    if post_df_kpi.empty:
        post_df_kpi = post_df
    cw_df = _resolve_cw_tcv_dataframe(df_loaded, df)
    cw_kpi = _cw_dataframe_for_kpis(cw_df, df)

    total_spend = float(spend_df["cost"].sum()) if "cost" in spend_df.columns else 0.0
    total_impr = int(spend_df["impressions"].sum()) if "impressions" in spend_df.columns else 0
    total_clicks = int(spend_df["clicks"].sum()) if "clicks" in spend_df.columns else 0
    total_leads = _lead_rows_count(leads_df)
    total_qualified = _qualified_count_from_leads(leads_df)
    total_pitching = int(post_df_kpi["pitching"].sum()) if "pitching" in post_df_kpi.columns else 0
    total_cw = _sum_closed_won_unique_opportunities(post_df)
    if total_cw == 0 and "closed_won" in df.columns:
        pl_only = _dedupe_post_lead_rows(_tab_subset(df, list(_POST_LEAD_SOURCE_TAB_PATTERNS)))
        if not pl_only.empty:
            total_cw = _sum_closed_won_unique_opportunities(pl_only)
    total_new = int(post_df["new"].sum()) if "new" in post_df.columns else 0
    total_working = int(post_df["working"].sum()) if "working" in post_df.columns else 0
    total_negotiation = int(post_df_kpi["negotiation"].sum()) if "negotiation" in post_df_kpi.columns else 0
    total_commitment = int(post_df_kpi["commitment"].sum()) if "commitment" in post_df_kpi.columns else 0
    total_qualifying = int(post_df_kpi["qualifying"].sum()) if "qualifying" in post_df_kpi.columns else 0
    # Total Live (CRM): Qualifying + Pitching + Negotiation + Commitment — not the broader sheet `total_live` flag.
    total_total_live = total_qualifying + total_pitching + total_negotiation + total_commitment
    total_closed_lost = int(post_df_kpi["closed_lost"].sum()) if "closed_lost" in post_df_kpi.columns else 0
    total_tcv = float(cw_kpi["tcv"].sum()) if "tcv" in cw_kpi.columns else 0.0
    total_first_month_lf = float(cw_kpi["first_month_lf"].sum()) if "first_month_lf" in cw_kpi.columns else 0.0
    total_new_working = _new_working_count_from_leads(leads_df)

    # Per-metric safety fallbacks.
    if total_spend == 0.0 and "cost" in df.columns:
        total_spend = float(df["cost"].sum())
    gid0_spend_sum = float(st.session_state.get("_gid0_spend_sum", 0.0) or 0.0)
    if total_spend == 0.0 and gid0_spend_sum > 0.0:
        total_spend = gid0_spend_sum
    if total_cw == 0 and "closed_won" in df.columns:
        pl_only = _dedupe_post_lead_rows(_tab_subset(df, list(_POST_LEAD_SOURCE_TAB_PATTERNS)))
        if not pl_only.empty:
            total_cw = _sum_closed_won_unique_opportunities(pl_only)

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
        elif _gpq is not None:
            try:
                _sid2 = _extract_sheet_id(_default_sheet_id_from_secrets())
                _fp3 = _secret_fingerprint(_service_account_from_streamlit_secrets())
                _dir2 = load_worksheet_by_gid_preprocessed(_sid2, int(_gpq), _fp3)
                if not _dir2.empty:
                    _pk = _dir2
            except Exception:
                pass
        _pk = _pk if not _pk.empty else df
        total_pitching = int(_pk["pitching"].sum()) if "pitching" in _pk.columns else 0
        pl_only = _dedupe_post_lead_rows(_tab_subset(df, list(_POST_LEAD_SOURCE_TAB_PATTERNS)))
        total_cw = (
            _sum_closed_won_unique_opportunities(pl_only)
            if (not pl_only.empty and "closed_won" in pl_only.columns)
            else 0
        )
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

    _pqw = post_df_kpi if not post_df_kpi.empty else post_df
    cw_for_qwin, qual_for_qwin = _q_win_rate_inputs(_pqw, leads_df)

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
    )

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

    spend_g = _agg_for_master(
        _normalize_master_merge_frame(spend_sheet_master), ["cost", "clicks", "impressions"]
    )
    leads_g = _agg_for_master(_normalize_master_merge_frame(leads_df), ["leads", "qualified"])
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
    cw_g = _agg_for_master(_normalize_master_merge_frame(cw_kpi), ["tcv", "first_month_lf"])

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
    if master_df.empty:
        master_df = df.copy()
    else:
        metric_probe = [c for c in ("cost", "leads", "qualified", "closed_won", "tcv", "first_month_lf") if c in master_df.columns]
        if metric_probe:
            probe_total = float(master_df[metric_probe].sum(numeric_only=True).sum())
            if probe_total == 0.0:
                master_df = df.copy()

    _master_performance_table(master_df, key_suffix=key_suffix)


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
                "Total Leads": _lead_rows_count(df),
            }
        ]
    )
    st.caption("Grand total (filtered)")
    st.dataframe(grand, use_container_width=True, hide_index=True, key=f"{key_suffix}_df_grand")

    monthly = (
        df.groupby("month", as_index=False)
        .agg(cw=("closed_won", "sum"), qualified=("qualified", "sum"))
        .sort_values("month")
    )
    month_leads: list[int] = []
    for m in monthly["month"].tolist():
        gm = df[df["month"] == m]
        month_leads.append(_lead_rows_count(gm))
    monthly["leads"] = month_leads
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
            title="SQL % and Q Win % (CW ÷ Qualified, by month)",
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
        .agg(spend=("cost", "sum"), clicks=("clicks", "sum"), cw=("closed_won", "sum"))
        .sort_values("spend", ascending=False)
    )
    grp_leads: list[int] = []
    for k in agg[group_col].tolist():
        gk = df[df[group_col] == k]
        grp_leads.append(_lead_rows_count(gk))
    agg["leads"] = grp_leads
    agg["CPL"] = agg.apply(lambda r: (r["spend"] / r["leads"]) if r["leads"] else float("nan"), axis=1)
    st.dataframe(agg, use_container_width=True, hide_index=True, key=f"{key_suffix}_df_ch")

    m1, m2 = st.columns(2)
    with m1:
        fig = px.bar(agg.head(20), x=group_col, y="spend", title="Spend")
        fig.update_traces(marker_color="#4f8483")
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


def render_main_dashboard(
    start_date: date,
    end_date: date,
) -> None:
    """Load Google Sheets workbook (all tabs), then route to report pages."""
    sheet_id = _extract_sheet_id(_default_sheet_id_from_secrets())
    _fp = _secret_fingerprint(_service_account_from_streamlit_secrets())
    try:
        # Source of truth is the entire spreadsheet; aggregate data across tabs.
        df_loaded = load_all_worksheets_combined(sheet_id, _fp)
        # Hard requirement: ensure Spend sheet is present (sheet: Spend, column: Spend).
        needs_spend_inject = True
        if not df_loaded.empty and "source_tab" in df_loaded.columns:
            spend_rows = df_loaded[df_loaded["source_tab"].astype(str).str.strip().str.lower() == "spend"]
            if not spend_rows.empty and "cost" in spend_rows.columns and float(spend_rows["cost"].sum()) > 0:
                needs_spend_inject = False
        if needs_spend_inject:
            spend_norm = load_named_worksheet_normalized(sheet_id, "Spend", _fp)
            if spend_norm.empty:
                spend_norm = load_spend_worksheet_fallback(sheet_id, _fp)
            if not spend_norm.empty:
                if df_loaded.empty:
                    df_loaded = spend_norm
                else:
                    df_loaded = pd.concat([df_loaded, spend_norm], ignore_index=True)
        # Hardwired spend source from gid=0 (requested source tab).
        spend_gid0 = load_spend_gid0_normalized(sheet_id, _fp)
        st.session_state["_gid0_spend_sum"] = load_spend_gid0_raw_sum(sheet_id, _fp)
        if not spend_gid0.empty:
            if not df_loaded.empty and "source_tab" in df_loaded.columns:
                df_loaded = df_loaded[df_loaded["source_tab"].astype(str) != "gid:0_spend"]
            if df_loaded.empty:
                df_loaded = spend_gid0
            else:
                df_loaded = pd.concat([df_loaded, spend_gid0], ignore_index=True)

        # Explicitly ensure core business tabs are loaded by title-match.
        spend_named = load_first_matching_worksheet_normalized(sheet_id, (r"^spend$", r"raw\s*spend", r"sum\s*spend"), _fp)
        leads_named = load_first_matching_worksheet_normalized(sheet_id, (r"^leads?$", r"raw\s*leads?"), _fp)
        post_named = load_first_matching_worksheet_normalized(
            sheet_id,
            (r"post\s*leads?", r"raw.*post.*qual", r"post\s+qual", r"post.*qualif"),
            _fp,
        )
        cw_named = load_first_matching_worksheet_normalized(
            sheet_id,
            tuple(_RAW_CW_TAB_PATTERNS),
            _fp,
        )
        extras = [x for x in (spend_named, leads_named, post_named, cw_named) if not x.empty]
        extras = _extras_skip_tabs_already_loaded(df_loaded, extras)
        if extras:
            if df_loaded.empty:
                df_loaded = pd.concat(extras, ignore_index=True)
            else:
                df_loaded = pd.concat([df_loaded] + extras, ignore_index=True)
    except Exception as exc:
        st.error(f"Failed to load spreadsheet: {exc}")
        return

    if df_loaded.empty:
        st.warning("No data rows were returned. Check tabs and column headers against the ME X-Ray template.")
        return

    tab_mpo, tab_mom, tab_pmc, tab_inbound = st.tabs(list(LOOKER_PAGES))
    with tab_mpo:
        render_page_marketing_performance(df_loaded, start_date, end_date)
    with tab_mom:
        render_page_market_mom(df_loaded, start_date, end_date)
    with tab_pmc:
        render_page_channels(df_loaded, start_date, end_date, inbound=False)
    with tab_inbound:
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
    .stApp { background: #f4f6f8; font-family: 'Segoe UI', system-ui, sans-serif; font-size: 0.50rem !important; }
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
    .looker-header-title { font-size: 0.48rem; font-weight: 700; color: #111827; margin: 0; line-height: 1.05; }
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
    .stButton > button {
        border: 1px solid #b7d9d5;
        border-radius: 999px;
        background: #eef8f7;
        color: #19766f;
        font-size: 7px;
        width: 22px;
        min-width: 22px;
        height: 22px;
        padding: 0;
        line-height: 1;
    }
    .stButton > button:hover { border-color: #4f8483; color: #0f766e; }
    .looker-page-h1 { font-size: 0.86rem; font-weight: 400; color: #202124; margin: 8px 0 16px 0; }
    .looker-table-title { font-size: 1.0rem; font-weight: 700; color: #202124; margin: 20px 0 8px 0; }
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
    .stTabs [aria-selected="true"] { background: #4f8483 !important; color: white !important; }
    [data-testid="stMetric"] {
        background: #e9f3f8;
        border: 1px solid #d5e4ec;
        border-left: 3px solid #4f8483;
        border-radius: 8px;
        padding: 6px 10px;
    }
    [data-testid="stMetricLabel"] { font-size: 8px !important; color: #4b5563 !important; }
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; color: #1f2937 !important; }
    .stRadio [role="radiogroup"] { gap: 14px; }
    .stSelectbox > label, .stRadio > label, .stTextInput > label { font-size: 11px !important; color: #6b7280 !important; }
    .stTabs [aria-selected="true"] span { color: white !important; }
    .streamlit-expanderHeader { background: #F8FAFC; border-radius: 8px; border-left: 4px solid #4f8483; }
    .stTextInput input, .stSelectbox > div, .stDateInput input {
        border-radius: 6px !important;
        background: #F8FAFC !important;
        border: 1px solid #E2E8F0 !important;
    }
    /* Multiselect selected chips: force app green instead of default red */
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"],
    .stMultiSelect [data-baseweb="tag"] {
        background: #4f8483 !important;
        color: #ffffff !important;
        border-color: #4f8483 !important;
    }
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"] *,
    .stMultiSelect [data-baseweb="tag"] * {
        color: #ffffff !important;
    }
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"] [role="button"] svg,
    .stMultiSelect [data-baseweb="tag"] [role="button"] svg {
        fill: #ffffff !important;
        color: #ffffff !important;
    }
    .stMultiSelect [data-baseweb="select"] [data-baseweb="tag"] [role="button"] svg path,
    .stMultiSelect [data-baseweb="tag"] [role="button"] svg path {
        fill: #ffffff !important;
        stroke: #ffffff !important;
    }
    .stDataFrame { border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: 1px solid #E2E8F0; }
    [data-testid="stMetricValue"] { color: #1E293B !important; }
    [data-testid="stMetricLabel"] { color: #64748B !important; }
    .stCaption { color: #64748B !important; }
    .stAlert { border-radius: 8px; border-left: 4px solid #4f8483; }
    /* Replace red-like status accents with app green palette */
    [data-testid="stAlert"] svg, [data-testid="stNotification"] svg { color: #4f8483 !important; fill: #4f8483 !important; }
    [data-baseweb="tag"][class*="danger"], [class*="danger"], [class*="error"] { color: #19766f !important; }
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
    hl, hr = st.columns([8, 2])
    with hl:
        st.markdown(
            f"""
        <div style="display:flex;align-items:center;gap:8px;">
          {logo_html}
          <h1 class="looker-header-title">KitchenPark Marketing Dashboard</h1>
          <span class="live-pill">● Live</span>
          <span class="refresh-note">{refreshed_text}</span>
        </div>
        """,
            unsafe_allow_html=True,
        )
    with hr:
        b1, b2, b3 = st.columns([1, 1, 1])
        with b1:
            if st.button("?", key="hdr_help_btn"):
                st.info("Help panel coming soon.")
        with b2:
            st.button("MA", key="hdr_user_btn")
        with b3:
            if st.button("↗", key="hdr_signout_btn"):
                st.info("Signed out.")
    st.markdown("</div>", unsafe_allow_html=True)

    _end = date.today()
    _start = _end - timedelta(days=730)

    render_main_dashboard(_start, _end)


if __name__ == "__main__":
    main()

"""
Marketing dashboard data loader.
Supports BigQuery, Google Sheets (hourly-refreshed), and CSV upload for Spend, Lead, and Post Lead (Opportunity) data.
"""

import pandas as pd
from datetime import datetime, date
from typing import Optional, Tuple, List, Union

# BigQuery queries (same as your existing queries + Lead query).
# Parameters: @ds_start_date, @ds_end_date (DATE).
QUERY_SPEND = """
SELECT
  date,
  country AS market,
  datasource AS platform,
  cost_usd AS spend
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN @ds_start_date AND @ds_end_date
  AND country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
ORDER BY date, country, datasource
"""

QUERY_OPPORTUNITY = """
SELECT
  createddate AS created_date,
  DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH) AS month_date,
  closedate AS close_date,
  COALESCE(tcv_realised__c, amount, 0) AS tcv_usd,
  account_market__c AS kitchen_country,
  stagename AS stage,
  name AS opportunity_name,
  approved__c
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND iswon = TRUE
  AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN @ds_start_date AND @ds_end_date)
ORDER BY closedate DESC, kitchen_country
"""

QUERY_LEAD = """
SELECT
  createddate AS created_date,
  DATE_TRUNC(createddate, MONTH) AS month_date,
  COALESCE(country, 'Unknown') AS market,
  qualified_date__c AS qualified_date
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND DATE(createddate) BETWEEN @ds_start_date AND @ds_end_date
  AND COALESCE(country, '') IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
ORDER BY createddate DESC, market
"""


def load_from_bigquery(
    ds_start_date: date,
    ds_end_date: date,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str]]:
    """
    Load Spend, Lead, and Opportunity data from BigQuery.
    Returns (df_spend, df_lead, df_opportunity, error_message).
    If error_message is set, some data may be None.
    """
    try:
        from google.cloud import bigquery
    except ImportError:
        return None, None, None, "Install google-cloud-bigquery: pip install google-cloud-bigquery"

    client = bigquery.Client()
    ds_start = ds_start_date.isoformat()
    ds_end = ds_end_date.isoformat()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ds_start_date", "DATE", ds_start),
            bigquery.ScalarQueryParameter("ds_end_date", "DATE", ds_end),
        ]
    )

    def run(q: str) -> pd.DataFrame:
        return client.query(q, job_config=job_config).to_dataframe()

    try:
        df_spend = run(QUERY_SPEND)
        df_opportunity = run(QUERY_OPPORTUNITY)
        df_lead = run(QUERY_LEAD)
        return df_spend, df_lead, df_opportunity, None
    except Exception as e:
        return None, None, None, str(e)


def build_aggregated_metrics(
    df_spend: pd.DataFrame,
    df_lead: pd.DataFrame,
    df_opportunity: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a single aggregated table by month + market (aligned with Looker Studio query).
    Uses approved__c for CW count and TCV; qualified_date for qualified leads.
    """
    if df_spend is None or df_spend.empty:
        return pd.DataFrame()

    # Normalize date to month
    df_spend = df_spend.copy()
    df_spend["date"] = pd.to_datetime(df_spend["date"], errors="coerce")
    df_spend["unified_date"] = df_spend["date"].dt.to_period("M").dt.to_timestamp()

    spend_agg = (
        df_spend.groupby(["unified_date", "market"])
        .agg(spend=("spend", "sum"))
        .reset_index()
    )

    # Opportunity: approved won only, by month + market
    if df_opportunity is not None and not df_opportunity.empty:
        opp = df_opportunity.copy()
        opp["close_date"] = pd.to_datetime(opp["close_date"], errors="coerce")
        opp["month_date"] = pd.to_datetime(opp["month_date"], errors="coerce")
        opp["unified_date"] = opp["month_date"].dt.to_period("M").dt.to_timestamp()
        opp["market"] = opp["kitchen_country"].fillna("Unknown")
        approved = opp.copy()
        if "approved__c" in approved.columns:
            approved = approved[approved["approved__c"] == True]
        cw_agg = (
            approved.groupby(["unified_date", "market"])
            .agg(
                cw_inc_approved=("opportunity_name", "count"),
                actual_tcv=("tcv_usd", "sum"),
            )
            .reset_index()
        )
    else:
        cw_agg = pd.DataFrame(columns=["unified_date", "market", "cw_inc_approved", "actual_tcv"])

    # Lead: by month + market, total and qualified
    if df_lead is not None and not df_lead.empty:
        lead = df_lead.copy()
        lead["created_date"] = pd.to_datetime(lead["created_date"], errors="coerce")
        lead["month_date"] = pd.to_datetime(lead["month_date"], errors="coerce")
        lead["unified_date"] = lead["month_date"].dt.to_period("M").dt.to_timestamp()
        lead["qualified"] = lead["qualified_date"].notna().astype(int)
        lead_agg = (
            lead.groupby(["unified_date", "market"])
            .agg(total_leads=("created_date", "count"), qualified=("qualified", "sum"))
            .reset_index()
        )
    else:
        lead_agg = pd.DataFrame(columns=["unified_date", "market", "total_leads", "qualified"])

    # Join
    merged = spend_agg.merge(
        cw_agg,
        on=["unified_date", "market"],
        how="left",
    ).merge(
        lead_agg,
        on=["unified_date", "market"],
        how="left",
    )
    merged["cw_inc_approved"] = merged["cw_inc_approved"].fillna(0).astype(int)
    merged["actual_tcv"] = merged["actual_tcv"].fillna(0)
    merged["total_leads"] = merged["total_leads"].fillna(0).astype(int)
    merged["qualified"] = merged["qualified"].fillna(0).astype(int)

    # Calculated metrics
    merged["cpcw"] = merged.apply(
        lambda r: r["spend"] / r["cw_inc_approved"] if r["cw_inc_approved"] else None,
        axis=1,
    )
    merged["cpl"] = merged.apply(
        lambda r: r["spend"] / r["total_leads"] if r["total_leads"] else None,
        axis=1,
    )
    merged["cost_tcv_pct"] = merged.apply(
        lambda r: 100 * r["spend"] / r["actual_tcv"] if r["actual_tcv"] else None,
        axis=1,
    )
    merged["sql_pct"] = merged.apply(
        lambda r: 100 * r["qualified"] / r["total_leads"] if r["total_leads"] else None,
        axis=1,
    )

    return merged


def load_from_csv(
    spend_file,
    lead_file,
    opportunity_file,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str]]:
    """
    Load from 3 uploaded CSV files (Spend, Lead, Post Lead).
    Column names are normalized to match BigQuery output where possible.
    """
    try:
        df_spend = pd.read_csv(spend_file)
        # Normalize columns: date, market/country, platform/datasource, spend/cost_usd
        for old, new in [("cost_usd", "spend"), ("country", "market"), ("datasource", "platform")]:
            if old in df_spend.columns and new not in df_spend.columns:
                df_spend = df_spend.rename(columns={old: new})
        if "date" not in df_spend.columns and "Date" in df_spend.columns:
            df_spend = df_spend.rename(columns={"Date": "date"})
    except Exception as e:
        return None, None, None, f"Spend CSV: {e}"

    try:
        df_lead = pd.read_csv(lead_file)
        for old, new in [("country", "market"), ("createddate", "created_date"), ("qualified_date__c", "qualified_date")]:
            if old in df_lead.columns and new not in df_lead.columns:
                df_lead = df_lead.rename(columns={old: new})
        if "created_date" not in df_lead.columns and "Created Date" in df_lead.columns:
            df_lead = df_lead.rename(columns={"Created Date": "created_date"})
        if "qualified_date" not in df_lead.columns and "Qualified Date" in df_lead.columns:
            df_lead = df_lead.rename(columns={"Qualified Date": "qualified_date"})
    except Exception as e:
        return None, None, None, f"Lead CSV: {e}"

    try:
        df_opp = pd.read_csv(opportunity_file)
        for old, new in [
            ("account_market__c", "kitchen_country"),
            ("closedate", "close_date"),
            ("createddate", "created_date"),
            ("tcv_realised__c", "tcv_usd"),
            ("stagename", "stage"),
            ("Name", "opportunity_name"),
            ("approved__c", "approved__c"),
        ]:
            if old in df_opp.columns and new not in df_opp.columns:
                df_opp = df_opp.rename(columns={old: new})
        if "amount" in df_opp.columns and "tcv_usd" not in df_opp.columns:
            df_opp["tcv_usd"] = df_opp["amount"]
    except Exception as e:
        return None, None, None, f"Opportunity CSV: {e}"

    return df_spend, df_lead, df_opp, None


# Default spreadsheet ID (your 3-tab Sheet); tab order: Spend, Lead, Post Lead
DEFAULT_SHEET_ID = "1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8"
# Sheet names or indices (0-based) for Spend, Lead, Post Lead tabs
DEFAULT_SHEET_NAMES = ["Spend", "Lead", "Post Lead"]


def _normalize_spend_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Spend tab columns to date, market, platform, spend."""
    for old, new in [
        ("cost_usd", "spend"), ("country", "market"), ("datasource", "platform"),
        ("Cost (USD)", "spend"), ("Spend", "spend"), ("Spend (USD)", "spend"),
        ("Platform", "platform"), ("Market", "market"), ("Country", "market"),
    ]:
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    if "date" not in df.columns and "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    return df


def _normalize_lead_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Lead tab columns."""
    for old, new in [("country", "market"), ("createddate", "created_date"), ("qualified_date__c", "qualified_date"), ("Created Date", "created_date"), ("Qualified Date", "qualified_date"), ("Market", "market")]:
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    return df


def _normalize_opportunity_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Post Lead (Opportunity) tab columns."""
    renames = [
        ("account_market__c", "kitchen_country"),
        ("closedate", "close_date"),
        ("createddate", "created_date"),
        ("tcv_realised__c", "tcv_usd"),
        ("stagename", "stage"),
        ("Name", "opportunity_name"),
        ("approved__c", "approved__c"),
        ("Close Date", "close_date"),
        ("Account Market", "kitchen_country"),
        ("TCV (USD)", "tcv_usd"),
        ("Stage", "stage"),
        ("Opportunity Name", "opportunity_name"),
        ("Approved", "approved__c"),
    ]
    for old, new in renames:
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    if "amount" in df.columns and "tcv_usd" not in df.columns:
        df["tcv_usd"] = df["amount"]
    if "Name" in df.columns and "opportunity_name" not in df.columns:
        df = df.rename(columns={"Name": "opportunity_name"})
    return df


def load_from_sheets(
    spreadsheet_id: str,
    sheet_names: Optional[List[str]] = None,
    credentials_path: Optional[str] = None,
    credentials_json: Optional[Union[dict, bytes]] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str]]:
    """
    Load Spend, Lead, and Post Lead from a Google Sheets workbook (e.g. refreshed hourly).
    Uses the first 3 worksheets as Spend, Lead, Post Lead unless sheet_names is provided.
    credentials_path: path to service account JSON key file.
    credentials_json: service account JSON as dict or bytes (e.g. from Streamlit file_uploader).
    If no credentials and sheet is publicly viewable, tries public export URLs.
    """
    sheet_names = sheet_names or DEFAULT_SHEET_NAMES
    use_auth = credentials_path or credentials_json is not None

    if use_auth:
        try:
            import gspread
            from google.oauth2.service_account import Credentials
        except ImportError:
            return None, None, None, "Install gspread and google-auth: pip install gspread google-auth"

        try:
            if credentials_path:
                gc = gspread.service_account(filename=credentials_path)
            else:
                if isinstance(credentials_json, bytes):
                    import json
                    credentials_json = json.loads(credentials_json.decode("utf-8"))
                creds = Credentials.from_service_account_info(credentials_json, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
                gc = gspread.authorize(creds)
            sh = gc.open_by_key(spreadsheet_id)
            worksheets = sh.worksheets()
            # Resolve by name or index
            sheets_to_read = []
            for name_or_idx in sheet_names[:3]:
                if isinstance(name_or_idx, int):
                    sheets_to_read.append(worksheets[name_or_idx])
                else:
                    ws = next((w for w in worksheets if w.title == name_or_idx), None)
                    if ws is None:
                        return None, None, None, f"Sheet tab '{name_or_idx}' not found. Tabs: {[w.title for w in worksheets]}"
                    sheets_to_read.append(ws)
            df_spend = pd.DataFrame(sheets_to_read[0].get_all_records())
            df_lead = pd.DataFrame(sheets_to_read[1].get_all_records())
            df_opp = pd.DataFrame(sheets_to_read[2].get_all_records())
        except Exception as e:
            return None, None, None, f"Google Sheets (auth): {e}"
    else:
        # Public export URLs (sheet must be "Anyone with link can view")
        try:
            # Get gids from the spreadsheet metadata (we'll try 0, 1, 2 for first 3 sheets)
            import urllib.request
            import re
            url_meta = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
            req = urllib.request.Request(url_meta, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req) as resp:
                html = resp.read().decode()
            # Extract gids: "gid=123456"
            gids = re.findall(r'"gid":(\d+)|gid=(\d+)', html)
            gids = [int(g[0] or g[1]) for g in gids[:3]]
            if len(gids) < 3:
                gids = [0, 1, 2]  # fallback
            dfs = []
            for gid in gids:
                export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
                df = pd.read_csv(export_url)
                dfs.append(df)
            df_spend, df_lead, df_opp = dfs[0], dfs[1], dfs[2]
        except Exception as e:
            return None, None, None, f"Google Sheets (public): {e}. Use a service account for private sheets."

    if df_spend.empty:
        return None, None, None, "Spend sheet is empty."
    df_spend = _normalize_spend_columns(df_spend.copy())
    for col in ["date", "spend", "market"]:
        if col not in df_spend.columns:
            return None, None, None, (
                f"Spend tab must have a column for date, spend, and market. "
                f"Found columns: {list(df_spend.columns)}. "
                f"Use headers like Date, Country/Market, Platform, Spend (or cost_usd)."
            )
    df_lead = _normalize_lead_columns(df_lead.copy()) if not df_lead.empty else df_lead
    df_opp = _normalize_opportunity_columns(df_opp.copy()) if not df_opp.empty else df_opp
    return df_spend, df_lead, df_opp, None

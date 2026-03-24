-- ============================================================
-- 05 UNIFIED BY CHANNEL (one row per month × platform/channel)
-- Same metrics as 04 but grouped by datasourcetype/datasource; CW and leads at month level.
-- Use for: Performance by channel (Meta, Google, Snap, etc.).
-- ============================================================
-- BigQuery Console: run full script. Add date filter in WITH if needed.
-- ============================================================

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

WITH
arab_countries AS (
  SELECT market FROM UNNEST([
    'UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar'
  ]) AS market
),

spend_by_month_channel AS (
  SELECT
    DATE(DATE_TRUNC(date, MONTH)) AS unified_date,
    COALESCE(datasourcetype, datasource, 'Unknown') AS unified_channel,
    SUM(cost_usd) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
  FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
  WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
    AND country IN (SELECT market FROM arab_countries)
  GROUP BY 1, 2
),

cw_by_month_market AS (
  SELECT
    DATE(DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH)) AS unified_date,
    COALESCE(account_market__c, 'Unknown') AS market,
    COUNT(*) AS cw_inc_approved,
    SUM(COALESCE(tcv_realised__c, amount, 0)) AS actual_tcv
  FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
  WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
    AND iswon = TRUE
    AND approved__c = TRUE
    AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date)
    AND COALESCE(account_market__c, '') IN (SELECT market FROM arab_countries)
  GROUP BY 1, 2
),

leads_by_month_market AS (
  SELECT
    DATE(DATE_TRUNC(createddate, MONTH)) AS unified_date,
    COALESCE(country, 'Unknown') AS market,
    COUNT(*) AS total_leads,
    COUNTIF(qualified_date__c IS NOT NULL) AS qualified
  FROM `css-dw-sync.salesforce_cloudkitchens.lead`
  WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
    AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
    AND COALESCE(country, '') IN (SELECT market FROM arab_countries)
  GROUP BY 1, 2
),

cw_by_month AS (
  SELECT unified_date, SUM(cw_inc_approved) AS cw_inc_approved, SUM(actual_tcv) AS actual_tcv
  FROM cw_by_month_market
  GROUP BY 1
),

leads_by_month AS (
  SELECT unified_date, SUM(total_leads) AS total_leads, SUM(qualified) AS qualified
  FROM leads_by_month_market
  GROUP BY 1
)

SELECT
  s.unified_date,
  s.unified_channel,
  ROUND(COALESCE(s.spend, 0), 2) AS spend,
  COALESCE(c.cw_inc_approved, 0) AS cw_inc_approved,
  ROUND(COALESCE(c.actual_tcv, 0), 2) AS actual_tcv,
  COALESCE(l.total_leads, 0) AS total_leads,
  COALESCE(l.qualified, 0) AS qualified,
  ROUND(s.impressions, 0) AS impressions,
  ROUND(s.clicks, 0) AS clicks,
  ROUND(SAFE_DIVIDE(s.spend, NULLIF(c.cw_inc_approved, 0)), 2) AS cpcw,
  ROUND(SAFE_DIVIDE(s.spend, NULLIF(l.total_leads, 0)), 2) AS cpl,
  ROUND(100.0 * SAFE_DIVIDE(s.spend, NULLIF(c.actual_tcv, 0)), 2) AS cost_tcv_pct,
  ROUND(100.0 * SAFE_DIVIDE(l.qualified, NULLIF(l.total_leads, 0)), 2) AS sql_pct
FROM spend_by_month_channel s
LEFT JOIN cw_by_month c ON s.unified_date = c.unified_date
LEFT JOIN leads_by_month l ON s.unified_date = l.unified_date
ORDER BY s.unified_date DESC, s.unified_channel;

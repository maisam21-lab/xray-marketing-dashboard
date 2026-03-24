-- ============================================================
-- 04 UNIFIED BY MARKET (one row per month × market)
-- Joins Spend + CW (approved) + Leads; adds cpcw, cpl, cost_tcv_pct, sql_pct.
-- Use for: Looker Studio, exports, single-table reporting.
-- ============================================================
-- BigQuery Console: run full script (DECLARE + WITH + SELECT).
-- ============================================================

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

WITH
arab_countries AS (
  SELECT market FROM UNNEST([
    'UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar'
  ]) AS market
),

spend_by_month_market AS (
  SELECT
    DATE(DATE_TRUNC(date, MONTH)) AS unified_date,
    country AS market,
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
)

SELECT
  s.unified_date,
  s.market,
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
FROM spend_by_month_market s
LEFT JOIN cw_by_month_market c
  ON s.unified_date = c.unified_date AND s.market = c.market
LEFT JOIN leads_by_month_market l
  ON s.unified_date = l.unified_date AND s.market = l.market
ORDER BY s.unified_date DESC, s.market;

-- ============================================================
-- BIGQUERY → SAME DATA AS YOUR GOOGLE SHEET TABS
-- ============================================================
-- Markets: UAE, Kuwait, Saudi Arabia, Bahrain, Qatar
-- Run ONE block at a time in BigQuery Console (project: css-dw-sync).
-- Export each result to the matching Sheet tab.
-- See SHEET_TAB_TO_BIGQUERY.md for tab mapping and troubleshooting.
-- ============================================================

-- ############################################################################
-- BLOCK 1: SPEND (Platform data) – raw rows from funnel_data_join
-- ############################################################################
-- Export/save as: Spend tab or spend_raw.csv

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  date,
  country AS market,
  datasource AS platform,
  datasourcetype AS channel_type,
  currency,
  campaigntype,
  campaign,
  cost,
  cost_usd AS spend,
  impressions,
  clicks
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
  AND country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
ORDER BY date, country, datasource;


-- ############################################################################
-- BLOCK 2: LEAD – salesforce_cloudkitchens.lead
-- ############################################################################
-- Required: created_date, month_date, market, qualified_date.

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  id AS lead_id,
  createddate AS created_date,
  DATE_TRUNC(createddate, MONTH) AS month_date,
  COALESCE(country, 'Unknown') AS market,
  qualified_date__c AS qualified_date
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
  AND COALESCE(country, '') IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
ORDER BY createddate DESC, market;


-- ############################################################################
-- BLOCK 3: OPPORTUNITY (Post Lead) – salesforce_cloudkitchens.opportunity
-- ############################################################################
-- Required: created_date, month_date, close_date, tcv_usd, kitchen_country,
--          stage, opportunity_name, approved__c. Won only, MENA markets.
-- Market filter includes KSA/Saudi variants (account_market__c often stored as "KSA" not "Saudi Arabia").
-- kitchen_country is normalized so sheet shows "Saudi Arabia" for KSA.

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  id AS opportunity_id,
  createddate AS created_date,
  DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH) AS month_date,
  closedate AS close_date,
  closed_won_date__c AS closed_won_date,
  COALESCE(tcv_realised__c, amount, 0) AS tcv_usd,
  amount,
  tcv_realised__c,
  CASE
    WHEN UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI ARABIA', 'SAUDI') THEN 'Saudi Arabia'
    WHEN TRIM(COALESCE(account_market__c, '')) = '' THEN 'Unknown'
    ELSE TRIM(account_market__c)
  END AS kitchen_country,
  stagename AS stage,
  name AS opportunity_name,
  approved__c,
  iswon,
  isdeleted
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND iswon = TRUE
  AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date)
  AND (
    TRIM(COALESCE(account_market__c, '')) IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
    OR UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI', 'SAUDI ARABIA')
  )
ORDER BY closedate DESC, kitchen_country;


-- ############################################################################
-- BLOCK 3 MENA VIA ACCOUNT: Post Lead by Account country (use this for MENA sheet)
-- ############################################################################
-- account_market__c on Opportunity = region/city (e.g. Los Angeles), NOT country.
-- This block joins to Account and filters by Account.BillingCountry = MENA.
-- Run debug query 7 to see distinct BillingCountry values; adjust list if needed
-- (e.g. "United Arab Emirates" vs "UAE"). If Account table name differs, fix the JOIN.

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  o.id AS opportunity_id,
  o.createddate AS created_date,
  DATE_TRUNC(COALESCE(o.closed_won_date__c, o.closedate), MONTH) AS month_date,
  o.closedate AS close_date,
  o.closed_won_date__c AS closed_won_date,
  COALESCE(o.tcv_realised__c, o.amount, 0) AS tcv_usd,
  o.amount,
  o.tcv_realised__c,
  COALESCE(a.billingcountry, 'Unknown') AS kitchen_country,
  o.stagename AS stage,
  o.name AS opportunity_name,
  o.approved__c,
  o.iswon,
  o.isdeleted
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity` o
INNER JOIN `css-dw-sync.salesforce_cloudkitchens.account` a ON o.accountid = a.id
WHERE (o.isdeleted IS FALSE OR o.isdeleted IS NULL)
  AND o.iswon = TRUE
  AND (DATE(COALESCE(o.closed_won_date__c, o.closedate)) BETWEEN ds_start_date AND ds_end_date)
  AND TRIM(COALESCE(a.billingcountry, '')) IN (
    'United Arab Emirates', 'UAE',
    'Saudi Arabia', 'KSA',
    'Kuwait', 'Bahrain', 'Qatar'
  )
ORDER BY o.closedate DESC, kitchen_country;


-- ############################################################################
-- BLOCK 3 ALT: OPPORTUNITY – NO MARKET FILTER (use if Block 3 returns no data)
-- ############################################################################
-- Returns ALL won opportunities in the date range. Use for Post Lead tab when
-- account_market__c has unexpected values (cities, other countries). Filter
-- by kitchen_country in Google Sheets or add the values from debug query 3 to Block 3.
-- Same columns as Block 3; kitchen_country is raw account_market__c (no KSA normalization).

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  id AS opportunity_id,
  createddate AS created_date,
  DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH) AS month_date,
  closedate AS close_date,
  closed_won_date__c AS closed_won_date,
  COALESCE(tcv_realised__c, amount, 0) AS tcv_usd,
  amount,
  tcv_realised__c,
  account_market__c AS kitchen_country,
  stagename AS stage,
  name AS opportunity_name,
  approved__c,
  iswon,
  isdeleted
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND iswon = TRUE
  AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date)
ORDER BY closedate DESC, kitchen_country;


-- ############################################################################
-- BLOCK 4: UNIFIED BY MARKET (Sheet tab: Unified by Market)
-- ############################################################################
-- One row per month × market: spend, CW (approved), leads, cpcw, cpl, etc.
-- No impressions/clicks so it works even if funnel_data_join lacks those columns.
-- To add impressions/clicks, include in spend CTE: SUM(impressions), SUM(clicks)
-- and in final SELECT: ROUND(s.impressions,0), ROUND(s.clicks,0).

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

WITH
arab_countries AS (
  SELECT market FROM UNNEST(['UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar']) AS market
),
spend_by_month_market AS (
  SELECT
    DATE(DATE_TRUNC(date, MONTH)) AS unified_date,
    country AS market,
    SUM(cost_usd) AS spend
  FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
  WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
    AND country IN (SELECT market FROM arab_countries)
  GROUP BY 1, 2
),
cw_by_month_market AS (
  SELECT
    DATE(DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH)) AS unified_date,
    CASE
      WHEN UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI ARABIA', 'SAUDI') THEN 'Saudi Arabia'
      WHEN TRIM(COALESCE(account_market__c, '')) = '' THEN 'Unknown'
      ELSE TRIM(account_market__c)
    END AS market,
    COUNT(*) AS cw_inc_approved,
    SUM(COALESCE(tcv_realised__c, amount, 0)) AS actual_tcv
  FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
  WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
    AND iswon = TRUE
    AND approved__c = TRUE
    AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date)
    AND (
      TRIM(COALESCE(account_market__c, '')) IN (SELECT market FROM arab_countries)
      OR UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI', 'SAUDI ARABIA')
    )
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
  ROUND(COALESCE(s.impressions, 0), 0) AS impressions,
  ROUND(COALESCE(s.clicks, 0), 0) AS clicks,
  ROUND(SAFE_DIVIDE(s.spend, NULLIF(c.cw_inc_approved, 0)), 2) AS cpcw,
  ROUND(SAFE_DIVIDE(s.spend, NULLIF(l.total_leads, 0)), 2) AS cpl,
  ROUND(100.0 * SAFE_DIVIDE(s.spend, NULLIF(c.actual_tcv, 0)), 2) AS cost_tcv_pct,
  ROUND(100.0 * SAFE_DIVIDE(l.qualified, NULLIF(l.total_leads, 0)), 2) AS sql_pct
FROM spend_by_month_market s
LEFT JOIN cw_by_month_market c ON s.unified_date = c.unified_date AND s.market = c.market
LEFT JOIN leads_by_month_market l ON s.unified_date = l.unified_date AND s.market = l.market
ORDER BY s.unified_date DESC, s.market;


-- ############################################################################
-- BLOCK 4b: UNIFIED BY MARKET – no impressions/clicks
-- ############################################################################
-- Use this if Block 4 fails with "Unrecognized name: impressions" or "clicks".

-- DECLARE ds_start_date DATE DEFAULT '2025-01-01';
-- DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

-- WITH
-- arab_countries AS (
--   SELECT market FROM UNNEST(['UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar']) AS market
-- ),
-- spend_by_month_market AS (
--   SELECT
--     DATE(DATE_TRUNC(date, MONTH)) AS unified_date,
--     country AS market,
--     SUM(cost_usd) AS spend
--   FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
--   WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
--     AND country IN (SELECT market FROM arab_countries)
--   GROUP BY 1, 2
-- ),
-- cw_by_month_market AS (
--   SELECT
--     DATE(DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH)) AS unified_date,
--     CASE WHEN UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI ARABIA', 'SAUDI') THEN 'Saudi Arabia'
--          WHEN TRIM(COALESCE(account_market__c, '')) = '' THEN 'Unknown' ELSE TRIM(account_market__c) END AS market,
--     COUNT(*) AS cw_inc_approved,
--     SUM(COALESCE(tcv_realised__c, amount, 0)) AS actual_tcv
--   FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
--   WHERE (isdeleted IS FALSE OR isdeleted IS NULL) AND iswon = TRUE AND approved__c = TRUE
--     AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date)
--     AND (TRIM(COALESCE(account_market__c, '')) IN (SELECT market FROM arab_countries)
--          OR UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI', 'SAUDI ARABIA'))
--   GROUP BY 1, 2
-- ),
-- leads_by_month_market AS (
--   SELECT DATE(DATE_TRUNC(createddate, MONTH)) AS unified_date, COALESCE(country, 'Unknown') AS market,
--     COUNT(*) AS total_leads, COUNTIF(qualified_date__c IS NOT NULL) AS qualified
--   FROM `css-dw-sync.salesforce_cloudkitchens.lead`
--   WHERE (isdeleted IS FALSE OR isdeleted IS NULL) AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
--     AND COALESCE(country, '') IN (SELECT market FROM arab_countries)
--   GROUP BY 1, 2
-- )
-- SELECT s.unified_date, s.market,
--   ROUND(COALESCE(s.spend, 0), 2) AS spend,
--   COALESCE(c.cw_inc_approved, 0) AS cw_inc_approved,
--   ROUND(COALESCE(c.actual_tcv, 0), 2) AS actual_tcv,
--   COALESCE(l.total_leads, 0) AS total_leads,
--   COALESCE(l.qualified, 0) AS qualified,
--   ROUND(SAFE_DIVIDE(s.spend, NULLIF(c.cw_inc_approved, 0)), 2) AS cpcw,
--   ROUND(SAFE_DIVIDE(s.spend, NULLIF(l.total_leads, 0)), 2) AS cpl,
--   ROUND(100.0 * SAFE_DIVIDE(s.spend, NULLIF(c.actual_tcv, 0)), 2) AS cost_tcv_pct,
--   ROUND(100.0 * SAFE_DIVIDE(l.qualified, NULLIF(l.total_leads, 0)), 2) AS sql_pct
-- FROM spend_by_month_market s
-- LEFT JOIN cw_by_month_market c ON s.unified_date = c.unified_date AND s.market = c.market
-- LEFT JOIN leads_by_month_market l ON s.unified_date = l.unified_date AND s.market = l.market
-- ORDER BY s.unified_date DESC, s.market;


-- ############################################################################
-- BLOCK 5: UNIFIED BY CHANNEL (Sheet tab: Unified by Channel)
-- ############################################################################
-- One row per month × channel. If "Unrecognized name: impressions/clicks", use BLOCK 5b.

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

WITH
arab_countries AS (
  SELECT market FROM UNNEST(['UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar']) AS market
),
spend_by_month_channel AS (
  SELECT
    DATE(DATE_TRUNC(date, MONTH)) AS unified_date,
    COALESCE(datasourcetype, datasource, 'Unknown') AS unified_channel,
    SUM(cost_usd) AS spend,
    SUM(COALESCE(impressions, 0)) AS impressions,
    SUM(COALESCE(clicks, 0)) AS clicks
  FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
  WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
    AND country IN (SELECT market FROM arab_countries)
  GROUP BY 1, 2
),
cw_by_month_market AS (
  SELECT
    DATE(DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH)) AS unified_date,
    CASE WHEN UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI ARABIA', 'SAUDI') THEN 'Saudi Arabia'
         WHEN TRIM(COALESCE(account_market__c, '')) = '' THEN 'Unknown' ELSE TRIM(account_market__c) END AS market,
    COUNT(*) AS cw_inc_approved, SUM(COALESCE(tcv_realised__c, amount, 0)) AS actual_tcv
  FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
  WHERE (isdeleted IS FALSE OR isdeleted IS NULL) AND iswon = TRUE AND approved__c = TRUE
    AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date)
    AND (TRIM(COALESCE(account_market__c, '')) IN (SELECT market FROM arab_countries)
         OR UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI', 'SAUDI ARABIA'))
  GROUP BY 1, 2
),
leads_by_month_market AS (
  SELECT DATE(DATE_TRUNC(createddate, MONTH)) AS unified_date, COALESCE(country, 'Unknown') AS market,
    COUNT(*) AS total_leads, COUNTIF(qualified_date__c IS NOT NULL) AS qualified
  FROM `css-dw-sync.salesforce_cloudkitchens.lead`
  WHERE (isdeleted IS FALSE OR isdeleted IS NULL) AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
    AND COALESCE(country, '') IN (SELECT market FROM arab_countries)
  GROUP BY 1, 2
),
cw_by_month AS (SELECT unified_date, SUM(cw_inc_approved) AS cw_inc_approved, SUM(actual_tcv) AS actual_tcv FROM cw_by_month_market GROUP BY 1),
leads_by_month AS (SELECT unified_date, SUM(total_leads) AS total_leads, SUM(qualified) AS qualified FROM leads_by_month_market GROUP BY 1)
SELECT
  s.unified_date, s.unified_channel,
  ROUND(COALESCE(s.spend, 0), 2) AS spend,
  COALESCE(c.cw_inc_approved, 0) AS cw_inc_approved,
  ROUND(COALESCE(c.actual_tcv, 0), 2) AS actual_tcv,
  COALESCE(l.total_leads, 0) AS total_leads,
  COALESCE(l.qualified, 0) AS qualified,
  ROUND(COALESCE(s.impressions, 0), 0) AS impressions,
  ROUND(COALESCE(s.clicks, 0), 0) AS clicks,
  ROUND(SAFE_DIVIDE(s.spend, NULLIF(c.cw_inc_approved, 0)), 2) AS cpcw,
  ROUND(SAFE_DIVIDE(s.spend, NULLIF(l.total_leads, 0)), 2) AS cpl,
  ROUND(100.0 * SAFE_DIVIDE(s.spend, NULLIF(c.actual_tcv, 0)), 2) AS cost_tcv_pct,
  ROUND(100.0 * SAFE_DIVIDE(l.qualified, NULLIF(l.total_leads, 0)), 2) AS sql_pct
FROM spend_by_month_channel s
LEFT JOIN cw_by_month c ON s.unified_date = c.unified_date
LEFT JOIN leads_by_month l ON s.unified_date = l.unified_date
ORDER BY s.unified_date DESC, s.unified_channel;


-- ############################################################################
-- BLOCK 5b: UNIFIED BY CHANNEL – no impressions/clicks
-- ############################################################################
-- Uncomment and use if Block 5 fails on impressions/clicks.

-- DECLARE ds_start_date DATE DEFAULT '2025-01-01';
-- DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

-- WITH
-- arab_countries AS (SELECT market FROM UNNEST(['UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar']) AS market),
-- spend_by_month_channel AS (
--   SELECT DATE(DATE_TRUNC(date, MONTH)) AS unified_date,
--     COALESCE(datasourcetype, datasource, 'Unknown') AS unified_channel, SUM(cost_usd) AS spend
--   FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
--   WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date AND country IN (SELECT market FROM arab_countries)
--   GROUP BY 1, 2
-- ),
-- cw_by_month_market AS (
--   SELECT DATE(DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH)) AS unified_date,
--     CASE WHEN UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI ARABIA', 'SAUDI') THEN 'Saudi Arabia'
--          WHEN TRIM(COALESCE(account_market__c, '')) = '' THEN 'Unknown' ELSE TRIM(account_market__c) END AS market,
--     COUNT(*) AS cw_inc_approved, SUM(COALESCE(tcv_realised__c, amount, 0)) AS actual_tcv
--   FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
--   WHERE (isdeleted IS FALSE OR isdeleted IS NULL) AND iswon = TRUE AND approved__c = TRUE
--     AND (DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date)
--     AND (TRIM(COALESCE(account_market__c, '')) IN (SELECT market FROM arab_countries)
--          OR UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI', 'SAUDI ARABIA'))
--   GROUP BY 1, 2
-- ),
-- leads_by_month_market AS (
--   SELECT DATE(DATE_TRUNC(createddate, MONTH)) AS unified_date, COALESCE(country, 'Unknown') AS market,
--     COUNT(*) AS total_leads, COUNTIF(qualified_date__c IS NOT NULL) AS qualified
--   FROM `css-dw-sync.salesforce_cloudkitchens.lead`
--   WHERE (isdeleted IS FALSE OR isdeleted IS NULL) AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
--     AND COALESCE(country, '') IN (SELECT market FROM arab_countries)
--   GROUP BY 1, 2
-- ),
-- cw_by_month AS (SELECT unified_date, SUM(cw_inc_approved) AS cw_inc_approved, SUM(actual_tcv) AS actual_tcv FROM cw_by_month_market GROUP BY 1),
-- leads_by_month AS (SELECT unified_date, SUM(total_leads) AS total_leads, SUM(qualified) AS qualified FROM leads_by_month_market GROUP BY 1)
-- SELECT s.unified_date, s.unified_channel,
--   ROUND(COALESCE(s.spend, 0), 2) AS spend,
--   COALESCE(c.cw_inc_approved, 0) AS cw_inc_approved,
--   ROUND(COALESCE(c.actual_tcv, 0), 2) AS actual_tcv,
--   COALESCE(l.total_leads, 0) AS total_leads, COALESCE(l.qualified, 0) AS qualified,
--   ROUND(SAFE_DIVIDE(s.spend, NULLIF(c.cw_inc_approved, 0)), 2) AS cpcw,
--   ROUND(SAFE_DIVIDE(s.spend, NULLIF(l.total_leads, 0)), 2) AS cpl,
--   ROUND(100.0 * SAFE_DIVIDE(s.spend, NULLIF(c.actual_tcv, 0)), 2) AS cost_tcv_pct,
--   ROUND(100.0 * SAFE_DIVIDE(l.qualified, NULLIF(l.total_leads, 0)), 2) AS sql_pct
-- FROM spend_by_month_channel s
-- LEFT JOIN cw_by_month c ON s.unified_date = c.unified_date
-- LEFT JOIN leads_by_month l ON s.unified_date = l.unified_date
-- ORDER BY s.unified_date DESC, s.unified_channel;

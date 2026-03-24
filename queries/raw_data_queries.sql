-- ============================================================
-- MARKETING DASHBOARD – RAW DATA QUERIES (correct data for your dash)
-- ============================================================
-- Match: QUERY_SPEC.md (required columns and filters).
-- Run each block separately in BigQuery Console. Export → Sheet (Spend / Lead / Post Lead).
-- ============================================================
-- Date range: set ds_start_date, ds_end_date in each block.
-- Markets: UAE, Kuwait, Saudi Arabia, Bahrain, Qatar
-- ============================================================


-- ############################################################################
-- BLOCK 1: SPEND – funnel_data_join
-- ############################################################################
-- Required columns: date, market, platform, spend.
-- Optional: impressions, clicks (remove the two lines if your table doesn’t have them).

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  date,
  country AS market,
  datasource AS platform,
  cost_usd AS spend
  -- Optional: add if your table has these columns (uncomment one line at a time if needed):
  -- , impressions
  -- , clicks
  -- , datasourcetype AS channel_type
  -- , currency, campaigntype, campaign, cost
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
--          stage, opportunity_name, approved__c. Won only.
--
-- NOTE: account_market__c in this table holds city/region (e.g. Los Angeles,
-- SF Bay Area) or null—not country names like UAE/Kuwait/Saudi Arabia. So we
-- do NOT filter by MENA here. Filter to MENA in Looker Studio or your Sheet
-- (e.g. by region, or by another field that indicates MENA).

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

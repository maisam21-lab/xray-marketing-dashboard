-- ============================================================
-- VALIDATE WE'RE GETTING THE RIGHT DATA
-- ============================================================
-- Run ONE block at a time in BigQuery (project: css-dw-sync).
-- Copy from "DECLARE" through the ";" of the block you want. Same date range as your sheet.
-- See DATA_RIGHT_CHECKLIST.md for how to use results.
-- ============================================================


-- ############################################################################
-- V1: SPEND – funnel_data_join (row count, date range, total spend, markets)
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
-- Expect: row_count >= 1, markets only UAE/Kuwait/Saudi Arabia/Bahrain/Qatar,
--         min_date / max_date within your range, total_spend > 0.

SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT date) AS distinct_dates,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  ROUND(SUM(cost_usd), 2) AS total_spend_usd,
  COUNT(DISTINCT country) AS distinct_markets
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
  AND country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar');


-- ############################################################################
-- V1b: SPEND by market (sanity: only MENA?)
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT country AS market, COUNT(*) AS rows_, ROUND(SUM(cost_usd), 2) AS spend_usd
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
  AND country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
GROUP BY 1
ORDER BY 2 DESC;


-- ############################################################################
-- V2: LEAD – row count, date range, distinct markets
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
-- Expect: row_count >= 0. If 0, run debug_block2_lead.sql and fix country list.

SELECT
  COUNT(*) AS row_count,
  MIN(createddate) AS min_created,
  MAX(createddate) AS max_created,
  COUNT(DISTINCT country) AS distinct_markets
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
  AND COALESCE(country, '') IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar');


-- ############################################################################
-- V2b: LEAD by market
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT COALESCE(country, 'Unknown') AS market, COUNT(*) AS leads
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
  AND COALESCE(country, '') IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
GROUP BY 1
ORDER BY 2 DESC;


-- ############################################################################
-- V3: OPPORTUNITY by account_market__c (often 0 for MENA – that field is city/region)
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
-- Expect: row_count may be 0 (account_market__c = city/region, not country).

SELECT
  COUNT(*) AS row_count,
  MIN(COALESCE(closed_won_date__c, closedate)) AS min_close,
  MAX(COALESCE(closed_won_date__c, closedate)) AS max_close,
  ROUND(SUM(COALESCE(tcv_realised__c, amount, 0)), 2) AS total_tcv_usd
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity` o
WHERE (o.isdeleted IS FALSE OR o.isdeleted IS NULL)
  AND o.iswon = TRUE
  AND (DATE(COALESCE(o.closed_won_date__c, o.closedate)) BETWEEN ds_start_date AND ds_end_date)
  AND (
    TRIM(COALESCE(o.account_market__c, '')) IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
    OR UPPER(TRIM(COALESCE(o.account_market__c, ''))) IN ('KSA', 'SAUDI', 'SAUDI ARABIA')
  );


-- ############################################################################
-- V4: OPPORTUNITY by Account.BillingCountry (use this for MENA Post Lead)
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
-- Expect: row_count >= 0. If 0, run debug_block3 query 7 to see BillingCountry values.

SELECT
  COUNT(*) AS row_count,
  MIN(COALESCE(o.closed_won_date__c, o.closedate)) AS min_close,
  MAX(COALESCE(o.closed_won_date__c, o.closedate)) AS max_close,
  ROUND(SUM(COALESCE(o.tcv_realised__c, o.amount, 0)), 2) AS total_tcv_usd
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity` o
INNER JOIN `css-dw-sync.salesforce_cloudkitchens.account` a ON o.accountid = a.id
WHERE (o.isdeleted IS FALSE OR o.isdeleted IS NULL)
  AND o.iswon = TRUE
  AND (DATE(COALESCE(o.closed_won_date__c, o.closedate)) BETWEEN ds_start_date AND ds_end_date)
  AND TRIM(COALESCE(a.billingcountry, '')) IN (
    'United Arab Emirates', 'UAE', 'Saudi Arabia', 'KSA', 'Kuwait', 'Bahrain', 'Qatar'
  );


-- ############################################################################
-- V4b: POST LEAD by country (which MENA countries have data?)
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  COALESCE(a.billingcountry, 'Unknown') AS kitchen_country,
  COUNT(*) AS opps,
  ROUND(SUM(COALESCE(o.tcv_realised__c, o.amount, 0)), 2) AS tcv_usd
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity` o
INNER JOIN `css-dw-sync.salesforce_cloudkitchens.account` a ON o.accountid = a.id
WHERE (o.isdeleted IS FALSE OR o.isdeleted IS NULL)
  AND o.iswon = TRUE
  AND (DATE(COALESCE(o.closed_won_date__c, o.closedate)) BETWEEN ds_start_date AND ds_end_date)
  AND TRIM(COALESCE(a.billingcountry, '')) IN (
    'United Arab Emirates', 'UAE', 'Saudi Arabia', 'KSA', 'Kuwait', 'Bahrain', 'Qatar'
  )
GROUP BY 1
ORDER BY 2 DESC;


-- ############################################################################
-- V5: SANITY – date range by source (Spend, Lead, Opp)
-- ############################################################################
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
-- Quick check: compare min/max dates across Spend, Lead, Opportunity (Account).
-- They don't have to match exactly but should overlap your ds_start_date / ds_end_date.

SELECT 'Spend' AS source, MIN(date) AS min_d, MAX(date) AS max_d
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
  AND country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
UNION ALL
SELECT 'Lead', MIN(createddate), MAX(createddate)
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND DATE(createddate) BETWEEN ds_start_date AND ds_end_date
  AND COALESCE(country, '') IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
UNION ALL
SELECT 'Opp (Account)', MIN(COALESCE(o.closed_won_date__c, o.closedate)), MAX(COALESCE(o.closed_won_date__c, o.closedate))
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity` o
INNER JOIN `css-dw-sync.salesforce_cloudkitchens.account` a ON o.accountid = a.id
WHERE (o.isdeleted IS FALSE OR o.isdeleted IS NULL) AND o.iswon = TRUE
  AND (DATE(COALESCE(o.closed_won_date__c, o.closedate)) BETWEEN ds_start_date AND ds_end_date)
  AND TRIM(COALESCE(a.billingcountry, '')) IN (
    'United Arab Emirates', 'UAE', 'Saudi Arabia', 'KSA', 'Kuwait', 'Bahrain', 'Qatar'
  );

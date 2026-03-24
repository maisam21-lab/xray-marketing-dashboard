-- ============================================================
-- DEBUG: Why does Block 3 (Opportunity) return no data?
-- Run each query below SEPARATELY in BigQuery. Use results to fix Block 3.
--
-- FINDING: account_market__c = region/city (Los Angeles, SF Bay Area, null),
-- NOT country. So filtering by UAE/KSA on Opportunity returns 0 rows.
-- Use "Block 3 MENA via Account" (join to Account, filter by BillingCountry)
-- or "Block 3 ALT" (all won opps, filter in Sheet).
-- ============================================================

-- 0) Table size and isdeleted – does the table have rows at all?
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(isdeleted IS FALSE OR isdeleted IS NULL) AS not_deleted,
  COUNTIF(isdeleted = TRUE) AS deleted
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`;


-- 1) How is iswon stored? (TRUE/false vs string – use the EXACT value in Block 3)
SELECT iswon, COUNT(*) AS cnt
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE isdeleted IS FALSE OR isdeleted IS NULL
GROUP BY 1
ORDER BY 2 DESC;


-- 2) Won opportunities count (same filter as Block 3 but NO market, NO date filter)
--    If this is 0, the issue is iswon or isdeleted, not market/date.
SELECT COUNT(*) AS won_count_no_filters
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND iswon = TRUE;


-- 3) Distinct account_market__c for WON opportunities
--    → Add any MENA-related values you see to Block 3's WHERE (or use Block 3 ALT).
SELECT
  account_market__c AS market_value,
  COUNT(*) AS cnt
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND iswon = TRUE
GROUP BY 1
ORDER BY 2 DESC;


-- 4) Date range of won opportunities (what dates exist?)
SELECT
  MIN(COALESCE(closed_won_date__c, closedate)) AS min_close_date,
  MAX(COALESCE(closed_won_date__c, closedate)) AS max_close_date,
  COUNT(*) AS total_won
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND iswon = TRUE;


-- 5) Sample: all won opps, NO market filter, NO date filter
--    If this returns rows → use Block 3 ALT for Post Lead tab, then filter by market in Sheet.
SELECT
  id,
  account_market__c AS kitchen_country,
  closedate,
  COALESCE(closed_won_date__c, closedate) AS close_won_date,
  COALESCE(tcv_realised__c, amount, 0) AS tcv_usd,
  stagename,
  name
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity`
WHERE (isdeleted IS FALSE OR isdeleted IS NULL)
  AND iswon = TRUE
  AND COALESCE(closed_won_date__c, closedate) IS NOT NULL
ORDER BY closedate DESC
LIMIT 50;


-- 7) Account country for WON opportunities (run only if you have Account table)
--    If this returns UAE/KSA etc., use "Block 3 MENA via Account" in bigquery_for_sheet_tabs.sql
SELECT
  a.billingcountry AS account_country,
  COUNT(*) AS cnt
FROM `css-dw-sync.salesforce_cloudkitchens.opportunity` o
JOIN `css-dw-sync.salesforce_cloudkitchens.account` a ON o.accountid = a.id
WHERE (o.isdeleted IS FALSE OR o.isdeleted IS NULL)
  AND o.iswon = TRUE
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50;

-- ============================================================
-- DEBUG: Why does Block 2 (Lead) return no data?
-- Run each query below SEPARATELY in BigQuery.
-- ============================================================

-- 1) Total leads and isdeleted
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(isdeleted IS FALSE OR isdeleted IS NULL) AS not_deleted
FROM `css-dw-sync.salesforce_cloudkitchens.lead`;


-- 2) Distinct country values (use these exact values in Block 2's IN list if different)
SELECT
  country AS market_value,
  COUNT(*) AS cnt
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE isdeleted IS FALSE OR isdeleted IS NULL
GROUP BY 1
ORDER BY 2 DESC;


-- 3) Date range of leads
SELECT
  MIN(createddate) AS min_created,
  MAX(createddate) AS max_created,
  COUNT(*) AS total
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE isdeleted IS FALSE OR isdeleted IS NULL;


-- 4) Sample: all leads, NO country and NO date filter
SELECT id, createddate, country, qualified_date__c
FROM `css-dw-sync.salesforce_cloudkitchens.lead`
WHERE isdeleted IS FALSE OR isdeleted IS NULL
ORDER BY createddate DESC
LIMIT 50;

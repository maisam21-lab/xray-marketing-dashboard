-- ============================================================
-- 02 LEAD (Lead data)
-- Source: salesforce_cloudkitchens.lead
-- Use for: Lead tab, total leads + qualified by market/date.
-- ============================================================
-- BigQuery Console: run DECLARE + SELECT.
-- App: uses @ds_start_date, @ds_end_date parameters.
-- ============================================================

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

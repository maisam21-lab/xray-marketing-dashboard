-- ============================================================
-- 03 OPPORTUNITY (Post Lead – won opportunities in SF)
-- Source: salesforce_cloudkitchens.opportunity
-- Use for: Post Lead tab, TCV analysis, CW count (filter approved__c in app or in 04).
-- Market filter includes KSA/Saudi so account_market__c stored as "KSA" returns rows.
-- ============================================================
-- BigQuery Console: run DECLARE + SELECT.
-- App: uses @ds_start_date, @ds_end_date parameters.
-- ============================================================

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

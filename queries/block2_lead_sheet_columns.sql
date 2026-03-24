-- ============================================================
-- LEAD – Team sheet headers (MENA)
-- ============================================================
-- Headers: Create Date, First Name, Last Name, Company / Account, Email,
-- Lead Source, Lead Owner, Lead Source Detail, UTM*, Lead Status, Market,
-- Original Lead Source Detail, Month, Unified Channel, Unified Date,
-- Is_Qualified, Is_CNC, Is_New, Is_Working
--
-- 1) Run discover_post_lead_columns.sql (lead section) for custom API names.
-- 2) Replace CAST(NULL AS …) with real columns when you find them.
-- 3) Tune Is_CNC / Is_New / Is_Working REGEXP to match your Lead Status picklist.
-- Project: css-dw-sync
-- ============================================================

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

SELECT
  l.createddate AS `Create Date`,
  l.firstname AS `First Name`,
  l.lastname AS `Last Name`,
  l.company AS `Company / Account`,
  l.email AS `Email`,
  l.leadsource AS `Lead Source`,
  u.name AS `Lead Owner`,

  CAST(NULL AS STRING) AS `Lead Source Detail`,
  CAST(NULL AS STRING) AS `UTM Source`,
  CAST(NULL AS STRING) AS `UTM Medium`,
  CAST(NULL AS STRING) AS `UTM Campaign`,
  CAST(NULL AS STRING) AS `UTM Content`,

  l.status AS `Lead Status`,
  COALESCE(l.country, 'Unknown') AS `Market`,

  CAST(NULL AS STRING) AS `Original Lead Source Detail`,

  DATE_TRUNC(l.createddate, MONTH) AS `Month`,
  CAST(NULL AS STRING) AS `Unified Channel`,
  DATE(l.createddate) AS `Unified Date`,

  (l.qualified_date__c IS NOT NULL) AS `Is_Qualified`,

  -- CNC: adjust to your org (e.g. status = 'Closed - Not Converted', disqualified, junk)
  (
    REGEXP_CONTAINS(LOWER(COALESCE(l.status, '')), r'closed.*not converted|unqualified|disqualified|junk|duplicate')
  ) AS `Is_CNC`,

  (LOWER(TRIM(COALESCE(l.status, ''))) = 'new') AS `Is_New`,

  REGEXP_CONTAINS(LOWER(COALESCE(l.status, '')), r'working|contacted|engaged|open') AS `Is_Working`

FROM `css-dw-sync.salesforce_cloudkitchens.lead` l
LEFT JOIN `css-dw-sync.salesforce_cloudkitchens.user` u
  ON l.ownerid = u.id
WHERE (l.isdeleted IS FALSE OR l.isdeleted IS NULL)
  AND DATE(l.createddate) BETWEEN ds_start_date AND ds_end_date
  AND COALESCE(l.country, '') IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
ORDER BY l.createddate DESC, l.country;


-- ============================================================
-- OPTIONAL: Map NULL columns after INFORMATION_SCHEMA on `lead` table
-- ============================================================
-- Common Salesforce API names (use only if they exist):
--   l.lead_source_detail__c AS `Lead Source Detail`,
--   l.utm_source__c AS `UTM Source`,
--   l.utm_medium__c AS `UTM Medium`,
--   l.utm_campaign__c AS `UTM Campaign`,
--   l.utm_content__c AS `UTM Content`,
--   l.original_lead_source_detail__c AS `Original Lead Source Detail`,
--   l.unified_channel__c AS `Unified Channel`,
-- If `user` join fails: CAST(l.ownerid AS STRING) AS `Lead Owner`

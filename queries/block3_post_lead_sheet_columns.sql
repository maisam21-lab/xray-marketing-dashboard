-- ============================================================
-- POST LEAD – Sheet column layout (MENA via Account)
-- ============================================================
-- Matches headers: Opportunity Owner, Account Name, Opportunity Name,
-- Market, Close Date, Created Date, Lead Source, First Lead Created Date,
-- UTM fields, Stage, license/TCV fields, Deal Type, Unified*, Month,
-- flags Is_CW, Is_Qualifying, …, 1st Month LF, Actual TCV.
--
-- 1) Run discover_post_lead_columns.sql and replace CAST(NULL AS …) lines
--    below with real column names from Opportunity / Lead when you find them.
-- 2) If `user` table or join fails, use o.ownerid AS `Opportunity Owner` temporarily.
-- 3) Date filter: adjust WHERE (created vs closed vs all open) to match your sheet.
-- Project: css-dw-sync
-- ============================================================

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();

WITH
lead_first AS (
  SELECT
    convertedopportunityid AS opp_id,
    MIN(createddate) AS first_lead_created_date
  FROM `css-dw-sync.salesforce_cloudkitchens.lead`
  WHERE convertedopportunityid IS NOT NULL
    AND (isdeleted IS FALSE OR isdeleted IS NULL)
  GROUP BY 1
)

SELECT
  u.name AS `Opportunity Owner`,
  a.name AS `Account Name`,
  o.name AS `Opportunity Name`,
  COALESCE(a.billingcountry, 'Unknown') AS `Market`,
  o.closedate AS `Close Date`,
  o.createddate AS `Created Date`,
  o.leadsource AS `Lead Source`,
  lf.first_lead_created_date AS `First Lead Created Date`,

  -- Replace NULLs after discover_post_lead_columns.sql finds the real API names:
  CAST(NULL AS STRING) AS `Lead Source Detail`,
  CAST(NULL AS STRING) AS `Original Lead Source`,
  CAST(NULL AS STRING) AS `UTM Source`,
  CAST(NULL AS STRING) AS `UTM Medium`,
  CAST(NULL AS STRING) AS `UTM Campaign`,
  CAST(NULL AS STRING) AS `UTM Content`,
  CAST(NULL AS STRING) AS `UTM Term`,

  o.stagename AS `Stage`,

  CAST(NULL AS FLOAT64) AS `Monthly License Fee`,
  CAST(NULL AS FLOAT64) AS `Monthly License Fee (converted)`,
  CAST(NULL AS INT64) AS `License Initial Term (Months)`,

  -- "TCV (converted)" – often company-currency TCV; adjust field if you have one:
  COALESCE(o.amount, o.tcv_realised__c) AS `TCV (converted)`,

  o.type AS `Deal Type`,

  CAST(NULL AS STRING) AS `Unified Channel`,
  DATE(COALESCE(o.closedate, o.createddate)) AS `Unified Date`,
  DATE_TRUNC(COALESCE(o.closedate, o.createddate), MONTH) AS `Month`,

  -- Monthly LF in USD – wire to your USD field when known:
  CAST(NULL AS FLOAT64) AS `Monthly LF USD`,

  COALESCE(o.tcv_realised__c, o.amount, 0) AS `TCV USD`,

  (o.iswon IS TRUE) AS `Is_CW`,
  REGEXP_CONTAINS(LOWER(COALESCE(o.stagename, '')), r'qualif') AS `Is_Qualifying`,
  REGEXP_CONTAINS(LOWER(COALESCE(o.stagename, '')), r'pitch') AS `Is_Pitching`,
  REGEXP_CONTAINS(LOWER(COALESCE(o.stagename, '')), r'negotiat') AS `Is_Negotiation`,
  REGEXP_CONTAINS(LOWER(COALESCE(o.stagename, '')), r'commit') AS `Is_Commitment`,
  (
    (o.isclosed IS TRUE AND (o.iswon IS FALSE OR o.iswon IS NULL))
    OR REGEXP_CONTAINS(LOWER(COALESCE(o.stagename, '')), r'closed\s*lost')
  ) AS `Is_ClosedLost`,

  CAST(NULL AS FLOAT64) AS `1st Month LF`,
  o.tcv_realised__c AS `Actual TCV`

FROM `css-dw-sync.salesforce_cloudkitchens.opportunity` o
INNER JOIN `css-dw-sync.salesforce_cloudkitchens.account` a
  ON o.accountid = a.id
LEFT JOIN `css-dw-sync.salesforce_cloudkitchens.user` u
  ON o.ownerid = u.id
LEFT JOIN lead_first lf
  ON lf.opp_id = o.id

WHERE (o.isdeleted IS FALSE OR o.isdeleted IS NULL)
  AND TRIM(COALESCE(a.billingcountry, '')) IN (
    'United Arab Emirates', 'UAE',
    'Saudi Arabia', 'KSA',
    'Kuwait', 'Bahrain', 'Qatar'
  )
  -- Population: created OR closed in window (add open pipeline if sheet includes it):
  AND (
    DATE(o.createddate) BETWEEN ds_start_date AND ds_end_date
    OR DATE(o.closedate) BETWEEN ds_start_date AND ds_end_date
    -- OR (o.isclosed IS NOT TRUE OR o.isclosed IS NULL)
  )

ORDER BY o.closedate DESC, o.createddate DESC;


-- ============================================================
-- OPTIONAL: WON-ONLY (same as earlier Block 3 MENA via Account)
-- ============================================================
-- Replace the WHERE above with:
--   AND (o.isdeleted IS FALSE OR o.isdeleted IS NULL)
--   AND o.iswon = TRUE
--   AND (DATE(COALESCE(o.closed_won_date__c, o.closedate)) BETWEEN ds_start_date AND ds_end_date)
--   AND [same MENA billingcountry IN list]

-- ============================================================
-- OPTIONAL: Map NULL columns after discover_post_lead_columns.sql
-- ============================================================
-- Example replacements (use only if those columns exist on opportunity):
--   o.lead_source_detail__c AS `Lead Source Detail`,
--   o.original_lead_source__c AS `Original Lead Source`,
--   o.utm_source__c AS `UTM Source`,
--   o.utm_medium__c AS `UTM Medium`,
--   o.utm_campaign__c AS `UTM Campaign`,
--   o.utm_content__c AS `UTM Content`,
--   o.utm_term__c AS `UTM Term`,
--   o.monthly_license_fee__c AS `Monthly License Fee`,
--   o.monthly_license_fee_converted__c AS `Monthly License Fee (converted)`,
--   o.license_initial_term__c AS `License Initial Term (Months)`,
--   o.first_month_license_fee__c AS `1st Month LF`,
-- Or pull UTM from converted Lead via extra JOIN / ANY_VALUE in lead_first CTE.

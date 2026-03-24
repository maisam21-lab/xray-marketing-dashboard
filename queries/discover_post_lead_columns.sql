-- ============================================================
-- Discover column names for Post Lead export (run in BigQuery)
-- ============================================================
-- Use results to fix names in block3_post_lead_sheet_columns.sql if you get
-- "Unrecognized name" errors. Project: css-dw-sync
-- ============================================================

-- Opportunity columns (search for utm, license, lead, deal, term, tcv, month)
SELECT 'opportunity' AS tbl, column_name, data_type
FROM `css-dw-sync.salesforce_cloudkitchens.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'opportunity'
  AND (
    LOWER(column_name) LIKE '%utm%'
    OR LOWER(column_name) LIKE '%license%'
    OR LOWER(column_name) LIKE '%lead%'
    OR LOWER(column_name) LIKE '%deal%'
    OR LOWER(column_name) LIKE '%term%'
    OR LOWER(column_name) LIKE '%tcv%'
    OR LOWER(column_name) LIKE '%unified%'
    OR LOWER(column_name) LIKE '%channel%'
    OR LOWER(column_name) IN ('leadsource', 'type', 'stagename', 'amount', 'ownerid', 'accountid')
  )
ORDER BY column_name;

-- Account columns (market / country)
SELECT 'account' AS tbl, column_name, data_type
FROM `css-dw-sync.salesforce_cloudkitchens.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'account'
  AND (
    LOWER(column_name) LIKE '%country%'
    OR LOWER(column_name) LIKE '%market%'
    OR LOWER(column_name) = 'name'
  )
ORDER BY column_name;

-- User (owner name)
SELECT 'user' AS tbl, column_name, data_type
FROM `css-dw-sync.salesforce_cloudkitchens.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'user'
  AND LOWER(column_name) IN ('id', 'name', 'firstname', 'lastname', 'email')
ORDER BY column_name;

-- Lead (first touch / UTM on conversion)
SELECT 'lead' AS tbl, column_name, data_type
FROM `css-dw-sync.salesforce_cloudkitchens.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'lead'
  AND (
    LOWER(column_name) LIKE '%utm%'
    OR LOWER(column_name) LIKE '%lead%'
    OR LOWER(column_name) LIKE '%source%'
    OR LOWER(column_name) = 'convertedopportunityid'
    OR LOWER(column_name) = 'createddate'
  )
ORDER BY column_name;

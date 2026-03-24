DECLARE ds_start_date DATE DEFAULT '2025-09-01';
DECLARE ds_end_date DATE DEFAULT '2026-02-28';
DECLARE use_campaign_market_fallback BOOL DEFAULT TRUE;
DECLARE mena_countries ARRAY<STRING> DEFAULT ['UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain'];

CREATE TEMP FUNCTION campaign_fallback_country(campaign STRING)
RETURNS STRING AS (
  CASE
    WHEN campaign IS NULL OR TRIM(campaign) = '' THEN NULL
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'^bh\s*\|') THEN 'Bahrain'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'^kw\s*\|') THEN 'Kuwait'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'^sa\s*\(') THEN 'Saudi Arabia'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'^sa\s*\|') THEN 'Saudi Arabia'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'^uae\s*\(') THEN 'UAE'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'^uae\s*\|') THEN 'UAE'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'^uae\s*&') THEN 'UAE'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'\^kw\^') THEN 'Kuwait'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'\^sa\^') THEN 'Saudi Arabia'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'\^uae\^') THEN 'UAE'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'_kw_en|_kw_ar') THEN 'Kuwait'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'_ksa_en|_ksa_ar|_ksa(?:_|$)') THEN 'Saudi Arabia'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'_uae_en|_uae_ar') THEN 'UAE'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'\bkuwait\b') THEN 'Kuwait'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'\buae\b') THEN 'UAE'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'\bsaudi arabia\b|\briyadh\b|\bjeddah\b|\bksa\b') THEN 'Saudi Arabia'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'_bh(?:_|$)|\^bh\^') THEN 'Bahrain'
    WHEN REGEXP_CONTAINS(LOWER(TRIM(campaign)), r'bahrain') THEN 'Bahrain'
    ELSE NULL
  END
);

-- Month × market spend vs Looker (DECLARE first, then temp function). Project: css-dw-sync

WITH
raw AS (
  SELECT
    DATE(date) AS d,
    COALESCE(NULLIF(TRIM(country), ''), campaign_fallback_country(campaign)) AS country,
    cost_usd,
    COALESCE(NULLIF(TRIM(campaign), ''), CONCAT('(', COALESCE(datasource, 'unknown'), ')')) AS campaign_key
  FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
  WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
    AND (
      NULLIF(TRIM(country), '') IN UNNEST(mena_countries)
      OR (
        use_campaign_market_fallback
        AND COALESCE(TRIM(country), '') = ''
        AND campaign_fallback_country(campaign) IS NOT NULL
      )
    )
),
filtered AS (
  SELECT d, country, cost_usd
  FROM raw
  WHERE NOT REGEXP_CONTAINS(campaign_key, r'(?i)MILAN-IT')
)
SELECT
  FORMAT_DATE('%Y-%m', d) AS ym,
  country AS market,
  ROUND(SUM(cost_usd), 2) AS spend_usd,
  COUNT(*) AS raw_rows
FROM filtered
GROUP BY 1, 2
ORDER BY 1, 2;

-- ---------------------------------------------------------------------------
-- Optional second run: monthly total only (one row per month)
-- ---------------------------------------------------------------------------
-- WITH raw AS ( ... same as above ... )
-- SELECT FORMAT_DATE('%Y-%m', d) AS ym, ROUND(SUM(cost_usd), 2) AS spend_usd
-- FROM raw GROUP BY 1 ORDER BY 1;

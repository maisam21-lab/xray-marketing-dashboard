DECLARE ds_start_date DATE DEFAULT '2025-09-01';
DECLARE ds_end_date DATE DEFAULT '2026-02-28';
DECLARE exclude_xray_orphans BOOL DEFAULT TRUE;
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

-- ONE ROW: totals vs Xray (DECLARE must be first in BigQuery; then temp function; then query)
-- Project: css-dw-sync

WITH
base AS (
  SELECT
    DATE_TRUNC(DATE(date), MONTH) AS month_start,
    COALESCE(NULLIF(TRIM(country), ''), campaign_fallback_country(campaign)) AS country,
    LOWER(TRIM(COALESCE(datasource, ''))) AS ds,
    LOWER(TRIM(COALESCE(datasourcetype, ''))) AS dst,
    LOWER(TRIM(COALESCE(campaigntype, ''))) AS ctype,
    LOWER(TRIM(COALESCE(campaign, ''))) AS camp,
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
with_channel AS (
  SELECT
    cost_usd,
    campaign_key,
    CASE
      WHEN ds LIKE '%facebook%' OR ds LIKE '%meta%' OR dst LIKE '%facebook%' OR dst LIKE '%meta%' THEN 'Meta'
      WHEN ds LIKE '%snap%' OR dst LIKE '%snap%' THEN 'Snapchat'
      WHEN ds LIKE '%linkedin%' OR dst LIKE '%linkedin%' THEN 'LinkedIn'
      WHEN ds LIKE '%adwords%' OR ds LIKE '%google%' OR ds LIKE '%google_ads%' THEN
        CASE
          WHEN ctype LIKE '%performance%' OR ctype LIKE '%pmax%'
            OR camp LIKE '%performance max%' OR camp LIKE '%pmax%' THEN 'PMax'
          ELSE 'Google Search'
        END
      WHEN ds LIKE '%organic%' OR dst LIKE '%organic%' THEN
        CASE WHEN camp LIKE '%instagram%' OR ctype LIKE '%instagram%' THEN 'Instagram Organic' ELSE 'Organic' END
      WHEN camp LIKE '%test%' OR ctype LIKE '%test%' THEN 'Test'
      WHEN camp LIKE '%ai search%' OR ctype LIKE '%ai%' THEN 'Ai Search'
      WHEN camp LIKE '%alta%' OR ctype LIKE '%alta%' THEN 'Alta Ai'
      WHEN camp LIKE '%express%kitchen%' THEN 'Express Kitchens'
      ELSE INITCAP(REPLACE(COALESCE(NULLIF(ds, ''), NULLIF(dst, ''), 'unknown'), '_', ' '))
    END AS unified_channel
  FROM base
  WHERE NOT exclude_xray_orphans OR NOT REGEXP_CONTAINS(campaign_key, r'(?i)MILAN-IT')
)
SELECT
  ds_start_date AS period_start,
  ds_end_date AS period_end,
  exclude_xray_orphans AS milan_excluded,
  ROUND(SUM(cost_usd), 2) AS total_match_mirror_export,
  ROUND(SUM(IF(unified_channel IN ('Meta', 'Google Search', 'PMax'), cost_usd, 0)), 2) AS total_meta_google_pmax_only,
  ROUND(SUM(IF(unified_channel IN ('Meta', 'Google Search'), cost_usd, 0)), 2) AS total_meta_google_search_only,
  COUNT(*) AS raw_line_rows_after_milan_filter
FROM with_channel;

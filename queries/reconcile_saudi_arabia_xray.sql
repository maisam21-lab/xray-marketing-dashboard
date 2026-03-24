-- =============================================================================
-- SAUDI ARABIA — reconcile BigQuery vs ME X-Ray (Spend tab)
-- =============================================================================
-- Grain: calendar month × unified_channel (same rules as block1_spend_xray_mirror.sql)
-- Market: effective country = 'Saudi Arabia' (warehouse country OR NULL + campaign fallback)
-- Excludes: MILAN-IT in campaign_key (same as mirror)
--
-- X-Ray reference totals (non-zero lines only, from your sheet — verify in Excel):
--   Sept 2025  ~4,612  |  Oct ~6,088  |  Nov ~6,427  |  Dec ~6,679
--   Jan 2026   ~6,467  |  Feb ~5,934  |  Period ~36,207 USD
--
-- After you run this: pivot spend_usd by year_month × unified_channel in Sheets
-- and subtract X-Ray for each cell — largest deltas = where to fix mapping or ETL.
--
-- Project: css-dw-sync
-- =============================================================================

DECLARE ds_start_date DATE DEFAULT '2025-09-01';
DECLARE ds_end_date DATE DEFAULT '2026-02-28';
DECLARE exclude_xray_orphans BOOL DEFAULT TRUE;
DECLARE use_campaign_market_fallback BOOL DEFAULT TRUE;
DECLARE saudi_shared_meta_allocation_rate FLOAT64 DEFAULT 0.263;
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

WITH
base AS (
  SELECT
    DATE_TRUNC(DATE(date), MONTH) AS month_start,
    NULLIF(TRIM(country), '') AS raw_country,
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
    month_start,
    raw_country,
    country,
    campaign_key,
    camp,
    cost_usd,
    CASE
      WHEN ds LIKE '%facebook%' OR ds LIKE '%meta%' OR dst LIKE '%facebook%' OR dst LIKE '%meta%'
        THEN 'Meta'
      WHEN ds LIKE '%snap%' OR dst LIKE '%snap%'
        THEN 'Snapchat'
      WHEN ds LIKE '%linkedin%' OR dst LIKE '%linkedin%'
        THEN 'LinkedIn'
      WHEN ds LIKE '%adwords%' OR ds LIKE '%google%' OR ds LIKE '%google_ads%'
        THEN
          CASE
            WHEN ctype LIKE '%performance%' OR ctype LIKE '%pmax%'
              OR camp LIKE '%performance max%' OR camp LIKE '%pmax%'
              THEN 'PMax'
            ELSE 'Google Search'
          END
      WHEN ds LIKE '%organic%' OR dst LIKE '%organic%'
        THEN CASE WHEN camp LIKE '%instagram%' OR ctype LIKE '%instagram%' THEN 'Instagram Organic' ELSE 'Organic' END
      WHEN camp LIKE '%test%' OR ctype LIKE '%test%'
        THEN 'Test'
      WHEN camp LIKE '%ai search%' OR ctype LIKE '%ai%'
        THEN 'Ai Search'
      WHEN camp LIKE '%alta%' OR ctype LIKE '%alta%'
        THEN 'Alta Ai'
      WHEN camp LIKE '%express%kitchen%'
        THEN 'Express Kitchens'
      ELSE INITCAP(REPLACE(COALESCE(NULLIF(ds, ''), NULLIF(dst, ''), 'unknown'), '_', ' '))
    END AS unified_channel
  FROM base
  WHERE (
      country = 'Saudi Arabia'
      OR (
        raw_country IS NULL
        AND (
          camp LIKE '%engagement_post boost%'
          OR camp LIKE '%all market (engagement)%'
        )
      )
    )
    AND (NOT exclude_xray_orphans OR NOT REGEXP_CONTAINS(campaign_key, r'(?i)MILAN-IT'))
)
SELECT
  FORMAT_DATE('%Y-%m', month_start) AS year_month,
  EXTRACT(MONTH FROM month_start) AS month_num,
  unified_channel,
  ROUND(SUM(
    CASE
      WHEN unified_channel = 'Meta'
        AND raw_country IS NULL
        AND (
          camp LIKE '%engagement_post boost%'
          OR camp LIKE '%all market (engagement)%'
        )
        THEN cost_usd * saudi_shared_meta_allocation_rate
      ELSE cost_usd
    END
  ), 2) AS spend_usd,
  COUNT(*) AS bq_row_count
FROM with_channel
GROUP BY 1, 2, 3
ORDER BY year_month, unified_channel;

-- -----------------------------------------------------------------------------
-- Optional: same as above but STRICT warehouse country only (no NULL fallback).
-- Replace the with_channel CTE filter with:
--   WHERE NULLIF(TRIM(country), '') = 'Saudi Arabia'
-- and set use_campaign_market_fallback unused for base (or use a duplicate script).
-- -----------------------------------------------------------------------------

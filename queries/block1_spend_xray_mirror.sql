DECLARE ds_start_date DATE DEFAULT '2025-09-01';
DECLARE ds_end_date DATE DEFAULT '2026-02-28';
DECLARE exclude_xray_orphans BOOL DEFAULT TRUE;
DECLARE use_data_first_month BOOL DEFAULT FALSE;
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

-- SPEND – Mirror Xray (DECLARE first, then CREATE TEMP FUNCTION, then query — BigQuery rule)
-- Output: Month Name | Market | Unified Channel | Spend | Month | Unified Date
-- CAMPAIGN_NULL_COUNTRY_FALLBACK.md | Project: css-dw-sync

WITH
-- First month with warehouse activity (MENA, in window); NULL if no rows
data_first_month AS (
  SELECT MIN(DATE_TRUNC(DATE(date), MONTH)) AS first_m
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
-- Month grid for scaffold + zeros (1st of each month)
dim_month AS (
  SELECT d AS month_start
  FROM data_first_month dfm,
  UNNEST(
    GENERATE_DATE_ARRAY(
      CASE
        WHEN use_data_first_month THEN GREATEST(
          DATE_TRUNC(ds_start_date, MONTH),
          COALESCE(dfm.first_m, DATE_TRUNC(ds_start_date, MONTH))
        )
        ELSE DATE_TRUNC(ds_start_date, MONTH)
      END,
      DATE_TRUNC(ds_end_date, MONTH),
      INTERVAL 1 MONTH
    )
  ) AS d
),
dim_market AS (
  SELECT market
  FROM UNNEST(mena_countries) AS market
),
-- Same channel list as typical Xray matrix (order for sorting)
xray_channel AS (
  SELECT channel, sort_order
  FROM UNNEST([
    STRUCT('Meta' AS channel, 1 AS sort_order),
    STRUCT('Google Search', 2),
    STRUCT('LinkedIn', 3),
    STRUCT('Snapchat', 4),
    STRUCT('PMax', 5),
    STRUCT('Organic', 6),
    STRUCT('Instagram Organic', 7),
    STRUCT('Test', 8),
    STRUCT('Ai Search', 9),
    STRUCT('Alta Ai', 10),
    STRUCT('Express Kitchens', 11)
  ])
),
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
    month_start,
    country,
    campaign_key,
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
  WHERE NOT exclude_xray_orphans
    OR NOT REGEXP_CONTAINS(campaign_key, r'(?i)MILAN-IT')
),
facts_agg AS (
  SELECT
    month_start,
    country,
    unified_channel,
    campaign_key,
    ROUND(SUM(cost_usd), 2) AS spend
  FROM with_channel
  GROUP BY 1, 2, 3, 4
),
channels_with_positive_spend AS (
  SELECT DISTINCT month_start, country, unified_channel
  FROM facts_agg
  WHERE spend > 0
),
zero_scaffold AS (
  SELECT
    dm.month_start,
    mk.market AS country,
    xc.channel AS unified_channel,
    CAST(NULL AS STRING) AS campaign_key,
    0.0 AS spend
  FROM dim_month dm
  CROSS JOIN dim_market mk
  CROSS JOIN xray_channel xc
  WHERE NOT EXISTS (
    SELECT 1
    FROM channels_with_positive_spend c
    WHERE c.month_start = dm.month_start
      AND c.country = mk.market
      AND c.unified_channel = xc.channel
  )
),
combined AS (
  SELECT month_start, country, unified_channel, campaign_key, spend
  FROM facts_agg
  WHERE spend != 0

  UNION ALL

  SELECT month_start, country, unified_channel, campaign_key, spend
  FROM zero_scaffold
),
with_sort AS (
  SELECT
    c.*,
    COALESCE(x.sort_order, 99) AS ch_sort
  FROM combined c
  LEFT JOIN xray_channel x ON x.channel = c.unified_channel
)

SELECT
  -- Xray style "Sep-25" → use "Sept-25"
  REPLACE(FORMAT_DATE('%b-%y', month_start), 'Sep-', 'Sept-') AS `Month Name`,
  country AS `Market`,
  unified_channel AS `Unified Channel`,
  spend AS `Spend`,
  EXTRACT(MONTH FROM month_start) AS `Month`,
  FORMAT_DATE('%d/%m/%Y', month_start) AS `Unified Date`
FROM with_sort
ORDER BY
  month_start,
  country,
  ch_sort,
  unified_channel,
  campaign_key NULLS LAST;

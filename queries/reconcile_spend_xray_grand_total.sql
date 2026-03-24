DECLARE ds_start_date DATE DEFAULT '2025-09-01';
DECLARE ds_end_date DATE DEFAULT '2026-02-28';
DECLARE compared_through_date DATE DEFAULT '2025-09-30';  -- Block 8: last day Xray includes (YTD-style)
DECLARE mena_countries ARRAY<STRING> DEFAULT ['UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain'];
DECLARE use_campaign_market_fallback BOOL DEFAULT TRUE;

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

-- RECONCILE: MENA vs funnel (DECLARE first, then CREATE TEMP FUNCTION). Run whole file or paste both above a block.
-- Project: css-dw-sync

-- ---------------------------------------------------------------------------
-- 1) Grand total (raw warehouse) — MENA countries, date window
-- ---------------------------------------------------------------------------
SELECT
  ROUND(SUM(cost_usd), 2) AS grand_total_usd,
  COUNT(*) AS raw_row_count
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
  AND (
      NULLIF(TRIM(country), '') IN UNNEST(mena_countries)
      OR (
        use_campaign_market_fallback
        AND COALESCE(TRIM(country), '') = ''
        AND campaign_fallback_country(campaign) IS NOT NULL
      )
    );


-- ---------------------------------------------------------------------------
-- 2) Same total AFTER excluding MILAN-IT (same rule as block1_spend_xray_mirror)
-- ---------------------------------------------------------------------------
WITH base AS (
  SELECT cost_usd,
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
)
SELECT
  ROUND(SUM(cost_usd), 2) AS grand_total_excl_milan_it
FROM base
WHERE NOT REGEXP_CONTAINS(campaign_key, r'(?i)MILAN-IT');


-- ---------------------------------------------------------------------------
-- 3) Spend by calendar month (find which months Xray might omit)
-- ---------------------------------------------------------------------------
SELECT
  FORMAT_DATE('%Y-%m', DATE(date)) AS ym,
  ROUND(SUM(cost_usd), 2) AS spend_usd
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
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- 4) Spend by market (Xray might exclude Bahrain/Qatar, etc.)
-- ---------------------------------------------------------------------------
SELECT
  COALESCE(NULLIF(TRIM(country), ''), campaign_fallback_country(campaign)) AS market,
  ROUND(SUM(cost_usd), 2) AS spend_usd
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
GROUP BY 1
ORDER BY spend_usd DESC;


-- ---------------------------------------------------------------------------
-- 5) “Suspicious” campaign_key patterns (often Xray omits these; extend regex as needed)
-- ---------------------------------------------------------------------------
WITH base AS (
  SELECT
    COALESCE(NULLIF(TRIM(campaign), ''), CONCAT('(', COALESCE(datasource, 'unknown'), ')')) AS campaign_key,
    cost_usd,
    country
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
tagged AS (
  SELECT
    *,
    CASE
      WHEN REGEXP_CONTAINS(campaign_key, r'(?i)MILAN-IT') THEN 'MILAN-IT'
      WHEN REGEXP_CONTAINS(campaign_key, r'(?i)/IT/') THEN 'path_/IT/'
      WHEN REGEXP_CONTAINS(campaign_key, r'(?i)EUROPE|EMEA|UK\b|FRANCE|GERMANY') THEN 'EUROPE-ish'
      WHEN REGEXP_CONTAINS(campaign_key, r'(?i)US\b|USA|UNITED STATES') THEN 'US-ish'
      ELSE 'other'
    END AS orphan_bucket
  FROM base
)
SELECT
  orphan_bucket,
  ROUND(SUM(cost_usd), 2) AS spend_usd,
  COUNT(DISTINCT campaign_key) AS distinct_campaigns
FROM tagged
GROUP BY 1
ORDER BY spend_usd DESC;


-- ---------------------------------------------------------------------------
-- 6) Top 40 campaigns by total spend (spot-check vs Xray line items)
-- ---------------------------------------------------------------------------
WITH base AS (
  SELECT
    COALESCE(NULLIF(TRIM(campaign), ''), CONCAT('(', COALESCE(datasource, 'unknown'), ')')) AS campaign_key,
    cost_usd
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
)
SELECT
  campaign_key,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM base
GROUP BY 1
ORDER BY spend_usd DESC
LIMIT 40;


-- ---------------------------------------------------------------------------
-- 7) Mapped “Unified Channel” totals (includes ELSE → unknown-style labels)
-- ---------------------------------------------------------------------------
WITH
base AS (
  SELECT
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
  WHERE NOT REGEXP_CONTAINS(campaign_key, r'(?i)MILAN-IT')
)
SELECT
  unified_channel,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM with_channel
GROUP BY 1
ORDER BY spend_usd DESC;


-- ---------------------------------------------------------------------------
-- 8) Full window vs “cut-off” month (typical Xray YTD through Sept, etc.)
--     Set compared_through_date at top of file to LAST day Xray includes.
-- ---------------------------------------------------------------------------
SELECT
  ROUND(SUM(cost_usd), 2) AS spend_full_ds_window,
  ROUND(SUM(IF(DATE(date) <= compared_through_date, cost_usd, 0)), 2) AS spend_through_cutoff,
  ROUND(SUM(IF(DATE(date) > compared_through_date, cost_usd, 0)), 2) AS spend_after_cutoff
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
  AND (
      NULLIF(TRIM(country), '') IN UNNEST(mena_countries)
      OR (
        use_campaign_market_fallback
        AND COALESCE(TRIM(country), '') = ''
        AND campaign_fallback_country(campaign) IS NOT NULL
      )
    );

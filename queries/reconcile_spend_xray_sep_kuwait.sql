-- ============================================================
-- RECONCILE: September 2025 + Kuwait (Xray sheet vs funnel_data_join)
-- ============================================================
-- Run each block separately. Compare totals to your Xray export.
-- Project: css-dw-sync
-- ============================================================

-- ---------------------------------------------------------------------------
-- A) Totals by mapped Unified Channel (should match Xray if you SUM Spend per channel)
-- ---------------------------------------------------------------------------
WITH
base AS (
  SELECT
    DATE_TRUNC(DATE(date), MONTH) AS month_start,
    country,
    LOWER(TRIM(COALESCE(datasource, ''))) AS ds,
    LOWER(TRIM(COALESCE(datasourcetype, ''))) AS dst,
    LOWER(TRIM(COALESCE(campaigntype, ''))) AS ctype,
    LOWER(TRIM(COALESCE(campaign, ''))) AS camp,
    cost_usd,
    COALESCE(NULLIF(TRIM(campaign), ''), CONCAT('(', COALESCE(datasource, 'unknown'), ')')) AS campaign_key
  FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
  WHERE DATE(date) BETWEEN '2025-09-01' AND '2025-09-30'
    AND country = 'Kuwait'
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
      ELSE INITCAP(REPLACE(COALESCE(NULLIF(ds, ''), NULLIF(dst, ''), 'unknown'), '_', ' '))
    END AS unified_channel
  FROM base
)
SELECT
  unified_channel,
  ROUND(SUM(cost_usd), 2) AS spend_usd,
  COUNT(*) AS line_rows_in_raw_before_agg
FROM with_channel
GROUP BY unified_channel
ORDER BY spend_usd DESC;


-- ---------------------------------------------------------------------------
-- B) Google Search + Meta: spend per campaign_key (matches row-level Xray / BQ export)
-- ---------------------------------------------------------------------------
WITH
base AS (
  SELECT
    DATE_TRUNC(DATE(date), MONTH) AS month_start,
    country,
    LOWER(TRIM(COALESCE(datasource, ''))) AS ds,
    LOWER(TRIM(COALESCE(datasourcetype, ''))) AS dst,
    LOWER(TRIM(COALESCE(campaigntype, ''))) AS ctype,
    LOWER(TRIM(COALESCE(campaign, ''))) AS camp,
    cost_usd,
    COALESCE(NULLIF(TRIM(campaign), ''), CONCAT('(', COALESCE(datasource, 'unknown'), ')')) AS campaign_key
  FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
  WHERE DATE(date) BETWEEN '2025-09-01' AND '2025-09-30'
    AND country = 'Kuwait'
),
with_channel AS (
  SELECT
    cost_usd,
    campaign_key,
    CASE
      WHEN ds LIKE '%facebook%' OR ds LIKE '%meta%' OR dst LIKE '%facebook%' OR dst LIKE '%meta%' THEN 'Meta'
      WHEN ds LIKE '%adwords%' OR ds LIKE '%google%' OR ds LIKE '%google_ads%' THEN
        CASE
          WHEN ctype LIKE '%performance%' OR ctype LIKE '%pmax%'
            OR camp LIKE '%performance max%' OR camp LIKE '%pmax%' THEN 'PMax'
          ELSE 'Google Search'
        END
      ELSE 'Other'
    END AS unified_channel
  FROM base
)
SELECT
  unified_channel,
  campaign_key,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM with_channel
WHERE unified_channel IN ('Meta', 'Google Search')
GROUP BY unified_channel, campaign_key
ORDER BY unified_channel, spend_usd DESC;


-- ---------------------------------------------------------------------------
-- C) Raw check: grand total Kuwait Sept 2025 in warehouse
-- ---------------------------------------------------------------------------
SELECT
  ROUND(SUM(cost_usd), 2) AS total_spend_kuwait_sept_2025
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-09-01' AND '2025-09-30'
  AND country = 'Kuwait';

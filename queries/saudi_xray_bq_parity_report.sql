-- =============================================================================
-- SAUDI ARABIA — stakeholder parity report: BQ vs X-Ray
-- =============================================================================
-- Output columns: month | channel | bq_spend | xray_spend | delta | delta_reason
--
-- BQ side: css-dw-sync.funnel_data_join (same rules as reconcile_saudi_arabia_xray.sql)
-- X-Ray side: EDIT `xray_ref` below (VALUES) OR replace with your own table, e.g.:
--   SELECT ym, unified_channel AS channel, spend_xray
--   FROM `your_project.your_dataset.xray_saudi_spend_seed`
--
-- delta = xray_spend - bq_spend (positive => X-Ray higher than BQ)
--
-- reported_spend (X-Ray–aligned): COALESCE(xray_spend, bq_spend)
--   Use this when the **deliverable must match X-Ray** line-by-line:
--   - Every month × channel in `xray_ref` uses the X-Ray number.
--   - Rows that exist only in BQ (not on your X-Ray sheet) still show BQ.
--   Warehouse truth stays in `bq_spend` for audit; `reported_spend` is the
--   “management / X-Ray parity” column until LinkedIn/Snap (etc.) land in BQ.
--
-- Project (warehouse): css-dw-sync
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
-- ---------------------------------------------------------------------------
-- X-Ray reference: REPLACE with your exact pivot from ME X-Ray Spend (Saudi)
-- Numbers below are example placeholders from your working sheet — verify.
-- ---------------------------------------------------------------------------
xray_ref AS (
  SELECT ym, channel, spend_xray
  FROM UNNEST([
    STRUCT('2025-09' AS ym, 'Google Search' AS channel, 3882.87 AS spend_xray),
    STRUCT('2025-09', 'Meta', 729.45),
    STRUCT('2025-10', 'Google Search', 3688.62),
    STRUCT('2025-10', 'Meta', 2398.92),
    STRUCT('2025-11', 'Google Search', 4360.38),
    STRUCT('2025-11', 'Meta', 2066.40),
    STRUCT('2025-12', 'Google Search', 3844.96),
    STRUCT('2025-12', 'Meta', 2167.22),
    STRUCT('2025-12', 'LinkedIn', 409.58),
    STRUCT('2025-12', 'Snapchat', 257.68),
    STRUCT('2026-01', 'Google Search', 2626.17),
    STRUCT('2026-01', 'Meta', 1298.43),
    STRUCT('2026-01', 'PMax', 575.80),
    STRUCT('2026-01', 'LinkedIn', 974.83),
    STRUCT('2026-01', 'Snapchat', 992.14),
    STRUCT('2026-02', 'Google Search', 1851.04),
    STRUCT('2026-02', 'Meta', 1257.87),
    STRUCT('2026-02', 'PMax', 1249.66),
    STRUCT('2026-02', 'LinkedIn', 775.00),
    STRUCT('2026-02', 'Snapchat', 800.00)
  ])
),
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
),
bq_agg AS (
  SELECT
    FORMAT_DATE('%Y-%m', month_start) AS ym,
    unified_channel AS channel,
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
    ), 2) AS bq_spend
  FROM with_channel
  GROUP BY 1, 2
)
SELECT
  COALESCE(x.ym, b.ym) AS month,
  COALESCE(x.channel, b.channel) AS channel,
  ROUND(COALESCE(b.bq_spend, 0), 2) AS bq_spend,
  ROUND(COALESCE(x.spend_xray, 0), 2) AS xray_spend,
  -- Matches X-Ray wherever `xray_ref` has a row; else falls back to BQ-only rows.
  ROUND(COALESCE(x.spend_xray, b.bq_spend, 0), 2) AS reported_spend,
  CASE
    WHEN x.spend_xray IS NOT NULL THEN 'xray_seed'
    WHEN b.bq_spend IS NOT NULL THEN 'bq_only_not_in_xray_seed'
    ELSE 'no_data'
  END AS reported_spend_basis,
  ROUND(COALESCE(x.spend_xray, 0) - COALESCE(b.bq_spend, 0), 2) AS delta,
  CASE
    WHEN x.ym IS NULL AND b.ym IS NOT NULL
      THEN 'xray_row_missing_add_to_seed_or_confirm_channel_name'
    WHEN b.ym IS NULL AND x.ym IS NOT NULL
      THEN
        CASE
          WHEN x.channel IN ('LinkedIn', 'Snapchat')
            THEN 'bq_not_in_funnel_data_join_for_period_use_other_source_or_etl'
          ELSE 'bq_no_rows_for_xray_channel_check_mapping_or_warehouse'
        END
    WHEN ABS(COALESCE(x.spend_xray, 0) - COALESCE(b.bq_spend, 0)) <= 1.00
      THEN 'aligned_within_1_usd'
    ELSE 'mismatch_review_mapping_timing_or_currency'
  END AS delta_reason
FROM xray_ref x
FULL OUTER JOIN bq_agg b
  ON x.ym = b.ym AND x.channel = b.channel
ORDER BY month, channel;

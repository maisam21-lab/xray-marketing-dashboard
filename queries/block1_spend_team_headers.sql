-- ============================================================
-- SPEND – Team sheet headers (match sheet grain + channel names)
-- ============================================================
-- Headers: Month Name | Market | Unified Channel | Spend | Month | Unified Date
--
-- WHY YOUR ROW COUNT WAS LOWER:
-- 1) Old query grouped only by datasourcetype → "adwords" / "facebookads".
--    Sheet uses labels: Meta, Google Search, PMax, LinkedIn, Snapchat, etc.
-- 2) Sheet often has MULTIPLE rows per month × market × channel (campaign / line split).
--    This query groups by month × market × mapped channel × campaign (and datasource).
-- 3) Sheet $0 rows (Organic, Test, Ai Search, …) are usually scaffold / matrix fills.
--    They only appear in BigQuery if those rows exist in funnel_data_join with cost 0.
--
-- Project: css-dw-sync
-- ============================================================

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT '2026-02-28';

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
  WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
    AND country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
),
with_channel AS (
  SELECT
    month_start,
    country,
    campaign_key,
    cost_usd,
    -- Map warehouse datasource → sheet-style "Unified Channel"
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
)

SELECT
  -- Match common sheet style: "Sept" not "Sep"; full month names also OK in Sheets
  TRIM(REPLACE(FORMAT_DATE('%b %Y', month_start), 'Sep ', 'Sept ')) AS `Month Name`,
  country AS `Market`,
  unified_channel AS `Unified Channel`,
  ROUND(SUM(cost_usd), 2) AS `Spend`,
  EXTRACT(MONTH FROM month_start) AS `Month`,
  FORMAT_DATE('%d/%m/%Y', month_start) AS `Unified Date`
FROM with_channel
GROUP BY month_start, country, unified_channel, campaign_key
ORDER BY month_start DESC, country, unified_channel, campaign_key;


-- ############################################################################
-- OPTIONAL: Roll up to ONE row per month × market × channel (fewer rows, like a pivot)
-- ############################################################################
-- Remove campaign_key from GROUP BY and SELECT if you only need channel totals:

-- WITH base AS ( ... same as above ... ),
-- with_channel AS ( ... same ... )
-- SELECT
--   TRIM(REPLACE(FORMAT_DATE('%b %Y', month_start), 'Sep ', 'Sept ')) AS `Month Name`,
--   country AS `Market`,
--   unified_channel AS `Unified Channel`,
--   ROUND(SUM(cost_usd), 2) AS `Spend`,
--   EXTRACT(MONTH FROM month_start) AS `Month`,
--   FORMAT_DATE('%d/%m/%Y', month_start) AS `Unified Date`
-- FROM with_channel
-- GROUP BY month_start, country, unified_channel
-- ORDER BY month_start DESC, country, unified_channel;

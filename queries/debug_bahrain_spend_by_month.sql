-- ============================================================
-- Bahrain — why "no data" for Dec 2025–Feb 2026?
-- ============================================================
-- If Block A returns **no rows**, the warehouse has **zero** `funnel_data_join` lines
-- with `country = 'Bahrain'` in that window (confirms gap vs Looker).
-- Run B–E to find mis-tagged spend (BH campaigns under another `country`).
-- Project: css-dw-sync
-- ============================================================

-- ---------------------------------------------------------------------------
-- A) Bahrain only — Dec 2025 through Feb 2026 (expect empty if ETL gap)
-- ---------------------------------------------------------------------------
SELECT
  FORMAT_DATE('%Y-%m', DATE(date)) AS ym,
  COUNT(*) AS row_count,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE country = 'Bahrain'
  AND DATE(date) BETWEEN '2025-12-01' AND '2026-02-28'
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- B) Bahrain — any row in full Looker window (Sept 2025 – Feb 2026)
-- ---------------------------------------------------------------------------
SELECT
  FORMAT_DATE('%Y-%m', DATE(date)) AS ym,
  COUNT(*) AS row_count,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE country = 'Bahrain'
  AND DATE(date) BETWEEN '2025-09-01' AND '2026-02-28'
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- C) Distinct `country` values that look like Bahrain (typos / alternate names)
-- ---------------------------------------------------------------------------
SELECT DISTINCT country
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-09-01' AND '2026-02-28'
  AND country IS NOT NULL
  AND REGEXP_CONTAINS(LOWER(TRIM(country)), r'bahrain|bahrein')
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- D) Campaigns named BH / Bahrain but `country` is NOT Bahrain (mis-tag check)
-- ---------------------------------------------------------------------------
SELECT
  country,
  FORMAT_DATE('%Y-%m', DATE(date)) AS ym,
  COUNT(*) AS row_count,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-12-01' AND '2026-02-28'
  AND country != 'Bahrain'
  AND (
    REGEXP_CONTAINS(LOWER(COALESCE(campaign, '')), r'\bbh\b|bahrain')
    OR REGEXP_CONTAINS(LOWER(COALESCE(campaign, '')), r'^\s*bh\s*\|')
  )
GROUP BY 1, 2
ORDER BY spend_usd DESC;


-- ---------------------------------------------------------------------------
-- E) All distinct countries with spend Dec 2025 – Feb 2026 (sanity list)
-- ---------------------------------------------------------------------------
SELECT
  country,
  ROUND(SUM(cost_usd), 2) AS spend_usd,
  COUNT(*) AS row_count
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-12-01' AND '2026-02-28'
GROUP BY 1
ORDER BY spend_usd DESC;

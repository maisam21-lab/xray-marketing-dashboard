-- The 10 rows with country NULL + BH/Bahrain-style campaign (~$121) — inspect details
-- Run after investigate_bahrain_mislabeled.sql shows null / small spend.

SELECT
  DATE(date) AS d,
  country,
  campaign,
  datasource,
  datasourcetype,
  ROUND(cost_usd, 2) AS cost_usd
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-12-01' AND '2026-02-28'
  AND country IS NULL
  AND REGEXP_CONTAINS(LOWER(COALESCE(campaign, '')), r'^\s*bh\s*\||\bbahrain\b')
ORDER BY d, campaign;

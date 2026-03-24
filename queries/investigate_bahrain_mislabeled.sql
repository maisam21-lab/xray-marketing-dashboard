-- Phase 2a — BH / Bahrain campaign names but which `country`? (Dec 2025 – Feb 2026)
-- If spend appears under UAE or NULL, you can fix reporting with a VIEW (re-map to Bahrain).
-- Remove the account_* line if your table has no such column (check Schema first).

SELECT
  country,
  COUNT(*) AS row_count,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-12-01' AND '2026-02-28'
  AND (
    REGEXP_CONTAINS(LOWER(COALESCE(campaign, '')), r'^\s*bh\s*\||\bbahrain\b')
    -- OR REGEXP_CONTAINS(LOWER(COALESCE(account_name, '')), r'bahrain')
  )
GROUP BY 1
ORDER BY spend_usd DESC;

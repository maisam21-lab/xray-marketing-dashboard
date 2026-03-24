-- ============================================================
-- 01 SPEND (Platform data) – raw rows from funnel_data_join
-- Source: funnel_data_join
-- Use for: Spend tab, app BigQuery source, spend_raw.csv.
-- ============================================================
-- BigQuery Console: run DECLARE + SELECT.
-- App: uses @ds_start_date, @ds_end_date parameters.
-- ============================================================

DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT '2026-02-28';

SELECT
  date,
  country AS market,
  datasource AS platform,
  datasourcetype AS channel_type,
  currency,
  campaigntype,
  campaign,
  cost,
  cost_usd AS spend,
  impressions,
  clicks
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN ds_start_date AND ds_end_date
  AND country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')
ORDER BY date, country, datasource;

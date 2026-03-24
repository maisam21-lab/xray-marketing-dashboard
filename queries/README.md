# Marketing Dashboard – BigQuery Queries

All queries use **css-dw-sync** and filter to Arab markets: **UAE, Kuwait, Saudi Arabia, Bahrain, Qatar**.

---

## Generate raw data from BigQuery for your dashboard

Your dashboard is fed by **three raw datasets**. Generate them from BigQuery:

| File | Purpose |
|------|--------|
| **[`QUERY_SPEC.md`](QUERY_SPEC.md)** | **Correct data** – required columns and filters for Spend, Lead, Opportunity. Use this to validate queries. |
| **[`raw_data_queries.sql`](raw_data_queries.sql)** | The 3 queries (3 blocks). Run each in BigQuery → export → load into Sheet (Spend / Lead / Post Lead). |
| **[`BIGQUERY_RAW_DATA_FOR_DASH.md`](BIGQUERY_RAW_DATA_FOR_DASH.md)** | Short guide: run the 3 queries → raw data for your dash. |
| **[`RUN_RAW_QUERIES.md`](RUN_RAW_QUERIES.md)** | How to run each block in BigQuery Console. |

---

## Query index

| # | File | Purpose | Output |
|---|------|---------|--------|
| **Raw (run these first)** | **[`raw_data_queries.sql`](raw_data_queries.sql)** | **All 3 raw queries in one file** | Run Block 1→Spend, Block 2→Lead, Block 3→Opportunity. See [`RUN_RAW_QUERIES.md`](RUN_RAW_QUERIES.md). |
| 01 | `01_spend.sql` | Platform spend (raw) | date, market, platform, spend, impressions, clicks, campaign, etc. |
| 02 | `02_lead.sql` | Lead data (raw) | created_date, month_date, market, qualified_date |
| 03 | `03_opportunity.sql` | Post Lead / won opportunities (raw) | created_date, close_date, tcv_usd, kitchen_country, stage, opportunity_name, approved__c |
| 04 | `04_unified_by_market.sql` | Aggregated: month × market | spend, cw_inc_approved, actual_tcv, total_leads, qualified, cpcw, cpl, cost_tcv_pct, sql_pct |
| 05 | `05_unified_by_channel.sql` | Aggregated: month × channel | same metrics by platform (CW/leads at month level) |

---

## Tables used

| Data | Table |
|------|--------|
| Spend | `css-dw-sync.ck_emea_apac_marketing.funnel_data_join` |
| Lead | `css-dw-sync.salesforce_cloudkitchens.lead` |
| Opportunity | `css-dw-sync.salesforce_cloudkitchens.opportunity` |

---

## Definitions

- **Market** = country (Spend: `country`; Opportunity: `account_market__c`; Lead: `country`).
- **CW (Inc Approved)** = count of won opportunities where `approved__c = TRUE`; **Actual TCV** = `tcv_realised__c` or `amount`.
- **Qualified** = leads with `qualified_date__c` not null.
- **CpCW** = spend / CW count; **CPL** = spend / total leads; **Cost/TCV%** = 100 × spend / TCV; **SQL%** = 100 × qualified / total leads.

---

## Notes

- **Impressions / clicks:** Queries 04 and 05 use `impressions` and `clicks` from `funnel_data_join`. If those columns are not yet in the table, delete or comment out the `SUM(impressions)`, `SUM(clicks)` lines in the spend CTE and the corresponding columns in the final SELECT. Query 01 has them commented for when you add them.

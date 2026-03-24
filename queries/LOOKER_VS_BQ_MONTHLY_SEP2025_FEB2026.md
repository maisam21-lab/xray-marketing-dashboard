# Looker Excel vs BigQuery — monthly totals (Sept 2025 – Feb 2026)

**Scope:** Four markets (no Qatar), MILAN-IT excluded in BQ, dates aligned to Looker export.

## Month totals (all markets summed)

| Month   | Looker Excel (Spend tab) | BigQuery (`spend_gap_monthly_market`) | Gap (Looker − BQ) |
|---------|--------------------------:|--------------------------------------:|------------------:|
| 2025-09 | 13,860.80                 | 12,878.30                             | ~982              |
| 2025-10 | 17,589.52                 | 13,129.70                             | ~4,460            |
| 2025-11 | 18,173.87                 | 13,915.35                             | ~4,259            |
| 2025-12 | 19,779.24                 | 12,937.92                             | ~6,841            |
| 2026-01 | 20,348.09                 | 11,299.78                             | ~9,048            |
| 2026-02 | 19,479.11                 | 10,062.63                             | ~9,416            |
| **Total** | **109,230.63**          | **~74,223.69**                        | **~35,007**       |

BQ row check: 12,878.30 + 13,129.70 + 13,915.35 + 12,937.92 + 11,299.78 + 10,062.63 = **74,223.68** ✓

## Bahrain — strong signal

In your BQ output, **Bahrain** has spend in **Sept–Nov 2025** only; there is **no Bahrain row** for **Dec 2025, Jan 2026, Feb 2026** → **$0 in the warehouse** for those months.

The Looker export still shows **large Bahrain Meta (and other) lines** in **Oct–Nov** sample rows; by **Dec–Feb** you should confirm in Excel, but **missing Bahrain rows in BQ** usually means:

- **No (or wrong) `country` attribution** for Bahrain campaigns in `funnel_data_join`, or  
- **Sync / account scope** missing Bahrain ad accounts for those months, or  
- **Rows dropped** in ETL for that market after November.

## What to do next (ETL / RevOps)

1. In BigQuery, filter `funnel_data_join` for **`country = 'Bahrain'`** and **`DATE(date) >= '2025-12-01'`** — confirm **row count and SUM(cost_usd)** vs **Nov 2025**.  
2. Compare **same date range** in Looker (account list + geography) to **connector scope** for the warehouse.  
3. Repeat for **UAE / KSA / Kuwait** where monthly gaps are large (**Jan–Feb** especially): partial loads vs platform UI.

The SQL mapping is aligned enough to explain **~$1K in Sept**; the **~$35K** gap is **missing or mis-tagged cost in the warehouse**, not the Xray mirror logic.

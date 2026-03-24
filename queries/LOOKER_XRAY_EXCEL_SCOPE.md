# `ME X-Ray Looker Studio.xlsx` — Spend tab scope (parsed)

Source file: **Downloads\ME X-Ray Looker Studio.xlsx** (sheets: **Spend**, **Leads**, **Post Leads**).

## Spend tab — what’s inside

| Field | Value |
|--------|--------|
| **Rows** | 338 (incl. header row → 337 data rows in export tools) |
| **SUM(Spend)** | **109,230.63** |
| **Unified Date range** | **2025-09-01** through **2026-02-01** (first of month; **6 months**: Sept–Feb) |
| **Markets** | **Bahrain, Kuwait, UAE, Saudi Arabia** — **Qatar is not in this file** |
| **Channels** | Meta, Google Search, Snapchat, LinkedIn, PMax, Organic, Instagram Organic, Test, Ai Search, Alta Ai, Express Kitchens |

### Spend by month label in Excel (sums)

| Month label in file | Spend USD |
|---------------------|-----------|
| Sept | 13,860.80 |
| Oct | 17,589.52 |
| Nov | 18,173.87 |
| December | 19,779.24 |
| January | 20,348.09 |
| February | 19,479.11 |
| **Total** | **109,230.63** |

Note: month names mix **abbrev** (Sept, Oct, Nov) and **full** (December, January, February).

## Why BigQuery was ~146K / ~167K vs ~109K

- **Warehouse / earlier SQL** often used **Mar–Dec 2025** (or **Jan 2025 → rolling today**) and sometimes **five countries including Qatar**.
- This Excel is only **Sept 2025 → Feb 2026** and **four countries** → **~109K** is expected for **this** export.

## SQL defaults aligned to this file

In **`block1_spend_xray_mirror.sql`** (and parity / reconcile spend scripts):

- `ds_start_date` = **`2025-09-01`**
- `ds_end_date` = **`2026-02-28`** (inclusive end of February)
- `mena_countries` = **4 markets** (no Qatar). Add **`'Qatar'`** to the array for full GCC.
- `use_data_first_month` = **`FALSE`** so the month scaffold matches **Sept → Feb** even if older warehouse rows exist elsewhere.

After re-running BigQuery with these settings, compare **`total_match_mirror_export`** from **`spend_xray_parity_one_row.sql`** to **109,230.63**. Remaining small gaps are usually **MILAN-IT**, **PMax vs Search** mapping, or **campaign line splits**, not calendar scope.

# How to run the raw data queries in BigQuery

**Goal:** Generate the 3 raw datasets from BigQuery for your dashboard (Looker + Google Sheet).

1. Open [BigQuery Console](https://console.cloud.google.com/bigquery) and select project **css-dw-sync**.

2. Open **`raw_data_queries.sql`**. It has three blocks:
   - **Block 1** = Spend → for your **Spend** tab
   - **Block 2** = Lead → for your **Lead** tab
   - **Block 3** = Opportunity (Post Lead) → for your **Post Lead** tab

3. **Run one block at a time** (BigQuery shows only the last query’s result):
   - Copy **only Block 1** (from `DECLARE` through the first `ORDER BY ...;`) → Run → Export or save.
   - Repeat for **Block 2**, then **Block 3**.

4. **Set date range** at the top of each block:
   ```sql
   DECLARE ds_start_date DATE DEFAULT '2025-01-01';
   DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
   ```

5. **If a column errors** (e.g. "Unrecognized name: impressions"): remove or comment out that column in that block.

6. **If Block 3 returns no data:** Run **[`debug_block3_opportunity.sql`](debug_block3_opportunity.sql)** (each of the 4 queries in turn). Check: (1) how `iswon` is stored, (2) distinct `account_market__c` values, (3) date range of won opps, (4) a sample with no market filter. Then either add the exact market values to Block 3’s `IN` list or use **Block 3 ALT** in `raw_data_queries.sql` (all regions, no market filter) and filter by market in Looker/Sheet.

**Then:** Load the three result sets into your Google Sheet (Spend, Lead, Post Lead tabs) so your dashboard has the raw data from BigQuery.

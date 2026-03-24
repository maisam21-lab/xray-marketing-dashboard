# Generate raw data from BigQuery for your dashboard

Your dashboard (Looker Studio + Google Sheet) is fed by **three raw datasets**. Generate them from BigQuery using the queries below.

**Correct data:** Column names and filters are defined in **[`QUERY_SPEC.md`](QUERY_SPEC.md)**. Use that to validate what each query must return.

---

## 1. Where the queries are

**File: [`raw_data_queries.sql`](raw_data_queries.sql)**

It contains **3 blocks**. Each block is one query. Run them in **BigQuery Console** (project: **css-dw-sync**). Columns match the spec so your dash gets correct data.

| Block | Data source (BigQuery table) | Use for dashboard |
|-------|------------------------------|-------------------|
| **Block 1** | `ck_emea_apac_marketing.funnel_data_join` | **Spend** tab / spend data |
| **Block 2** | `salesforce_cloudkitchens.lead` | **Lead** tab / lead data |
| **Block 3** | `salesforce_cloudkitchens.opportunity` | **Post Lead** tab / opportunity data |

Markets: **UAE, Kuwait, Saudi Arabia, Bahrain, Qatar**.

---

## 2. How to run

1. Open [BigQuery Console](https://console.cloud.google.com/bigquery) → project **css-dw-sync**.
2. Open **`raw_data_queries.sql`**.
3. **Run one block at a time** (copy one full block from `DECLARE` to the `;` before the next block, then Run).
4. For each block: **Export** the result (e.g. CSV) or **Save results** to a table, then load that into your Google Sheet tab (Spend, Lead, or Post Lead).

**Date range:** Edit at the top of each block:

```sql
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
```

---

## 3. After you have the data

- **Google Sheet:** Put Block 1 results in the **Spend** tab, Block 2 in **Lead**, Block 3 in **Post Lead** (same sheet you use for Looker).
- **Scheduled refresh:** In BigQuery you can set up scheduled queries for each block and export/sync to the Sheet (e.g. hourly) so your dashboard stays up to date.

That’s it: run the 3 blocks in BigQuery → you have the raw data for your dash.

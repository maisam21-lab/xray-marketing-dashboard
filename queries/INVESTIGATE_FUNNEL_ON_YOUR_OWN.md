# Figure out the spend gap on your own — investigation playbook

Use this in order. Each step answers one question. Stop when you find the cause or hit an access wall.

---

## Phase 1 — Prove where the data breaks (you mostly did this)

- [x] Same date range + 4 markets: BQ total **<** Xray total.  
- [x] Bahrain in BQ: **Sept–Nov 2025 only**; **Dec–Feb 2026 = empty**.  
- [x] Xray shows **non-zero Bahrain** Dec–Feb.

**Conclusion so far:** Missing or mis-labeled rows in **`funnel_data_join`**, not your mirror SQL.

---

## Phase 2 — Find if Bahrain spend is hiding under another label

Run in BigQuery (adjust project/dataset if needed).

### 2a) BH campaigns, any `country` (Dec–Feb)

```sql
SELECT
  country,
  COUNT(*) AS row_count,
  ROUND(SUM(cost_usd), 2) AS spend_usd
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-12-01' AND '2026-02-28'
  AND (
    REGEXP_CONTAINS(LOWER(COALESCE(campaign, '')), r'^\s*bh\s*\||\bbahrain\b')
    OR REGEXP_CONTAINS(LOWER(COALESCE(account_name, '')), r'bahrain')
  )
GROUP BY 1
ORDER BY spend_usd DESC;
```

- If **big spend** shows under **UAE** (or `NULL`) → **attribution bug** you can document and optionally fix with a **SQL view** (re-map those rows to Bahrain for reporting).  
- If **empty** → spend isn’t in this table at all for those campaigns.

*(If `account_name` doesn’t exist, remove that line or discover real column names in 2d.)*

### 2b) All distinct `country` values (any spelling)

```sql
SELECT country, COUNT(*) AS n
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-12-01' AND '2026-02-28'
GROUP BY 1
ORDER BY n DESC;
```

Look for **BH**, **Kingdom of Bahrain**, blanks, or **GCC** buckets.

### 2c) Same as 2a but **no country filter** — only `campaign` LIKE `%BH%`

If you get rows but 2a with `country` group was empty, you already saw the issue in 2a.

---

## Phase 3 — Schema: are you summing the wrong column?

### 3d) List columns and sample one row

In BQ UI: **table preview** → note every **cost / spend / amount** field.

```sql
SELECT *
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE country = 'Bahrain'
  AND DATE(date) BETWEEN '2025-09-01' AND '2025-09-30'
LIMIT 5;
```

- If you see **`cost_local`**, **`spend`**, **`amount`**, compare **`SUM(cost_usd)`** vs **`SUM(other_column)`** for Sept. If another column **matches Xray** better, your reports should use that column (document why).

---

## Phase 4 — Is the table incomplete by time (load jobs)?

### 4e) Row counts by month for **all** countries (spot drops)

```sql
SELECT
  FORMAT_DATE('%Y-%m', DATE(date)) AS ym,
  COUNT(*) AS row_count,
  COUNTIF(country = 'Bahrain') AS bahrain_rows
FROM `css-dw-sync.ck_emea_apac_marketing.funnel_data_join`
WHERE DATE(date) BETWEEN '2025-09-01' AND '2026-02-28'
GROUP BY 1
ORDER BY 1;
```

- **`bahrain_rows` → 0** from Dec onward confirms **Bahrain-specific** gap.  
- If **global** `row_count` also **dives** in Dec, wider **sync** issue.

### 4f) Table metadata

In BQ: table **Details** → **Last modified**, **Partitioning**, **Streaming buffer**. Note for your notes.

---

## Phase 5 — Other tables in the same project (self-serve discovery)

You don’t need “support” if you have **BigQuery read access**.

```sql
SELECT table_schema, table_name
FROM `css-dw-sync.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE '%funnel%'
   OR table_name LIKE '%ads%'
   OR table_name LIKE '%marketing%'
ORDER BY 1, 2;
```

Open promising tables, check for **Bahrain** or **same campaign names** as Xray. Sometimes **staging** or **raw** is complete while **join** is not.

---

## Phase 6 — If you find nothing in BQ

Then the fix you can do **alone** is **operational**, not warehouse:

1. **Canonical spend** = Xray / Looker export.  
2. **Optional:** Load that CSV into **your own** BQ table on a schedule (**NO_ETL_SUPPORT_OPTIONS.md**).  
3. **Optional:** A **view** that `UNION`s manual import over incomplete months.

---

## What “figured it out” looks like

| Finding | Your move |
|--------|-----------|
| Spend under **wrong `country`** | Document + **view** that corrects `country` for known campaign/account patterns. |
| Wrong **cost column** | Switch SUM to the column that matches Xray; document. |
| **No rows** anywhere for BH Dec–Feb | **Cannot fix inside `funnel_data_join`** without new ingest → use **export → BQ** or Xray as truth. |
| **Another table** has full data | Point reports at that table or **join** into a view. |

---

## Files to keep open

| File | Use |
|------|-----|
| `debug_bahrain_spend_by_month.sql` | Bahrain + mis-tag blocks |
| `spend_gap_monthly_market.sql` | Month × market |
| `NO_ETL_SUPPORT_OPTIONS.md` | If warehouse can’t be fixed |
| `BAHRAIN_XRAY_VS_WAREHOUSE.md` | Evidence for stakeholders |

You’re not stuck on “SQL wrong vs right” — you’re doing **detective work** on **where the dollars live in GCP**. Each query above narrows that down.

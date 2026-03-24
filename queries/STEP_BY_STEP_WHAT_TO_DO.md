# Step-by-step: What to do (BigQuery → Google Sheet)

Do these steps in order. Use project **css-dw-sync** in BigQuery.

---

## Step 1: Set your date range

Decide the date range for your sheet (e.g. 2025-01-01 to today). You will use the same range in every block. In each query block you’ll see:

```sql
DECLARE ds_start_date DATE DEFAULT '2025-01-01';
DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
```

Change `'2025-01-01'` if you need a different start date.

---

## Step 2: Run Block 1 (Spend)

1. Open **`bigquery_for_sheet_tabs.sql`**.
2. Find **BLOCK 1: SPEND** (starts with `DECLARE ds_start_date`).
3. Copy the whole block from that `DECLARE` down to the first `;` (before Block 2).
4. In BigQuery Console, paste and **Run**.
5. **Export** the result (e.g. Save results → CSV, or Export to Google Sheets).
6. Put that data into your Google Sheet **Spend** tab.

---

## Step 3: Run Block 2 (Lead)

1. In **`bigquery_for_sheet_tabs.sql`**, find **BLOCK 2: LEAD**.
2. Copy that full block (from `DECLARE` to the `;` before Block 3).
3. In BigQuery, paste and **Run**.
4. Export and put the result into your Sheet **Lead** tab.
5. If you get **no rows**: run the Lead debug queries (see “If Block 2 returns no data” at the end).

---

## Step 4: Run Block 3 MENA via Account (Post Lead)

1. In **`bigquery_for_sheet_tabs.sql`**, find **BLOCK 3 MENA VIA ACCOUNT** (the one that says “Post Lead by Account country”).
2. Copy that full block (from `DECLARE` to `ORDER BY ... kitchen_country;`).
3. In BigQuery, paste and **Run**.
4. If it runs and returns rows: **Export** and put the result into your Sheet **Post Lead** tab. Then go to Step 5.
5. If you get an error like **“Not found: Table account”**:
   - Use **BLOCK 3 ALT** instead (the next block in the file: “NO MARKET FILTER”).
   - Run Block 3 ALT, export, put into Post Lead tab. You’ll get all won opps; filter to MENA in the sheet if you need.
   - Tell me the exact error message so we can fix the Account table name.

---

## Step 5: (Optional) Run Block 4 and Block 5

Only if your sheet has **Unified by Market** and **Unified by Channel** tabs:

1. Find **BLOCK 4: UNIFIED BY MARKET** in **`bigquery_for_sheet_tabs.sql`**.
2. Copy that full block, run in BigQuery, export, put into the **Unified by Market** tab.
3. Find **BLOCK 5: UNIFIED BY CHANNEL**.
4. Copy that full block, run in BigQuery, export, put into the **Unified by Channel** tab.

If Block 4 or 5 fails with “Unrecognized name: impressions” or “clicks”, use the **Block 4b** or **Block 5b** version in the same file (commented section without impressions/clicks).

---

## Step 6: Refresh your sheet / dashboard

- Make sure each tab has the latest export (Spend, Lead, Post Lead, and 4/5 if you use them).
- Refresh Looker Studio or any dashboard that reads this sheet.

---

## If Block 2 returns no data

1. Open **`debug_block2_lead.sql`** (if it exists) or ask for a Lead debug script.
2. Run the debug queries one by one to see: distinct `country` values, date range, and `isdeleted` values.
3. Use those results to fix the Lead filter (e.g. add the exact country values or fix the date range).

---

## If Block 3 MENA via Account returns no data (but no error)

1. Open **`debug_block3_opportunity.sql`**.
2. Run **query 7** (Account country for won opportunities).
3. Check the list of `account_country` values. If MENA countries are spelled differently (e.g. “Saudi Arabia” vs “KSA”), add those exact values to the `AND TRIM(COALESCE(a.billingcountry, '')) IN (...)` list in Block 3 MENA via Account.
4. Re-run Block 3 MENA via Account.

---

## Quick reference: which file, which block

| Step | File | Block | Sheet tab |
|------|------|--------|-----------|
| 2 | bigquery_for_sheet_tabs.sql | Block 1 – SPEND | Spend |
| 3 | bigquery_for_sheet_tabs.sql | Block 2 – LEAD | Lead |
| 4 | bigquery_for_sheet_tabs.sql | Block 3 MENA VIA ACCOUNT (or Block 3 ALT) | Post Lead |
| 5 | bigquery_for_sheet_tabs.sql | Block 4, Block 5 | Unified by Market, Unified by Channel |

Run one block at a time; export each result to the matching tab.

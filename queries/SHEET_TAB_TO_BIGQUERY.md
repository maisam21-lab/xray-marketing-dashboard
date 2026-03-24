# Google Sheet tabs → BigQuery queries (same data, same markets)

**Sheet (source of truth for “right data”):**  
[RevOps Marketing – tab gid=8109573](https://docs.google.com/spreadsheets/d/1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8/edit?pli=1&gid=8109573#gid=8109573)

**Markets:** UAE, Kuwait, Saudi Arabia, Bahrain, Qatar

BigQuery exports should be **equivalent** to what’s in this sheet (same date range, same markets, same or compatible columns and totals per tab). See **DATA_RIGHT_CHECKLIST.md** for how to verify equivalence.

Use the queries in **`bigquery_for_sheet_tabs.sql`** (in this folder). Run one block at a time in BigQuery Console; export each result to the matching Sheet tab.

---

## Tab → Query mapping

| Sheet tab (typical name) | BigQuery query file | Block in `bigquery_for_sheet_tabs.sql` | Source table(s) |
|--------------------------|---------------------|----------------------------------------|------------------|
| **Spend**                | `01_spend.sql`      | Block 1 – SPEND                         | `funnel_data_join` |
| **Lead**                 | `02_lead.sql`       | Block 2 – LEAD                         | `salesforce_cloudkitchens.lead` |
| **Post Lead** / Opportunity | `03_opportunity.sql` | Block 3 – OPPORTUNITY              | `salesforce_cloudkitchens.opportunity` |
| **Unified by Market**    | `04_unified_by_market.sql` | Block 4 – UNIFIED BY MARKET      | funnel_data_join + opportunity + lead |
| **Unified by Channel**   | `05_unified_by_channel.sql` | Block 5 – UNIFIED BY CHANNEL   | Same, grouped by channel |

---

## Why you might not be getting what you want

### 1. **Opportunity (Post Lead) tab is empty or missing rows**

- **Cause:** In Salesforce/BigQuery, `account_market__c` often has values like **"KSA"**, **"Saudi"**, or city names instead of **"Saudi Arabia"**. Your filter uses `IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')`, so "KSA" is excluded.
- **Fix:** Use **Block 3 with market normalization** in `bigquery_for_sheet_tabs.sql`: it maps `KSA` / `Saudi Arabia` → `Saudi Arabia` and keeps only MENA. If you’re still missing data, run `debug_block3_opportunity.sql` and add any other distinct `account_market__c` values you want to the list (or use Block 3 ALT with no market filter and filter in the Sheet).

### 2. **Spend tab: "Unrecognized name: impressions" or "clicks"**

- **Cause:** `funnel_data_join` may not have `impressions` or `clicks` columns yet.
- **Fix:** In Block 1, keep only `date`, `market`, `platform`, `spend`. Do not select `impressions` or `clicks` unless the table has them. Block 1 in `bigquery_for_sheet_tabs.sql` does not use impressions/clicks so it should run as-is.

### 3. **Unified by Market / Unified by Channel (Blocks 4 & 5) fail on impressions/clicks**

- **Cause:** Same as above: `funnel_data_join` without `impressions` or `clicks`.
- **Fix:** In the spend CTE inside Block 4 and Block 5, remove or comment out:
  - `SUM(impressions) AS impressions`
  - `SUM(clicks) AS clicks`  
  and in the final SELECT remove:
  - `ROUND(s.impressions, 0) AS impressions`
  - `ROUND(s.clicks, 0) AS clicks`  
  An alternate version is in the same file (Blocks 4b / 5b) without impressions/clicks.

### 4. **Date range doesn’t match the sheet**

- **Cause:** The sheet might be built from a different date range.
- **Fix:** Set the same range in every block:
  ```sql
  DECLARE ds_start_date DATE DEFAULT '2025-01-01';  -- match your sheet
  DECLARE ds_end_date DATE DEFAULT CURRENT_DATE();
  ```

### 5. **Lead or Spend has different totals**

- **Cause:** Lead uses `country`; Spend uses `country` in `funnel_data_join`. Slight spelling/space differences (e.g. "UAE " vs "UAE") can split counts.
- **Fix:** Queries already use `COALESCE(country, 'Unknown')` and `TRIM`. If you have other variants, add them to the `IN` list or normalize in the query.

### 6. **Unified tabs: CW or leads show 0 for a month/market**

- **Cause:** Unified queries **LEFT JOIN** spend to CW and leads. If there’s no spend in a month/market, that row can still appear with 0 spend; if there’s spend but no CW/leads, CW/leads show 0. Also, **04/05 use `approved__c = TRUE`** for CW; if your sheet counts all won opps, you’d see a mismatch.
- **Fix:** Confirm whether the sheet’s “CW” is approved-only. If the sheet uses all won opportunities, use the Opportunity tab (Block 3) for that view and don’t rely on 04/05 for CW definition, or change the CW CTE to drop `AND approved__c = TRUE`.

---

## Quick checklist

- [ ] Same `ds_start_date` and `ds_end_date` in all blocks.
- [ ] Block 3: if empty, run debug_block3_opportunity.sql and add/normalize market values.
- [ ] No impressions/clicks in any query unless `funnel_data_join` has those columns.
- [ ] Unified (04/05): CW definition matches sheet (approved-only vs all won).

After running each block, export results to the matching Sheet tab so the sheet and BigQuery stay in sync.

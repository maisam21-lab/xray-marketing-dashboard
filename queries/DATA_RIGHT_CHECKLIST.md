# Make sure we're getting the right data

**“Right data” = equivalent to the data in this Google Sheet:**

**[RevOps Marketing Sheet](https://docs.google.com/spreadsheets/d/1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8/edit?pli=1&gid=8109573#gid=8109573)**

Same metrics, same markets (UAE, Kuwait, Saudi Arabia, Bahrain, Qatar), same date range, and the same (or compatible) columns per tab. BigQuery exports should match or replace what’s in each tab without changing the meaning of the dashboard.

---

## 1. Set one date range

Use the **same** `ds_start_date` and `ds_end_date` everywhere (e.g. `2025-01-01` to today). Set them at the top of **`validate_right_data.sql`** and in **`bigquery_for_sheet_tabs.sql`** when you run the real blocks.

---

## 2. Run validation queries

Open **`validate_right_data.sql`** in BigQuery (project: **css-dw-sync**). Run **each block** (you can run the whole file if your client allows multiple statements; otherwise run block by block).

| Validation | What to check | “Right” means |
|------------|----------------|----------------|
| **V1**     | Spend row count, dates, total spend, distinct markets | `row_count` ≥ 1, `total_spend_usd` > 0, `distinct_markets` up to 5 |
| **V1b**    | Spend by market | Only UAE, Kuwait, Saudi Arabia, Bahrain, Qatar; spend per market makes sense |
| **V2**     | Lead row count, date range, distinct markets | If you expect leads in MENA: `row_count` ≥ 1. If 0, run `debug_block2_lead.sql` and fix country list |
| **V2b**    | Lead by market | Same 5 markets (or Unknown); counts look reasonable |
| **V3**     | Opportunity by `account_market__c` | Often 0 (that field is city/region). If 0, use Post Lead from **Block 3 MENA via Account** or **Block 3 ALT** |
| **V4**     | Opportunity by Account.BillingCountry | `row_count` ≥ 0. If 0, run debug_block3 query 7 and add the exact BillingCountry values to the query |
| **V4b**    | Post Lead by country (Account) | Shows which countries you actually got (UAE, Saudi Arabia, etc.); TCV per country looks right |
| **V5**     | Min/max dates by source | Spend, Lead, Opp dates overlap your chosen date range (no requirement they match exactly) |

---

## 3. Match validation to the blocks you use

- **Spend tab**  
  Use **Block 1** in `bigquery_for_sheet_tabs.sql`.  
  Right data = V1 and V1b look good (MENA only, date range, spend > 0).

- **Lead tab**  
  Use **Block 2**.  
  Right data = V2 and V2b look good. If V2 row_count = 0, fix the Lead country filter (debug_block2_lead.sql) then re-run V2/V2b and Block 2.

- **Post Lead tab**  
  - Prefer **Block 3 MENA via Account** (join to Account, filter by BillingCountry).  
    Right data = V4 and V4b look good (MENA countries, TCV makes sense).  
  - If the Account join fails or V4 returns 0 and you can’t fix BillingCountry list, use **Block 3 ALT** (all won opps, no market filter) and filter to MENA in the sheet; then spot-check that your sheet filters match what you expect.

---

## 4. Spot-check the actual exports

After running Block 1, 2, and 3 (or 3 MENA via Account / 3 ALT):

1. **Spend:** Open the export. Check: `market` only MENA, `date` in range, `spend` = cost_usd, and that platforms (e.g. Meta, Google) match expectations.
2. **Lead:** Check: `market` only MENA (or Unknown), `created_date` in range, `qualified_date` present when leads are qualified.
3. **Post Lead:** Check: `kitchen_country` is country (from Account if using Block 3 MENA via Account), `close_date` in range, `tcv_usd` and `stage` look correct.

---

## 5. Column checklist (right shape)

Use **QUERY_SPEC.md** for full definitions. Quick check:

- **Spend:** `date`, `market`, `platform`, `spend` (and optionally impressions, clicks, campaign, etc.).
- **Lead:** `created_date`, `month_date`, `market`, `qualified_date` (and optionally `lead_id`).
- **Post Lead:** `created_date`, `month_date`, `close_date`, `tcv_usd`, `kitchen_country`, `stage`, `opportunity_name`, `approved__c` (and optional id, amount, etc.).

---

## 6. If something is wrong

| Issue | What to do |
|-------|------------|
| V1 row_count = 0 or wrong markets | Check `funnel_data_join` has data for MENA; confirm `country` values are exactly UAE, Kuwait, Saudi Arabia, Bahrain, Qatar. |
| V2 row_count = 0 | Run `debug_block2_lead.sql`; add the exact `country` values from the debug to Block 2’s IN list (or fix date range). |
| V4 row_count = 0 (Account join) | Run debug_block3_opportunity.sql **query 7**; add the exact `billingcountry` values for MENA to Block 3 MENA via Account (and to V4/V4b). |
| Account table not found | Use **Block 3 ALT** for Post Lead; filter to MENA in the sheet until Account table/schema is available. |
| Dates outside expected range | Adjust `ds_start_date` / `ds_end_date` in both `validate_right_data.sql` and `bigquery_for_sheet_tabs.sql` to match your reporting period. |

---

---

## 7. Equivalence check: BigQuery vs the sheet

To confirm BigQuery data is **equivalent** to the sheet:

1. **Use the same date range**  
   In the sheet, note the date range used (e.g. from filters or the data). Set `ds_start_date` and `ds_end_date` in the BigQuery blocks to that same range.

2. **Same markets**  
   The sheet should only show MENA: UAE, Kuwait, Saudi Arabia, Bahrain, Qatar. Our blocks already filter to these; no extra step unless the sheet uses different labels (e.g. “KSA” vs “Saudi Arabia”). If the sheet uses “KSA”, our queries that normalize to “Saudi Arabia” are still equivalent for reporting.

3. **Compare totals per tab**  
   - **Spend:** In the sheet, sum spend (or use any total you trust). Run Block 1 in BigQuery, sum `spend`. The two totals should match (or be very close if the sheet is from a different refresh).  
   - **Lead:** Sheet total lead count vs BigQuery Block 2 row count for the same period and markets.  
   - **Post Lead:** Sheet total closed-won count and/or TCV vs BigQuery Block 3 MENA via Account (or Block 3 ALT filtered to MENA) row count and sum of `tcv_usd`.

4. **Compare columns**  
   For each tab, the BigQuery export should have at least the columns the sheet (and any downstream Looker/dashboard) uses. Extra columns are fine. Missing columns mean we’re not equivalent—add them in the query or in the sheet.

5. **If the sheet has multiple tabs**  
   Map each tab to a block (see SHEET_TAB_TO_BIGQUERY.md). Run the block, export, and compare that tab’s data to the export as above. When every tab matches (or is replaced by) a BigQuery export, the data is equivalent.

**Summary:** Right data = equivalent to the [RevOps Marketing Sheet](https://docs.google.com/spreadsheets/d/1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8/edit?pli=1&gid=8109573#gid=8109573). Use the same date range and markets, then compare totals and columns per tab. Run **validate_right_data.sql** → fix any source (debug scripts) → run **bigquery_for_sheet_tabs.sql** blocks → export and compare to the sheet.

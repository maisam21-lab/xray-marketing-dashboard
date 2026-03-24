# Changelog – updates from chat (BigQuery ↔ Google Sheet)

Summary of everything updated in this chat. Keep this file as the record of changes.

---

## 1. New files created

### `SHEET_TAB_TO_BIGQUERY.md`
- **Purpose:** Maps each Google Sheet tab to the right BigQuery query and explains why data might be missing.
- **Contents:**
  - Tab → query mapping (Spend, Lead, Post Lead, Unified by Market, Unified by Channel).
  - Troubleshooting: empty Opportunity (KSA vs Saudi Arabia), missing impressions/clicks, date range, CW definition.
  - Quick checklist for running queries and exporting to the sheet.

### `bigquery_for_sheet_tabs.sql`
- **Purpose:** Single file with all query blocks to get the same data as the sheet (same markets: UAE, Kuwait, Saudi Arabia, Bahrain, Qatar).
- **Contents:** Blocks 1–5 (Spend, Lead, Opportunity, Unified by Market, Unified by Channel), plus optional Block 4b/5b without impressions/clicks if the table doesn’t have them.

### `CHANGELOG_FROM_CHAT.md` (this file)
- **Purpose:** Record of all updates made in the chat.

---

## 2. Block 1 – SPEND (user’s version applied)

**Files updated:** `bigquery_for_sheet_tabs.sql`, `01_spend.sql`

**Changes:**
- Added full column set from `funnel_data_join`:
  - `datasourcetype AS channel_type`
  - `currency`, `campaigntype`, `campaign`, `cost`
  - `cost_usd AS spend`
  - `impressions`, `clicks`
- Comment updated: “Export/save as: Spend tab or spend_raw.csv”.

---

## 3. Block 2 – LEAD (user’s version applied)

**Files updated:** `bigquery_for_sheet_tabs.sql`, `02_lead.sql`

**Changes:**
- Added `id AS lead_id` to the SELECT.
- Comment: “Required: created_date, month_date, market, qualified_date.”
- Filter and other columns unchanged.  
- **Note:** If Block 2 returns no data, use a Lead debug script (e.g. distinct `country`, date range, `isdeleted`) similar to `debug_block3_opportunity.sql`.

---

## 4. Block 3 – OPPORTUNITY / POST LEAD (fix for “no data”)

**Files updated:** `bigquery_for_sheet_tabs.sql`, `03_opportunity.sql`

**Problem:** Block 3 returned no data because `account_market__c` in Salesforce is often stored as **"KSA"** (or "Saudi"), not **"Saudi Arabia"**, so the filter excluded those rows.

**Changes:**
- **WHERE clause:** Allow MENA variants:
  - Keep: `TRIM(COALESCE(account_market__c, '')) IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')`
  - Add: `OR UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI', 'SAUDI ARABIA')`
- **SELECT – normalize `kitchen_country`:**
  - `CASE WHEN UPPER(TRIM(COALESCE(account_market__c, ''))) IN ('KSA', 'SAUDI ARABIA', 'SAUDI') THEN 'Saudi Arabia'`
  - `WHEN TRIM(COALESCE(account_market__c, '')) = '' THEN 'Unknown'`
  - `ELSE TRIM(account_market__c) END AS kitchen_country`
- So the sheet always sees “Saudi Arabia” for KSA/Saudi, and the query returns rows when data is stored as KSA.

---

## 5. Block 4 – UNIFIED BY MARKET

**File:** `bigquery_for_sheet_tabs.sql`

**Changes (earlier in chat):**
- Spend CTE uses only `SUM(cost_usd) AS spend` (no `impressions`/`clicks`) so it runs even if `funnel_data_join` doesn’t have those columns.
- CW and lead CTEs already use the same KSA/Saudi normalization as Block 3.
- Block 4b (commented) is the alternate without impressions/clicks if needed.

---

## 6. Block 5 – UNIFIED BY CHANNEL

**File:** `bigquery_for_sheet_tabs.sql`

**Changes (earlier in chat):**
- Same idea as Block 4: spend CTE can be run without impressions/clicks; Block 5b is the commented alternate.
- CW/lead logic aligned with Block 3 market handling.

---

## 7. Existing file referenced (unchanged)

- **`debug_block3_opportunity.sql`** – Use when Block 3 (Post Lead) still returns no data: run its 4 queries to see `iswon` values, distinct `account_market__c`, date range of won opps, and a sample without market filter.

---

## Quick reference – files touched

| File | Action |
|------|--------|
| `SHEET_TAB_TO_BIGQUERY.md` | Created |
| `bigquery_for_sheet_tabs.sql` | Created, then updated (Blocks 1–5) |
| `01_spend.sql` | Updated (full columns) |
| `02_lead.sql` | Updated (added `lead_id`) |
| `03_opportunity.sql` | Updated (KSA filter + `kitchen_country` normalization) |
| `CHANGELOG_FROM_CHAT.md` | Created (this file) |

---

---

## 8. Block 3 still no data – root cause and new block (Post Lead)

**Debug results showed:** `account_market__c` holds **region/city** (Los Angeles, SF Bay Area, New York, null), not country. There are no values "UAE", "KSA", "Saudi Arabia" in that field; 29,290 won opps have `account_market__c` = null (including MENA names like "Deira", "Bur Dubai", "KSA" in the opportunity name).

**Changes:**
- **`debug_block3_opportunity.sql`:** Added query 7 – Account country for won opps (join to `account`, group by `a.billingcountry`). Run it to see how country is stored on Account.
- **`bigquery_for_sheet_tabs.sql`:** Added **Block 3 MENA via Account** – joins Opportunity to Account on `accountid`, filters by `Account.BillingCountry` IN ('United Arab Emirates', 'UAE', 'Saudi Arabia', 'KSA', 'Kuwait', 'Bahrain', 'Qatar'), outputs `kitchen_country` from Account. Use this for the Post Lead tab to get MENA-only. If the table is not `account` or the field is not `billingcountry`, adjust the JOIN and WHERE.
- **Fallback:** Block 3 ALT still returns all won opps (no market filter); filter in the sheet if the Account join is not available or returns no rows.

*Last updated from chat: same session as the BigQuery ↔ Sheet alignment work.*

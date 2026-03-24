# Post Lead export – sheet columns

## Files

| File | Purpose |
|------|---------|
| **`block3_post_lead_sheet_columns.sql`** | Main query: all sheet headers, MENA via `Account.BillingCountry`, joins Owner + Account + first converted Lead date |
| **`discover_post_lead_columns.sql`** | Find real API names for custom fields (UTM, license fee, etc.) |

## Column mapping (current query)

| Sheet column | Source (today) |
|--------------|----------------|
| Opportunity Owner | `user.name` (LEFT JOIN on `opportunity.ownerid`) |
| Account Name | `account.name` |
| Opportunity Name | `opportunity.name` |
| Market | `account.billingcountry` |
| Close Date | `opportunity.closedate` |
| Created Date | `opportunity.createddate` |
| Lead Source | `opportunity.leadsource` |
| First Lead Created Date | `MIN(lead.createddate)` for `convertedopportunityid = opportunity.id` |
| Lead Source Detail | `NULL` → replace with e.g. `lead_source_detail__c` |
| Original Lead Source | `NULL` → replace with e.g. `original_lead_source__c` |
| UTM Source … Term | `NULL` → replace with `utm_*__c` on Opportunity or from Lead |
| Stage | `opportunity.stagename` |
| Monthly License Fee | `NULL` → e.g. `monthly_license_fee__c` |
| Monthly License Fee (converted) | `NULL` → e.g. `monthly_license_fee_converted__c` |
| License Initial Term (Months) | `NULL` → e.g. `license_initial_term__c` |
| TCV (converted) | `COALESCE(amount, tcv_realised__c)` (adjust if you have a dedicated field) |
| Deal Type | `opportunity.type` |
| Unified Channel | `NULL` → custom field or join to marketing |
| Unified Date | `DATE(COALESCE(closedate, createddate))` |
| Month | `DATE_TRUNC(..., MONTH)` on same |
| Monthly LF USD | `NULL` → USD-specific field when known |
| TCV USD | `COALESCE(tcv_realised__c, amount, 0)` |
| Is_CW | `iswon = TRUE` |
| Is_Qualifying / Pitching / Negotiation / Commitment | Regex on `LOWER(stagename)` |
| Is_ClosedLost | `isclosed AND NOT iswon` or “Closed Lost” in stage |
| 1st Month LF | `NULL` → e.g. `first_month_license_fee__c` |
| Actual TCV | `tcv_realised__c` |

## Filters

- **MENA:** `billingcountry` IN (United Arab Emirates, UAE, Saudi Arabia, KSA, Kuwait, Bahrain, Qatar).
- **Date:** Created **or** closed between `ds_start_date` and `ds_end_date`. Uncomment `OR isclosed` line in the SQL if the sheet includes all open MENA opps.
- **Won-only:** See commented block at bottom of `block3_post_lead_sheet_columns.sql` (match old Block 3 MENA via Account).

## If something breaks

1. **`user` table / join:** Use `o.ownerid` as `Opportunity Owner` until User sync is confirmed.
2. **Missing custom fields:** Run `discover_post_lead_columns.sql`, then swap `CAST(NULL AS …)` for real columns in the main query.
3. **Stage flags wrong:** Adjust `REGEXP_CONTAINS` patterns to match your exact `stagename` values (sample: `SELECT DISTINCT stagename FROM opportunity …`).

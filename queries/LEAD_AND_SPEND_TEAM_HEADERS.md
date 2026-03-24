# Spend & Lead – team sheet headers

## Spend

**File:** `block1_spend_team_headers.sql`  

| Header | Source |
|--------|--------|
| Month Name | `FORMAT_DATE('%B %Y', month_start)` |
| Market | `country` (MENA only) |
| Unified Channel | `COALESCE(datasourcetype, datasource, 'Unknown')` |
| Spend | `SUM(cost_usd)` |
| Month | `DATE_TRUNC(date, MONTH)` |
| Unified Date | Same as `Month` (month bucket) |

## Lead

**File:** `block2_lead_sheet_columns.sql`  

| Header | Source |
|--------|--------|
| Create Date | `createddate` |
| First Name | `firstname` |
| Last Name | `lastname` |
| Company / Account | `company` |
| Email | `email` |
| Lead Source | `leadsource` |
| Lead Owner | `user.name` (join on `ownerid`) |
| Lead Source Detail | `NULL` → e.g. `lead_source_detail__c` |
| UTM Source … Content | `NULL` → e.g. `utm_*__c` on Lead |
| Lead Status | `status` |
| Market | `country` |
| Original Lead Source Detail | `NULL` → e.g. `original_lead_source_detail__c` |
| Month | `DATE_TRUNC(createddate, MONTH)` |
| Unified Channel | `NULL` → e.g. `unified_channel__c` |
| Unified Date | `DATE(createddate)` |
| Is_Qualified | `qualified_date__c IS NOT NULL` |
| Is_CNC | Regex on `status` (tune for your picklist) |
| Is_New | `status` = 'New' |
| Is_Working | Regex on `status` (working / contacted / etc.) |

Run `discover_post_lead_columns.sql` (lead section) to replace `NULL` columns with real fields.

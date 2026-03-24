# Query spec – correct data for the dashboard

Use this to check that your BigQuery queries return the right shape and filters.

---

## Required output columns

### 1. SPEND (from `funnel_data_join`)

| Column   | Required | Description |
|----------|----------|-------------|
| `date`   | Yes      | Date of spend (DATE or TIMESTAMP) |
| `market` | Yes      | Country (alias of `country`). Values: UAE, Kuwait, Saudi Arabia, Bahrain, Qatar. |
| `platform` | Yes    | Datasource / channel (alias of `datasource`). |
| `spend`  | Yes      | Cost in USD (alias of `cost_usd`). |

Optional (include if the table has them): `impressions`, `clicks`, `channel_type` (datasourcetype), `campaign`, `campaigntype`, `currency`, `cost`.

**Filters:** `DATE(date) BETWEEN ds_start_date AND ds_end_date`, `country IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')`.

---

### 2. LEAD (from `salesforce_cloudkitchens.lead`)

| Column          | Required | Description |
|-----------------|----------|-------------|
| `created_date`  | Yes      | Lead creation date (alias of `createddate`). |
| `month_date`    | Yes      | Month of created_date: `DATE_TRUNC(createddate, MONTH)`. |
| `market`        | Yes      | Country. Use `COALESCE(country, 'Unknown')`. |
| `qualified_date`| Yes      | Alias of `qualified_date__c`. NULL = not qualified. |

Optional: `lead_id` (id).

**Filters:** `isdeleted IS FALSE OR isdeleted IS NULL`, `DATE(createddate) BETWEEN ds_start_date AND ds_end_date`, `COALESCE(country, '') IN ('UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar')`.

---

### 3. OPPORTUNITY / POST LEAD (from `salesforce_cloudkitchens.opportunity`)

| Column            | Required | Description |
|-------------------|----------|-------------|
| `created_date`    | Yes      | Alias of `createddate`. |
| `month_date`      | Yes      | `DATE_TRUNC(COALESCE(closed_won_date__c, closedate), MONTH)`. |
| `close_date`      | Yes      | Alias of `closedate`. |
| `tcv_usd`         | Yes      | `COALESCE(tcv_realised__c, amount, 0)`. |
| `kitchen_country` | Yes      | Market (alias of `account_market__c`). |
| `stage`           | Yes      | Alias of `stagename`. |
| `opportunity_name`| Yes      | Alias of `name`. |
| `approved__c`     | Yes      | Boolean; used for “CW inc approved” count. |

Optional: `opportunity_id` (id), `closed_won_date`, `amount`, `tcv_realised__c`, `iswon`, `isdeleted`.

**Filters:** `isdeleted IS FALSE OR isdeleted IS NULL`, `iswon = TRUE`, `DATE(COALESCE(closed_won_date__c, closedate)) BETWEEN ds_start_date AND ds_end_date`.  
**Note:** In this table `account_market__c` is often city/region (e.g. Los Angeles, SF Bay Area) or null—not country names like UAE/Kuwait. Filter to MENA in Looker/Sheet if needed.

---

## Date range

All three queries must use the same parameters:

- `ds_start_date` (DATE), e.g. `'2025-01-01'`
- `ds_end_date` (DATE), e.g. `CURRENT_DATE()`

---

## Markets (MENA)

Exact list for filters: **'UAE', 'Kuwait', 'Saudi Arabia', 'Bahrain', 'Qatar'**.  
Spend uses `country`, Lead uses `country`, Opportunity uses `account_market__c`. Values must match exactly (same spelling and spaces).

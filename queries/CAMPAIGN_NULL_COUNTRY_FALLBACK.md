# NULL / empty `country` → infer market from `campaign`

When **`use_campaign_market_fallback = TRUE`** (default), rows with **no `country`** in the warehouse can still be included if **`campaign_fallback_country(campaign)`** returns one of the four MENA markets.

## Order of patterns (first match wins)

| Pattern (on `LOWER(TRIM(campaign))`) | Market |
|----------------------------------------|--------|
| `^bh \|` | Bahrain |
| `^kw \|` | Kuwait |
| `^sa (` or `^sa \|` | Saudi Arabia |
| `^uae (` or `^uae \|` or `^uae &` | UAE |
| `^kw^` … (caret-delimited Nexa style) | Kuwait |
| `^sa^` | Saudi Arabia |
| `^uae^` | UAE |
| `_kw_en`, `_kw_ar` | Kuwait |
| `_ksa_en`, `_ksa_ar`, `_ksa` (suffix/underscore) | Saudi Arabia |
| `_uae_en`, `_uae_ar` | UAE |
| whole word `kuwait` | Kuwait |
| whole word `uae` | UAE |
| `saudi arabia`, `riyadh`, `jeddah`, `ksa` | Saudi Arabia |
| `_bh` (suffix/underscore), `^bh^` | Bahrain |
| `bahrain` (substring) | Bahrain |

## Limits

- Only runs when **`TRIM(country)` is empty** — does **not** override a non-empty wrong country.
- **False positives** are possible on odd names; set **`use_campaign_market_fallback = FALSE`** for strict warehouse `country` only.
- Does **not** recover spend that **never exists** in `funnel_data_join`.

## SQL

In BigQuery scripting, **`DECLARE` must come before any other statements** (including `CREATE TEMP FUNCTION`). Each file is ordered:

1. **`DECLARE …`** (variables)
2. **`CREATE TEMP FUNCTION campaign_fallback_country …`**
3. **`WITH` / `SELECT`**

Files:

- `block1_spend_xray_mirror.sql`
- `spend_xray_parity_one_row.sql`
- `spend_gap_monthly_market.sql`
- `reconcile_spend_xray_grand_total.sql` — run the whole file, or copy the **`DECLARE` + `CREATE TEMP FUNCTION`** block above any single block you run.

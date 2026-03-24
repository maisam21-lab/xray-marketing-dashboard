# BigQuery export CSV vs Looker total

## Your file (`script_job_*_0.csv`)

| Check | Value |
|--------|--------|
| **Rows** | 332 |
| **SUM(Spend)** | **~74,344.81** |
| **Matches** | `spend_xray_parity_one_row` **~74,344.78** ✓ |

The export is **internally consistent** with the warehouse + mirror logic (incl. **~$121** Bahrain Meta from **NULL `country` fallback**).

## SUM(Spend) by market (this CSV)

| Market | Spend USD |
|--------|----------:|
| UAE | 36,199.70 |
| Saudi Arabia | 22,627.10 |
| Kuwait | 13,899.55 |
| **Bahrain** | **1,618.46** |

### Bahrain breakdown (non-zero only)

| Unified Channel | Spend USD |
|-----------------|----------:|
| Google Search | 1,497.37 |
| Meta | **121.09** |

## Why it still doesn’t match Looker (~109K)

Looker’s **Bahrain** slice has **large Meta (and other) lines** month after month. In your BQ export, **Bahrain Meta is almost only the $121 fallback**; most Bahrain **Meta** (and some other) spend from Xray **never appears** as positive rows in `funnel_data_join` under a MENA market + channel mapping.

So:

- **~74.3K** = what the **warehouse** can show after SQL rules.  
- **~109K** = what **Looker** shows from its **data source**.  
- **~35K gap** ≈ **missing / different** rows in BigQuery vs that source — **not** a CSV summing error.

## How to sum the CSV correctly

- **SUM(Spend)** over **all rows** (including **0.0** scaffold rows) = same as summing **only rows with Spend ≠ 0** for the total.  
- Do **not** add **subtotals** from a pivot **and** detail rows.

## If you need one number to match Looker

Use the **Looker / Excel export** as the official total, or load that file into BigQuery as a **manual table** (`NO_ETL_SUPPORT_OPTIONS.md`) until the warehouse catches up.

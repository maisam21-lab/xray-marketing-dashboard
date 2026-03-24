# Why team spend sheet has more rows than the old BigQuery query

## 1. Channel labels (main fix)

| Warehouse (`datasource` / `datasourcetype`) | Sheet “Unified Channel” |
|---------------------------------------------|-------------------------|
| facebookads, meta, etc. | **Meta** |
| adwords / google + not PMax | **Google Search** |
| adwords / google + Performance Max / PMax | **PMax** |
| linkedin | **LinkedIn** |
| snapchat | **Snapchat** |

The old query used raw `datasourcetype` → you only saw **adwords** and **facebookads**, so everything rolled into **two** channels per market/month.

**Updated query:** `block1_spend_team_headers.sql` maps to the names above using `datasource`, `datasourcetype`, `campaigntype`, and `campaign`.

## 2. Row grain (multiple lines per channel)

The sheet often shows **several rows** for the same month, market, and channel (e.g. two “Sept | UAE | Meta” lines) because spend is split by **campaign** (or account / line item).

The updated query groups by **month × market × mapped channel × campaign** so you get **more rows**, closer to the sheet.

If you only need **one row per channel per month**, use the optional rollup at the bottom of `block1_spend_team_headers.sql` (group without `campaign_key`).

## 3. $0 scaffold rows

Rows like **Organic**, **Test**, **Ai Search** with **$0** are often a **full matrix** built in Sheets/Looker (every market × every channel). They are **not** always stored as rows in `funnel_data_join`. BigQuery will only show $0 if those rows exist in the table.

## 4. Totals should align

Sum of **Spend** for a month + market across all rows should match the **sum of `cost_usd`** in raw `funnel_data_join` for that month + market (same date filter). If totals still differ, check:

- Date range (`ds_start_date` / `ds_end_date`) vs sheet period  
- Whether the sheet includes **taxes**, **other fees**, or a **different currency** column  
- Whether some sheet data is **manual** or from another source  

## 5. Mirror Xray in BigQuery

Use **`block1_spend_xray_mirror.sql`** when you need the same **shape** as Xray:

- **$0 scaffold rows** for the standard channel matrix (Organic, Test, PMax shell, etc.) whenever there is no positive spend for that month × market × channel.
- **Excludes** campaigns Xray omits by default (`MILAN-IT` in `campaign_key`). Set `exclude_xray_orphans = FALSE` to match raw warehouse instead.
- **Month Name** = `Sept-25` style (`Sep` → `Sept`). Edit `FORMAT_DATE` in the query if your Xray uses only `Sept` or full `September 2025`.
- **`use_data_first_month`** (default `TRUE`): month grid starts at the **first month with any spend row** in the warehouse (within your date filter), so you don’t get huge **all-$0** blocks for Jan/Feb when data starts in March. Set to `FALSE` if you need every calendar month from `ds_start_date` filled.
- **`Month`** = `EXTRACT(MONTH)` → `1` for both Jan-25 and Jan-26; use **`Unified Date`** or **`Month Name`** as the unique time key in the sheet.

## 6. Column formats (sheet style)

In `block1_spend_team_headers.sql`:

- **Month** = calendar month number `1`–`12`  
- **Unified Date** = first of month as `DD/MM/YYYY` (e.g. `01/09/2025`)  
- **Month Name** = `Sept 2025` style (`Sep` → `Sept`)

Adjust `FORMAT_DATE` if your sheet uses only `Sept` without year.

## 7. Xray grand total lower than BigQuery (e.g. ~109K vs ~167K)

Usually **not** the $0 scaffold rows (they add **0**). Typical causes:

1. **`ds_end_date`** in `block1_spend_xray_mirror.sql` is **`CURRENT_DATE()`** → BigQuery includes **more months** than the Xray export (e.g. through 2026 while Xray stops at Dec 2025).
2. **Extra campaigns** still in `funnel_data_join` (only **MILAN-IT** is excluded by default); Xray may hide more via filters or manual rules.
3. **Different scope** — fewer markets or only **Meta + Google** in Xray’s total.

**Fix:** Set `ds_start_date` / `ds_end_date` to **exactly** match Xray, then run **`reconcile_spend_xray_grand_total.sql`** and read **`XRAY_VS_BQ_GRAND_TOTAL.md`**.

**Still off?** Run **`spend_xray_parity_one_row.sql`** (one row, three totals) and follow **`XRAY_PARITY_CHECKLIST.md`** — usually Xray is summing **Meta+Google (±PMax)** only, or uses different filters/currency than `cost_usd`.

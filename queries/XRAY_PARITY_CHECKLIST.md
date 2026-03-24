# Still not matching Xray — checklist

## 1) Compare **one** BigQuery number to **one** Xray number

Run **`spend_xray_parity_one_row.sql`** (same `ds_start_date` / `ds_end_date` as Xray).

| Column | Meaning |
|--------|--------|
| **`total_match_mirror_export`** | Use this vs Xray if Xray = **all channels** in the sheet (after MILAN-IT exclusion). |
| **`total_meta_google_pmax_only`** | Use if Xray “total” is **paid search/social** only (Meta + Google Search + PMax). |
| **`total_meta_google_search_only`** | Use if Xray **drops PMax** from the total. |

If **one** of these equals Xray and the others don’t, the mismatch is **scope** (which channels Xray sums), not BigQuery being wrong.

---

## 2) Same **period** and **markets**

- **Dates:** Xray filter must be the same as `ds_start_date` / `ds_end_date` (inclusive).
- **Markets:** SQL uses all five: UAE, Kuwait, Saudi Arabia, Bahrain, Qatar. If Xray hides a country, filter the sheet or add `AND country IN (...)` in SQL.

---

## 3) How you **SUM** in the sheet

- **`$0` scaffold rows** do not change the total (they add 0).
- Don’t **add subtotals + detail rows** (double count). Use **one** `SUM(Spend)` on the **detail** rows only, or **one** total from Xray’s own grand total.
- If you use a pivot, confirm **each dollar is counted once**.

---

## 4) Xray rules we **do not** copy in SQL

BigQuery uses **`funnel_data_join.cost_usd`** and your **campaign mapping**. Xray / Looker may also use:

- Account / MCC filters  
- “Active campaigns only”  
- **Taxes, fees, or local currency** then converted differently  
- **Blended** or **invoice** costs  

If **`total_match_mirror_export`** ≠ Xray after steps 1–3, you need whoever owns Xray to confirm **exact measure and filters** and then we can mirror them in SQL if the fields exist.

---

## 5) Find **which month** breaks (optional)

Run **Block 3** in **`reconcile_spend_xray_grand_total.sql`**, then in Xray get **spend by month** for the same range. The first month where totals diverge narrows the issue (timezone boundary, partial month in one tool, etc.).

---

## 6) BigQuery **lower** than Looker (~74K vs ~109K)

If **`total_match_mirror_export`** is **below** the Excel/Looker total **with the same dates and four markets**:

- Looker often pulls **live ad platform** totals; **`funnel_data_join`** may **lag**, **miss accounts**, or use a **different cost field** (`cost_usd` rules).
- Run **`spend_gap_monthly_market.sql`** and compare **each month × market** to a Looker pivot. If **every** cell is ~proportionally low, it’s a **systematic warehouse gap** (ETL / scope). If **one month** collapses, check **sync jobs** and **timezone** for that month.

---

## Files

| File | Purpose |
|------|--------|
| `spend_xray_parity_one_row.sql` | Single-row totals vs Xray |
| `spend_gap_monthly_market.sql` | Month × market breakdown to find missing spend |
| `reconcile_spend_xray_grand_total.sql` | Monthly / market / campaign breakdown |
| `block1_spend_xray_mirror.sql` | Full export (many rows + $0 scaffolds) |
| `LOOKER_XRAY_EXCEL_SCOPE.md` | Parsed scope of ME X-Ray Excel |

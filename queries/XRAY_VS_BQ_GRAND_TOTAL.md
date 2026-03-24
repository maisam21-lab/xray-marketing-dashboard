# Xray total (~109K) vs BigQuery (~167K) — how to close the gap

## First check: same period and same SUM rule

| Check | What to do |
|--------|------------|
| **End date** | In `block1_spend_xray_mirror.sql`, `ds_end_date` defaults to **`CURRENT_DATE()`**. Xray exports are often **through a fixed month** (e.g. 31 Dec 2025 or 30 Sep 2025). Set `ds_end_date` **exactly** to the last day Xray uses. |
| **Start date** | Align `ds_start_date` with Xray (often `2025-01-01`). |
| **Grand total in Sheets** | `$0` scaffold rows do **not** change the total. Use one **SUM** over **Spend** for the same filtered rows as Xray (same markets, same months). |

**Rule of thumb:** If BQ is ~50% higher, you often have **more months** in BQ (e.g. through Mar 2026) or **extra campaigns** still in the warehouse.

## Second check: run `reconcile_spend_xray_grand_total.sql`

1. At the top, set `ds_start_date` / `ds_end_date` to **match Xray**.
2. Run **Block 1** → raw MENA total for that window.  
3. Run **Block 2** → total after **MILAN-IT** exclusion (matches default mirror logic).  
4. Run **Block 3** → spend by **month**; compare to which months exist in Xray.  
5. Run **Block 4** → spend by **market**; confirm Xray includes all five (UAE, Kuwait, Saudi Arabia, Bahrain, Qatar).  
6. Run **Block 5** → spend in **suspicious** `campaign_key` buckets (`/IT/`, EUROPE-ish, etc.). If this is tens of thousands, Xray may be excluding those while BQ still includes them.  
7. Run **Block 6** → **top campaigns**; line-by-line compare to Xray.  
8. Run **Block 7** → totals by **Unified Channel**; if Xray “total” is only Meta + Google (+ PMax), compare that subset only.

## Third check: scope differences

- **Channels:** Some Xray views total only **paid search + paid social** (e.g. Meta + Google Search ± PMax). If so, sum only those channels in BigQuery (Block 7).  
- **Markets:** If Xray is “GCC” without Bahrain, remove that market in SQL or filter the sheet.  
- **Tax / fees / currency:** Confirm both sides use **USD** and the same **cost** field (`cost_usd`).

## If you need SQL to match Xray stricter rules

After Block 5–6 identify patterns (e.g. all `/IT/` campaigns, or a list of `campaign_key` prefixes), add a **single** `REGEXP_CONTAINS` or `NOT IN (...)` filter in `block1_spend_xray_mirror.sql` next to the existing MILAN-IT rule — **only after** marketing agrees those rows should not count toward MENA Xray.

---

**File:** `reconcile_spend_xray_grand_total.sql`  
**Mirror query:** `block1_spend_xray_mirror.sql` (set `ds_end_date` to Xray’s last day before comparing totals).

---

## Example from your reconciliation (Mar–Dec 2025 window)

| Metric | Value |
|--------|--------|
| **Grand total (raw)** | **146,517.70** |
| **Excl. MILAN-IT** | **145,552.69** (Δ ≈ **965** = Milan row) |
| **Google Search + Meta** (Block 7, excl. Milan) | **108,293.11 + 37,259.58 = 145,552.69** ✓ |
| **Orphan buckets** | Almost all in **other**; **MILAN-IT** alone ≈ **965** |

**Monthly sum check:** Mar–Sep 2025 from your table ≈ **105,735**; **Oct–Dec** ≈ **40,782** → **105,735 + 40,783 ≈ 146,518** ✓  

So if **Xray total ≈ 109K**, it is very close to **Mar–Sep only (~106K)** — not to **full Mar–Dec (~146K)**. The gap is almost certainly **Oct–Nov–Dec** (and any later months if `ds_end_date` was `CURRENT_DATE()`).

**Action:** In Xray, confirm the **last included month**. Then set the same **`ds_end_date`** (and **`compared_through_date`** in **Block 8** of `reconcile_spend_xray_grand_total.sql`) and compare again.

**Markets:** UAE + KSA + Kuwait + Bahrain sum to the total; **Qatar** shows **0** in this extract — if Xray includes Qatar as a column with zeros, totals still match; if Xray omits a market, filter BigQuery the same way.

**Qatar spend:** If the sheet expects Qatar but warehouse has 0, that’s a data/attribution topic, not a double-count.

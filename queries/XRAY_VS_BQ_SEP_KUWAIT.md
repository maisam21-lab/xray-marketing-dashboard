# Example: September · Kuwait — Xray sheet vs BigQuery

## What your screenshots show

### Xray-style table (many channels, incl. $0 scaffold)

| Unified Channel   | Example rows (Spend)     | Notes                          |
|-------------------|--------------------------|--------------------------------|
| Meta              | $615.41 + $90.62         | **Subtotal ≈ $706.03**         |
| Google Search     | $922.86 + $308.89 + $189.49 | **Subtotal ≈ $1,521.24**   |
| Organic, Test, …  | $0.00 each               | Usually **not** in warehouse |

### Tighter export (e.g. BigQuery-style “Sep-25”)

| Unified Channel   | Rows                         | Subtotal   |
|-------------------|------------------------------|------------|
| Google Search     | 165.78, 189.49, 921.38, 308.89 | **$1,585.54** |
| Meta              | 90.62, 615.41                | **$706.03**   |

## What matches

- **Meta total = $706.03** in both views (same two line items, same amounts).

## What differs

1. **Google Search row count & subtotal**  
   - Xray (first image): **3** rows → sum **$1,521.24**  
   - Second image: **4** rows → sum **$1,585.64** (about **+$64** vs first image)  
   So the two screenshots are **not** the same cut of the data: different **campaign / line splits** or one side **excludes/includes** a campaign (e.g. **$165.78** line only on one side).

2. **$0 channels** on Xray (Organic, PMax shell, etc.)  
   BigQuery only shows them if `funnel_data_join` actually has **$0** rows for those labels. Most Xray $0 lines are **matrix / template** rows, not warehouse facts.

3. **Labels**  
   - Xray: **Sept**, **01/09/2025**  
   - Other export: **Sep-25**, **1/9/2025**  
   Same month; formatting only. You can change `FORMAT_DATE` in `block1_spend_team_headers.sql` if you need **Sep-25** exactly.

## How to prove BigQuery vs Xray for Kuwait Sept

1. Run **`reconcile_spend_xray_sep_kuwait.sql`**  
   - **Block A** → totals per **Unified Channel** (compare to Xray column totals).  
   - **Block B** → same as your **row-level** export (campaign_key vs each Spend line).  
   - **Block C** → raw **grand total** for Kuwait Sept in the warehouse.

2. In the sheet, **SUM Spend** for Kuwait + Sept for **Meta** and **Google Search** only (ignore $0 scaffolds). Those two sums should equal Block A for those channels **if** the date range and market filter match **2025-09-01 … 2025-09-30** and **Kuwait**.

3. If **Google Search** still differs by ~$64, use **Block B** to see which **campaign_key** is extra or missing vs Xray, then check whether Xray **filters** (account, campaign group, “active only”, etc.) differ from the warehouse.

## Earlier JSON check (grouped query)

When everything was rolled up to **adwords** / **facebookads** only:

- **facebookads** Kuwait Sept ≈ **$706.03** → matches **Meta** total above.  
- **adwords** Kuwait Sept ≈ **$1,585.55** → matches the **four-row Google** subtotal (~**$1,585.54**), not the **three-row Xray** subtotal (~**$1,521.24**).

So the **warehouse + channel mapping** aligns with the **second** screenshot / rolled-up adwords total; the **first** Xray screenshot’s **Google** sum is **lower**, which usually means **Xray dropped a campaign row** or uses a **different rule** (e.g. different date timezone, filter, or manual edit).

---

## Confirmed from your reconciliation run (Kuwait · Sept 2025)

| Check | Result |
|--------|--------|
| **Google Search** | **$1,585.55** (116 raw rows rolled to 4 campaigns) |
| **Meta** | **$706.03** (23 raw rows → 2 campaigns) |
| **Grand total** | **$2,291.58** = 1585.55 + 706.03 ✓ |

**Campaign breakdown (Google Search):**

| campaign_key | spend_usd |
|--------------|-----------|
| KW \| Delivery [New Structure] | 921.38 |
| KW \| Generic [New Structure] | 308.89 |
| KW \| CPU [New Structure] | 189.49 |
| **/IT/MILAN-IT/WEBCON/CPU/KW** | **165.78** |

**Meta:**

| campaign_key | spend_usd |
|--------------|-----------|
| ^KW^CB-Landing Page Test | 615.41 |
| Nexa_Kitchen Park_Website Conversion_KW | 90.62 |

**Why Xray showed ~$1,521 for Google (3 rows):** that sum is **922.86 + 308.89 + 189.49** — it matches the three **KW | … [New Structure]** campaigns but **drops the Milan campaign** (**$165.78**). So **1,521.24 + 165.78 ≈ 1,587.02** (small cents diff vs 921.38 vs 922.86). The **~$64 gap** is essentially **that fourth campaign**.

**Note on `/IT/MILAN-IT/WEBCON/CPU/KW`:** name looks Italy-related but spend is in **Kuwait · Sept** rows in `funnel_data_join` (same `country` as other Kuwait lines). If that’s wrong attribution, fix in **ads naming / ETL** or add a **SQL filter** to exclude campaigns that don’t match your MENA naming rules (coordinate with marketing ops before excluding).

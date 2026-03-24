# Bahrain: Xray vs `funnel_data_join` (BigQuery)

## What Xray / Looker shows (your export)

Bahrain has **non-zero spend every month** Sept 2025 → Feb 2026, including:

| Period | Examples from Xray (not exhaustive) |
|--------|-------------------------------------|
| **Sept 2025** | Meta (~$702 + ~$68), Google Search (several lines) |
| **Oct 2025** | Meta **~$1,693**, Google Search (multiple lines) |
| **Nov 2025** | Meta (~$1,289 + ~$68), Google Search (multiple lines) |
| **Dec 2025** | Google Search (~$941 + ~$70), Meta (~$929 + ~$137), Snapchat ~$216, LinkedIn (~$33 + ~$187) |
| **Jan 2026** | Meta (~$471 + ~$125), LinkedIn ~$375, Snapchat ~$374, PMax (~$235 + ~$300), Google Search (~$51 + ~$649) |
| **Feb 2026** | Meta (~$301 + ~$125), Google Search (~$259 + ~$17), PMax (~$197 + ~$100), Snapchat ~$231, LinkedIn ~$260 |

Plus scaffold channels at **$0** (Organic, Test, etc.) — same as other markets.

**Attach:** screenshot **“Bahrain data from xray”** to the data ticket.

---

## Fix you can apply in SQL (partial)

Campaign **`Bahrain (LP Conversion)`** (Facebook APAC) has **`country` NULL** in the warehouse.  
The mirror query used to **drop** those rows because `NULL NOT IN (mena list)`.

**Now:** `block1_spend_xray_mirror.sql` (and parity / gap / reconcile scripts) use  
`use_campaign_market_fallback = TRUE` (default) and **`campaign_fallback_country(campaign)`** — same idea as Bahrain for **Kuwait, Saudi Arabia, UAE** (e.g. `KW |`, `^KW^`, `_KW_EN`, `UAE |`, `Riyadh`, …).  
See **`CAMPAIGN_NULL_COUNTRY_FALLBACK.md`**. This only recovers rows **already in the table** with empty `country`; it does not create missing Xray spend.

---

## What BigQuery shows (`css-dw-sync` … `funnel_data_join`)

| Window | `country = 'Bahrain'` |
|--------|------------------------|
| **2025-09** | 78 rows, **$865.81** |
| **2025-10** | 45 rows, **$534.85** |
| **2025-11** | 11 rows, **$96.71** |
| **2025-12 – 2026-02** | **No rows** (empty) |

So **December 2025 through February 2026 Bahrain spend exists in Xray but not in the warehouse** (at least not under `country = 'Bahrain'`).

Even **Sept–Nov** BQ totals are **far below** Xray line-item sums → under-load or mapping issues before December as well.

---

## Ask for ETL

1. Ingest **Bahrain ad accounts** for **Dec 2025 – Feb 2026** into `funnel_data_join`.  
2. Confirm **`country` = `Bahrain`** on those rows (not rolled into UAE or `NULL`).  
3. Reconcile **Sept–Nov** BQ vs Xray (row counts and `SUM(cost_usd)`).

**Queries used:** `debug_bahrain_spend_by_month.sql`, `spend_gap_monthly_market.sql`, `spend_xray_parity_one_row.sql`.

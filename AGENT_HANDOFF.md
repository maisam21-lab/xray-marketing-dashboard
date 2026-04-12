# Agent handoff: X-Ray Marketing Dashboard (Streamlit / PMC)

Use this file after a machine restart or when a **new agent** picks up the work. Source of truth for code is **`xray-marketing-dashboard-git`** on branch **`main`**, pushed to **`https://github.com/maisam21-lab/xray-marketing-dashboard.git`**.

---

## 1. Repo & run

| Item | Value |
|------|--------|
| **Primary clone** | `C:\Users\MaysamAbuKashabeh\xray-marketing-dashboard-git` |
| **Entry** | `oracle_app.py` (Streamlit Cloud uses this on `main`) |
| **Run (Windows)** | `py -3 -m streamlit run oracle_app.py` from repo root |
| **Tests** | `py -3 -m pytest tests/ -q` — dev deps: `requirements-dev.txt` |

Do **not** confuse with other folders (`xray-marketing-dashboard\` without `-git`, Desktop copies); Cloud tracks **GitHub `main`**.

---

## 2. Streamlit Cloud

- App logs referenced repo **`xray-marketing-dashboard`**, branch **`main`**, module **`oracle_app.py`**.
- **Verify deploy**: header shows **`DASHBOARD_BUILD`** (see `oracle_app.py` top). If stale, check Cloud **Manage app** → correct **GitHub repo / branch**, then **Reboot app** + hard browser refresh.
- **Spend by channel** is the **last** tab (`_DASH_NAV_OPTIONS` order: Marketing performance → Market MoM → Inbound channels → **Spend by channel**).

---

## 3. Product intent (Spend by channel / PMC)

- **Spend** should align with **ME X-Ray** workbook (primary id `DEFAULT_SHEET_ID` / `1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8`) and **Marketing performance** spend merge (X-Ray + optional Supermetrics for clicks/impressions).
- **Channel grain** = sheet / Looker **Unified Channel** labels (Organic, Instagram Organic, Meta, PMax, Google Search, LinkedIn, Snapchat, brand rows like Express Kitchens / Alta Ai / Ai Search, **Test**, etc.) — **not** a single “Paid Media” bucket.
- **Tab**: non-inbound path `render_page_channels(..., inbound=False)` → `key_suffix="pmc"` → `_render_page_performance_marketing_channels`.

---

## 4. Major code changes (chronological themes)

### 4.1 PMC master table (replaced old narrow 5-channel + Other + ME grid)

- Wide **master-style** table via `_master_performance_table(..., pivot_dimension="channel")`.
- Title on tab: **“Channel spend by month”** (was “Channel spend (master layout)” earlier).
- Pivot builder: `_spend_sheet_pivot_by_month_channel` → groups by `_pmc_sheet_channel_series`, writes into **`country`** column for merge compatibility then renames to **Channel** in the grid.

### 4.2 Unified Channel column

- **`_NORM_TO_FIELD`**: `unified_channel` ← normalized header **`Unified Channel`** maps to canonical **`channel`**.
- **`_normalize`**: when both **Unified Channel** and **Channel** exist, **`Unified Channel` wins** (sorted first in `field_to_sources["channel"]`).
- **`_pmc_sheet_channel_series`**: resolution order = **Unified Channel** column (if present) → **channel** → **platform** → **source_tab** (with `_pmc_align_channel_label_for_xray_pivot`).
- **`_pmc_unified_channel_series`**: same priority for **charts** so axes match table logic.

### 4.3 “Only one channel” / Paid Media collapse — fixes

- **`_mpo_blend_paid_media_for_master_df`**: unknown **`channel`** filled with **`""`** (was `"Paid media"`), so pivot can fall back to **platform**.
- **`_pmc_sheet_channel_series` `_bad`**: treats **`paid media` / `paid_media`** as empty for fallback.
- **Preserve row-level `platform`**: coalesce with tab label only when row platform is blank (`_mpo_coalesce_str_series_with_tab_fallback`) — avoids collapsing Search vs PMax on the same Google tab.
- **`_pmc_align_channel_label_for_xray_pivot`**: removed YouTube-only branch; **Google → Google Search** only for **Ads-style** names (`ads`, `adwords`, bare `google` / `google_ads`), **not** when **`organic`** or **`seo`** appears (keeps e.g. **Google Organic**).
- **`_pmc_normalize_channel_label`**: same stricter Google rule for chart labels.

### 4.4 March floor + Spend-only table (PMC)

- **`_pmc_floor_march_or_later(start_date)`**: if window starts before **1 March** of that year, floor to **1 Mar**; else keep `start_date`.
- **`_pmc_filter_month_not_before(df, date)`**: drops rows with **`month`** strictly before that calendar month.
- **`_render_page_performance_marketing_channels`**: filters **`df_scope`** with March floor before **`_pmc_frame_with_metrics`** / pivot / charts.
- **`_master_performance_table`**: new kwargs **`table_mode`** (`"full"` \| `"spend_only"`) and **`month_not_before`** (optional `date`). PMC uses **`table_mode="spend_only"`** → displayed columns **Month**, **Channel**, **Spend** only; **`cell_metric_allowlist`** fixes cell-selection vs metrics list.

### 4.5 Streamlit / pandas hygiene

- **Pandas `str.contains` warnings**: use **non-capturing** groups in regexes, e.g. `(?:raw\s*)?`, `(?:chat)?`.
- **`use_container_width`**: not migrated project-wide (still deprecation warnings in logs); optional follow-up → `width="stretch"` / `"content"`.
- **`mpo_market` session state**: init **`mpo_market` / `mpo_month`** (and **`pmc_*`**) in `st.session_state` before widgets; **`st.multiselect`** without **`default=`** where normalized, to avoid “default + session_state” warning.

### 4.6 Deploy visibility

- **`DASHBOARD_BUILD`** in `oracle_app.py` (bumped across commits); header HTML includes a **deploy-build** pill next to “Live” for quick Cloud verification.

---

## 5. Key symbols (grep anchors)

| Symbol | Role |
|--------|------|
| `DASHBOARD_BUILD` | Build fingerprint + cache key |
| `_mpo_spend_sheet_for_channel_master` | PMC spend frame (X-Ray scoped + blend + CI merge) |
| `_apply_channel_tab_data_scope` | PMC Data scope: channels + month multiselects |
| `_spend_sheet_pivot_by_month_channel` | Month × channel (→ `country` in frame) spend pivot |
| `_pmc_sheet_channel_series` / `_pmc_unified_channel_series` | Channel label resolution |
| `_pmc_floor_march_or_later` / `_pmc_filter_month_not_before` | March onward |
| `_master_performance_table` | Master grid; `table_mode`, `month_not_before`, `pivot_dimension` |
| `_DASH_NAV_OPTIONS` / `tab_pmc` | Last tab = Spend by channel |

---

## 6. Tests

- **`tests/test_pmc_helpers.py`**: align rules, pivot aggregation, paid-media fallback, row-level platform split, **Unified Channel** wins over `channel`, **March floor/filter**, `_month_norm_key`, `_filter_spend_for_dashboard`.
- Run: `py -3 -m pytest tests/ -q`

---

## 7. Recent `main` commits (approximate chain)

Push history includes (among others): PMC master / X-Ray parity, pytest suite, session-state + regex fixes, header build pill, channel pivot / Paid media / row-platform fixes, **Unified Channel** mapping, **March + spend-only** PMC table. **Latest at time of writing**: check with:

`git log -8 --oneline origin/main`

---

## 8. Known follow-ups / not done

- Migrate **`use_container_width`** → **`width`** across `oracle_app.py` (large diff).
- **Data scope** month multiselect may still list months outside March floor for PMC (table/charts are floored); tighten if UX asks.
- **Charts** on Spend by channel still use leads/CW/SQL when present; user asked **table** spend-only — confirm if charts should become spend-only too.
- **Cross-year** March rule is “March of `start_date.year`”; if you need a fixed fiscal March across years, specify and adjust `_pmc_floor_march_or_later`.

---

## 9. Quick verification checklist

1. Open **Spend by channel** (last tab).
2. Header pill / caption: **`DASHBOARD_BUILD`** matches latest `main`.
3. Table: **Month | Channel | Spend** only; months **≥ March** of reporting-start year when window started before March.
4. **Channel** values reflect **Unified Channel** / sheet grain (many rows, not one “Paid Media”).
5. **`pytest`** passes locally.

---

*Generated for handoff before device restart. Update `DASHBOARD_BUILD` and this file when shipping further PMC changes.*

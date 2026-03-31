# ME X-Ray ↔ dashboard contract (frozen)

This document matches `oracle_app.py` in this folder (same code as [xray-marketing-dashboard](https://github.com/maisam21-lab/xray-marketing-dashboard) / Streamlit Cloud). Update it when you change tab names, regex patterns, or BigQuery export column headers.

## Workbook

- Default Google Sheet ID is defined as `DEFAULT_SHEET_ID` in `oracle_app.py`. Override with Streamlit secret `XRAY_SHEET_ID`.
- The service account in Secrets must have **Viewer** access on the spreadsheet. Enable **Google Sheets API** on the GCP project.

## Tab names (Excel local load)

Preferred tabs loaded in order (missing tabs are skipped; if none match, all non-empty sheet names are used):

| Tab name | Role |
|----------|------|
| `Raw Spend` | Spend, clicks, impressions |
| `Raw Leads` | Leads, qualified |
| `Raw Post Qualification` | Post-lead funnel, Closed Won (deduped by opportunity keys when present) |
| `RAW CW` | TCV, first-month LF |

## Tab regex (Marketing Performance Overview)

Spend / traffic is taken from tabs whose title matches any of:

- `raw\s*spend`, `^\s*spend\s*$`, `sum\s*spend`, `\bspend\b`

Leads:

- `raw\s*leads?`

Post-lead (CW + pipeline); **do not** point these patterns at RAW CW or spend-only tabs:

- `raw.*post.*qual`
- `post\s*leads?`

### Closed Won KPI (Post Lead `Stage`)

The **CW (Inc Approved)** card uses `closed_won` from post-lead rows where **Stage** includes *Closed Won* (not *Closed Lost*) and the deal counts as **approved**: the word *approved* in Stage, or Stage is exactly `Closed Won`, or an optional column such as **Approval Status** / **Approved** accepts the row. Rows with *Not Approved* in Stage are excluded; if an approval column says *Not Approved*, the row is excluded unless Stage text still contains *approved*.

RAW CW (TCV / LF):

- `raw\s*cw`

## Streamlit / deployment

- Secrets: `[gsheet_service_account]` or `GCP_SERVICE_ACCOUNT` (full JSON fields including PEM `private_key`).
- Optional: `XRAY_SHEET_ID`, `XRAY_TRUTH_GID`, `XRAY_EXCEL_PATH`, `XRAY_LOGO_PATH`.

## Smoke test (local Excel)

After exporting or saving the ME X-Ray workbook:

```text
pip install -r requirements.txt
python scripts/smoke_verify_excel.py "C:\path\to\ME_X-Ray.xlsx"
python scripts/smoke_verify_excel.py ".\book.xlsx" --month 2025-09 --market Kuwait
```

Totals are computed with `compute_mpo_totals()` — the same logic as the **Marketing Performance Overview** KPI block.

## BigQuery → Sheet pipeline

Operational steps for refreshing tabs from BigQuery live in `queries/STEP_BY_STEP_WHAT_TO_DO.md`.

## Looker-style UI

The app uses a fixed header, teal palette (`#4f8483`), hidden sidebar, and tab titles aligned with Looker page names (`LOOKER_PAGES`). Match new screenshots by adjusting the `<style>` block in `main()` only when needed.

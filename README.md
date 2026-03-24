# EurasiaX Marketing – Looker Studio (source of truth) + raw data

**Looker Studio is the source of truth** for MENA marketing performance and has the most recent data.

| Resource | Link |
|----------|------|
| **Looker Studio report (MENA)** | [Open Looker Studio report](https://lookerstudio.google.com/u/0/reporting/ea7c7905-baf6-40ad-ad97-f36bd67c4dbc/page/p_w4ql3rgi1d) |
| **Raw data (Google Sheet)** | [Marketing raw data – Spend, Lead, Post Lead](https://docs.google.com/spreadsheets/d/1eIE4d21-l0hNFg-9vdgtpnObyOm30cc7SOsQvUwE7x8/edit?pli=1&gid=8109573#gid=8109573) |

Use **Looker Studio** for official reporting. The **Google Sheet** holds all raw data (Spend, Lead, Post Lead) that feeds Looker.

---

## Optional: Streamlit app

The Streamlit app in this folder can read from the same Sheet (or BigQuery/CSV) for local analysis. It is **not** the source of truth.

```bash
cd streamlit_dashboard
pip install -r requirements.txt
streamlit run app.py
```

- **Data source:** Google Sheets (pre-filled with the Sheet ID above), BigQuery, or CSV upload.
- **Queries:** See [`queries/`](queries/README.md) for BigQuery SQL used to build or refresh the raw data.

Markets: UAE, Kuwait, Saudi Arabia, Bahrain, Qatar.

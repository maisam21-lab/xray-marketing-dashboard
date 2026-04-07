# Deploy to Streamlit Community Cloud

## 1. Push this repo to GitHub

Use the folder that contains `oracle_app.py` and `requirements.txt` as the repo root.

## 2. Create an app on Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
2. **New app** → pick the repository and branch.
3. **Main file path:** `oracle_app.py`
4. **App URL:** choose a subdomain (e.g. `yourname-xray-dashboard`).

## 3. Secrets (required for Google Sheets)

The app reads a **Google service account JSON** from Streamlit secrets so it can open your spreadsheet.

In the Cloud app → **Settings** → **Secrets**, paste a TOML block like:

```toml
GCP_SERVICE_ACCOUNT = """
{
  "type": "service_account",
  "project_id": "...",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
  "client_email": "your-sa@project.iam.gserviceaccount.com",
  ...
}
"""
```

Alternatively, use nested TOML tables if you prefer (see Streamlit [Secrets management](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management)).

**Sheet access:** share the Google Sheet with the service account **client email** (Viewer or Editor).

Optional secrets (see comments at the top of `oracle_app.py`):

- `XRAY_SHEET_ID` — workbook id or full Sheets URL  
- `PAID_MEDIA_SHEET_ID` / Supermetrics workbook  
- Worksheet `gid` values: `XRAY_SPEND_GID`, `XRAY_LEADS_GID`, etc.

## 4. Redeploy

After each push to the connected branch, Cloud rebuilds automatically. Confirm the build stamp in the **Marketing performance** hero matches `DASHBOARD_BUILD` in `oracle_app.py`.

## Local run (Windows)

```bat
cd xray-marketing-dashboard-git
py -m streamlit run oracle_app.py
```

If `streamlit` is not a command, use `py -m streamlit` (not `streamlit` alone).

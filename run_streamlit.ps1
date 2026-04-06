# Run the dashboard from the git repo root (correct working dir for assets and .streamlit).
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
py -3 -m streamlit run oracle_app.py @args

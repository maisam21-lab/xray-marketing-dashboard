# Copy oracle_app.py from this repo to other folders where Streamlit is often started (avoids "nothing changed").
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path $PSScriptRoot -Parent
$src = Join-Path $repoRoot "oracle_app.py"
if (-not (Test-Path -LiteralPath $src)) {
    throw "Missing $src"
}
$parent = Split-Path $repoRoot -Parent

$targets = @(
    (Join-Path $parent "xray-marketing-dashboard\oracle_app.py"),
    (Join-Path $env:USERPROFILE "Desktop\Q1'2026\Q1_task1_revops_marketing\streamlit_dashboard\oracle_app.py")
)

foreach ($t in $targets) {
    $dir = [System.IO.Path]::GetDirectoryName($t)
    if ($dir -and (Test-Path -LiteralPath $dir)) {
        Copy-Item -LiteralPath $src -Destination $t -Force
        Write-Host "Updated: $t"
    } else {
        Write-Host "Skip (folder missing): $dir"
    }
}
Write-Host "Done. Restart Streamlit (stop terminal, run again)."

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$demoPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $demoPython)) {
    $demoPython = (Get-Command python -ErrorAction SilentlyContinue).Source
}
if (-not $demoPython) {
    Write-Error 'Cần Python 3.11/3.12. Xem README.md để cài môi trường.'
}
$env:APP_MODE = 'demo'
& $demoPython -m backend.app

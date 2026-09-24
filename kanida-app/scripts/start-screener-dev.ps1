# DEV ONLY: the Options Screener on its own port, isolated from the pilot on :8082 (see scripts/screener_dev.py).
#   scripts/start-screener-dev.ps1 -Build      export the web app to dist-screener/ first
#   scripts/start-screener-dev.ps1             serve the existing dist-screener/
# Refuses to start if the port is taken, and never touches the pilot, its dist folder, or its scheduled tasks.
param([int]$Port = 8092, [switch]$Build)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root
$python = Join-Path $root '.pilot-venv\Scripts\python.exe'
if ($Port -eq 8082 -or $Port -eq 8765) { throw "Port $Port belongs to the running pilot / scanner. Pick another." }
if ($Build) {
    & npx.cmd expo export --platform web --output-dir dist-screener
    if ($LASTEXITCODE -ne 0) { throw 'Web export failed.' }
}
if (-not (Test-Path -LiteralPath (Join-Path $root 'dist-screener\index.html'))) { throw 'Build first: scripts/start-screener-dev.ps1 -Build' }
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { throw "Port $Port already has a listener." }
New-Item -ItemType Directory -Force -Path (Join-Path $root 'var\dev-screener') | Out-Null
$env:SCREENER_DEV_PORT = [string]$Port
$process = Start-Process -FilePath $python -ArgumentList @('scripts\screener_dev.py') -WorkingDirectory $root -WindowStyle Hidden `
  -RedirectStandardOutput (Join-Path $root 'var\dev-screener\server.stdout.log') -RedirectStandardError (Join-Path $root 'var\dev-screener\server.stderr.log') -PassThru
$process.Id | Set-Content -LiteralPath (Join-Path $root 'var\dev-screener\server.pid')
Write-Output "Screener dev server starting at http://127.0.0.1:$Port/__dev/session?as=owner (pid $($process.Id))"

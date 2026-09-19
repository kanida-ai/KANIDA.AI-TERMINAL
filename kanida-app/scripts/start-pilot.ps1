param([int]$Port = 8082, [switch]$Build)
$ErrorActionPreference = 'Stop'
$pilotRoot = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $pilotRoot
$python = Join-Path $pilotRoot '.pilot-venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw 'Create .pilot-venv and install server/requirements.lock.txt first. See PRIVATE_PILOT.md.' }
if ($Build) {
    & npx.cmd expo export --platform web --output-dir dist-pilot
    if ($LASTEXITCODE -ne 0) { throw 'Web export failed. The running preview was not changed.' }
}
if (-not (Test-Path -LiteralPath (Join-Path $pilotRoot 'dist-pilot\index.html'))) { throw 'Build the web app first: scripts/start-pilot.ps1 -Build' }
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { throw "Port $Port already has a listener. Stop only the verified old preview process before starting this pilot." }
New-Item -ItemType Directory -Force -Path (Join-Path $pilotRoot 'var') | Out-Null
$env:PYTHONPATH = Join-Path $pilotRoot 'server'
$env:PILOT_WEB_DIRECTORY = Join-Path $pilotRoot 'dist-pilot'
$env:PILOT_PORT = [string]$Port
$env:PILOT_BIND = '0.0.0.0'
& $python -m kanida_pilot.bootstrap
if ($LASTEXITCODE -ne 0) { throw 'Owner invitation preparation failed.' }
$process = Start-Process -FilePath $python -ArgumentList @('-m','kanida_pilot') -WorkingDirectory $pilotRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $pilotRoot 'var\pilot.stdout.log') -RedirectStandardError (Join-Path $pilotRoot 'var\pilot.stderr.log') -PassThru
$process.Id | Set-Content -LiteralPath (Join-Path $pilotRoot 'var\pilot-launcher.pid')
Write-Output "Pilot starting at http://127.0.0.1:$Port/welcome. Private owner invitation: var/OWNER_INVITATION.txt"

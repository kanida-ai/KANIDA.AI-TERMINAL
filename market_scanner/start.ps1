param([switch]$ReuseSnapshot)
$ErrorActionPreference = 'Stop'
$kanidaRoot = Split-Path -Parent $PSScriptRoot
$kanidaConfig = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'config.json') -Raw | ConvertFrom-Json
$kanidaUrl = "http://127.0.0.1:$($kanidaConfig.port)"
try {
    $kanidaHealth = Invoke-RestMethod "$kanidaUrl/health" -TimeoutSec 2
    if ($kanidaHealth.app -eq 'KANIDA') { Write-Output "KANIDA is already running: $kanidaUrl"; exit 0 }
} catch {}
$kanidaPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $kanidaPython)) { $kanidaPython = (Get-Command python -ErrorAction Stop).Source }
New-Item -ItemType Directory -Path (Join-Path $PSScriptRoot 'output') -Force | Out-Null
$kanidaArguments = @('-m','market_scanner.server')
if ($ReuseSnapshot) { $kanidaArguments += '--reuse-snapshot' }
$kanidaProcess = Start-Process -FilePath $kanidaPython -ArgumentList $kanidaArguments -WorkingDirectory $kanidaRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $PSScriptRoot 'output/server.log') -RedirectStandardError (Join-Path $PSScriptRoot 'output/server-error.log') -PassThru
$kanidaProcess.Id | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'server.pid')
for ($kanidaAttempt = 0; $kanidaAttempt -lt 30; $kanidaAttempt++) {
    Start-Sleep -Seconds 1
    if ($kanidaProcess.HasExited) { throw 'KANIDA failed to start. See output/server-error.log.' }
    try { if ((Invoke-RestMethod "$kanidaUrl/health" -TimeoutSec 1).app -eq 'KANIDA') { Write-Output "KANIDA ready: $kanidaUrl"; exit 0 } } catch {}
}
throw 'Startup exceeded 30 seconds. Inspect output/server-error.log.'

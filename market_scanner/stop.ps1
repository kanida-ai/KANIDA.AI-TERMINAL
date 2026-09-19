$ErrorActionPreference = 'Stop'
$kanidaPidFile = Join-Path $PSScriptRoot 'server.pid'
if (-not (Test-Path -LiteralPath $kanidaPidFile)) { Write-Output 'No recorded KANIDA server.'; exit 0 }
$kanidaPid = [int](Get-Content -LiteralPath $kanidaPidFile)
$kanidaProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$kanidaPid"
if ($kanidaProcess -and $kanidaProcess.CommandLine -match 'market_scanner\.server') {
    Stop-Process -Id $kanidaPid
    Remove-Item -LiteralPath $kanidaPidFile
    Write-Output 'KANIDA server stopped.'
} else { Write-Output 'Recorded process is absent or is not KANIDA; nothing was stopped.' }

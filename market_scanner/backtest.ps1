param([switch]$NewSnapshot)
$ErrorActionPreference = 'Stop'
$kanidaRoot = Split-Path -Parent $PSScriptRoot
$kanidaPidFile = Join-Path $PSScriptRoot 'backtest.pid'
if (Test-Path -LiteralPath $kanidaPidFile) {
    $kanidaRecordedPid = [int](Get-Content -LiteralPath $kanidaPidFile)
    $kanidaRunning = Get-CimInstance Win32_Process -Filter "ProcessId=$kanidaRecordedPid"
    if ($kanidaRunning -and $kanidaRunning.CommandLine -match 'market_scanner\.backtest_worker') {
        Write-Output 'KANIDA backtest is already running. Follow progress in Pattern backtests.'
        exit 0
    }
}
$kanidaPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $kanidaPython)) { throw 'Backtest runtime is missing. Create .venv and install requirements-backtest.txt first.' }
$kanidaArguments = @('-m','market_scanner.backtest_worker','--workers','8')
if ($NewSnapshot) { $kanidaArguments += '--new' }
$kanidaProcess = Start-Process -FilePath $kanidaPython -ArgumentList $kanidaArguments -WorkingDirectory $kanidaRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $PSScriptRoot 'output/backtest.log') -RedirectStandardError (Join-Path $PSScriptRoot 'output/backtest-error.log') -PassThru
$kanidaProcess.Id | Set-Content -LiteralPath $kanidaPidFile
Write-Output 'KANIDA backtest started. Open http://127.0.0.1:8765 and select Pattern backtests.'

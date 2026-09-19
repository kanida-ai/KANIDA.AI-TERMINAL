param([Parameter(ValueFromRemainingArguments = $true)][string[]]$ResearchArgs)
$ErrorActionPreference = 'Stop'
$KanidaRoot = Split-Path -Parent $PSScriptRoot
$KanidaPython = Join-Path $KanidaRoot 'market_scanner\.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $KanidaPython)) { throw 'KANIDA project Python environment was not found.' }
if (-not $ResearchArgs -or $ResearchArgs.Count -eq 0) { $ResearchArgs = @('capabilities') }
Push-Location -LiteralPath $KanidaRoot
try {
    & $KanidaPython -m research_orchestrator @ResearchArgs
    exit $LASTEXITCODE
} finally { Pop-Location }

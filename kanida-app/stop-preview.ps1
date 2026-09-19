$ErrorActionPreference = 'Stop'
foreach ($entry in @(@('product.pid','market_scanner.product'),@('expo.pid','expo[/\\]bin[/\\]cli'))) {
    $pidPath = Join-Path $PSScriptRoot $entry[0]
    if (Test-Path -LiteralPath $pidPath) {
        $savedPid = [int](Get-Content -LiteralPath $pidPath)
        $savedProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$savedPid"
        if ($savedProcess -and $savedProcess.CommandLine -match $entry[1]) {
            Get-CimInstance Win32_Process -Filter "ParentProcessId=$savedPid" | Where-Object { $_.CommandLine -match $entry[1] } | ForEach-Object { Stop-Process -Id $_.ProcessId }
            Stop-Process -Id $savedPid -ErrorAction SilentlyContinue
        }
        Remove-Item -LiteralPath $pidPath
    }
}
Write-Output 'Product preview stopped. The original research scanner is still running.'

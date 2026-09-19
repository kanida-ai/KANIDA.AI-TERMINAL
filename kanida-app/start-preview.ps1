param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$appRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent $appRoot
$venvPython = Join-Path $projectRoot 'market_scanner\.venv\Scripts\python.exe'
# Use the base interpreter directly so Windows does not leave a venv redirector child behind.
$pythonPath = (& $venvPython -c 'import sys; print(sys._base_executable)').Trim()
$nodePath = (Get-Command node).Source
$lanAddress = Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254.*' -and $_.AddressState -eq 'Preferred' -and $_.InterfaceAlias -notmatch 'vEthernet|Loopback' } | Select-Object -First 1 -ExpandProperty IPAddress
if (-not $lanAddress) { throw 'No local network address is available. Connect to Wi-Fi first.' }
$envText = "EXPO_PUBLIC_API_URL=http://${lanAddress}:8082`nEXPO_PUBLIC_EXPO_URL=exp://${lanAddress}:8081`n"
[System.IO.File]::WriteAllText((Join-Path $appRoot '.env.local'), $envText)
try { $null = Invoke-RestMethod 'http://127.0.0.1:8765/health' -TimeoutSec 3 }
catch { & (Join-Path $projectRoot 'market_scanner\start.ps1') -ReuseSnapshot }
if (-not $SkipBuild) {
    Push-Location $appRoot
    try { & npm.cmd run export:web; if ($LASTEXITCODE -ne 0) { throw 'Web export failed.' } }
    finally { Pop-Location }
}
$productPidFile = Join-Path $appRoot 'product.pid'
$expoPidFile = Join-Path $appRoot 'expo.pid'
foreach ($entry in @(@($productPidFile,'market_scanner.product'),@($expoPidFile,'expo[/\\]bin[/\\]cli'))) {
    if (Test-Path -LiteralPath $entry[0]) {
        $savedPid = [int](Get-Content -LiteralPath $entry[0])
        $savedProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$savedPid"
        if ($savedProcess -and $savedProcess.CommandLine -match $entry[1]) {
            Get-CimInstance Win32_Process -Filter "ParentProcessId=$savedPid" | Where-Object { $_.CommandLine -match $entry[1] } | ForEach-Object { Stop-Process -Id $_.ProcessId }
            Stop-Process -Id $savedPid -ErrorAction SilentlyContinue
        }
    }
}
$productProcess = Start-Process -FilePath $pythonPath -ArgumentList '-m','market_scanner.product' -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $appRoot 'product.log') -RedirectStandardError (Join-Path $appRoot 'product.error.log')
Set-Content -LiteralPath $productPidFile -Value $productProcess.Id
$expoCli = Join-Path $appRoot 'node_modules\expo\bin\cli'
$expoProcess = Start-Process -FilePath $nodePath -ArgumentList ('"'+$expoCli+'"'),'start','--go','--lan','--port','8081' -WorkingDirectory $appRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $appRoot 'expo.log') -RedirectStandardError (Join-Path $appRoot 'expo.error.log')
Set-Content -LiteralPath $expoPidFile -Value $expoProcess.Id
Write-Output "Web: http://127.0.0.1:8082/"
Write-Output "iPhone QR: http://127.0.0.1:8082/connect"
Write-Output "Expo Go: exp://${lanAddress}:8081"
Write-Output "Safari on the same Wi-Fi: http://${lanAddress}:8082/"

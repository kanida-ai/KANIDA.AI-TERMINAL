# Registers the "KanidaOrderFlowPoller" Windows Scheduled Task.
# Fires run_orderflow_poller.bat every 30 min in a fresh process. The poller
# self-gates to the IST market window (08:50-15:30 weekdays); out-of-window fires
# exit in <1s. Inside the window it loops every minute until 15:30 then exits.
# MultipleInstances=IgnoreNew -> only ONE all-day loop runs; a crash is recovered
# by the next 30-min trigger. Idempotent: re-running removes and recreates the task.
$ErrorActionPreference = "Stop"

$TaskName   = "KanidaOrderFlowPoller"
$ProjectDir = "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
$BatPath    = Join-Path $ProjectDir "scripts\run_orderflow_poller.bat"

if (-not (Test-Path $BatPath)) { throw "Poller batch not found at: $BatPath" }
Write-Host "=== Registering Scheduled Task $TaskName ===" -ForegroundColor Cyan

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) { Write-Host "Removing existing task..."; Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false }

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$BatPath`"" -WorkingDirectory (Join-Path $ProjectDir "backend")
$anchor = (Get-Date).Date
$trigger = New-ScheduledTaskTrigger -Once -At $anchor -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration ([TimeSpan]::FromDays(3650))
# ExecutionTimeLimit 7h covers the all-day loop; IgnoreNew keeps a single instance.
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 7) -RestartCount 0 -DontStopOnIdleEnd -WakeToRun
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Kanida order-flow poller - snapshots buy/sell qty + depth for the F&O universe every minute during market hours. Self-gates to 08:50-15:30 IST weekdays." | Out-Null

Write-Host "Registered. Verifying..." -ForegroundColor Green
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State | Format-Table -AutoSize
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Next run time: $($info.NextRunTime)"

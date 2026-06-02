# Registers the "KanidaZerodhaAuth" Windows Scheduled Task (Fix 1, 2026-06-02).
# Runs scripts\run_auth_worker.bat every 30 min in a FRESH process. The worker
# self-gates to the IST active window (weekday 06:00-16:30 IST), so out-of-window
# fires exit in under a second -- no PDT/IST/DST math needed in the schedule.
# Idempotent: re-running removes and recreates the task.

$ErrorActionPreference = "Stop"

$TaskName   = "KanidaZerodhaAuth"
$ProjectDir = "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
$BatPath    = Join-Path $ProjectDir "scripts\run_auth_worker.bat"

if (-not (Test-Path $BatPath)) { throw "Worker batch not found at: $BatPath" }

Write-Host "=== Registering Scheduled Task $TaskName ===" -ForegroundColor Cyan

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$BatPath`"" -WorkingDirectory (Join-Path $ProjectDir "backend")

$anchor = (Get-Date).Date
$trigger = New-ScheduledTaskTrigger -Once -At $anchor -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration ([TimeSpan]::FromDays(3650))

$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 8) -RestartCount 0 -DontStopOnIdleEnd

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Kanida Zerodha auth - fresh-process token refresh every 30 min. Self-gates to weekday 06:00-16:30 IST. Fix 1, 2026-06-02." | Out-Null

Write-Host "Registered. Verifying..." -ForegroundColor Green
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State | Format-Table -AutoSize
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Next run time: $($info.NextRunTime)"
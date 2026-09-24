# Registers the "KanidaWorkflowWatchdog" Windows Scheduled Task.
# Runs scripts\run_workflow_watchdog.bat every 15 min in a FRESH process. The
# watchdog self-gates to the IST active window (weekday ~05:30-16:30 IST), so
# out-of-window fires exit in under a second -- no PDT/IST/DST math needed in
# the schedule. Idempotent: re-running removes and recreates the task.
#
# CORRECT switch syntax: -WakeToRun (NO value). The 2026-06-29 incident root
# cause was `-WakeToRun $true` in register_auth_task.ps1 -- a switch parameter
# takes NO argument; passing $true is a binding error that could leave the task
# in a corrupt/unregistered state. We use the bare switch here.

$ErrorActionPreference = "Stop"

$TaskName   = "KanidaWorkflowWatchdog"
$ProjectDir = "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
$BatPath    = Join-Path $ProjectDir "scripts\run_workflow_watchdog.bat"

if (-not (Test-Path $BatPath)) { throw "Watchdog batch not found at: $BatPath" }

Write-Host "=== Registering Scheduled Task $TaskName ===" -ForegroundColor Cyan

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$BatPath`"" -WorkingDirectory (Join-Path $ProjectDir "backend")

# Repeat every 15 min, effectively forever. StartWhenAvailable catches up a
# missed fire (e.g. laptop was asleep) on the next wake.
$anchor  = (Get-Date).Date
$trigger = New-ScheduledTaskTrigger -Once -At $anchor -RepetitionInterval (New-TimeSpan -Minutes 15) -RepetitionDuration ([TimeSpan]::FromDays(3650))

# CORRECT: -WakeToRun and -StartWhenAvailable are switches (no value).
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 5) -RestartCount 0 -DontStopOnIdleEnd -WakeToRun

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Kanida workflow health watchdog - fresh-process self-heal + alert every 15 min. Self-gates to weekday ~05:30-16:30 IST. Self-heals a missing KanidaZerodhaAuth task; alerts on everything else." | Out-Null

Write-Host "Registered. Verifying..." -ForegroundColor Green
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State | Format-Table -AutoSize
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Next run time: $($info.NextRunTime)"

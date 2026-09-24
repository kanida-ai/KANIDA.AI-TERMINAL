# Registers the "KanidaVortexAuth" Windows Scheduled Task.
# Fires run_vortex_auth.bat every 30 min in a fresh process. The worker self-gates to
# weekday 06:00-15:30 IST and skips when the token is already valid, so it only actually
# logs in when the Vortex token has lapsed. Idempotent: re-running recreates the task.
$ErrorActionPreference = "Stop"

$TaskName   = "KanidaVortexAuth"
$ProjectDir = "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
$BatPath    = Join-Path $ProjectDir "scripts\run_vortex_auth.bat"

if (-not (Test-Path $BatPath)) { throw "Vortex auth batch not found at: $BatPath" }
Write-Host "=== Registering Scheduled Task $TaskName ===" -ForegroundColor Cyan

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) { Write-Host "Removing existing task..."; Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false }

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$BatPath`"" -WorkingDirectory (Join-Path $ProjectDir "backend")
$anchor = (Get-Date).Date
$trigger = New-ScheduledTaskTrigger -Once -At $anchor -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration ([TimeSpan]::FromDays(3650))
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 8) -RestartCount 0 -DontStopOnIdleEnd -WakeToRun
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Kanida Vortex auth - fresh-process daily token refresh every 30 min. Self-gates to weekday 06:00-15:30 IST, skips when token valid." | Out-Null

Write-Host "Registered. Verifying..." -ForegroundColor Green
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State | Format-Table -AutoSize
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Next run time: $($info.NextRunTime)"

# Registers "KANIDA Screener eval": every 5 minutes, evaluate every saved scanner when a NEW F&O reading has landed,
# and raise alerts. OWNER-RUN, after market close (owner decision Q5) - not run by the build.
#
#   scripts/register-screener-task.ps1            register (or replace) the task
#   scripts/register-screener-task.ps1 -Remove    unregister it
#
# Same shape as "KANIDA F&O capture" / "KANIDA F&O metrics": a plain repetition, and the job itself decides whether
# there is anything to do (this machine's clock is Pacific time, so no IST wall-clock trigger is used). A run with no
# new reading exits after one indexed query. The job reads db/derivatives.db read-only and writes only
# var/screener.db and var/screener-eval.log; it never touches the pilot, the other KANIDA tasks, or any vendor feed.
param([switch]$Remove)
$ErrorActionPreference = 'Stop'
$name = 'KANIDA Screener eval'
if ($Remove) { Unregister-ScheduledTask -TaskName $name -Confirm:$false; Write-Output "Removed $name"; return }
$root = Split-Path $PSScriptRoot -Parent
$python = Join-Path $root '.pilot-venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw "Missing $python" }
$command = "`$env:PYTHONPATH='$root\server'; & '$python' -m kanida_pilot.screener eval *>> '$root\var\screener-eval.log'"
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoProfile -WindowStyle Hidden -Command $command" -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 10) -MultipleInstances IgnoreNew -StartWhenAvailable
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Settings $settings -Description 'KANIDA Options Screener: evaluate saved scanners and raise alerts on each new 15-min reading (read-only on derivatives.db).' -Force | Out-Null
Write-Output "Registered $name. Log: var\screener-eval.log"

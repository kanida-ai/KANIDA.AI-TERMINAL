# Make the three data loops survive a reboot, a crash and a closed terminal.
#
# WHY THIS EXISTS. The F&O capture, the metrics loop and the equity price fetcher have all been
# hand-started. Every time the machine restarted, or the shell that launched them closed, they died
# silently — and on 18 Sep the capture died at 11:30 while its own log still said it was armed for
# the next reading. Four hours of that session were lost and had to be rebuilt from candles, which
# do not carry a traded average price, so premium could never be computed for that afternoon.
# A loop that only runs while someone is watching is not a pipeline.
#
# These become Windows Scheduled Tasks: they start at boot, they restart if they fail, and they run
# whether or not anyone is logged in at a terminal.
#
#   .\scripts\install-services.ps1            install (or update) all three
#   .\scripts\install-services.ps1 -Remove    remove them again
#   .\scripts\install-services.ps1 -Status    just report what is installed and running
#
# Needs an elevated PowerShell to register a task that runs at boot. It says so rather than failing
# obscurely. Nothing here touches the pilot on 8082, the scanner, or any database.
param([switch]$Remove, [switch]$Status)
$ErrorActionPreference = 'Stop'
$root   = Split-Path $PSScriptRoot -Parent
$python = Join-Path $root 'market_scanner\.venv\Scripts\python.exe'
$logs   = Join-Path $root 'logs'
$prefix = 'KANIDA '

# Each loop: the task name, what it is for, and the command. `-u` keeps the log unbuffered so a tail
# shows the current state rather than whatever last filled a 4 KB buffer.
$jobs = @(
  @{ Name = 'F&O capture'
     Desc = 'Quotes every in-scope F&O contract at each 15-minute reading of the session.'
     Args = "-u -m market_data.derivatives.cli --log-file logs/derivatives_capture_service.log run" }
  @{ Name = 'F&O metrics'
     Desc = 'Turns each captured reading into the numbers the Derivative tab reads.'
     Args = "-u `"$($root)\scripts\metrics_loop.py`" `"$($logs)\metrics_loop_service.log`"" }
  @{ Name = 'Equity prices'
     Desc = 'Keeps the 15-minute equity candles current for the scanner.'
     Args = "-u -m market_data.live.cli --workers 4 run --interval 300" }
)

function Show-Status {
  Get-ScheduledTask | Where-Object { $_.TaskName -like "$prefix*" } |
    ForEach-Object {
      $info = $_ | Get-ScheduledTaskInfo
      [pscustomobject]@{
        Task      = $_.TaskName
        State     = $_.State
        LastRun   = $info.LastRunTime
        LastResult= $info.LastTaskResult
        NextRun   = $info.NextRunTime
      }
    } | Format-Table -AutoSize
}

if ($Status) { Show-Status; return }

if ($Remove) {
  foreach ($job in $jobs) {
    $name = $prefix + $job.Name
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
      Unregister-ScheduledTask -TaskName $name -Confirm:$false
      Write-Output "removed: $name"
    }
  }
  return
}

if (-not (Test-Path -LiteralPath $python)) { throw "Python not found at $python" }
$elevated = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
            ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $elevated) {
  throw "Run this from an ELEVATED PowerShell (right-click > Run as administrator). Registering a task that starts at boot needs it."
}
New-Item -ItemType Directory -Force -Path $logs | Out-Null

foreach ($job in $jobs) {
  $name = $prefix + $job.Name
  # At boot, and once more a minute later: the first attempt can land before the disk holding the
  # databases is ready, and a task that failed at boot would otherwise sit dead until someone looked.
  $triggers = @(
    (New-ScheduledTaskTrigger -AtStartup),
    (New-ScheduledTaskTrigger -Once -At ((Get-Date).AddMinutes(1)) `
       -RepetitionInterval (New-TimeSpan -Minutes 10))
  )
  $action = New-ScheduledTaskAction -Execute $python -Argument $job.Args -WorkingDirectory $root
  # RestartCount/-Interval is what turns a crash into a recovery instead of a silent stop.
  # MultipleInstances IgnoreNew means the 10-minute re-trigger is a no-op while the loop is healthy,
  # and a restart when it is not.
  $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
                -StartWhenAvailable -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
                -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero)
  Register-ScheduledTask -TaskName $name -Description $job.Desc -Action $action `
    -Trigger $triggers -Settings $settings -User $env:USERNAME -RunLevel Limited -Force | Out-Null
  Write-Output "installed: $name"
}

Write-Output ""
Write-Output "Installed. They now start at boot and restart themselves if they fail."
Write-Output "Check any time with:  .\scripts\install-services.ps1 -Status"
Show-Status

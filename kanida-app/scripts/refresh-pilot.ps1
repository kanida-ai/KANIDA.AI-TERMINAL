# Put the CURRENT code in front of the owner, in one step.
#
# WHY THIS EXISTS. The pilot on 8082 serves two things that go stale independently: the exported web
# bundle in dist-pilot/, and the Python server, which is loaded once at start and never reloads. Three
# separate times the owner opened the tab and saw nothing, because a bundle had been exported without
# restarting the server, or the server had been restarted hours before the code changed. The page looked
# fine; it was just old. Doing the two halves by hand means they can drift, and they did.
#
#   .\scripts\refresh-pilot.ps1              export the bundle, then restart the pilot
#   .\scripts\refresh-pilot.ps1 -NoBuild     restart only (use when the bundle is already current)
#
# It refuses to restart if the export fails, so a broken build never replaces a working pilot.
param([switch]$NoBuild, [int]$Port = 8082)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $root

if (-not $NoBuild) {
    Write-Output "Exporting the web bundle..."
    & npx.cmd expo export --platform web --output-dir dist-pilot | Select-Object -Last 1
    if ($LASTEXITCODE -ne 0) { throw 'Web export failed. The running pilot was NOT touched.' }
}

$listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($listener) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)"
    if ($proc.CommandLine -notmatch 'kanida_pilot') {
        throw "Port $Port is held by something that is not the pilot. Refusing to stop it: $($proc.CommandLine)"
    }
    Write-Output "Stopping the pilot (pid $($listener.OwningProcess), started $((Get-Process -Id $listener.OwningProcess).StartTime))..."
    Stop-Process -Id $listener.OwningProcess -Force
    Start-Sleep -Seconds 4
}

& .\scripts\start-pilot.ps1 -Port $Port | Select-Object -Last 1

# Prove it: the server must have started AFTER the newest thing it serves, or it is already stale.
$newest = Get-ChildItem -Recurse -File -Path "$root\server\kanida_pilot", "$root\dist-pilot\index.html" |
          Sort-Object LastWriteTime -Descending | Select-Object -First 1
for ($i = 0; $i -lt 40; $i++) {
    $l = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($l) { break }
    Start-Sleep -Seconds 2
}
if (-not $l) { throw "The pilot did not come up on port $Port." }
$started = (Get-Process -Id $l.OwningProcess).StartTime
Write-Output ""
Write-Output "  newest code/bundle : $($newest.LastWriteTime)  ($($newest.Name))"
Write-Output "  pilot started      : $started"
if ($started -lt $newest.LastWriteTime) {
    throw "STALE: the pilot is older than the code it is meant to serve. Something changed mid-restart; run this again."
}
Write-Output "  OK - the pilot is serving the current code. Hard-refresh the browser (Ctrl+Shift+R)."

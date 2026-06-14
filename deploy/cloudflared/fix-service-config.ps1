# ============================================================================
#  fix-service-config.ps1 -- move config to where the Windows Service looks
# ============================================================================
#
#  WHY THIS EXISTS
#  ---------------
#  `cloudflared service install` on Windows registers the service to run
#  under the LocalSystem account. LocalSystem's home is
#    C:\Windows\System32\config\systemprofile\
#  ...NOT C:\Users\SPS\. The newer cloudflared versions do NOT auto-copy
#  the config + credentials from your user profile into LocalSystem's,
#  so the freshly-installed service has nothing to read and just sits
#  there doing nothing -- which is why api.kanida.ai returns 502/530.
#
#  This script: stops the service, copies the three required files into
#  the LocalSystem profile, starts the service, waits, and smoke-tests.
#
#  Run as Administrator.
# ============================================================================

$ErrorActionPreference = 'Stop'
$SRC = "C:\Users\SPS\.cloudflared"
$DST = "C:\Windows\System32\config\systemprofile\.cloudflared"
$TEST_URL = "https://api.kanida.ai/api/power/personas"

# Privilege check
$me = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not ([Security.Principal.WindowsPrincipal]$me).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "ERROR: must be run as Administrator." -ForegroundColor Red
    exit 1
}

Write-Host "=== fix-service-config.ps1 ===" -ForegroundColor Cyan

# Find the credentials JSON (UUID.json) in the source dir
$credFile = Get-ChildItem -Path $SRC -Filter "*.json" -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $credFile) {
    Write-Host "ERROR: No tunnel credentials JSON found at $SRC" -ForegroundColor Red
    exit 1
}
Write-Host "[1/6] credentials file found: $($credFile.Name)"

# Stop the service first so it releases any file handles
$svc = Get-Service Cloudflared -ErrorAction SilentlyContinue
if ($svc -and $svc.Status -eq 'Running') {
    Write-Host "[2/6] stopping Cloudflared service..."
    Stop-Service Cloudflared -Force
    Start-Sleep -Seconds 2
} else {
    Write-Host "[2/6] service not running, skipping stop"
}

# Create the destination dir
Write-Host "[3/6] creating $DST"
New-Item -ItemType Directory -Force -Path $DST | Out-Null

# Copy the three required files
Write-Host "[4/6] copying config.yml, $($credFile.Name), cert.pem"
Copy-Item "$SRC\config.yml"        "$DST\config.yml" -Force
Copy-Item $credFile.FullName       "$DST\$($credFile.Name)" -Force
Copy-Item "$SRC\cert.pem"          "$DST\cert.pem" -Force

# Show what's there
Get-ChildItem $DST | Format-Table Name, Length -AutoSize

# Restart the service to pick up the config
Write-Host "[5/6] starting Cloudflared service..."
Start-Service Cloudflared
Start-Sleep -Seconds 10

$svc = Get-Service Cloudflared
Write-Host "       service status: $($svc.Status)  startType: $($svc.StartType)"

# Smoke test
Write-Host "[6/6] smoke testing $TEST_URL ..."
try {
    $resp = Invoke-WebRequest -Uri $TEST_URL -UseBasicParsing -TimeoutSec 20
    Write-Host "       HTTP $($resp.StatusCode) -- response body $($resp.RawContent.Length) bytes" -ForegroundColor Green
    Write-Host ""
    Write-Host "SUCCESS -- kanida-api tunnel is alive as a Windows service." -ForegroundColor Green
    Write-Host "Next: set NEXT_PUBLIC_API_URL=https://api.kanida.ai on Vercel + redeploy."
} catch {
    $code = if ($_.Exception.Response) { $_.Exception.Response.StatusCode.Value__ } else { 'NO_RESPONSE' }
    Write-Host "       smoke test FAILED: HTTP $code -- $($_.Exception.Message)" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Service is running but tunnel still not responding. Check:"
    Write-Host "  Get-Content '$DST\cloudflared.log' -Tail 30"
    Write-Host "  Get-Content '$SRC\cloudflared.log' -Tail 30"
}

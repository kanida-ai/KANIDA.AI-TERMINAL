# ============================================================================
#  install-tunnel.ps1 -- register cloudflared as a Windows Service
# ============================================================================
#
#  PURPOSE
#  -------
#  After you've created the named tunnel (`cloudflared tunnel create kanida-api`)
#  and saved config.yml at C:\Users\SPS\.cloudflared\config.yml, this script:
#
#    1. Stops any quick-tunnel cloudflared.exe still running
#    2. Validates config.yml + credentials file existence
#    3. Installs cloudflared as a Windows service (auto-start on boot)
#    4. Starts the service
#    5. Smoke-tests the public URL https://api.kanida.ai
#
#  Run as Administrator.
#
#  USAGE
#  -----
#    powershell -ExecutionPolicy Bypass -File install-tunnel.ps1
#
#  NOTE
#  ----
#  All output is ASCII-only -- Windows PowerShell 5 defaults to cp1252 and
#  chokes on UTF-8 bullets/em-dashes without a BOM. Do not paste fancy
#  characters into this file.
#
# ============================================================================

$ErrorActionPreference = 'Stop'
$CLOUDFLARED  = "C:\Users\SPS\bin\cloudflared.exe"
$CONFIG_PATH  = "C:\Users\SPS\.cloudflared\config.yml"
$SERVICE_NAME = "Cloudflared"
$TEST_URL     = "https://api.kanida.ai/api/power/personas"

Write-Host "=== install-tunnel.ps1 ===" -ForegroundColor Cyan
Write-Host ""

# ---- Step 1: privilege check ------------------------------------------------
$me = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not ([Security.Principal.WindowsPrincipal]$me).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "ERROR: must be run as Administrator (Windows service install needs admin)." -ForegroundColor Red
    exit 1
}
Write-Host "[1/7] running as Administrator: OK"

# ---- Step 2: binary exists --------------------------------------------------
if (-not (Test-Path $CLOUDFLARED)) {
    Write-Host "ERROR: cloudflared.exe not at $CLOUDFLARED" -ForegroundColor Red
    exit 1
}
Write-Host "[2/7] cloudflared binary found at $CLOUDFLARED"

# ---- Step 3: config.yml exists ----------------------------------------------
if (-not (Test-Path $CONFIG_PATH)) {
    Write-Host "ERROR: $CONFIG_PATH missing." -ForegroundColor Red
    Write-Host "       Did you copy config.yml.template after editing UUID?" -ForegroundColor Yellow
    exit 1
}
Write-Host "[3/7] config.yml found at $CONFIG_PATH"

# ---- Step 4: verify config.yml has real UUID (not placeholder) -------------
$cfg = Get-Content $CONFIG_PATH -Raw
if ($cfg -match '<TUNNEL_UUID_HERE>') {
    Write-Host "ERROR: config.yml still has '<TUNNEL_UUID_HERE>' placeholder." -ForegroundColor Red
    Write-Host "       Replace it with the UUID from 'cloudflared tunnel create kanida-api'." -ForegroundColor Yellow
    exit 1
}
Write-Host "[4/7] config.yml UUID looks valid"

# ---- Step 5: stop any existing cloudflared.exe (quick-tunnel mode) ---------
Get-Process cloudflared -ErrorAction SilentlyContinue | ForEach-Object {
    Write-Host "       killing stray cloudflared PID $($_.Id)"
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
}
Write-Host "[5/7] no stray cloudflared processes"

# ---- Step 6: uninstall existing service if any, then re-install ------------
$existing = Get-Service $SERVICE_NAME -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "       removing existing Cloudflared service to re-install with new config"
    if ($existing.Status -eq 'Running') {
        Stop-Service $SERVICE_NAME -Force
    }
    & $CLOUDFLARED service uninstall 2>&1 | Out-Null
    Start-Sleep -Seconds 2
}

Write-Host "[6/7] installing Cloudflared as Windows service..."
& $CLOUDFLARED service install
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: 'cloudflared service install' failed with exit $LASTEXITCODE" -ForegroundColor Red
    exit 1
}

# Set to auto-start on boot
Set-Service $SERVICE_NAME -StartupType Automatic
Start-Service $SERVICE_NAME

Start-Sleep -Seconds 5

$svc = Get-Service $SERVICE_NAME
Write-Host "       service status: $($svc.Status)  start: $($svc.StartType)"

# ---- Step 7: smoke test -----------------------------------------------------
Write-Host "[7/7] smoke testing $TEST_URL ..."
Start-Sleep -Seconds 3
try {
    $resp = Invoke-WebRequest -Uri $TEST_URL -UseBasicParsing -TimeoutSec 15
    Write-Host "       HTTP $($resp.StatusCode) in $($resp.RawContent.Length) bytes" -ForegroundColor Green
    Write-Host ""
    Write-Host "SUCCESS -- kanida-api tunnel is live as a Windows service." -ForegroundColor Green
    Write-Host "         Set NEXT_PUBLIC_API_URL=https://api.kanida.ai on Vercel and redeploy."
} catch {
    $msg = $_.Exception.Message
    Write-Host "       smoke test FAILED: $msg" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Service is installed and running, but api.kanida.ai is not responding yet."
    Write-Host "Likely causes:"
    Write-Host "  - DNS for api.kanida.ai not yet propagated globally (wait 5-15 min)"
    Write-Host "  - Backend on port 8001 is not running"
    Write-Host "  - 'cloudflared tunnel route dns kanida-api api.kanida.ai' was not run"
    Write-Host ""
    Write-Host "Inspect logs:  Get-Content C:\Users\SPS\.cloudflared\cloudflared.log -Tail 40"
}

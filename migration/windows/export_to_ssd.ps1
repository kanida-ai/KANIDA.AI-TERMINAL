<#
Export everything the Mac needs onto an external SSD.  Run in your own PowerShell
window (the secrets step asks you for a passphrase):

    powershell -ExecutionPolicy Bypass -File migration\windows\export_to_ssd.ps1 -Ssd E:

Safe to re-run: robocopy only copies what changed and finished database
snapshots are skipped.  Nothing on this laptop is modified or stopped.

Layout written to <Ssd>\KanidaMove\
  files\Kanida_Falcon, files\engine, files\KANIDA.AI_TERMINAL, files\_kanida_deploy
  files\archive\Desktop\*, files\archive\Documents\*   everything else, as-is
  claude\                                              ~/.claude (no credentials)
  env-freeze\                                          package lists + task XML
  secrets.tar.enc                                      every .env/key/password file, AES-256
  manifest\dbs.jsonl, manifest\sizes.txt              what the Mac verifies against
#>
param(
    [Parameter(Mandatory = $true)][string]$Ssd,
    [switch]$SkipData,
    [switch]$SkipSecrets
)
$ErrorActionPreference = 'Stop'
$U = 'C:\Users\SPS'
$Falcon = "$U\Documents\Kanida_Falcon"
$Out = Join-Path $Ssd 'KanidaMove'
$Py = "$Falcon\market_scanner\.venv\Scripts\python.exe"
$OpenSsl = 'C:\Program Files\Git\usr\bin\openssl.exe'

# Folders cloned from GitHub on the Mac travel WITHOUT .git; everything else goes as-is.
$Mapped = [ordered]@{
    "$Falcon"                                                  = 'files\Kanida_Falcon'
    "$U\Desktop\Kanida.ai Terminal Quant Intelligence Engine" = 'files\engine'
    "$U\Desktop\KANIDA.AI_TERMINAL"                            = 'files\KANIDA.AI_TERMINAL'
    "$U\Desktop\_kanida_deploy"                                = 'files\_kanida_deploy'
}
$FromGit = @("$Falcon", "$U\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
$SkipArchive = @('Kanida_Falcon', 'Kanida.ai Terminal Quant Intelligence Engine', 'KANIDA.AI_TERMINAL', '_kanida_deploy',
                 'Visual Studio 2022', 'IISExpress', 'My Web Sites', 'Custom Office Templates')
foreach ($side in 'Desktop', 'Documents') {
    Get-ChildItem "$U\$side" -Directory | Where-Object { $SkipArchive -notcontains $_.Name } | ForEach-Object {
        $Mapped[$_.FullName] = "files\archive\$side\$($_.Name)"
    }
}
# Downloads goes whole (its secret-named files go into the encrypted bundle like the rest)
$Mapped["$U\Downloads"] = 'files\archive\Downloads'

$XD = @('node_modules', '.venv', '.pilot-venv', 'venv', '__pycache__', '.pytest_cache', '.expo', 'worktrees')
$DbFiles = @('*.db', '*.sqlite', '*.sqlite3', '*.db-wal', '*.db-shm', '*-wal', '*-shm', '*-journal')
$SecretFiles = @('.env', '.env.*', '*.env', '*.key', '*.pem', '*.p12', '*password*', '*secret*', '*API_KEY*',
                 '*credential*', '*token*.json', 'kite_tokens*')

function Say($m) { Write-Host "`n== $m" -ForegroundColor Cyan }
function Robo($src, $dst, [string[]]$extraXD, [string[]]$xf) {
    $a = @($src, $dst, '/E', '/R:1', '/W:1', '/MT:16', '/XJ', '/NFL', '/NDL', '/NP', '/DCOPY:T', '/COPY:DAT')
    $a += '/XD'; $a += ($XD + $extraXD)
    if ($xf) { $a += '/XF'; $a += $xf }
    & robocopy @a | Select-String -Pattern '^\s*(Dirs|Files|Bytes)\s*:' | ForEach-Object { "   $($_.Line.Trim())" }
    if ($LASTEXITCODE -ge 8) { throw "robocopy failed ($LASTEXITCODE): $src" }
}

# ---- 0. preflight -----------------------------------------------------------
if (-not (Test-Path $Ssd)) { throw "SSD $Ssd not found" }
$free = (Get-PSDrive ($Ssd.TrimEnd(':\'))).Free / 1GB
Say ("SSD free space: {0:N0} GB (need ~300 GB)" -f $free)
if ($free -lt 300 -and -not $SkipData) { throw 'Not enough space on the SSD.' }
New-Item -ItemType Directory -Force "$Out\manifest", "$Out\env-freeze\tasks" | Out-Null

# ---- 1. package lists (so the Mac venvs can be rebuilt exactly) ---------------
Say 'Freezing Python environments'
$envs = @{
    'falcon-market_scanner' = $Py
    'falcon-pilot'          = "$Falcon\kanida-app\.pilot-venv\Scripts\python.exe"
    'engine-anaconda'       = "$U\anaconda3\python.exe"
    'terminal-miniconda'    = "$U\miniconda3\python.exe"
}
foreach ($k in $envs.Keys) {
    if (Test-Path $envs[$k]) {
        cmd /c "`"$($envs[$k])`" -m pip list --format=freeze > `"$Out\env-freeze\$k.txt`" 2>nul"
        cmd /c "`"$($envs[$k])`" --version > `"$Out\env-freeze\$k.python-version.txt`" 2>&1"
        "   $k"
    }
}
cmd /c "node --version > `"$Out\env-freeze\node-version.txt`""
Get-ScheduledTask | Where-Object { $_.TaskName -match 'kanida' } | ForEach-Object {
    Export-ScheduledTask -TaskName $_.TaskName -TaskPath $_.TaskPath | Out-File -Encoding utf8 "$Out\env-freeze\tasks\$($_.TaskName -replace '[^\w&-]','_').xml"
}

# ---- 2. files (databases and secrets excluded here; they have their own steps) --
if (-not $SkipData) {
    foreach ($src in $Mapped.Keys) {
        if (-not (Test-Path $src)) { continue }
        Say "Copying $src"
        $extra = @(); if ($FromGit -contains $src) { $extra = @('.git') }
        Robo $src (Join-Path $Out $Mapped[$src]) $extra ($DbFiles + $SecretFiles)
    }
    foreach ($side in 'Desktop', 'Documents') {
        Say "Loose files on $side"
        & robocopy "$U\$side" "$Out\files\archive\$side\_loose_files" /R:1 /W:1 /NFL /NDL /NP /XF ($DbFiles + $SecretFiles + 'desktop.ini') | Out-Null
    }

    Say 'Claude Code config (rules, agents, skills, memory; no login credentials)'
    Robo "$U\.claude" "$Out\claude" @('cache', 'downloads', 'telemetry', 'shell-snapshots', 'session-env', 'sessions', 'file-history', 'backups') @('.credentials.json')

    # ---- 3. databases: consistent online snapshots -------------------------------
    Say 'Database snapshots (large; kanida.db alone is ~150 GB)'
    $failed = $false
    foreach ($src in $Mapped.Keys) {
        if (-not (Test-Path $src)) { continue }
        & $Py "$Falcon\migration\windows\sqlite_snapshot.py" $src (Join-Path $Out $Mapped[$src]) "$Out\manifest\dbs.jsonl" $Mapped[$src]
        if ($LASTEXITCODE -ne 0) { $failed = $true }
    }
    if ($failed) { Write-Warning 'Some database snapshots failed - re-run this script; finished ones are skipped.' }

    Say 'Size summary'
    Get-ChildItem "$Out\files" -Directory | ForEach-Object {
        $s = (Get-ChildItem $_.FullName -Recurse -File -Force -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
        "{0,-24} {1,10:N1} GB" -f $_.Name, ($s / 1GB)
    } | Tee-Object "$Out\manifest\sizes.txt"
}

# ---- 4. secrets: one encrypted bundle --------------------------------------------
if (-not $SkipSecrets) {
    Say 'Secrets bundle'
    $stage = Join-Path $env:TEMP "kanida-secrets-$(Get-Random)"
    try {
        foreach ($src in $Mapped.Keys) {
            if (-not (Test-Path $src)) { continue }
            $rel = $Mapped[$src] -replace '^files\\', ''
            & robocopy $src (Join-Path $stage $rel) $SecretFiles /S /R:1 /W:1 /NFL /NDL /NP /XJ /XD ($XD + '.git') | Out-Null
        }
        foreach ($d in '.aws', '.ssh') { if (Test-Path "$U\$d") { & robocopy "$U\$d" "$stage\_home\$d" /E /NFL /NDL /NP | Out-Null } }
        # the api.kanida.ai tunnel: credentials + origin cert + config (logs left behind)
        if (Test-Path "$U\.cloudflared") { & robocopy "$U\.cloudflared" "$stage\_home\.cloudflared" *.json *.pem *.yml /NFL /NDL /NP | Out-Null }
        if (Test-Path "$U\.gitconfig") { Copy-Item "$U\.gitconfig" "$stage\_home\.gitconfig" -ErrorAction SilentlyContinue }
        # the pilot's encryption key lives in var\, which the data copy excluded by *.key
        Get-ChildItem $stage -Recurse -File | ForEach-Object { $_.FullName.Substring($stage.Length + 1) } | Sort-Object |
            Tee-Object "$Out\manifest\secrets-list.txt" | ForEach-Object { "   $_" }
        & tar.exe -cf "$stage.tar" -C $stage .
        Write-Host "`nChoose a passphrase for the bundle (you will type it again on the Mac)." -ForegroundColor Yellow
        & $OpenSsl enc -aes-256-cbc -pbkdf2 -iter 600000 -salt -in "$stage.tar" -out "$Out\secrets.tar.enc"
        if ($LASTEXITCODE -ne 0) { throw 'encryption failed' }
        (Get-FileHash "$Out\secrets.tar.enc").Hash | Out-File -Encoding ascii "$Out\manifest\secrets.sha256"
        '   written: secrets.tar.enc'
    } finally {
        Remove-Item -Recurse -Force $stage, "$stage.tar" -ErrorAction SilentlyContinue
    }
}

Say "Done. Eject $Ssd safely, then on the Mac follow migration/MAC_MIGRATION.md step 3."

# Uses an authenticated Claude subscription only. It does not fall back to API billing.
$ErrorActionPreference = 'Stop'
$KanidaRoot = Split-Path -Parent $PSScriptRoot
$ClaudeExecutable = Join-Path $env:USERPROFILE '.local\bin\claude.exe'
$ReviewPrompt = Join-Path $PSScriptRoot 'CLAUDE_REVIEW_TASK.md'
$ReviewFolder = Join-Path $KanidaRoot 'reports\research-orchestrator'
$OriginalApiKey = [Environment]::GetEnvironmentVariable('ANTHROPIC_API_KEY', 'Process')
try {
    [Environment]::SetEnvironmentVariable('ANTHROPIC_API_KEY', $null, 'Process')
    $StatusText = & $ClaudeExecutable auth status
    $AuthStatus = $StatusText | ConvertFrom-Json
    if (-not $AuthStatus.loggedIn -or $AuthStatus.authMethod -ne 'claude.ai') {
        throw 'Claude subscription login is required. Run claude auth login --claudeai in your own terminal. No review or API request was sent.'
    }
    $Timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
    $ReviewOutput = Join-Path $ReviewFolder "claude-review-$Timestamp.json"
    Push-Location -LiteralPath $KanidaRoot
    try {
        $PromptText = Get-Content -LiteralPath $ReviewPrompt -Raw
        $Review = $PromptText | & $ClaudeExecutable --safe-mode --no-chrome --disable-slash-commands --no-session-persistence --permission-mode dontAsk --tools 'Read,Glob,Grep' --allowedTools 'Read,Glob,Grep' --output-format json --print
        if ($LASTEXITCODE -ne 0) { throw 'Claude review did not complete; see the CLI error.' }
        $Review | Set-Content -LiteralPath $ReviewOutput -Encoding UTF8
        Write-Output "Claude review saved: $ReviewOutput"
    } finally { Pop-Location }
} finally {
    [Environment]::SetEnvironmentVariable('ANTHROPIC_API_KEY', $OriginalApiKey, 'Process')
}

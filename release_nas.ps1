param(
    [string]$Message = "",

    [ValidateSet("quick", "deploy", "fast", "full", "fastfull")]
    [string]$Mode = "deploy",

    [string]$Branch = "main",

    [switch]$SkipConfirm
)

$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Title,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Action
    )

    Write-Host ""
    Write-Host "[step] $Title" -ForegroundColor Cyan
    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Title"
    }
}

function Test-GitAvailable {
    git --version *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "git is not available in this shell."
    }
}

function Test-DeployConfig {
    $localConfig = Join-Path $PSScriptRoot "deploy_nas.config.local.bat"
    if (-not (Test-Path $localConfig)) {
        throw "deploy_nas.config.local.bat not found. Create it before running release."
    }
}

function Get-ChangedFiles {
    $lines = git status --short
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read git status."
    }
    return @($lines | Where-Object { $_ -and $_.Trim() })
}

function New-AutoCommitMessage {
    $statusLines = Get-ChangedFiles
    $paths = @()
    foreach ($line in $statusLines) {
        $trimmed = $line.Trim()
        if ($trimmed.Length -ge 4) {
            $paths += $trimmed.Substring(3).Trim()
        }
    }

    $top = @($paths | Select-Object -First 3)
    $summary = if ($top.Count -gt 0) {
        ($top -join ", ")
    } else {
        "workspace update"
    }

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
    return "chore: release $timestamp ($summary)"
}

function Confirm-Release {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$StatusLines,

        [Parameter(Mandatory = $true)]
        [string]$CommitMessage,

        [Parameter(Mandatory = $true)]
        [string]$DeployMode,

        [Parameter(Mandatory = $true)]
        [string]$CurrentBranch
    )

    Write-Host ""
    Write-Host "[review] branch: $CurrentBranch" -ForegroundColor Yellow
    Write-Host "[review] deploy mode: $DeployMode" -ForegroundColor Yellow
    Write-Host "[review] commit message: $CommitMessage" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "[review] changed files" -ForegroundColor Yellow
    foreach ($line in $StatusLines) {
        Write-Host "  $line"
    }
    Write-Host ""

    $answer = Read-Host "Continue with commit/push/deploy? (y/N)"
    if ($answer -notin @("y", "Y", "yes", "YES", "Yes")) {
        throw "Release cancelled by user."
    }
}

Test-GitAvailable
Test-DeployConfig

$currentBranch = (git branch --show-current).Trim()
if (-not $currentBranch) {
    throw "Could not determine current git branch."
}

if ($currentBranch -ne $Branch) {
    Write-Host "[warn] current branch is '$currentBranch', expected '$Branch'." -ForegroundColor Yellow
}

if ([string]::IsNullOrWhiteSpace($Message)) {
    $Message = New-AutoCommitMessage
}

$statusBefore = Get-ChangedFiles
if ($statusBefore.Count -eq 0) {
    throw "No local changes detected."
}

if (-not $SkipConfirm) {
    Confirm-Release -StatusLines $statusBefore -CommitMessage $Message -DeployMode $Mode -CurrentBranch $currentBranch
}

Invoke-Step -Title "git add" -Action {
    git add .
}

git diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    throw "No staged changes to commit."
}

Invoke-Step -Title "git commit" -Action {
    git commit -m $Message
}

Invoke-Step -Title "git push" -Action {
    git push origin $currentBranch
}

Invoke-Step -Title "deploy_nas.bat $Mode" -Action {
    & (Join-Path $PSScriptRoot "deploy_nas.bat") $Mode
}

Write-Host ""
Write-Host "[done] release complete" -ForegroundColor Green
Write-Host "branch: $currentBranch"
Write-Host "deploy mode: $Mode"

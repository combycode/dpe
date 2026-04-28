# new-tool.ps1 — scaffold + headless-generate a DPE tool from a spec file.
#
# Usage:
#   powershell -File scripts/new-tool.ps1 <name> <runtime> <spec-path>
#
# Example:
#   powershell -File scripts/new-tool.ps1 slugify-path bun ./fixtures/slugify-path.yaml

param(
    [Parameter(Mandatory, Position=0)][string]$Name,
    [Parameter(Mandatory, Position=1)][ValidateSet('rust','bun','python')][string]$Runtime,
    [Parameter(Mandatory, Position=2)][string]$SpecPath
)

$ErrorActionPreference = 'Stop'

$WorkspaceRoot = Split-Path -Parent $PSScriptRoot
$env:DPE_FRAMEWORKS_DIR = $WorkspaceRoot

$DpeDev   = Join-Path $WorkspaceRoot 'dpe-dev/target/release/dpe-dev.exe'
$Experiments = Join-Path $WorkspaceRoot 'tool-experiments'
$ToolDir  = Join-Path $Experiments "tools/$Name"
$Timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$LogFile  = Join-Path $Experiments "logs/$Name-run-$Timestamp.jsonl"

if (-not (Test-Path $DpeDev)) {
    Write-Error "dpe-dev not built: $DpeDev"
    exit 1
}
if (-not (Test-Path $SpecPath)) {
    Write-Error "spec not found: $SpecPath"
    exit 1
}

# Resolve spec path to absolute
$SpecAbs = (Resolve-Path $SpecPath).Path

Write-Host "[1/4] scaffold $Name ($Runtime) -> $ToolDir" -ForegroundColor Cyan
& $DpeDev scaffold --name $Name --runtime $Runtime --out $ToolDir --description "generated from spec"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "[2/4] copy spec -> spec.yaml" -ForegroundColor Cyan
Copy-Item $SpecAbs (Join-Path $ToolDir 'spec.yaml') -Force

Write-Host "[3/4] claude headless (log: $LogFile)" -ForegroundColor Cyan
$Prompt = "Read spec.yaml in the current working directory. Follow the dpe-tool skill from $Experiments/.claude/skills/dpe-tool/SKILL.md. Implement src/main.* per spec, expand tests, regenerate verify/ from spec.yaml tests. Then run: $DpeDev build . ; $DpeDev test . ; $DpeDev verify . -- iterate until all three exit 0."

New-Item -ItemType Directory -Path (Split-Path $LogFile) -Force | Out-Null

Push-Location $ToolDir
cmd /c "claude -p `"$Prompt`" --output-format stream-json --verbose --permission-mode bypassPermissions --add-dir `"$Experiments`" < NUL > `"$LogFile`" 2>&1"
$claudeExit = $LASTEXITCODE
Pop-Location

Write-Host "       claude exited: $claudeExit" -ForegroundColor Gray

Write-Host "[4/4] independent verification" -ForegroundColor Cyan
& $DpeDev build  $ToolDir
if ($LASTEXITCODE -ne 0) { Write-Error "build failed"; exit 1 }
& $DpeDev test   $ToolDir
if ($LASTEXITCODE -ne 0) { Write-Error "tests failed"; exit 1 }
& $DpeDev verify $ToolDir
if ($LASTEXITCODE -ne 0) { Write-Error "verify failed"; exit 1 }

Write-Host ""
Write-Host "[done] $Name ready at $ToolDir" -ForegroundColor Green
Write-Host "       log: $LogFile" -ForegroundColor Gray

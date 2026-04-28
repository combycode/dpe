# Scaffold a new tool and run the full scaffold -> build -> test -> verify cycle.
#
# Usage:
#   powershell scripts/scaffold-and-run.ps1 -Name <kebab-name> -Runtime <rust|bun|python> -Out <dir> [-Description "..."]

param(
    [Parameter(Mandatory=$true)][string]$Name,
    [Parameter(Mandatory=$true)][ValidateSet('rust','bun','python')][string]$Runtime,
    [Parameter(Mandatory=$true)][string]$Out,
    [string]$Description = "TODO"
)

$ErrorActionPreference = 'Stop'
$WorkspaceRoot = Split-Path -Parent $PSScriptRoot
$env:DPE_FRAMEWORKS_DIR = $WorkspaceRoot
$DpeDev = Join-Path $WorkspaceRoot 'dpe-dev/target/release/dpe-dev.exe'

if (-not (Test-Path $DpeDev)) {
    Write-Error "dpe-dev binary not found at $DpeDev"
    exit 1
}

Write-Host "[scaffold-and-run] scaffold $Name ($Runtime) -> $Out"
& $DpeDev scaffold --name $Name --runtime $Runtime --out $Out --description $Description
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[scaffold-and-run] build"
& $DpeDev build $Out
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[scaffold-and-run] test"
& $DpeDev test $Out
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[scaffold-and-run] verify"
& $DpeDev verify $Out
exit $LASTEXITCODE

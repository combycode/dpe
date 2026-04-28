# DPE dev wrapper.
#
# Sets DPE_FRAMEWORKS_DIR to the workspace root and runs dpe-dev with forwarded
# args. This is what Claude (headless or interactive) should call; no per-
# command permission prompts because the script body is a single stable
# invocation pattern.
#
# Usage:
#   pwsh scripts/dev.ps1 scaffold --name foo --runtime bun --out .\foo
#   pwsh scripts/dev.ps1 build  .\foo
#   pwsh scripts/dev.ps1 test   .\foo
#   pwsh scripts/dev.ps1 verify .\foo
#   pwsh scripts/dev.ps1 check  .\foo

$ErrorActionPreference = 'Stop'
$WorkspaceRoot = Split-Path -Parent $PSScriptRoot
$env:DPE_FRAMEWORKS_DIR = $WorkspaceRoot

$DpeDev = Join-Path $WorkspaceRoot 'dpe-dev/target/release/dpe-dev.exe'
if (-not (Test-Path $DpeDev)) {
    Write-Error "dpe-dev binary not found at $DpeDev — run: cd $WorkspaceRoot/dpe-dev && cargo build --release"
    exit 1
}

& $DpeDev @args
exit $LASTEXITCODE

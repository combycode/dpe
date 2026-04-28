#!/usr/bin/env pwsh
# Runs every test suite across the monorepo. Exits non-zero on first failure.
$ErrorActionPreference = 'Stop'

$Root = Resolve-Path (Join-Path $PSScriptRoot '..')
Set-Location $Root

Write-Host '[test] cargo test --workspace'
cargo test --workspace
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host '[test] bun test (frameworks/ts)'
Push-Location frameworks/ts
try {
    bun test
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}

Write-Host '[test] pytest (frameworks/python)'
Push-Location frameworks/python
try {
    python -m pytest -q
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}

Write-Host '[test] test-pipeline regression'
bash test-pipeline/run-all.sh
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host '[test] ALL PASSED'

#!/usr/bin/env pwsh
# Runs every linter across the monorepo. Exits non-zero on first failure.
$ErrorActionPreference = 'Stop'

$Root = Resolve-Path (Join-Path $PSScriptRoot '..')
Set-Location $Root

Write-Host '[lint] cargo clippy --workspace (all targets, -D warnings)'
cargo clippy --workspace --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host '[lint] biome check (TS)'
bunx --bun '@biomejs/biome@2.4.13' check frameworks/ts frameworks/ts/template tools
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host '[lint] tsc --noEmit (TS framework)'
Push-Location frameworks/ts
try {
    bun x tsc --noEmit
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}

Write-Host '[lint] ruff check (Python framework)'
Push-Location frameworks/python
try {
    ruff check .
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}

Write-Host '[lint] mypy (Python framework)'
Push-Location frameworks/python
try {
    python -m mypy src/dpe
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}

Write-Host '[lint] ALL PASSED'

#!/usr/bin/env bash
# Runs every test suite across the monorepo. Exits non-zero on first failure.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "[test] cargo test --workspace"
cargo test --workspace

echo "[test] bun test (frameworks/ts)"
( cd frameworks/ts && bun test )

echo "[test] pytest (frameworks/python)"
( cd frameworks/python && python -m pytest -q )

echo "[test] test-pipeline regression"
# run-all.sh runs target/release/{dpe,tools}: build them from this checkout first, so the
# regression exercises the code under test (not a stale build or a dpe installed on PATH).
cargo build --release --workspace
bash test-pipeline/run-all.sh

echo "[test] ALL PASSED"

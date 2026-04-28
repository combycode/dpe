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
bash test-pipeline/run-all.sh

echo "[test] ALL PASSED"

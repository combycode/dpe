#!/usr/bin/env bash
# Regression runner for the standard-tool pipeline.
# Runs every variant under standard/variants/ and reports pass/fail.
#
# Usage:
#   ./test-pipeline/run-all.sh                         # this checkout's target/release/dpe (else PATH)
#   DPE_BIN=/path/to/dpe ./test-pipeline/run-all.sh    # override binary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PIPE_DIR="$SCRIPT_DIR/standard"
INPUT="$PIPE_DIR/data/input"
OUTPUT="$PIPE_DIR/data/output"

# Git Bash on Windows: native dpe.exe expects Windows-style paths.
# cygpath -w converts /d/... â†’ D:\..., then tr to forward slashes for safety.
if command -v cygpath >/dev/null 2>&1; then
    PIPE_DIR="$(cygpath -w "$PIPE_DIR" | tr '\\' '/')"
    INPUT="$(cygpath -w "$INPUT" | tr '\\' '/')"
    OUTPUT="$(cygpath -w "$OUTPUT" | tr '\\' '/')"
fi

# Which dpe: DPE_BIN if set, else this checkout's own release build, else `dpe` on PATH.
# The checkout's build comes before PATH so the code under test is what runs - an installed
# dpe on PATH (e.g. an older npm release) would silently test something else.
workspace="$(cd "$SCRIPT_DIR/.." && pwd)"
candidate="$workspace/target/release/dpe"
if [ -n "${DPE_BIN:-}" ]; then
    DPE="$DPE_BIN"
elif [ -x "$candidate.exe" ]; then
    DPE="$candidate.exe"
elif [ -x "$candidate" ]; then
    DPE="$candidate"
elif command -v dpe >/dev/null 2>&1; then
    DPE=dpe
else
    echo "dpe not found. Build it first: cargo build --release -p dpe" >&2
    exit 1
fi

echo "[run-all] dpe: $DPE"
echo "[run-all] pipeline: $PIPE_DIR"

# Clean outputs from any prior run
rm -rf "$PIPE_DIR/data/output"/*
rm -rf "$PIPE_DIR/sessions"

# Regenerate pipeline-local tool proxies. Resolution order:
#   1. /opt/dpe/tools/<t>/<t>        -- pre-installed standard tools (Docker base)
#   2. <ws>/target/release/<t>(.exe) -- local release build (dev workstation)
#
# (1) lets the script work in-container without rebuilding when the host
# workspace is bind-mounted with foreign-platform binaries (e.g. Windows
# .exe mounted into a Linux container). (2) is the everyday dev path.
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_DIR="/opt/dpe/tools"
if command -v cygpath >/dev/null 2>&1; then
    TARGET_BIN="$(cygpath -w "$WORKSPACE/target/release" | tr '\\' '/')"
else
    TARGET_BIN="$WORKSPACE/target/release"
fi

resolve_tool() {
    local name="$1"
    if [ -x "$INSTALL_DIR/$name/$name" ]; then echo "$INSTALL_DIR/$name/$name"; return 0; fi
    if [ -x "$TARGET_BIN/$name.exe" ];     then echo "$TARGET_BIN/$name.exe";     return 0; fi
    if [ -x "$TARGET_BIN/$name" ];         then echo "$TARGET_BIN/$name";         return 0; fi
    return 1
}

echo "[run-all] resolving tool binaries"
for t in scan-fs read-file-stream write-file-stream write-file-stream-hashed \
         normalize gate checkpoint; do
    if ! bin="$(resolve_tool "$t")"; then
        echo "[run-all] FAIL -- '$t' not found in $INSTALL_DIR/ or $TARGET_BIN/" >&2
        exit 1
    fi
    echo "[run-all]   $t -> $bin"
    mkdir -p "$SCRIPT_DIR/standard/tools/$t"
    cat > "$SCRIPT_DIR/standard/tools/$t/meta.json" <<META
{
  "name": "$t",
  "version": "2.0.0-rc1",
  "runtime": "rust",
  "description": "Pipeline-local proxy.",
  "entry": "$bin",
  "run":   "$bin"
}
META
done

pass=0
fail=0
failures=()

for variant_file in "$PIPE_DIR/variants"/*.yaml; do
    variant="$(basename "$variant_file" .yaml)"
    echo
    echo "=== $variant ==="
    if "$DPE" run "$PIPE_DIR:$variant" \
         -i "$INPUT" \
         -o "$OUTPUT" \
         --clear session 2>&1 | tail -1; then
        pass=$((pass+1))
    else
        fail=$((fail+1))
        failures+=("$variant")
    fi
done

# 06-errors-port: the dead-letter file must hold one error envelope per seed envelope.
dl="$SCRIPT_DIR/standard/data/output/errors-port.ndjson"
seeds=$(grep -c . "$SCRIPT_DIR/standard/data/input/_seed.ndjson")
got=$(grep -c '"stage":"read"' "$dl" 2>/dev/null || true)
if [ "$got" != "$seeds" ] || ! grep -q '"message":"missing ' "$dl"; then
    echo "[run-all] FAIL -- 06-errors-port: $dl has ${got:-0} error envelopes, want $seeds" >&2
    fail=$((fail+1)); failures+=("06-errors-port (content)")
fi

echo
echo "[run-all] $pass passed, $fail failed"
if [ $fail -gt 0 ]; then
    for f in "${failures[@]}"; do echo "  FAIL: $f"; done
    exit 1
fi

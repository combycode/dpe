#!/usr/bin/env bash
# Regression runner for the standard-tool pipeline.
# Runs every variant under standard/variants/ and reports pass/fail.
#
# Usage:
#   ./test-pipeline/run-all.sh                         # use built dpe binary
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

DPE="${DPE_BIN:-dpe}"

if ! command -v "$DPE" >/dev/null 2>&1; then
    # Fall back to the monorepo's own release binary
    workspace="$(cd "$SCRIPT_DIR/.." && pwd)"
    candidate="$workspace/target/release/dpe"
    [ -x "$candidate.exe" ] && DPE="$candidate.exe" || DPE="$candidate"
    if [ ! -x "$DPE" ]; then
        echo "dpe not found. Build it first: cargo build --release -p dpe" >&2
        exit 1
    fi
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

echo
echo "[run-all] $pass passed, $fail failed"
if [ $fail -gt 0 ]; then
    for f in "${failures[@]}"; do echo "  FAIL: $f"; done
    exit 1
fi

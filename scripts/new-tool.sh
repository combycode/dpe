#!/usr/bin/env bash
# new-tool.sh — scaffold + headless-generate a DPE tool from a spec file.
#
# Usage:
#   ./scripts/new-tool.sh <name> <runtime> <spec-path>
#
# Example:
#   ./scripts/new-tool.sh slugify-path bun ./fixtures/slugify-path.yaml

set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <name> <runtime:rust|bun|python> <spec-path>" >&2
    exit 2
fi

NAME="$1"
RUNTIME="$2"
SPEC="$3"

case "$RUNTIME" in
    rust|bun|python) ;;
    *) echo "error: runtime must be rust | bun | python (got: $RUNTIME)" >&2; exit 2 ;;
esac

# Resolve this script's own dir -> workspace root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(dirname "$SCRIPT_DIR")"
export DPE_FRAMEWORKS_DIR="$WORKSPACE_ROOT"

DPE_DEV="$WORKSPACE_ROOT/dpe-dev/target/release/dpe-dev.exe"
EXPERIMENTS="$WORKSPACE_ROOT/tool-experiments"
TOOL_DIR="$EXPERIMENTS/tools/$NAME"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$EXPERIMENTS/logs/$NAME-run-$TIMESTAMP.jsonl"

if [ ! -x "$DPE_DEV" ]; then
    echo "error: dpe-dev not built at $DPE_DEV" >&2
    exit 1
fi
if [ ! -f "$SPEC" ]; then
    echo "error: spec not found: $SPEC" >&2
    exit 1
fi

# Resolve spec to absolute path
SPEC_ABS="$(cd "$(dirname "$SPEC")" && pwd)/$(basename "$SPEC")"

echo "[1/4] scaffold $NAME ($RUNTIME) -> $TOOL_DIR"
"$DPE_DEV" scaffold --name "$NAME" --runtime "$RUNTIME" --out "$TOOL_DIR" --description "generated from spec"

echo "[2/4] copy spec -> spec.yaml"
cp "$SPEC_ABS" "$TOOL_DIR/spec.yaml"

echo "[3/4] claude headless (log: $LOG_FILE)"
mkdir -p "$(dirname "$LOG_FILE")"

PROMPT="Read spec.yaml in the current working directory. Follow the dpe-tool skill from $EXPERIMENTS/.claude/skills/dpe-tool/SKILL.md. Implement src/main.* per spec, expand tests, regenerate verify/ from spec.yaml tests. Then run: $DPE_DEV build . ; $DPE_DEV test . ; $DPE_DEV verify . -- iterate until all three exit 0."

(
    cd "$TOOL_DIR"
    claude -p "$PROMPT" \
        --output-format stream-json --verbose \
        --permission-mode bypassPermissions \
        --add-dir "$EXPERIMENTS" \
        < /dev/null > "$LOG_FILE" 2>&1
) || echo "       claude exited: $?"

echo "[4/4] independent verification"
"$DPE_DEV" build  "$TOOL_DIR"
"$DPE_DEV" test   "$TOOL_DIR"
"$DPE_DEV" verify "$TOOL_DIR"

echo ""
echo "[done] $NAME ready at $TOOL_DIR"
echo "       log: $LOG_FILE"

# Authoring a DPE tool

Build a new streaming pipeline tool from a one-file specification. Two modes:

- **Autonomous** — hand a `spec.yaml` to the scaffolding pipeline, walk away, come back to a tested tool. One command.
- **Manual** — scaffold the skeleton, edit the source by hand. Same end state.

Both modes use the same primitives: per-framework templates, a Rust `dpe-dev` CLI, and (for autonomous mode) a Claude skill pack.

---

## Quick start — autonomous

```powershell
# PowerShell
powershell -File scripts/new-tool.ps1 uppercase-text bun fixtures/uppercase-text.yaml
```

```bash
# Bash (Git Bash on Windows, Linux, macOS)
./scripts/new-tool.sh uppercase-text bun fixtures/uppercase-text.yaml
```

2–5 minutes later you have a tool directory under `tool-experiments/tools/<name>/` with:

- Source code implementing the spec
- Unit tests covering the happy and edge paths
- `verify/<case>/` directories one-per-test-case from the spec
- All three checks passing: `dpe-dev build`, `dpe-dev test`, `dpe-dev verify`

The script does:

1. **scaffold** — copies the matching framework template, substitutes `{{tool_name_kebab}}`, `{{tool_name_snake}}`, `{{description}}`, `{{framework_path}}`, `{{framework_abs_path}}`.
2. **copy spec** — drops your `spec.yaml` into the tool directory as the authoritative description.
3. **claude headless** — invokes `claude -p` with `--permission-mode bypassPermissions`, loads the `.claude/skills/dpe-tool/SKILL.md` skill, and lets the agent loop until `build + test + verify` all exit 0.
4. **independent verification** — re-runs `dpe-dev build / test / verify` from the wrapper, fails hard on any non-zero exit, so a stuck agent can't silently leave a broken tool.

---

## spec.yaml — single source of truth

The spec file describes the tool. Templates, tests, and verify cases are derived from it.

Minimum viable shape:

```yaml
name: "my-tool"                               # kebab-case
runtime: "bun"                                # rust | bun | python
description: "One-line purpose."

settings:
  type: object
  properties:
    marker:
      type: string
      default: ""
  additionalProperties: false

input:
  v:
    type: object
    properties:
      text: { type: string, required: true }

output:
  v:
    type: object
    properties:
      text: { type: string }

tests:
  - name: happy-path
    settings: {}
    input:
      - '{"t":"d","id":"1","src":"s","v":{"text":"hello"}}'
    expected:
      - '{"t":"d","id":"1","src":"s","v":{"text":"HELLO"}}'

  - name: with-setting
    settings: { marker: "X:" }
    input:
      - '{"t":"d","id":"1","src":"s","v":{"text":"hi"}}'
    expected:
      - '{"t":"d","id":"1","src":"s","v":{"text":"X:HI"}}'
```

### Field reference

| Field | Purpose |
|---|---|
| `name` | Kebab-case canonical name. Becomes crate/package name. |
| `runtime` | `rust`, `bun`, or `python`. Picks the template. |
| `description` | One-line purpose. Rendered into generated meta.json / source comments. |
| `settings` | JSON-schema-lite (`type`, `properties`, `required`, `additionalProperties`, `default`). The tool's argv[1] shape. |
| `input` / `output` | Descriptive shape of `v.*` on incoming and outgoing envelopes. Prose is fine — tests are authoritative. |
| `tests` | Array of acceptance cases. Each has `name`, `settings`, `input` (array of NDJSON lines), `expected` (array of NDJSON lines). |

### How tests drive verification

Each `tests[i]` entry lands in `verify/<name>/` as three files:

- `settings.json` — pretty-printed from `settings`
- `input.ndjson` — each `input[]` entry on its own line
- `expected.ndjson` — each `expected[]` entry on its own line

`dpe-dev verify .` walks every subdirectory of `verify/`, spawns the built tool, pipes `input.ndjson` to stdin with `settings.json` as argv[1], captures stdout, and line-by-line canonical-JSON compares against `expected.ndjson`. Any mismatch fails the case.

Canonical JSON = `serde_json` with sorted keys — object-key order in expected vs actual doesn't matter.

---

## Templates — where they live

Each framework owns its template, colocated with the framework source:

```
frameworks/rust/template/               # Rust
frameworks/ts/template/                 # Bun/TypeScript
frameworks/python/template/             # Python
```

When the frameworks ship as standalone packages (crates.io / npm / PyPI),
the template directory is also embedded in the `dpe-dev` binary via
`include_dir!` so scaffolding still works without the monorepo checked out.

Template update = edit the framework's template files + commit. Next `dpe-dev scaffold` uses the new version automatically; nothing to rebuild.

Why colocated:

- Framework API changes and template updates ship in the same commit
- No binary-embedded templates → no redistribute on template change
- Framework maintainer owns template quality (one source of truth)

### Placeholder substitution

Scaffold walks the template tree, substitutes these tokens in every text file:

| Token | Expansion |
|---|---|
| `{{tool_name_kebab}}` | `my-tool` |
| `{{tool_name_snake}}` | `my_tool` |
| `{{tool_name}}` | kebab form (alias) |
| `{{description}}` | `--description` flag value |
| `{{framework_path}}` | relative path from out to framework dir (for `{ path = "../..." }` style deps) |
| `{{framework_abs_path}}` | absolute framework dir, forward-slashed (for Python `file:///...` URLs) |

Directory name token: `__PKG__` → `{{tool_name_snake}}`. Used in the Python template to rename `src/__PKG__/` to the tool's package name.

---

## dpe-dev — the CLI

Single Rust binary built from the `dpe-dev/` workspace member (`dpe-dev/target/release/dpe-dev` on Linux/macOS, `dpe-dev.exe` on Windows). Subcommands:

```
dpe-dev scaffold --name <n> --runtime <r> --out <dir> [--description "..."] [--frameworks-dir <path>]
dpe-dev build    <tool-dir>    # runtime-aware: cargo build --release / bun install / pip install -e .
dpe-dev test     <tool-dir>    # cargo test / bun test / pytest
dpe-dev verify   <tool-dir>    # spawn tool, feed verify cases, diff stdout
dpe-dev check    <tool-dir>    # static: meta.json parses, spec.yaml parses
```

### Framework discovery

Priority order:

1. `--frameworks-dir <path>` CLI flag
2. `DPE_FRAMEWORKS_DIR` env var
3. Walk upward from current directory, looking for `frameworks/{rust,ts,python}` as a child of an ancestor (the monorepo layout)
4. Walk upward from the binary's own location (fallback so the binary finds its home anywhere)

The wrapper scripts (`scripts/dev.ps1`, `scripts/new-tool.*`) set `DPE_FRAMEWORKS_DIR` to the workspace root explicitly. `dpe-dev` invoked directly usually finds frameworks via option 3 or 4.

### Build behaviour per runtime

| Runtime | `dpe-dev build` |
|---|---|
| Rust | `cargo build --release` — produces `target/release/<tool-name>` (`.exe` on Windows) |
| Bun | `bun install` — resolves the framework dep, no compile step |
| Python | creates a per-tool `.venv/` (via `uv venv --seed`, falling back to `python3 -m venv`), then `pip install -e .[dev]` inside it |

Future flags (`--full`, `--sign`, `--bundle`, `--wheel`) are stubbed but not yet implemented.

---

## Claude skill pack

Lives at `tool-experiments/.claude/skills/dpe-tool/`. Auto-discovered when Claude runs with `--add-dir tool-experiments` or inside that directory.

### Files

- `SKILL.md` — main instruction. What a tool is, the contract, the workflow, anti-patterns.
- `references/rust.md` — Rust-specific framework API, settings handling, test patterns
- `references/bun.md` — same for Bun/TS
- `references/python.md` — same for Python

Cross-referenced from `SKILL.md`. The agent reads what it needs.

### Permission defaults

`tool-experiments/.claude/settings.json` pre-authorises Bash for `cargo`, `bun`, `python`, `pytest`, `pip`, `uv`, `powershell`, and the `dpe-dev.exe` path. Read/Edit/Write/Glob/Grep also allow-listed.

---

## Headless invocation — flags that matter

```bash
claude -p "<prompt>" \
    --output-format stream-json \
    --verbose \
    --permission-mode bypassPermissions \
    --add-dir <tool-experiments> \
    < /dev/null > <log-file> 2>&1
```

- **`-p` / `--print`** — non-interactive mode. Runs the full agent loop (read → edit → bash → iterate) until the agent considers the task complete, then exits. Not literally "one turn" — the loop can span dozens of tool calls.
- **`--output-format stream-json`** — line-delimited events for post-run review. Requires `--verbose`.
- **`--verbose`** — required by `stream-json` in `-p` mode.
- **`--permission-mode bypassPermissions`** — skips per-tool-call prompts. Safest alternative: `dontAsk` + `--allowedTools "Bash(cargo *),..."` whitelist. `bypassPermissions` is fine for a scoped, reviewable workspace like `tool-experiments/`.
- **`--add-dir`** — adds the directory to the agent's scope and loads any `.claude/skills/` inside it. Without this, headless mode skips project-level `.claude/`.
- **`< /dev/null`** — suppresses the "no stdin in 3s" warning.

### Cost note

The `total_cost_usd` field in the JSON log is the **equivalent API cost** computed from token usage at list price. If you're authenticated via a Claude Pro / Max subscription, you pay nothing incremental — usage counts against quota, not wallet. The number is budgetary-reference only.

---

## Manual mode — without Claude

If you want to write the tool yourself:

```bash
# Scaffold only
dpe-dev scaffold --name my-tool --runtime bun --out ./tools/my-tool --description "what it does"

# Copy spec in manually (or author it in place)
cp path/to/spec.yaml ./tools/my-tool/spec.yaml

# Edit src/main.*, tests/, verify/ by hand

# Run the same verification cycle
dpe-dev build  ./tools/my-tool
dpe-dev test   ./tools/my-tool
dpe-dev verify ./tools/my-tool
```

Same exit criteria: all three zero = done.

---

## Anti-patterns

Every runtime:

- **Don't print to stdout directly.** Only `ctx.output` / `ctx.meta` produce envelope data. `println!` / `console.log` / `print()` pollute the stream.
- **Don't print to stderr directly.** Use `ctx.error`, `ctx.log`, `ctx.trace`. The framework serialises typed events correctly.
- **Don't self-exit while stdin is open.** On EOF, drain queues, then exit 0. Signal handlers (SIGTERM) get same behaviour.
- **Don't re-parse settings per envelope.** Parse once at startup, treat as read-only.
- **Don't add dependencies the template doesn't already have.** Solve with stdlib first; add deps only when justified.
- **Don't modify `id` / `src`** on output envelopes unless the spec requires it. These are provenance.
- **Don't buffer all of stdin.** Process line-by-line (framework handles this — don't override the loop).

---

## Related

| Doc | What |
|---|---|
| [Tool contract](tools/README.md) | argv/stdin/stdout/stderr contract, stderr event types, catalogue |
| [Frameworks](frameworks.md) | SDK reference for Rust, Python, TypeScript |
| [Writing pipelines](writing-pipelines.md) | How to compose tools into a DAG |
| [Concepts](concepts.md) | Envelope, stage, DAG kinds, session layout |
| `tool-experiments/README.md` | Per-workspace quick start for the skill-pack flow |

---

## Troubleshooting

**"no frameworks root found"**
Set `DPE_FRAMEWORKS_DIR` or pass `--frameworks-dir`. If you're running from outside the DataStudio workspace, auto-discovery can't see the framework siblings.

**Python build fails with "Dependency ... cannot be a direct reference"**
The Python template's `pyproject.toml` needs `[tool.hatch.metadata] allow-direct-references = true`. Already set in the template; if your generated tool misses it, the scaffold drifted.

**Bun tests run the tool instead of testing**
Wrap the `await run(...)` block in `if (import.meta.main) { ... }` so imports don't auto-start the framework. The autonomous agent usually does this; check `src/main.ts` if tests hang.

**Headless Claude hits permission denials**
Use `--permission-mode bypassPermissions` for scoped experiments. Or `dontAsk` + explicit `--allowedTools` whitelist. Check the log's `permission_denials` array to see what it tried to call.

**PowerShell script parse errors on non-ASCII characters**
Windows PowerShell 5.1 reads `.ps1` files as Windows-1252 unless there's a UTF-8 BOM. Keep script bodies ASCII-only (use `--` not `—`, `->` not `→`) or save with BOM.

**`pwsh` command not found**
Scripts target Windows PowerShell 5.1 (`powershell`). PowerShell 7 (`pwsh`) isn't assumed — scripts are written to work on both where possible, but the invocation commands in docs use `powershell`.

# `dpe` CLI reference

One binary covers every user-facing operation. Built from `dpe/runner` —
ships at `~/.dpe/bin/dpe` (npm), `/usr/local/bin/dpe` (Linux package),
`%LOCALAPPDATA%\dpe\dpe.exe` (Windows install), or
`<repo>/runner/target/release/dpe(.exe)` from a workspace build.

Most commands accept `--config <path>` to override the runner config
file. Resolution order (first hit wins):

1. `--config <path>` argument
2. `DPE_CONFIG` env var
3. `<cwd>/config.toml` — pipeline-local override (auto-picked when running from a pipeline dir)
4. `~/.dpe/config.toml` — standard user install
5. `<dpe-binary-dir>/config.toml` — portable / ad-hoc installs
6. Built-in defaults

### Global flags

| Flag | Purpose |
|---|---|
| `--config <PATH>` | Runner config file (resolution order above) |
| `--env-file <PATH>` | Load env vars from a `.env`-style file before running. Repeatable. **First occurrence wins**, and any var **already in the process environment is never overridden** (CI secrets stay authoritative). Path must exist — there is no silent CWD pickup. Loaded once at startup, so every subcommand sees the values. |

`--env-file` example:
```sh
dpe --env-file .env run my-pipeline:main -i ./input -o ./output
dpe --env-file .env --env-file .env.local test my-pipeline    # merge two files
```

Target syntax for commands that take one:
```
<pipeline-dir>:<variant-name>
```
`pipeline-dir` is a relative or absolute path; its basename is the
pipeline name. `variant-name` is the filename stem under
`<pipeline-dir>/variants/`.

---

## `run`

Execute a variant end-to-end.

```sh
dpe run my-pipeline:main \
    --input  /path/to/inputs \
    --output /path/to/outputs \
    [--seed '<json>'] [--seed-file <path>] \
    [--temp-dir <path>] [--storage-dir <path>] \
    [--cache use|refresh|bypass|off] \
    [--clear session|temp|storage|all] \
    [--json] [--stats <ms>]
```

Creates `<pipeline>/sessions/<id>_<variant>/` and streams envelopes
through the DAG. Default mode prints a one-line summary at end:

```
[OK] my-pipeline:main — 4 stage(s), 4 succeeded, 0 failed, 14490ms
```

### Seed input (`--seed`, `--seed-file`)

Two ways to inject the run's first envelope:

- `--seed '<json-object>'` — single envelope. The object is treated as
  the `v` field; runner wraps it as `{t:"d", id:<hash>, src:"seed", v:<obj>}`.
  Path-prefix expansion runs automatically — `$input`, `$output`,
  `$temp`, `$storage`, `$session`, `$configs` resolve before the
  envelope hits the pipeline. Power users can pass an already-wrapped
  envelope (`{"t":"d","v":{...},...}`) and it's passed through as-is.
- `--seed-file <path>` — file with one JSON object per line. Same
  per-line wrap + prefix-expansion. Mutex with `--seed`.

Without either flag, the runner falls back to:
- `<input>` is a file → fed as seed bytes
- `<input>` is a dir → look for `_seed.ndjson` inside; else empty

### Path overrides (`--temp-dir`, `--storage-dir`)

By default `$temp` resolves to `<pipeline>/temp` and `$storage` to
`<pipeline>/storage`. Override per-run for parallel execution
(prevents two concurrent runs from colliding on checkpoint spool dirs
or dedup indexes):

```sh
dpe run my-pipeline:main -i ... -o ... \
    --temp-dir    /tmp/run-A \
    --storage-dir /var/dpe/store-A
```

### `--json` (machine-readable mode)

Emits NDJSON to stdout instead of human banner + summary. One event
per line.

```jsonc
// First line — session metadata.
{"event":"started","sessionId":"20260502-185253-b430","sessionDir":"...","controlAddr":"...","pid":19844,"pipeline":"my-pipeline","variant":"main"}

// (zero-or-more periodic stats events, see --stats below)

// Last lines — final stats snapshot + summary.
{"event":"stats","t":1777747974374,"stages":{
  "scan":["succeeded",1,4,0,0],
  "marker":["succeeded",4,4,0,0],
  ...
}}
{"event":"summary","sessionId":"...","pipeline":"...","variant":"...","stagesRun":4,"stagesSucceeded":4,"stagesFailed":0,"durationMs":551}
```

Used by editors and automation to capture session metadata + final
state without text-scraping.

### `--stats [<ms>]`

Periodic per-stage counter + state snapshots, in addition to the
unconditional start + end snapshots `--json` always emits.

- `--stats 250` — snapshot every 250 ms during the run
- `--stats` (no value) — defaults to 500 ms
- omitted — only the start + end snapshots fire

Each snapshot's `stages` map is keyed by sid; the value is a
**5-element array** `[state, rows_in, rows_out, meta, errors]`:

| Index | Field | Source |
|------:|---|---|
| 0 | `state` | `pending` / `running` / `succeeded` / `failed` / `cancelled` |
| 1 | `rows_in` | count of `{type:"input"}` events on the stage's stderr |
| 2 | `rows_out` | count of `{type:"trace", channel:"data"}` events |
| 3 | `meta` | count of `{type:"trace", channel:"meta"}` events |
| 4 | `errors` | count of `{type:"error"}` events |

Pending → Running is derived from `rows_in > 0`. Terminal transitions
come from child exit codes (single/replicas) or builtin task results
(route/filter/dedup/group-by). See [concepts.md](concepts.md#per-stage-state-machine).

### Exit codes

- `0` — every stage succeeded AND `errors == 0` across the board
- non-zero — at least one stage exited non-zero, or validation failed
  pre-run

---

## `check`

Parse + resolve + validate a variant without running it.

```sh
dpe check my-pipeline:main
dpe check my-pipeline:main --all       # every variant in the pipeline
dpe check my-pipeline:main --plan      # also print the compiled ExecutionPlan
```

Returns 0 on success, 1 on any validation error. Always safe; touches
no session artefacts.

What it validates:
- Every `tool:` resolves (local `tools/`, then `tools_paths`, then built-ins)
- Every `input:` references an existing stage or `$input`
- `stage.channel` input: upstream must be a `route` stage with that channel declared
- DAG has no cycles
- Route + filter expressions compile
- `settings_file:` paths exist and parse as JSON

`--plan` prints the compiled `ExecutionPlan` as JSON. Resolved tool
invocations, planned execution kind per stage (`spawn_single` /
`spawn_replicas` / `call_builtin`), settings with `$prefix/...`
expanded. `$session/...` stays literal — it's bound only at run time.
Mutually exclusive with `--all`.

CI use:
```sh
dpe check --all my-pipeline || exit 1
```

---

## `test`

Per-stage isolated snapshot test. Spawns ONE stage as a child process
(no DAG), pipes `tests/<variant>/<stage>/<case>/input/seed.ndjson` to
its stdin, captures every output channel, and runs four diff steps
(channel shape → filesystem tree → per-channel envelope diff → optional
assert script). Multi-phase test cases are supported.

Full per-case schema, channel semantics, multi-phase examples, and
matcher / mode reference live in [testing.md](testing.md). This page
is the CLI surface.

```sh
dpe test my-pipeline:main:scan                       # every case under the stage
dpe test my-pipeline:main:scan:case-baseline         # one case
dpe test my-pipeline:main                            # bulk: every stage of variant
dpe test my-pipeline                                 # bulk: every variant × stage
dpe test :main:scan                                  # leading `:` = cwd as pipeline
dpe test .:main:scan                                 # `.` = same
```

Target syntax: `[<pipeline>:]<variant>:<stage>[:<case>]`. Empty/`.`
pipeline = current directory. 1- or 2-part target = bulk; 3- or 4-part
= stage-explicit (skip-list and `test_exclusive` BYPASSED).

### Layout

```
my-pipeline/tests/<variant>/<stage>/<case>/
├── test.yaml                   # optional; per-case spec (full schema in testing.md)
├── input/seed.ndjson           # one envelope per line; piped to stdin
├── expected/                   # reference: per-channel ndjson + filesystem tree
│   ├── data.ndjson             # t="d" envelopes
│   ├── meta.ndjson             # t="m" envelopes (only if asserted)
│   ├── errors.ndjson           # stderr type="error" (only if asserted)
│   └── output/                 # files the tool wrote under $output
├── assert.py                   # optional assertion script
└── .run/                       # per-test ephemerals; gitignored
    ├── actual/                 # ← runner writes captured streams here
    │   ├── data.ndjson
    │   ├── meta.ndjson
    │   ├── errors.ndjson
    │   ├── logs.ndjson
    │   ├── trace.ndjson
    │   └── stats.ndjson
    ├── temp/                   # $temp
    ├── session/                # $session
    ├── storage/                # $storage (cache + batch state live here)
    └── output/                 # $output
```

For multi-phase cases, `expected/` contains one subdir per phase:
`expected/<phase.name>/`. `.run/` is wiped ONCE at case start;
`.run/actual/` is re-wiped between phases (so `output/`, `temp/`,
`storage/`, `session/` persist across phases).

### `test.yaml` skeleton

```yaml
# All fields optional. Empty file = inherit; auto-detect channels from
# expected file presence.

settings_override: { marker: "TEST:" }
env:               { ANTHROPIC_API_KEY: "${REAL_KEY}" }
cache:             bypass                       # use | refresh | bypass | off

compare:
  channels: ["data", "meta"]                    # opt-in or auto-detect
  global:
    scrub_paths:
      - { from: "msgbatch_[A-Za-z0-9]+", to: "msgbatch_<ID>" }
      - { from: "<run_dir>",             to: "<run>" }
  data:
    ignore_envelope: ["id", "src"]
    matchers:
      - { path: "v.duration_ms", kind: "is_int" }
      - { path: "v.batch_id",    kind: "regex", pattern: "^msgbatch_" }
  fs_check:  ["output"]
  files:
    - { path: "output/summary.md", mode: "fuzzy", threshold_pct: 5 }
    - { path: "output/report.json", mode: "schema", schema: "expected/output/report.schema.json" }

assert:
  engine: "python"          # python | bun | node
  script: "assert.py"
  timeout_ms: 30000

phases:                                          # optional — multi-shot
  - name: "cold", cache: bypass, expected: "expected/cold"
  - name: "warm", cache: use,    expected: "expected/warm"
```

See [testing.md § channels](testing.md#channels) for the six channel
sources and the strict/opt-in distinction; § how the diff works for
the four steps; § multi-phase tests for cache / batch / idempotency
patterns.

### Output

```
PASS    main:scan:case-baseline      (12ms)
FAIL    main:scan:case-edge          (8ms)
        Step 1 — channel shape:
          • channel 'meta' declared in compare.channels but expected/meta.ndjson is missing
        Step 3 — channel diff:
          channel 'data':
            --- expected
            +++ actual
            -{"t":"d","v":{"path":"a"}}
            +{"t":"d","v":{"path":"b"}}
SKIP    main:gate:case-baseline      (skip-list: gate)

Summary: 1 passed, 1 failed, 1 skipped  in 0.02s
```

For multi-phase cases each phase's failure block is prefixed `── phase "name" ──`.

### Snapshot regeneration

```sh
dpe test my:main:scan --update                   # rewrite expected/ from canonicalised actual
dpe test my:main:scan --update-if-missing        # only write if expected file absent
```

`--update` and `--update-if-missing` are mutually exclusive. Each
non-empty actual channel (`data` / `meta` / `errors` / etc.) is
canonicalised (envelope `id`+`src` stripped, `ignore_fields` dropped,
matchers replaced with sentinels, JSON keys sorted, `scrub_paths`
applied) and written as `expected/<channel>.ndjson`. Always review
the resulting `git diff` before committing.

For multi-phase cases, `--update` writes to `expected/<phase.name>/`.
Filesystem-tree expectations under `expected/<subdir>/` are NOT
auto-generated — author them manually.

### Cache override

```sh
dpe test my:main:llm --cache bypass               # this run hits the model every time
dpe test my-pipeline --cache refresh              # bulk: refresh all caches
```

Modes: `use` (default — read cache, write on miss) | `refresh` (ignore on read, write fresh) | `bypass` (ignore on read, ignore on write) | `off` (never read, never write).

Precedence (highest first): **CLI `--cache <mode>`** → **`test.yaml` `cache:` field** → **default (`use`)**. `--cache` applies uniformly to bulk runs (every case in the bulk inherits it).

Use `cache: bypass` in `test.yaml` when a case is *inherently* a stability test (committed, reviewable). Use `--cache` for ad-hoc overrides ("rerun all snapshots without cache once before release").

### Bulk-run filters

In bulk mode (target with no explicit stage — 1- or 2-part target),
two filters apply automatically:

| Filter | Behaviour |
|---|---|
| **Skip-list** | `toggle`, `gate`, `checkpoint`, `dedup` are silently skipped — control-layer plumbing isn't worth a snapshot test on its own; the variants that USE these stages get tested as part of the surrounding settings flow. |
| **`test_exclusive`** | A tool's `meta.json` may declare `test_exclusive: true`. Bulk runs skip such stages so they don't fail when their host environment is missing. Run them by naming the stage explicitly (3- or 4-part target). |

Stage-explicit targets (3- or 4-part) BYPASS both filters — the user
asked for that stage, the runner respects that.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | All cases PASS or UPDATED (or only skipped) |
| 1 | Any FAIL (snapshot mismatch) |
| 2 | Any ERROR (invocation problem — bad target, missing variant, spawn failure). Wins over FAIL. |

CI use:
```sh
dpe test my-pipeline || exit 1
```

---

## `coverage`

Snapshot-test coverage matrix for a pipeline. Informational — never gates.

```sh
dpe coverage my-pipeline                 # every variant
dpe coverage my-pipeline:main            # one variant
dpe coverage .:main                      # cwd as pipeline
dpe coverage my-pipeline --json          # machine-readable
```

Target syntax: `<pipeline>[:<variant>]`. `stage` and `case` parts are
ignored if present.

### Buckets

| Symbol | Bucket | In numerator | In denominator |
|:---:|---|:---:|:---:|
| ✓ | `covered` | yes | yes |
| ◐ | `excl+covered` (`test_exclusive=true` AND has tests) | yes | yes |
| ◔ | `excl+uncovered` (`test_exclusive=true` AND no tests) | no | yes |
| ✗ | `uncovered` (no tests) | no | yes |
| ⊘ | `skip-list` (control-layer tool) OR `test-skipped` | excluded | excluded |

Coverage % = (✓ + ◐) / (✓ + ◐ + ◔ + ✗). ⊘ stages don't count either way.

### Output

```
variant: 02-read-normalize-write
  ✗  read
  ✗  sink
  ✓  upper  2 cases
  1/3 covered (33%)

variant: 03-gate-checkpoint
  ⊘  check  (skip-list: checkpoint)
  ⊘  gate   (skip-list: gate)
  ✗  read
  ✗  sink
  0/2 covered (0%)

Total: 1/13 covered (8%)
```

### `--json`

```json
{
  "variants": [
    {
      "variant": "02-read-normalize-write",
      "stages": [
        {"stage":"read","tool":"read-file-stream","bucket":"uncovered","case_count":0},
        {"stage":"sink","tool":"write-file-stream","bucket":"uncovered","case_count":0},
        {"stage":"upper","tool":"normalize","bucket":"covered","case_count":2}
      ],
      "covered": 1, "total": 3, "pct": "33.3"
    }
  ],
  "covered": 1, "total": 13, "pct": "7.7"
}
```

`bucket` is one of: `covered`, `skip_list`, `test_skipped`, `exclusive_covered`, `exclusive_uncovered`, `uncovered`. `pct` is formatted to one decimal place as a string (rounded). The skip-listed tool's name lives in the sibling `tool` field — `bucket` is just the bucket kind.

Exit always 0 — this command reports, it doesn't enforce.

For the full testing guide (layout, writing cases, fixtures, troubleshooting),
see [Testing](testing.md).

---

## `log`

Per-stage log + error stream / tail. Replaces (and is more capable
than) `dpe logs` — see below for the difference.

```sh
dpe log <session> [--stage <name>]                  # last 50 entries, exit
dpe log <session> --stage scan --tail 100           # last 100 entries
dpe log <session> --stage scan --follow             # live tail until session ends
dpe log <session> --stage scan --error              # errors only
dpe log <session> --stage scan --log                # log lines only
dpe log <session> --stage scan --search 'zephyr'    # case-insensitive substring
dpe log <session> --stage scan --search 'zeph[yi]r' --regex   # full Rust regex
```

### Sources merged

Two on-disk files, time-merged by the `t` (ms-since-epoch) field:

- `<session>/log.ndjson` — every `ctx.log()` call across all stages, mixed
- `<session>/logs/<stage>_errors.log` — every `ctx.error()` call, one
  file per stage, NDJSON. `t` and `sid` are injected by the runner so
  the time-merge works.

Without `--stage`: every stage. With `--stage X`: filter `log.ndjson`
to `sid: X` and read only `<X>_errors.log`.

### Output

NDJSON. `kind` discriminates source:

```jsonc
{"t":...,"sid":"scan","kind":"log",  "level":"info","msg":"...",          "envelopeId":"..."}
{"t":...,"sid":"scan","kind":"error","error":"...","input":{...},"id":"...","src":"..."}
```

### Modes

| Flags | Behavior |
|---|---|
| (default) | Time-merge backlog, take last `--tail N` (default 50, configurable via `[log_sink].tail_default`), print, exit. |
| `--follow` / `-f` | Same backlog cap, then live-tail both files until the session goes terminal. **Auto-degrades** to default mode if the session is already terminal at start (journal state is succeeded/partial/failed/killed). |
| `--search <pattern>` | Filter entries whose `msg` (logs) or `error` field or stringified `input` payload (errors) matches. Default = case-insensitive substring; with `--regex`, full Rust regex syntax. Mutex with `--follow`. |

`--error` and `--log` are mutex — pick one source or get both
(default).

### `dpe log` vs `dpe logs`

- `dpe log` (singular) — what you want for editor / programmatic use.
  Per-stage, NDJSON, error+log merged, supports search.
- `dpe logs` (plural) — text-formatted human tail of `log.ndjson` only,
  no errors, no per-stage filter. Kept for terminal browsing.

---

## `logs`

Plain `tail -f`-like tail of `<session>/log.ndjson` with line
formatting. No errors, no filter, no search.

```sh
dpe logs /path/to/sessions/<id>_<variant>            # print what's there and exit
dpe logs /path/to/sessions/<id>_<variant> --follow    # -f: tail as new lines append
```

Each line: `[sid] level: msg`

Use `dpe log` (singular) for everything else.

---

## `journal`

Rebuild `<session>/journal.json` by scanning trace + error files on
disk. Useful after a killed run that didn't get to finalize the
journal itself.

```sh
dpe journal /abs/path/to/sessions/20260420-062637-a87d_main
```

Writes / overwrites `journal.json`, sets `state: "killed"`, prints:
```
[OK] rebuilt <session>/journal.json
  state=Killed  stages=17  envelopes=22  errors=0
```

---

## `status`

Query a live session's status via its control socket.

```sh
dpe status /path/to/sessions/<id>_<variant>
```

Reads `<session>/control.addr`, connects (named pipe on Windows, UDS
on Unix — never TCP), sends `{"cmd":"status"}`, prints the response as
pretty JSON. Per-stage shape:

```json
{
  "sid": "scan",
  "tool": "scan-fs",
  "state": "running",
  "rows_in": 4,
  "rows_out": 4,
  "meta": 0,
  "errors": 0,
  "replicas": 1
}
```

`state` is the per-stage lifecycle: `pending` / `running` / `succeeded`
/ `failed` / `cancelled`.

Exits non-zero if `control.addr` is missing or the server isn't
reachable — session probably already finished.

---

## `progress`

Same idea as status, but reports gate progress + roll-up totals:

```sh
dpe progress /path/to/sessions/<id>_<variant>
```

Returns gates from `<session>/gates/*.json` (if any) + total rows +
total errors across all stages.

---

## `stop`

Request a graceful stop on a live session.

```sh
dpe stop /path/to/sessions/<id>_<variant>
```

Sends `{"cmd":"stop"}` to the runner. The server acknowledges and the
runner begins draining: stop feeding leaves, let stages process what
they have, flush, exit.

---

## `monitor`

Live ratatui TUI dashboard.

```sh
dpe monitor /path/to/sessions/<id>_<variant>
```

Three tabs:
- **Stages** — table of every stage: sid, tool, state, in, out, meta, errors, replicas; red for stages with errors > 0
- **Pipeline** — overall state, elapsed time, per-gate progress bars
- **Logs** — tail of `<session>/log.ndjson`

Keys: `q` / `Esc` quit, `Tab` cycles tabs, `1` / `2` / `3` jumps directly.

Polls the control socket every 500 ms; falls back to reading
`journal.json` on disk if the session exited.

---

## `tools list`

Enumerate available tools.

```sh
dpe tools list                # human-readable table
dpe tools list --json         # machine-readable; used by the dag-editor
```

JSON shape includes every catalog entry, every path-discovered tool,
and the six builtins (`route`, `filter`, `dedup`, `group-by`, `spread`, `toggle`). Used
by editors to populate a tool palette without re-implementing the
resolution logic.

---

## `install <name>`

Pull a tool from a configured catalog into `~/.dpe/tools/<name>/`.

```sh
dpe install scan-fs
dpe install scan-fs --force      # overwrite existing
```

Catalog comes from `[tools_registries]` in config.toml. Without a
registry, prints an install hint instead of fetching.

---

## `config`

Inspect / edit runner config.

```sh
dpe config show                # print resolved config (after defaults + file loads)
dpe config init                # create ~/.dpe/config.toml + ~/.dpe/tools/ + ~/.dpe/registries/
dpe config init --force        # overwrite an existing one
dpe config add-path <dir>      # append <dir> to tools_paths in ~/.dpe/config.toml
dpe config path                # print the resolved config file path
```

---

## `init <name>`

Scaffold a new pipeline directory.

```sh
dpe init my-pipeline                  # creates ./my-pipeline/
dpe init my-pipeline --out /path/to/parent
```

Creates the standard layout:
```
my-pipeline/
├── pipeline.toml
├── config.toml                  (empty — uses defaults)
├── README.md
├── .gitignore                   (excludes sessions/, temp/, data/output/*, .dpe-editor/)
├── variants/
│   └── main.yaml                (sample 2-stage scan→write pipeline)
├── tools/                       (empty — for pipeline-local tools)
├── configs/                     (empty — for settings_file targets)
├── data/
│   ├── input/.gitkeep
│   └── output/.gitkeep
└── storage/.gitkeep
```

`temp/` and `sessions/` are NOT created here; runner creates them on
first run.

---

## Exit codes (all commands)

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Generic error — see stderr |
| 2 | Invalid settings / bad argv |

---

## Environment variables

CLI / runner reads:

| Variable | Purpose |
|---|---|
| `DPE_CONFIG` | Override runner config path |
| `DPE_TRACE_FLUSH_MS`, `DPE_TRACE_MAX_EVENTS`, `DPE_TRACE_MAX_SEGMENT_BYTES`, `DPE_TRACE_MAX_LABELS`, `DPE_TRACE_MAX_LABEL_CHARS`, `DPE_TRACE_CHANNEL_CAPACITY` | Override `[trace]` knobs |
| `DPE_LOG_SINK_FLUSH_MS`, `DPE_LOG_SINK_CHANNEL_CAPACITY`, `DPE_LOG_TAIL_DEFAULT` | Override `[log_sink]` knobs |
| `DPE_JOURNAL_FLUSH_MS`, `DPE_MONITOR_POLL_MS`, `DPE_DUPLEX_BUF_BYTES`, `DPE_HTTP_TIMEOUT_SECS`, `DPE_CONTROL_CHANNEL_CAP` | Override `[runtime]` knobs |

Tool-side env vars (`ANTHROPIC_API_KEY`, etc.) are not read by the CLI
itself — they're inherited by spawned tools as normal shell vars.

---

## Pattern: end-to-end dev workflow

```sh
# 1. Always validate first
dpe check --all my-pipeline

# 2. Load API keys for tools that need them
set -a && source .env && set +a

# 3. Run with periodic stats so a TUI / editor can render progress
dpe run my-pipeline:main \
    -i data/input \
    -o data/output \
    --seed '{"path":"$input"}' \
    --json --stats 250

# 4. Look at one stage's logs / errors
SESSION=$(ls -td my-pipeline/sessions/*_main | head -1)
dpe log "$SESSION" --stage scan
dpe log "$SESSION" --stage scan --error
dpe log "$SESSION" --search 'zephyr'

# 5. Or run the TUI from another shell while #3 executes
dpe monitor "$SESSION"
```

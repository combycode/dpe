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
(route/filter/dedup/group_by). See [concepts.md](concepts.md#per-stage-state-machine).

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
and the four builtins (`route`, `filter`, `dedup`, `group_by`). Used
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

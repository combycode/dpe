# `dpe` CLI reference

One binary (`runner/target/release/dpe`) covers every user-facing operation.

All commands accept `--config <path>` to point at a non-default runner config (otherwise `$DPE_CONFIG` env or `~/.dpe/config.toml`, with built-in defaults if nothing's present).

Target syntax for commands that take one:
```
<pipeline-dir>:<variant-name>
```
`pipeline-dir` is a relative or absolute path; its basename is the pipeline name. `variant-name` is the filename stem of a file under `<pipeline-dir>/variants/`.

---

## `check`

Parse + resolve + validate a variant without running it.

```sh
dpe check my-pipeline:main
dpe check my-pipeline:main --all       # every variant in the pipeline
dpe check my-pipeline:main --plan      # also print the compiled ExecutionPlan
```

Returns 0 on success, 1 on any validation error. Always safe to run; touches no session artefacts.

What it validates:
- Every `tool:` resolves (local `tools/`, then `tools_paths`, then built-ins)
- Every `input:` references an existing stage or `$input`
- `stage.channel` input: upstream must be a `route` stage with that channel declared
- DAG has no cycles
- Route expressions + filter expressions compile
- `settings_file` paths exist and parse as JSON

`--plan` prints the compiled `ExecutionPlan` as JSON after validation succeeds. The plan shows resolved tool invocations, planned execution kind per stage (`spawn_single` / `spawn_replicas` / `call_builtin`), and settings with any known `$prefix/...` paths expanded. `$session/...` always stays literal in check output — it is bound only at run time. Mutually exclusive with `--all`.

Typical CI use:
```sh
dpe check --all my-pipeline || exit 1
```

---

## `run`

Execute a variant end-to-end.

```sh
dpe run my-pipeline:main \
    --input   /path/to/inputs \
    --output  /path/to/outputs \
    [--cache  use|refresh|bypass|off] \
    [--clear  session|temp|storage|all]
```

Creates `<pipeline>/sessions/<id>_<variant>/` and streams envelopes through the DAG. Prints a one-line summary at end:

```
[OK] my-pipeline:main — 4 stage(s), 4 succeeded, 0 failed, 14490ms
```

Exit code:
- `0` — all stages OK AND no envelopes went to `*_errors.log`
- non-zero — at least one stage exited non-zero OR validation failed

Seed input convention:
- `--input` is a **directory** → runner looks for `_seed.ndjson` and feeds it to `$input` leaves
- `--input` is a **file** → used directly as seed
- neither → `$input` leaves get empty stdin

See [writing pipelines](writing-pipelines.md) and [session artefacts](sessions.md).

---

## `journal`

Rebuild `<session>/journal.json` by scanning trace + error files on disk. Useful after a killed run.

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

Reads `<session>/control.addr`, connects (named pipe on Windows, UDS on Unix — never TCP), sends `{"cmd":"status"}`, prints the response as pretty JSON:

```json
{
  "ok": true,
  "status": {
    "pipeline": "my-pipeline",
    "variant":  "main",
    "session":  "20260420-...",
    "state":    "running",
    "started_at": 1776716448568,
    "stages": [
      { "sid": "scan", "tool": "scan-fs",       "state": "running", "rows": 42, "errors": 0, "replicas": 1 },
      { "sid": "read", "tool": "read-file-stream","state": "running", "rows": 12, "errors": 0, "replicas": 1 }
    ]
  }
}
```

Fails cleanly (exit 1) if `control.addr` is missing or the server isn't reachable — session probably already finished.

---

## `progress`

Same idea, but queries `progress` instead of `status` — reports gate progress + roll-up totals:

```sh
dpe progress /path/to/sessions/<id>_<variant>
```

Returns gates from `<session>/gates/*.json` (if any) + total rows + total errors seen across all stages.

---

## `stop`

Request a graceful stop on a live session.

```sh
dpe stop /path/to/sessions/<id>_<variant>
```

Sends `{"cmd":"stop"}` to the runner. The server acknowledges and the runner begins draining: stop feeding leaves, let stages process what they have, flush, exit. (Actual pause / resume hooks on the DAG side are reserved — MVP stop is implemented as "close the CLI side and let the run complete".)

---

## `logs`

Tail `<session>/log.ndjson` with formatted output.

```sh
dpe logs /path/to/sessions/<id>_<variant>           # print what's there and exit
dpe logs /path/to/sessions/<id>_<variant> --follow   # -f: tail as new lines append
```

Formats each JSON record as `[stage] level: msg`:

```
[scan] info: opened /path/to/dir
[parse] warn: DOCX missing table row 5
```

---

## `monitor`

Live ratatui TUI dashboard.

```sh
dpe monitor /path/to/sessions/<id>_<variant>
```

Three tabs:
- **Stages (1)** — table of every stage: name, tool, state, rows, errors, replicas; red for stages with errors
- **Pipeline (2)** — overall state, elapsed time, per-gate progress bars (green = predicate_met)
- **Logs (3)** — tail of `<session>/log.ndjson` live

Keys: `q` / `Esc` quit, `Tab` cycles tabs, `1` / `2` / `3` jumps directly.

Polls the control socket every 500 ms; falls back to reading `journal.json` on disk if the session has already exited (shows *"(stale — server unreachable)"* in the header).

---

## Exit codes (all commands)

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Generic error — see stderr |
| 2 | Invalid settings / bad argv |
| other | Runtime error from a specific subsystem |

---

## Environment variables

Read by the CLI in addition to the ones the runner / tools read:

| Variable | Purpose |
|---|---|
| `DPE_CONFIG` | override default runner config path |

Tool-side env vars (`ANTHROPIC_API_KEY`, etc.) are NOT read by the CLI itself — they're inherited by spawned tools as normal shell vars.

---

## Pattern: end-to-end dev workflow

```sh
# 1. Always validate first
dpe check --all my-pipeline

# 2. Load API keys for tools that need them
set -a && source .env && set +a

# 3. Dry-run one variant
dpe run my-pipeline:main -i inputs -o outputs

# 4. Look at the session artefacts
SESSION=$(ls -td my-pipeline/sessions/*_main | head -1)
dpe logs "$SESSION"
dpe journal "$SESSION"   # rebuild/show summary

# 5. Or run the TUI from another shell while #3 executes
dpe monitor "$SESSION"
```

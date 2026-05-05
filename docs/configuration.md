# Configuration

`dpe` reads a single `config.toml`. Resolution order (first that exists wins):

1. `--config <path>` — CLI flag
2. `DPE_CONFIG` — env var pointing at a file
3. `<cwd>/config.toml` — pipeline-local override (auto-picks when running from a pipeline dir; **added v2.0.1**)
4. `~/.dpe/config.toml` — standard install
5. `<dpe-binary-dir>/config.toml` — portable installs (e.g. USB sticks, CI sandboxes)
6. Built-in defaults if none of the above exist

Everything is optional. An empty file yields the same behavior as no file.

## Schema

```toml
# Directories searched, in order, for tool meta.json files.
tools_paths = ["~/.dpe/tools"]

# Where `dpe install <name>` places new tools. Defaults to ~/.dpe/tools
default_install_path = "~/.dpe/tools"

# Overrides for runner transport (advanced — leave empty by default)
control_pipe = ""
logger_pipe  = ""

[trace]
max_events             = 10000      # in-memory buffer cap
flush_ms               = 1000       # periodic flush of trace.N.ndjson
max_segment_bytes      = 268435456  # 256 MiB rotation
max_labels_per_record  = 10
max_labels_chars_total = 1000
channel_capacity       = 100000     # mpsc channel size; overflow drops events

# Shared session log writer (`$session/log.ndjson`). Added v2.0.2.
[log_sink]
flush_ms          = 250             # periodic BufWriter flush — keeps `dpe log --follow` responsive
channel_capacity  = 4096            # mpsc channel size
tail_default      = 50              # default --tail N for `dpe log <session>`

[cache]
default_mode = "use"         # use | refresh | bypass | off
shard_depth  = 2

[spawn]
max_restarts     = 3
restart_backoff_ms = [500, 2000, 5000]
sigterm_grace_ms = 10000

[lifecycle]
recommended_max_sessions = 50

# Internal runtime tuning. Most users never touch this. Added v2.0.1+.
[runtime]
journal_flush_ms     = 2000        # how often `journal.json` is flushed during a run
control_channel_cap  = 32          # control-command channel size
duplex_buf_bytes     = 65536       # in-process tokio::io::duplex bridge buffer
monitor_poll_ms      = 500         # `dpe monitor` + `dpe log --follow` poll cadence
http_timeout_secs    = 120         # `dpe install` HTTP timeout

# Dev-specific — consumed by dpe-dev, ignored by dpe runner
[dev]
workspace       = "~/.dpe/dev-workspace"
frameworks_cache = "~/.dpe/frameworks"

# Tool registries — used by `dpe install` + `dpe tools list`. List paths to
# `catalog.json`-shaped files. Files load in order and merge with
# first-match-wins on tool name (so a private registry listed first shadows a
# default one). Missing files emit a warning, never an error.
# When this list is empty, dpe falls back to <binary_dir>/catalog.json
# if it exists.
tools_registries = [
    "~/.dpe/my-tools.json",
    "/etc/dpe/company.json",
    "/opt/dpe/bin/catalog.json",     # default, ships with installer
]
```

## ENV-var overrides

Every numeric knob in `[trace]`, `[log_sink]`, and `[runtime]` is
ENV-overridable for deploy-time tuning without editing config.toml:

| Var | Overrides |
|---|---|
| `DPE_TRACE_FLUSH_MS`, `DPE_TRACE_MAX_EVENTS`, `DPE_TRACE_MAX_SEGMENT_BYTES`, `DPE_TRACE_MAX_LABELS`, `DPE_TRACE_MAX_LABEL_CHARS`, `DPE_TRACE_CHANNEL_CAPACITY` | `[trace]` knobs |
| `DPE_LOG_SINK_FLUSH_MS`, `DPE_LOG_SINK_CHANNEL_CAPACITY`, `DPE_LOG_TAIL_DEFAULT` | `[log_sink]` knobs |
| `DPE_JOURNAL_FLUSH_MS`, `DPE_MONITOR_POLL_MS`, `DPE_DUPLEX_BUF_BYTES`, `DPE_HTTP_TIMEOUT_SECS`, `DPE_CONTROL_CHANNEL_CAP` | `[runtime]` knobs |

ENV is applied AFTER config-file load, so it takes priority over file
values. Floors enforced via `effective_*()` accessors so a hostile or
typoed ENV can't pin to pathological values.

## Inspecting

```bash
dpe config show          # print the effective resolved config
dpe config path          # print which file dpe would read
dpe tools list           # list tools + which paths it searched
```

## Editing

```bash
# Register an additional tools directory
dpe config add-path /path/to/my-tools

# Or edit manually
$EDITOR ~/.dpe/config.toml
```

`dpe config add-path` appends to `tools_paths[]` and writes the file back. Safe to run repeatedly — duplicates are detected and skipped.

## Path expansion

- `~/...` expands to user home (equivalent to `$HOME/...` / `%USERPROFILE%\...`).
- `$DPE_INPUT` / `$DPE_OUTPUT` / `$DPE_SESSION` / etc. — runner-provided env vars resolved at pipeline runtime. See the runner SPEC for the full list.

## Per-pipeline config

Pipelines don't read config.toml directly. They use:
- `settings: {...}` per stage in variant YAML — the tool-specific config
- Pipeline-local `tools/<name>/meta.json` — overrides the globally-installed tool for just this pipeline

For pipeline-local tools, `entry:` can be an absolute path to a built binary. See [Authoring a tool](authoring-a-tool.md).

## Env vars at runtime

The runner injects these into every spawned tool's env:

| Var | Meaning |
|---|---|
| `DPE_PIPELINE_DIR` | Absolute path to the pipeline folder |
| `DPE_PIPELINE_NAME` | Pipeline basename |
| `DPE_VARIANT` | Variant name |
| `DPE_SESSION_ID` | `YYYYMMDD-HHMMSS-xxxx` |
| `DPE_SESSION` | Absolute path to the session dir |
| `DPE_STAGE_ID` | This stage's name |
| `DPE_STAGE_INSTANCE` | Replica index (0-based) |
| `DPE_INPUT` / `DPE_OUTPUT` / `DPE_TEMP` / `DPE_STORAGE` | Directory paths |
| `DPE_CONFIGS` | Pipeline's `configs/` dir |
| `DPE_CACHE_MODE` | `use` / `refresh` / `bypass` / `off` |

Tools should read these rather than hardcoding anything. Runners in containers set them based on volume mounts.

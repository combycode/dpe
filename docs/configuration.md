# Configuration

`dpe` reads a single `config.toml`. Resolution order (first that exists wins):

1. `--config <path>` — CLI flag
2. `DPE_CONFIG` — env var pointing at a file
3. `<dpe-binary-dir>/config.toml` — portable installs (e.g. USB sticks, CI sandboxes)
4. `~/.dpe/config.toml` — standard install
5. Built-in defaults if none of the above exist

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
max_events             = 10000
flush_ms               = 1000
max_segment_bytes      = 268435456
max_labels_per_record  = 10
max_labels_chars_total = 1000

[cache]
default_mode = "use"         # use | refresh | bypass | off
shard_depth  = 2

[spawn]
max_restarts     = 3
restart_backoff_ms = [500, 2000, 5000]
sigterm_grace_ms = 10000

[lifecycle]
recommended_max_sessions = 50

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

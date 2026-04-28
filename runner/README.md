# dpe

High-throughput streaming pipeline runner. Spawns tools (Rust, Python, Bun) as child processes, routes NDJSON envelopes between them over OS pipes, and coordinates the whole pipeline from a declarative YAML graph.

## Install

```bash
npm install -g dpe
```

Bundles 7 standard tools (`scan-fs`, `read-file-stream`, `write-file-stream`, `write-file-stream-hashed`, `normalize`, `gate`, `checkpoint`). The right native binary for your OS + CPU is selected automatically via `optionalDependencies`.

Other install paths:

```bash
docker pull ghcr.io/combycode/dpe-base:latest      # ready-to-run runtime image
docker pull ghcr.io/combycode/dpe-dev:latest       # adds Rust toolchain + Claude CLI for in-container tool authoring
```

## Quick start

```bash
dpe init my-pipeline
cd my-pipeline
dpe run .:main -i data/input -o data/output
```

## Commands

| Command | Purpose |
|---|---|
| `dpe init <name>` | scaffold a new pipeline directory |
| `dpe run <pipeline>:<variant>` | execute a pipeline variant |
| `dpe check <pipeline>:<variant>` | parse + validate without running |
| `dpe tools list` | show installed tools and their resolution paths |
| `dpe install <name>` | install a custom tool from the registered catalog |
| `dpe status <session>` | live status of a running session |
| `dpe progress <session>` | gate progress + roll-up totals |
| `dpe stop <session>` | request a graceful stop |
| `dpe logs <session>` | tail session log (`-f` to follow) |
| `dpe monitor <session>` | TUI dashboard |
| `dpe journal <session>` | rebuild journal after abnormal termination |
| `dpe config` | show / edit `~/.dpe/config.toml` |

`dpe --help` for full reference.

## Documentation

Full docs at <https://github.com/combycode/dpe> — concepts, CLI reference, tool authoring, framework SDKs, expressions, sessions, Docker images.

## License

AGPL-3.0-or-later. Commercial licensing available — contact CombyCode.

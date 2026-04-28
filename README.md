# DPE — Data Processing Engine

High-throughput streaming pipeline runner with a curated toolset. Rust runner spawns tools (Rust / Bun / Python) as child processes, wires NDJSON envelopes over OS pipes, and coordinates the whole pipeline from a declarative YAML graph.

[AGPL-3.0-or-later](./LICENSE) · v2.0.0

## Quick start

```bash
# Install
npm install -g dpe                                  # runner + 7 bundled standard tools
docker pull ghcr.io/combycode/dpe-base:latest       # or via Docker

# Scaffold + run
dpe init my-pipeline
cd my-pipeline
dpe run .:main -i data/input -o data/output

# Inspect
dpe tools list
dpe --help
```

## Standard tools

Bundled with `npm install -g dpe` and built into the `dpe-base` Docker image. Each can also be installed standalone via `dpe install <name>`.

| Tool | What it does | Docs |
|---|---|---|
| `scan-fs` | Walk a directory tree, emit one envelope per file/dir; optional content hashing; `diff` mode for change detection | [scan-fs](docs/tools/scan-fs.md) |
| `read-file-stream` | Stream rows from a text file: NDJSON, plain lines, CSV (with optional header → object mapping) | [read-file-stream](docs/tools/read-file-stream.md) |
| `write-file-stream` | Append envelopes to files; LRU-bounded handle pool, periodic flush, automatic mkdir | [write-file-stream](docs/tools/write-file-stream.md) |
| `write-file-stream-hashed` | Same as `write-file-stream` plus per-file content deduplication for idempotent reruns | [write-file-stream-hashed](docs/tools/write-file-stream-hashed.md) |
| `normalize` | Row-level normaliser: dict, parse, rename, compute, template, require — with profile dispatch for heterogeneous input | [normalize](docs/tools/normalize.md) |
| `gate` | Stateful pass-through that publishes progress to `$session/gates/<name>.json` for downstream coordination | [gate](docs/tools/gate.md) |
| `checkpoint` | Buffer stdin to disk; release downstream only after named gates report `predicate_met` | [checkpoint](docs/tools/checkpoint.md) |

In-runner built-ins (no child process): [`route` · `filter` · `dedup` · `group-by`](docs/tools/builtins.md).

## Documentation

- [Index](docs/README.md) — start here
- [Installing](docs/installing.md) — npm, install script, Docker
- [Concepts](docs/concepts.md) — envelopes, stages, DAG topology
- [Writing pipelines](docs/writing-pipelines.md) — YAML variants, fan-in/out, route, filter, replicas, dedup
- [CLI reference](docs/cli.md) — `init / run / check / install / tools / status / monitor`
- [Authoring a tool](docs/authoring-a-tool.md) — scaffold → build → test → verify
- [Frameworks](docs/frameworks.md) — Rust, TypeScript / Bun, Python SDK reference
- [Expressions](docs/expressions.md) — DSL for `route` / `filter` predicates
- [Path prefixes](docs/path-prefixes.md) — `$input`, `$output`, `$session`, etc.
- [Sessions](docs/sessions.md) — what lands in `sessions/<id>/`
- [Configuration](docs/configuration.md) — `~/.dpe/config.toml`
- [Docker](docs/docker.md) — `dpe-base` + `dpe-dev` images

## Test pipeline

`test-pipeline/` ships a regression suite that exercises every standard tool against synthesised inputs. See [test-pipeline/README.md](test-pipeline/README.md) for layout and how to run it locally.

## Frameworks

| Language | Package | Source |
|---|---|---|
| Rust | [`combycode-dpe`](https://crates.io/crates/combycode-dpe) | [frameworks/rust/](frameworks/rust/) |
| TypeScript / Bun | [`@combycode/dpe-framework-ts`](https://npmjs.com/package/@combycode/dpe-framework-ts) | [frameworks/ts/](frameworks/ts/) |
| Python | [`combycode-dpe`](https://pypi.org/project/combycode-dpe/) | [frameworks/python/](frameworks/python/) |

## Monorepo layout

| Component | Path | Purpose |
|---|---|---|
| Runner + CLI | `runner/` | the `dpe` binary |
| Tool-authoring CLI | `dpe-dev/` | `scaffold / build / test / verify / setup` |
| Frameworks | `frameworks/{rust,ts,python}/` | language SDKs for writing tools |
| Standard tools | `tools/<name>/` | the 7 shipped tools |
| Docker | `docker/` | multi-stage base + dev images |
| Test pipeline | `test-pipeline/` | end-to-end regression suite |
| Catalog | `catalog.json` | tool registry consumed by `dpe install` |
| Schemas | `runner/schemas/` | JSON Schemas for IDE validation of pipeline files, runner config, tool meta, stderr events |

## License

Copyright © 2026 CombyCode Inc.

Licensed under the **GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later)** — see [LICENSE](./LICENSE). You are free to use, modify, and redistribute DPE; if you make modifications and distribute them or expose them as a network service, you must share those modifications under the same license.

Commercial licensing is available for cases where AGPL terms don't fit — contact CombyCode.

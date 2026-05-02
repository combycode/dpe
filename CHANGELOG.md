# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

<!-- Add entries under the section that fits: BREAKING / Added / Changed / Deprecated / Removed / Fixed / Security. Keep them short — full context belongs in the PR description. -->

## [2.0.1] — 2026-05-01

Patch release — bug fixes and small CLI ergonomics around the v2.0.0 surface.
No breaking changes; existing pipelines and config files work unchanged.

### Added

- **`dpe run --temp-dir <path>`** and **`--storage-dir <path>`** — redirect `$temp` and `$storage` away from the pipeline dir for a single run. Use to isolate scratch + persistent state across concurrent runs of the same pipeline. (`SessionContext.temp_override` / `storage_override` plumbed end-to-end including DPE_TEMP / DPE_STORAGE env vars for spawned tools.)
- **`dpe run --seed <ndjson>`** and **`--seed-file <path>`** — inject envelopes as the run's first input without modifying the pipeline. `--seed` writes to `<session>/_seed.ndjson` then feeds it; `--seed-file` reads the file directly. Mutually exclusive.
- **`dpe tools list --json`** — JSON output for the merged tool catalog. Includes catalog entries plus path-discovered tools (any directory under `tools_paths` that has a `meta.json`+`spec.yaml` pair, even without a catalog entry — surfaced as `tier: "external"`). Bundles each tool's spec.yaml `settings` JSON Schema and `output.description`. Lets editors and other tooling consumers replace bespoke catalog parsers with one CLI call.
- **`dpe config init`** — bootstrap `~/.dpe/config.toml` + `~/.dpe/tools/` + `~/.dpe/registries/` with sensible defaults. Idempotent; refuses to clobber existing config without `--force`.
- **`dpe-dev --config <path>`** + `DPE_CONFIG` env support — global flag that mirrors the runner's resolution chain. `dpe-dev setup` now writes the workspace registration to the resolved config (override or default).
- **BOM-tolerant JSON / YAML readers** in both `dpe` and `dpe-dev`. Shared `bom::strip_bom` helper drops a leading UTF-8 BOM (`EF BB BF`) before parsing — Windows editors that save "UTF-8" files with BOM no longer trip "expected value at line 1 column 1". Applies to `meta.json`, `spec.yaml`, `catalog.json`, variant files, and `dpe-dev verify` settings.

### Changed

- **Config resolution order** — `<cwd>/config.toml` now takes priority over `~/.dpe/config.toml` (was last); `<binary-dir>/config.toml` moved from #3 to #5. Pipeline-local config wins automatically when `dpe ...` runs from inside the pipeline dir, removing `--config <pipeline>/config.toml` boilerplate. New order: `--config` flag → `DPE_CONFIG` env → `<cwd>/config.toml` → `~/.dpe/config.toml` → `<bin>/config.toml` → defaults.
- **`dpe config path`** always prints a path. Returns the would-be canonical path (`~/.dpe/config.toml`) when nothing exists yet, so users know where to create one. Previously could print empty.
- **`dpe check --plan`** emits pure JSON to stdout (the `[OK]` banner moved to stderr in `--plan` mode). `dpe check :v --plan | jq` just works. Without `--plan` the human-readable `[OK]` line stays on stdout.
- **npm wrapper auto-config** is now write-once, not write-every-invocation. The shim writes `<pkg>/bin/config.toml` only on first run or when its first line still matches the auto-generation marker. Manual edits to that file (or removing the marker line) make it untouchable forever.
- **`dpe init`** scaffold creates `storage/` (with `.gitkeep`) — pipelines that reference `$storage/...` no longer fail on first use.

### Fixed

- **`dpe --version`** + **`dpe-dev --version`** now report the binary version. v2.0.0 shipped without `#[command(version)]`; this restores the standard clap-derive flag and resolves the stale `2.0.0-rc1` string in dpe-dev.
- **`route`** stage's `routes` was a `BTreeMap` and alphabetized YAML declaration order, breaking the documented "first-truthy-channel-wins, in declaration order" semantics. Swapped to `IndexMap`. Regression test added.
- **dpe-base / dpe-dev Docker images** baked `DPE_STORAGE`/`DPE_TEMP`/`DPE_INPUT`/`DPE_OUTPUT` into the image's `ENV` layer pointing at `/workspace/{...}` — overlay-only paths that vanished on `--rm`. Removed the `ENV` block; the runtime `SessionContext` is the sole source of truth.

## [2.0.0] — 2026-04-28

### BREAKING

- **Complete rewrite from v1.4.** No API or configuration compatibility with v1.x. The v1.x line is fully **deprecated** and receives no further updates. There is no automated migration path; users on v1.x should treat v2.0.0 as a new product.

### Added

- **Streaming pipeline runner.** Single `dpe` binary spawns tools as child processes; OS pipes carry NDJSON envelopes between stages with kernel-managed backpressure.
- **Multi-language tool framework.** Write tools in Rust ([`combycode-dpe`](https://crates.io/crates/combycode-dpe)), TypeScript / Bun ([`@combycode/dpe-framework-ts`](https://www.npmjs.com/package/@combycode/dpe-framework-ts)), or Python ([`combycode-dpe`](https://pypi.org/project/combycode-dpe/)).
- **Seven bundled standard tools** — `scan-fs`, `read-file-stream`, `write-file-stream`, `write-file-stream-hashed`, `normalize`, `gate`, `checkpoint`. Shipped with `npm install -g dpe` and inside the `dpe-base` Docker image.
- **In-runner built-in DAG processors.** `route`, `filter`, `dedup`, `group-by` — no child process, zero per-row spawn overhead.
- **Declarative pipeline variants.** YAML or JSON, with inheritance and override merging. `dpe check` validates without running.
- **Expression DSL.** Compiled-once predicate language used by `route` and `filter` stages — typed accumulator vocab, no general scripting host in the hot path.
- **Session journal + control plane.** Per-run `sessions/<id>/` artefacts, live status / progress / stop via local socket (UDS on Unix, named pipe on Windows — never TCP), and a TUI dashboard (`dpe monitor`).
- **Tool-authoring CLI** (`dpe-dev`). Scaffold, build, test, verify — runtime-aware, no per-language plumbing in your hands.
- **Cross-platform.** Linux x64 / arm64, macOS x64 / arm64, Windows x64 — single npm install resolves the right native binary; Docker images for Linux x64.
- **Distribution.** Wrapper on npm (`dpe`, `dpe-dev`), framework SDKs on crates.io / PyPI / npm, runtime images on `ghcr.io/combycode/dpe-base` and `:dpe-dev`, per-tool tarballs on GitHub Releases for `dpe install <name>`.
- **OIDC-based publishing.** All registries publish via Trusted Publishing — no long-lived tokens stored in CI.

### Deprecated

- **All v1.x.** `combycode/dpe` v1.x is end-of-life as of v2.0.0. No security backports.

[Unreleased]: https://github.com/combycode/dpe/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/combycode/dpe/releases/tag/v2.0.0

# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

<!-- Add entries under the section that fits: BREAKING / Added / Changed / Deprecated / Removed / Fixed / Security. Keep them short — full context belongs in the PR description. -->

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

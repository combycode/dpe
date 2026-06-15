# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

<!-- Add entries under the section that fits: BREAKING / Added / Changed / Deprecated / Removed / Fixed / Security. Keep them short — full context belongs in the PR description. -->

## [2.0.3] — 2026-06-16

Patch release — three additive fixes against intermittent silent data loss / mis-routing. No breaking changes; every existing pipeline keeps its current behaviour modulo the bug paths described below.

### Fixed

- **`route` builtin: `OnError::Drop` now continues to the next rule** instead of returning `None`. The previous behaviour swallowed envelopes whose first rule errored on a missing field, defeating any catch-all `"true"` pattern. With this fix, a missing-field error on rule A lets rule B (or a later catch-all) still claim the envelope; `OnError::Fail` semantics unchanged. Inbox 0044 closeout. Tests: `dag_route_drop_continues_to_catchall_on_missing_field` + `dag_route_drop_with_no_catchall_still_drops` in `runner/tests/e2e_dag.rs`.
- **`env_interp`: nested fallback `${VAR:-${INNER}}` parses correctly.** Previously the closing `}` was found by a flat first-`}`-wins scan, which silently truncated any default clause containing a nested `${...}`. New `find_closing_brace` helper tracks brace depth; the default clause is recursively interpolated so chains like `${A:-${B:-${C}}}` resolve through. Errors at the terminal-unset case name the inner variable, not the outer. Five unit tests in `runner/src/env_interp.rs` cover nested-resolve, outer-overrides, deeply-nested chain, terminal-unset, and unbalanced-brace.
- **`frameworks/ts`: explicit stdout drain on `run()` exit** — `process.stdout.write('', cb)` + 50ms `setTimeout` before `run()` returns. Workaround for a Bun-on-Windows behaviour where the JS-level stdout buffer can be lost when the process exits within milliseconds of the last `ctx.output()`. Reproduced at ~30% rate in the finalyst `recon-extract-col-meta → recon-normalize-cols` hand-off (inbox 0045); fixed at 0/30 with this change. The bare callback alone was insufficient (~20% rate persisted); the 50ms grace is what truly settles the OS pipe. Tax: 50ms per tool exit, ~600ms per finalyst convert run (~1% of LLM-bound wall-clock). Same family as the inbox 0041 (main()-must-await-run) Bun-on-Windows class of issues.

## [2.0.2] — 2026-05-19

Feature release — adds a per-stage snapshot test runner (`dpe test` / `dpe coverage`), a `$path` resolver across all three SDKs, and broader observability + ergonomics improvements around stages, settings, and tools. No breaking changes to existing pipelines.

### Added

- **`dpe test`** — per-stage snapshot test runner. Spawns ONE stage in isolation (no DAG), pipes `tests/<variant>/<stage>/<case>/input/seed.ndjson` to its stdin, captures every output channel (`data` / `meta` / `errors` / `logs` / `trace` / `stats`), diffs against committed `expected/` after canonicalising (strip envelope `id` + `src`, sort JSON keys). Target syntax: `[<pipeline>:]<variant>:<stage>[:<case>]`. Bulk runs (`<variant>:<stage>` without a case) execute every `case-*` under the stage. Flags: `--update` (rewrite expected from actual), `--update-if-missing` (write only when expected doesn't exist), `--cache <use|refresh|bypass|off>` (override per-case `cache:` in `test.yaml`). Exit 0 = PASS, 1 = FAIL, 2 = invocation error.
- **`dpe test` declarative compare engine** — `test.yaml` `compare:` block controls how the diff runs. Per-channel and global matchers across seven kinds (`literal`, `glob`, `regex`, `prefix`, `suffix`, `contains`, `not_contains`) with token expansion (`$session`, `$cache_hash`, etc.); strict-channel asymmetry — silent leak detection on data/meta/errors, opt-in for logs/trace/stats; fs-tree compare across six file modes (`schema`, `fuzzy`, `contains`, `exists`, `bytes-equal`, `text-equal`); assert scripts (`python` / `bun` / `node`) resolved via `which::which`, invoked with 16 `DPE_*` env vars + exit-code protocol. Empty / missing `compare:` falls through to defaults + auto-detects which channels were produced. `--update` is scoped to data/meta/errors unless `compare.channels` opts in further.
- **`dpe test` multi-phase orchestration** — a single `test.yaml` may declare multiple phases that share a `.run/` state directory. Only `actual/` is wiped between phases; `output/`, `temp/`, `storage/`, `session/` persist. Enables cache-flow tests, batch-resume tests, idempotency tests. Phase composition rules in `docs/testing.md`.
- **`dpe test` builtin driver** — in-process testing for `filter`, `route`, `group_by`, `dedup` against seed input. Captures channel files via a synthetic writer. `spread` / `toggle` are marked `test_skipped: true` in synthetic builtin meta. `route` output is combined into a single annotated `data.ndjson` with a `_route_channel` field per envelope so cases can assert routing decisions.
- **`dpe coverage`** — per-variant × stage matrix of snapshot-test coverage. Five buckets: `covered`, `skip_list`, `test_skipped`, `exclusive_covered`, `exclusive_uncovered`, `uncovered`. `--json` mode for tooling consumption. Lets pipeline authors see at a glance which stages are still untested.
- **`dpe run --env-file <PATH>`** (global flag, applies to every subcommand) — load env vars from `.env`-style files before running. Repeatable; FIRST occurrence of a key wins; existing process env always overrides (CI secrets stay authoritative). The path must exist — no silent CWD pickup. Loaded once at startup.
- **`$paths` resolver framework module** — new `paths` namespace exposed in the Rust, TypeScript, and Python SDKs (`combycode_dpe::paths`, `@combycode/dpe-framework-ts/paths`, `dpe.paths`). Tools can resolve the same `$pipeline` / `$session` / `$temp` / `$storage` / `$cache` / `$input` / `$output` / `$configs` tokens the runner uses, with parity across all three languages. Replaces hand-rolled per-tool env-var parsing.
- **`scan-fs` `passthrough_input`** — when true, every field on the input envelope's `v` is copied onto every emitted entry. Reserved keys (scan-fs's own fields: `path` in full mode; canonical record fields `kind`/`root`/`directory`/`filename`/`ext`/`size`/`created`/`changed`/`hash` in diff mode) take precedence. Attach upstream tags (e.g. `category`, `batch_tag`) without a downstream `normalize` step.
- **`scan-fs` `min_size` / `max_size`** — skip files outside a byte-size range. `null` for either disables the bound. Dirs unaffected.
- **`read-file-stream` `passthrough_input`** — same semantics as scan-fs: copy input envelope's `v` fields onto every emitted row envelope. Reserved tool fields (`file`, `row_idx`, `row`) always win. Caller's top-level values take precedence over same-named fields inside `row.v` (which remain nested in `v.row`). Carry classification / metadata from upstream tools (`read-tables`, `classify`) onto every row without a separate normalize merge.
- **`docs/testing.md`** — comprehensive guide to the snapshot test runner: layout, channel classification, full `test.yaml` schema, the four-step compare engine (channel shape → fs tree → per-channel envelope diff → assert script), multi-phase composition, authoring workflow, skip-list semantics.

### Changed

- **`scan-fs` default `hash`** changed from `"none"` to `"xxhash"`. Most pipelines hash for downstream identity anyway; the default now matches the common case. Pipelines that explicitly want no hash can still set `hash: "none"`.
- **`read-file-stream` `csv_delim` validation** — now rejects multi-byte UTF-8 delimiters at startup with a clear error. Previously the code silently took the first byte and discarded the rest, producing a delimiter that matched nothing the user wrote.
- **runner: error-on-error propagation** — stages that fail with `ctx.error(...)` now surface their input record + stage name to the journal more consistently. Internal cleanup in `runner/src/dag/plan.rs` and `runner/src/main.rs`.

### Fixed

- **runner: multi-source fan-in into a single consumer no longer fails with `stdin already taken`.** Refactored `wire_stage_input` so an `input:` list mixing route-channel refs (`router.foo`) with plain stage refs, or containing multiple route-channel refs, builds one merged duplex bridge per consumer and claims the consumer's stdin exactly once. The historic failure mode affected any tool whose `input:` block had more than one route-channel ref or mixed route + plain refs. Pure-plain multi-input fan-in (`input: [a, b]`) was already supported; this fix extends it to all source-kind combinations. Two new regression tests in `runner/tests/e2e_dag.rs` cover the previously-broken shapes.
- **runner: env-interp + `$path` substitution** — `runner/src/env_interp.rs` and `runner/src/paths.rs` cleanups. `$session` / `$temp` paths now resolve consistently between runner and SDKs via the shared `paths` module.
- **runner: token boundary handling in NDJSON envelopes** — new `runner/tests/token_boundary.rs` proves the runner correctly handles split-line / partial-token edge cases.
- **runner: validation diagnostics** — `runner/src/validate/resolve.rs` improvements surface clearer messages for unresolved settings refs and missing tools.
- **`normalize` tool: `${...}` rejection bytes assertion** — internal test fixture cleanup; no user-visible behaviour change.



Patch release — bug fixes and small CLI ergonomics around the v2.0.0 surface.
No breaking changes; existing pipelines and config files work unchanged.

### Added

- **`spread` builtin** — broadcast every envelope from a single upstream to all downstream consumers (1→N tee). Topology defines the fan-out; no settings, no expressions. Exempted from the single-consumer validation check the same way `route` is.
- **`toggle` builtin** — env-gated 1→1 passthrough. Settings: `env`, `value` / `values`, `mode: on | off`. Decision is taken at plan-compile time from the env source — per envelope cost is byte-copy (pass) or constant-time skip (drop). Lets you turn whole branches on/off per run without copying the variant.
- **`dpe check --allow-undefined-env`** — treat unset `${VAR}` references as empty string instead of erroring at validation. For editor-time check flows where the runtime env isn't known yet. `dpe run` still requires every referenced env var to be set.
- **Per-stage `cache:` override** — stage-level setting overrides the run-level `--cache` flag and `[cache]` config. Use to mark specific stages (e.g. fast deterministic transforms) as `cache: off` or `cache: bypass` while the rest of the pipeline caches normally.
- **`dpe tools list` parity with `--json`** — the human-readable text output now also shows path-discovered tools (any `meta.json` under a `tools_paths` entry) and built-in stages. Path-discovered tools surface as `tier: pipeline-local` if their parent dir contains `pipeline.toml`, else `tier: external`. Built-ins (`route`, `spread`, `filter`, `dedup`, `group_by`, `toggle`) print in their own section.
- **Framework `ctx.cached(namespace, key, produce)` helper** in all three SDKs (Rust / TypeScript / Python). Memoise expensive per-envelope work (LLM calls, image rendering, large parses) by content key. Honours `DPE_CACHE_MODE` (`use` / `refresh` / `bypass` / `off`). Cache files live under `$DPE_STORAGE/<namespace>/<32-hex-hash>.json`.
- **Per-stage `accept_meta: true` setting** in the framework input loop. By default, tools' `process_input` only sees `t:"d"` envelopes — meta envelopes (`t:"m"`) are skipped at the framework's read loop. Set this flag on the stage's settings to deliver meta envelopes alongside data envelopes (e.g. for sinks that should write per-batch summaries to disk). Available across all three SDKs.
- **`dpe run --temp-dir <path>`** and **`--storage-dir <path>`** — redirect `$temp` and `$storage` away from the pipeline dir for a single run. Use to isolate scratch + persistent state across concurrent runs of the same pipeline. (`SessionContext.temp_override` / `storage_override` plumbed end-to-end including DPE_TEMP / DPE_STORAGE env vars for spawned tools.)
- **`dpe run --seed <ndjson>`** and **`--seed-file <path>`** — inject envelopes as the run's first input without modifying the pipeline. `--seed` writes to `<session>/_seed.ndjson` then feeds it; `--seed-file` reads the file directly. Mutually exclusive.
- **`dpe tools list --json`** — JSON output for the merged tool catalog. Includes catalog entries plus path-discovered tools (any directory under `tools_paths` that has a `meta.json`+`spec.yaml` pair, even without a catalog entry — surfaced as `tier: "external"`). Bundles each tool's spec.yaml `settings` JSON Schema and `output.description`. Lets editors and other tooling consumers replace bespoke catalog parsers with one CLI call.
- **`dpe config init`** — bootstrap `~/.dpe/config.toml` + `~/.dpe/tools/` + `~/.dpe/registries/` with sensible defaults. Idempotent; refuses to clobber existing config without `--force`.
- **`dpe-dev --config <path>`** + `DPE_CONFIG` env support — global flag that mirrors the runner's resolution chain. `dpe-dev setup` now writes the workspace registration to the resolved config (override or default).
- **BOM-tolerant JSON / YAML readers** in both `dpe` and `dpe-dev`. Shared `bom::strip_bom` helper drops a leading UTF-8 BOM (`EF BB BF`) before parsing — Windows editors that save "UTF-8" files with BOM no longer trip "expected value at line 1 column 1". Applies to `meta.json`, `spec.yaml`, `catalog.json`, variant files, and `dpe-dev verify` settings.

- **`normalize` tool inline rules / profiles** — settings now accept exactly one of `rulebook: <path>` (existing — external file), `rules: [...]` (new — synthesises a single always-on profile), or `profiles: [...]` (new — full multi-profile rulebook). Settings-level `${VAR}` env-interp + `$prefix` path substitution apply automatically when rules are inline; external rulebook files load fresh from disk and stay stable artefacts.
- **`normalize` template op** rejects literal `${` at compile time with a fix-pointing message. The `${VAR}` env-interp syntax visually collides with the template op's own `{name}` placeholder syntax — the loud error stops users from writing settings that silently render to `batch_$/raw/{hash}/`.
- **`write-file-stream` and `write-file-stream-hashed` `pass_through: true`** — when set, after a successful disk write the input envelope is forwarded unchanged via `ctx.output`, so downstream stages can chain off the same stream without an upstream `spread` builtin. Default `false` preserves the existing terminal-sink behaviour. Spec.yaml and module docs updated to reflect the two-mode contract.

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
- **Expression DSL** string literals now preserve UTF-8 multi-byte characters. The lexer previously read the source byte-by-byte and cast each byte to `char`, mangling Cyrillic / emoji / other non-ASCII content into mojibake. `includes(lower(v.x), 'файло')` and similar predicates with non-ASCII literals now match correctly.
- **Multi-input fan-in** (`input: [a, b, c]` to a non-builtin tool) no longer deadlocks when upstream branches have different throughputs. The previous implementation drained readers sequentially with `read_to_end`, so a slow source became a barrier the fast sources sat behind. Replaced with the same interleaved-drain pattern used by replicas fan-in (one tokio task per reader, mpsc channel, write loop forwards each line as it arrives).
- **Filter / route expression validation** with env-var references (`${VAR}`) is now performed AFTER env interpolation on both the validate path and the plan-compile path. Previously the plan-time pass interpolated and the validate pass did not, so `dpe check` failed on legal expressions like `includes(v.x, '${YEAR}')` while `dpe run` succeeded.

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

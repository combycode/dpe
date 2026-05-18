# Testing pipelines

DPE ships two test commands:

| Command | Scope | Purpose |
|---|---|---|
| [`dpe test`](#dpe-test) | One stage | Snapshot test — feed seed input, capture every output channel, diff against committed expected. |
| [`dpe coverage`](#dpe-coverage) | Whole pipeline | Per-variant matrix of which stages have snapshot tests. |

Tool-author unit tests (cargo test / pytest / bun test) are a separate concern — see [Authoring a tool](authoring-a-tool.md). This page is about pipeline-level stage tests.

---

## Why per-stage isolation

A snapshot test in DPE spawns ONE stage as a child process — not the whole DAG. The runner pipes seed input directly to the stage's stdin and captures its output. Why:

- **Hermetic.** No upstream/downstream stages to coordinate. A failing test points at exactly one tool.
- **Fast.** Each case is one subprocess; a full bulk run is the sum of those.
- **Forced.** Testing a stage in isolation makes the contract explicit: given these envelopes + this settings, produce that output.
- **Cacheable.** The tool's `ctx.cached(...)` results survive between runs (LLM calls, big parses) when seed input is stable.

---

## Layout

Tests live next to the pipeline, mirroring `variants/<variant>/<stage>`:

```
my-pipeline/
├── variants/
│   └── main.yaml
└── tests/
    └── main/
        └── llm/
            └── case-baseline/
                ├── test.yaml                # optional spec
                ├── input/seed.ndjson         # piped to stdin
                ├── expected/                 # reference result
                │   ├── data.ndjson           # t="d" envelopes
                │   ├── meta.ndjson           # t="m" envelopes (only if asserted)
                │   ├── errors.ndjson         # stderr type="error" events (only if asserted)
                │   └── output/               # files the tool wrote under $output
                │       └── report.md
                ├── assert.py                 # optional assertion script
                └── .run/                     # ephemerals; wipe-on-run
                    ├── actual/               # ← runner writes the captured streams here
                    │   ├── data.ndjson
                    │   ├── meta.ndjson
                    │   ├── errors.ndjson
                    │   ├── logs.ndjson
                    │   ├── trace.ndjson
                    │   └── stats.ndjson
                    ├── output/               # $output during the run
                    ├── temp/                 # $temp
                    ├── storage/              # $storage (cache + batch state live here)
                    └── session/              # $session
```

Positional rules:

| Part | Required | Notes |
|---|---|---|
| `tests/<variant>/` | yes | Matches a file in `variants/`. |
| `<stage>/` | yes | Matches a stage name in that variant. |
| `<case>/` | yes | Free-form. `case-baseline` is the conventional default. |
| `input/seed.ndjson` | yes | One envelope per line; piped to the spawned stage's stdin. |
| `expected/<channel>.ndjson` | yes — at least one | Reference for any channel the test asserts on. |
| `expected/<subdir>/` | optional | Files the tool wrote under that mount point — diffed too (see [filesystem tree](#filesystem-tree-comparison)). |
| `test.yaml` | optional | Per-case overrides + compare rules + assert script. Empty file = inherit. |
| `assert.py` / `assert.ts` / `assert.mjs` | optional | Custom assertion script. Required only when `assert:` is set. |
| `.run/` | auto-created | Per-test ephemerals. Wiped at case start; `actual/` re-wiped between phases. Add `.run/` to `.gitignore`. |

---

## Channels

Stage output isn't a single stream — it's seven discrete channels. The runner captures every one to its own file under `.run/actual/<channel>.ndjson` and asserts on the channels you opt into.

| Channel | Source | Filter | Default-checked? |
|---|---|---|:---:|
| `data` | stdout | `t == "d"` | yes (auto-detect) |
| `meta` | stdout | `t == "m"` | yes (auto-detect) |
| `errors` | stderr | `type == "error"` | yes (auto-detect) |
| `logs` | stderr | `type == "log"` | no — opt in |
| `trace` | stderr | `type == "trace"` | no — opt in |
| `stats` | stderr | `type == "stats"` | no — opt in |
| `input` | stderr | `type == "input"` | no — opt in |

**Auto-detect** means: any channel with an `expected/<channel>.ndjson` committed is implicitly declared. So a case with only `expected/data.ndjson` asserts on the data channel and ignores the rest.

**Strict channels** (`data` / `meta` / `errors`) trigger silent-leak detection: if the tool emits on a strict channel that you neither declared nor committed an expected file for, the test fails. This catches regressions like "tool started emitting unexpected meta envelopes."

**Framework-noise channels** (`logs` / `trace` / `stats` / `input`) are opt-in only. Emitting them is expected baseline behaviour; they don't fail unless you explicitly assert on them. The `input` channel carries the framework's per-envelope-read marker (`{"type":"input","id":...,"src":...}`) — one per envelope a stdin-reading tool consumes — and is what feeds the runner's `rows_in` journal counter.

### How lines are classified

The runner walks each stdout/stderr line and routes by discriminator:

| stream | discriminator | known values | line shape that "fails the test" |
|---|---|---|---|
| stdout | `t` | `"d"`, `"m"` | `t` is set but to something not in the known set (e.g. `"t":"meta"` -- typo of `"m"`). |
| stderr | `type` | `"error"`, `"log"`, `"trace"`, `"stats"`, `"input"` | same -- unrecognised `type` value. |

Lines whose discriminator is **missing** are NOT a test failure: stdout-without-`t` is routed to `data` (legacy envelope shape), stderr without `type` is treated as plain log text and dropped. Lines that **don't parse as JSON** are also dropped silently.

Lines that DO parse but carry an unknown discriminator value are surfaced as a test error with up to three sample lines reproduced verbatim. This prevents typos like `"t":"meta"` from silently vanishing and making the test green.

---

## `test.yaml` — full schema

Every field is optional. An empty file is equivalent to no file.

```yaml
# ─── Per-case settings & env (case-level; phases inherit & may override) ────
settings_override:
  marker: "TEST:"
  threshold: 0.8

env:
  ANTHROPIC_API_KEY: "${REAL_ANTHROPIC_API_KEY}"
  BATCH: "19"

# Per-case cache override. CLI --cache wins over this.
cache: bypass     # use | refresh | bypass | off

# Per-case wall-clock cap on the spawned tool. Default 60_000 ms.
# Bump for stages that make live API calls (LLMs, HTTP, etc.) that
# routinely exceed 60s. Phase-level `timeout_ms:` overrides this.
timeout_ms: 600000   # 10 minutes

# ─── Compare rules — per-channel + global + filesystem ──────────────────────
compare:
  # Which channels to assert on. Optional.
  #   - omitted → auto-detect from expected file presence
  #   - explicit list → exhaustive (logs/trace/stats/input opt in here)
  channels: ["data", "meta"]

  # Rules merged into every selected channel as the base layer.
  global:
    scrub_paths:
      - { from: "msgbatch_[A-Za-z0-9]+", to: "msgbatch_<ID>" }
      - { from: "<case_dir>",            to: "<case>" }
      - { from: "<run_dir>",             to: "<run>" }

  # Per-channel rules (merged on top of global; lists CONCATENATE).
  data:
    ignore_envelope: ["id", "src"]                # default: ["id", "src"]
    ignore_fields:   ["v.timestamp"]              # JSON pointer paths inside the envelope
    matchers:
      - { path: "v.duration_ms", kind: "is_int" }
      - { path: "v.uuid",        kind: "is_uuid" }
      - { path: "v.batch_id",    kind: "regex", pattern: "^msgbatch_[A-Za-z0-9]+$" }
      - { path: "v.size",        kind: "in_range", min: 1000, max: 100000 }
    ordered: true                                  # default true; false → multiset compare

  meta:
    ignore_envelope: ["id", "src"]

  errors:
    ignore_fields: ["input.source_path"]

  # Filesystem tree comparison.
  fs_check:  ["output", "session"]                 # subdirs to walk; default: any subdir present in expected/
  fs_ignore: ["**/*.lock", "**/.DS_Store"]         # globs ignored even if present

  # Per-file mode overrides. Default by extension:
  #   text (.md/.txt/.json/.ndjson/.csv/.html/.yaml/.toml/.log/.xml/.tsv) → "diff"
  #   anything else → "exact" (binary)
  files:
    - path: "output/report.json"
      mode: "schema"
      schema: "expected/output/report.schema.json"
    - path: "output/log.txt"
      mode: "contains"
      patterns:
        - "^processed \\d+ rows$"
        - "completed in \\d+ms"
    - path: "output/extracted.md"
      mode: "fuzzy"
      threshold_pct: 5
    - path: "output/llm-summary.md"
      mode: "exists"
    - path: "output/binary.pdf"
      mode: "exact"

# ─── Optional case-level assertion script ───────────────────────────────────
assert:
  engine:     "python"          # python | bun | node
  script:     "assert.py"       # relative to the case dir
  timeout_ms: 30000             # default 30s

# ─── Optional multi-phase orchestration ─────────────────────────────────────
# When absent, the case is one implicit phase. When present, each entry
# is run in order; .run/ is wiped ONCE at case start; .run/actual/ is
# re-wiped between phases (so output/temp/storage/session persist).
phases:
  - name: "cold"
    cache: "bypass"                              # OVERRIDES case-level
    timeout_ms: 600000                            # OVERRIDES case-level
    settings_override: { ... }                    # MERGED on top
    env: { ... }                                  # MERGED on top
    compare: { ... }                              # MERGED with case-level
    expected: "expected/cold"                     # default: expected/<phase.name>
    assert: { engine: "python", script: "assert_cold.py" }

  - name: "warm"
    cache: "use"
    expected: "expected/warm"
    assert: { engine: "python", script: "assert_warm.py" }
```

### Token expansion in `scrub_paths`

The `from:` regex has four substitutions applied BEFORE compilation, so you don't hardcode absolute paths into fixtures:

| Token | Replaced with |
|---|---|
| `<case_dir>` | absolute path to `tests/<variant>/<stage>/<case>/` |
| `<run_dir>` | absolute path to `<case_dir>/.run/` |
| `<cwd>` | `std::env::current_dir()` at run time |
| `<home>` | `$HOME` / `%USERPROFILE%` |

Paths get regex-escaped before substitution, so backslashes / dots in absolute paths don't blow up regex compilation.

### Matchers

Walk the JSON envelope to `path`, validate the value against `kind`, then replace the value with the sentinel `"<MATCHED:<kind>>"` BEFORE diffing. Both expected and actual go through the same canonicalisation, so the expected file commits the sentinel placeholder and the actual file's real value matches against the predicate.

| Kind | Match |
|---|---|
| `is_int` | integer |
| `is_float` | any number |
| `is_string` | string |
| `is_uuid` | RFC-4122 UUID v1-v5 |
| `is_iso8601` | ISO-8601 timestamp |
| `regex` | matches `pattern:` |
| `in_range` | numeric, `min ≤ v ≤ max` |

If the path doesn't exist in actual, the matcher is silently skipped (treated as optional). If the path exists but the value doesn't match → channel fails with a specific error.

### `compare` defaults (no `compare:` block)

When `compare:` is absent the runner uses these defaults:

```yaml
compare:
  channels:         <auto-detect from expected file presence>
  data:    { ignore_envelope: ["id", "src"], ordered: true }
  meta:    { ignore_envelope: ["id", "src"], ordered: true }
  errors:  { ignore_envelope: ["id", "src"], ordered: true }
  logs:    { ignore_envelope: ["id", "src"], ordered: true }
  trace:   { ignore_envelope: ["id", "src"], ordered: true }
  stats:   { ignore_envelope: ["id", "src"], ordered: true }
  fs_check:        <any subdir present in expected/>
  fs_ignore:       []
  files:           []
```

---

## How the diff works (per phase, four steps)

For each phase the runner runs four steps in order. Step 4 only runs if 1-3 all pass; the final phase result is `pass` iff every step passed.

### Step 1 — Channel shape

For each declared channel, `expected/<channel>.ndjson` must exist (empty file is OK). Plus, for the strict channels (`data` / `meta` / `errors`), if `.run/actual/<channel>.ndjson` is non-empty AND not declared AND no expected file exists → fail with "produced but not declared." This catches silent regressions.

### Step 2 — Filesystem tree

For each subdir in `compare.fs_check` (default: every subdir present under `expected/`):

- Walk recursively; pair each file by relative path.
- Files in expected only → fail (regression: tool didn't produce X).
- Files in actual only → fail (unexpected output: tool produced Y).
- Files in both → compare with the file's mode (default by extension or `compare.files[].mode` override).

Per-file modes:

| Mode | Algorithm |
|---|---|
| `diff` | apply `compare.global.scrub_paths` to each line; line-by-line equality; unified diff on mismatch. |
| `exact` | byte-by-byte; no scrubbing. |
| `schema` | parse actual as JSON; validate against `schema:` file. |
| `contains` | apply scrub; every regex in `patterns:` must match somewhere. |
| `fuzzy` | line-diff after scrub; pass if `mismatching / total * 100 ≤ threshold_pct`. |
| `exists` | only that the file exists; content unchecked. |

Default mode by extension: `.md/.txt/.json/.ndjson/.csv/.html/.yaml/.yml/.toml/.log/.xml/.tsv` → `diff`; everything else → `exact`.

### Step 3 — Per-channel envelope diff

For each channel, in this order:

1. Read `expected/<channel>.ndjson` and `.run/actual/<channel>.ndjson`.
2. Parse line-by-line as JSON.
3. Drop top-level keys named in `ignore_envelope`.
4. Drop fields at JSON-pointer paths in `ignore_fields` (e.g. `v.timestamp` or `/v/timestamp`).
5. Apply matchers: walk to path, validate value, replace with sentinel.
6. Sort keys recursively in the envelope.
7. Serialise; apply `compare.global.scrub_paths` then `compare.<channel>.scrub_paths`.
8. If `ordered: false` → sort lines lexicographically (multiset equality).
9. Line-by-line diff; report channel-level pass / fail with unified diff.

### Step 4 — Assert script

Runs only if all of steps 1-3 passed AND `assert:` is set.

```text
spawn:
  binary  = which::which(assert.engine)              # python|bun|node → absolute path
  argv    = [binary, <case_dir>/<assert.script>]
  cwd     = .run/
  timeout = assert.timeout_ms (default 30000)
  env     = (env_clear() then re-inject) PATH, PYTHONPATH, HOME, USERPROFILE
            + DPE_* variables below

  exit 0       → pass
  exit 1       → fail; stderr shown verbatim
  exit 2+      → ERROR (script crashed)
  timeout      → ERROR
  signal       → ERROR
```

The script receives 16 environment variables — absolute paths to every channel and state directory:

| Var | Value |
|---|---|
| `DPE_CASE_DIR` | absolute case dir |
| `DPE_RUN_DIR` | `.run/` |
| `DPE_EXPECTED_DIR` | `expected/<phase>` for multi-phase, else `expected/` |
| `DPE_ACTUAL_DATA` | `.run/actual/data.ndjson` |
| `DPE_ACTUAL_META` | `.run/actual/meta.ndjson` |
| `DPE_ACTUAL_ERRORS` | `.run/actual/errors.ndjson` |
| `DPE_ACTUAL_LOGS` | `.run/actual/logs.ndjson` |
| `DPE_ACTUAL_TRACE` | `.run/actual/trace.ndjson` |
| `DPE_ACTUAL_STATS` | `.run/actual/stats.ndjson` |
| `DPE_OUTPUT_DIR` | `.run/output/` |
| `DPE_TEMP_DIR` | `.run/temp/` |
| `DPE_STORAGE_DIR` | `.run/storage/` |
| `DPE_SESSION_DIR` | `.run/session/` |
| `DPE_VARIANT` | variant name |
| `DPE_STAGE` | stage name |
| `DPE_CASE` | case name |
| `DPE_PHASE` | phase name (only when multi-phase) |

Example assert script:

```python
# tests/.../case-llm/assert.py
import json, os, sys

with open(os.environ["DPE_ACTUAL_DATA"]) as f:
    rows = [json.loads(l) for l in f if l.strip()]

# Cross-channel invariant: every data envelope's page_count
# matches the meta envelope's total_pages.
with open(os.environ["DPE_ACTUAL_META"]) as f:
    metas = [json.loads(l) for l in f if l.strip()]
total = metas[0]["v"]["total_pages"]

for r in rows:
    if r["v"]["page_count"] != total:
        print(f"page_count mismatch: {r['v']['page_count']} != {total}", file=sys.stderr)
        sys.exit(1)

sys.exit(0)
```

---

## Multi-phase tests

Some flows can't be tested with a single shot:

- **Cache hit verification** — first miss, then hit; both produce identical data, but stderr trace differs.
- **Batch resume** — phase 1 submits & exits; phase 2 picks up state from `.run/storage/` and harvests.
- **Idempotency** — run twice, expect identical output.
- **Stability sampling** — same seed, repeat N times, all match the same expected.

`.run/` is wiped ONCE at case start. `.run/actual/` is re-wiped between phases. Everything else (`output/`, `temp/`, `storage/`, `session/`) persists — that's the whole point of phases.

#### Truth table — what persists between phases

| Path under `.run/` | Phase boundary behaviour | Why |
|---|---|---|
| `actual/` | **wiped** before every phase | Each phase asserts its own outputs in isolation; no bleed-through. |
| `output/` | **persists** | `$output` is the canonical "previously emitted" tree — phase 2 may inspect what phase 1 wrote. |
| `temp/` | **persists** | `$temp` carries scratch state (e.g. batch cursor files) the next phase needs to continue. |
| `storage/` | **persists** | `$storage` is the cache backing store; cache-hit phases require what cache-miss phases wrote. |
| `session/` | **persists** | Session-scoped sidecars (dedup index, checkpoint files). Phase 2 reading from phase 1's index is normal. |

Implication: if your phase-1 stage writes `output/x.ndjson` and phase 2 doesn't touch it, the file remains visible in phase 2's `output/` and in fs-tree diffs. Either explicitly expect it in phase 2's expected tree, or `fs_ignore` the path.

```yaml
# Cache-hit flow
phases:
  - name: "miss"
    cache: bypass
    expected: "expected/miss"
    assert: { engine: "python", script: "assert_no_cache.py" }
  - name: "hit"
    cache: use
    expected: "expected/hit"     # data.ndjson identical to miss
    assert: { engine: "python", script: "assert_cache_hit.py" }
```

```yaml
# Batch fire-and-forget then harvest
phases:
  - name: "submit"
    settings_override: { strategy: { per_page_mode: batch }, batch: { poll: null } }
    expected: "expected/submit"
    compare: { channels: ["meta"] }
  - name: "harvest"
    settings_override:
      strategy: { per_page_mode: batch }
      batch:    { poll: { start_after_ms: 5000, interval_ms: 5000, deadline_ms: 600000 } }
    expected: "expected/harvest"
    compare: { channels: ["data"] }
```

A phase failing does NOT abort the case — every phase runs to completion. The reporter shows per-phase pass / fail, then the case-level aggregate.

### Phase composition rules

| Field | Composition |
|---|---|
| `settings_override` | deep-merged: case → phase (phase wins) |
| `env` | merged: case ⊎ phase (phase wins on key collision) |
| `cache` | first-wins: CLI `--cache` → phase → case → `use` |
| `timeout_ms` | first-wins: phase → case → `60_000` ms |
| `compare.channels` | phase replaces case (no concat) |
| `compare.global.*` | per-channel rules merge — see compose rules below |
| `compare.<channel>.*` | per-channel rules merge — see compose rules below |
| `compare.fs_check` | phase replaces case |
| `compare.fs_ignore` | concatenated |
| `compare.files` | concatenated; phase entries win on path collision |
| `assert` | phase replaces case |
| `expected` | per-phase override; default `expected/<phase.name>` |

Compose rules for per-channel sections (`global`, `data`, etc.) layer four sources in order: `case.global` → `phase.global` → `case.<channel>` → `phase.<channel>`. Scalars (`ordered`, `ignore_envelope`) take the last non-None layer. Lists (`scrub_paths`, `ignore_fields`, `matchers`) concatenate across all four.

---

## Authoring workflow

```sh
# 1. Lay out a new case.
mkdir -p my-pipeline/tests/main/scan/case-baseline/{input,expected}

# 2. Write seed envelopes — what flows into the stage's stdin.
cat > my-pipeline/tests/main/scan/case-baseline/input/seed.ndjson <<'EOF'
{"t":"d","id":"a","src":"-","v":{"path":"data/input"}}
EOF

# 3. Capture expected output from the actual stage. --update-if-missing
#    writes expected/<channel>.ndjson the FIRST time and never overwrites
#    it, so it's safe to leave on in CI.
dpe test my-pipeline:main:scan --update-if-missing

# 4. Inspect the captured snapshot, edit if needed, commit it.
git add my-pipeline/tests/main/scan/case-baseline/
git commit -m "test: add case-baseline for main:scan"

# 5. From here on, it's a regular check.
dpe test my-pipeline:main:scan
```

To regenerate after an intentional change:

```sh
dpe test my-pipeline:main:scan --update          # rewrites expected/ on every run
git diff -- 'my-pipeline/tests/main/scan/'
git add my-pipeline/tests/main/scan/case-baseline/expected
```

`--update` and `--update-if-missing` are mutually exclusive. The captured actual is canonicalised (envelope `id`+`src` stripped, JSON keys sorted, matchers replaced with sentinels, scrub_paths applied) before being written, so the committed snapshot is stable across machines.

---

## Targets

```
[<pipeline>:]<variant>:<stage>[:<case>]
```

| Target | Selects |
|---|---|
| `my-pipe` | every variant × every stage × every case in `my-pipe/tests/` |
| `my-pipe:main` | every stage × every case in `tests/main/` |
| `my-pipe:main:scan` | every case in `tests/main/scan/` |
| `my-pipe:main:scan:case-edge` | exactly one case |
| `:main:scan` | same — empty pipeline = cwd |
| `.:main:scan` | same — `.` is alias for empty |

Bulk targets (1- or 2-part) apply the **skip-list** and the per-tool `test_exclusive` filter automatically. Stage-explicit targets (3- or 4-part) BYPASS both — when you name a stage, the runner respects that.

---

## Skip-list and `test_exclusive`

Some stages aren't worth a snapshot test on their own. Two mechanisms exclude them from bulk runs:

### Hard-coded skip-list

Built into the runner. These tool names are silently skipped in bulk mode and excluded from coverage % entirely:

| Tool | Why |
|---|---|
| `gate` | Stateful pass-through. State only matters in a real pipeline. |
| `checkpoint` | Spool-then-release on gates. Same — state-y, tested via the surrounding flow. |
| `dedup` | Indices live on disk; cross-run state is testable but only via multi-phase fixtures (use `phases:` to share `.run/storage/`). |
| `toggle` | (built-in) `test_skipped: true` on the synthetic meta — env-gated passthrough, decision is fixed at plan-compile time. |

### Built-in stages

Six built-ins are recognised by the runner: `filter`, `route`, `group-by`, `dedup`, `spread`, `toggle`. They run as in-process tokio tasks rather than child processes; the test runner has a per-built-in driver that constructs the task, pipes seed input, and captures every byte the writer emits to the standard channel files.

| Built-in | Testable via `dpe test`? | Output format |
|---|---|---|
| `filter` | yes | `data` channel (line-by-line, only lines whose expression evaluates truthy) |
| `route` | yes | `data` channel (combined: each routed line gets `_route_channel: "<name>"` injected; channels appear in alphabetical order) |
| `group-by` | yes | `data` channel (emitted aggregates per trigger) |
| `dedup` | yes (multi-phase recommended) | `data` channel (kept rows; `.run/storage/index-*.bin` persists across phases) |
| `spread` | no — `test_skipped` | n/a (pure 1→N tee) |
| `toggle` | no — `test_skipped` | n/a (decision fixed at plan-compile time) |

Built-ins read their config from variant `stage_def` fields (`expression`, `routes`, `dedup`, `group_by`) — NOT from the `settings:` blob. `${VAR}` and `$input` / `$session` etc. inside those fields are resolved before the driver runs, same as `dpe run` does.

### Per-tool `test_exclusive`

A tool's `meta.json` may set `test_exclusive: true` to signal "this stage requires explicit invocation, don't include it in bulk runs." Useful when the tool depends on a host-side resource (an LLM API key, a Mongo URL).

```json
{
  "name": "llm",
  "version": "0.4.1",
  "runtime": "bun",
  "test_exclusive": true
}
```

Bulk run skips it. `dpe test my:main:llm` runs it explicitly — the user has asked for that stage and owns the env setup.

A separate `test_skipped: true` flag declares "no logic worth snapshot-testing" — applies to pure I/O tools. Excluded from BOTH bulk runs AND the coverage denominator, so it can't drag % down.

---

## Coverage

`dpe coverage` reports which stages have at least one snapshot case. Informational, never a gate (exit 0 always).

```sh
dpe coverage my-pipeline                 # every variant
dpe coverage my-pipeline:main            # single variant
dpe coverage .:main                      # cwd as pipeline
dpe coverage my-pipeline --json          # machine-readable shape
```

### Buckets

| Symbol | Bucket | Numerator | Denominator |
|:---:|---|:---:|:---:|
| ✓ | covered (has tests) | ✓ | ✓ |
| ◐ | `test_exclusive=true` AND has tests | ✓ | ✓ |
| ◔ | `test_exclusive=true` AND no tests | — | ✓ |
| ✗ | uncovered (no tests) | — | ✓ |
| ⊘ | skip-list OR `test_skipped=true` | excluded | excluded |

Coverage % = (✓ + ◐) / (✓ + ◐ + ◔ + ✗).

### Reading the output

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

`upper` has 2 cases under `tests/02-read-normalize-write/upper/` and counts as covered. `read` and `sink` have no tests yet. `gate` and `check` are skip-listed (control plumbing) and don't count toward % — that variant's denominator is 2, not 4.

### `--json` shape

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

| Field | Type | Notes |
|---|---|---|
| `variants[].variant` | string | Variant name |
| `variants[].stages[].stage` | string | Stage name |
| `variants[].stages[].tool` | string | Tool name |
| `variants[].stages[].bucket` | string | One of: `covered`, `skip_list`, `test_skipped`, `exclusive_covered`, `exclusive_uncovered`, `uncovered` |
| `variants[].stages[].case_count` | int | Number of `<case>/` directories under the stage's tests dir |
| `variants[].covered` / `total` | int | Per-variant counts (skip-listed stages excluded from `total`) |
| `variants[].pct` | string | One-decimal percent |
| `covered` / `total` / `pct` (top-level) | same | Summed across all variants |

---

## CLI

### `dpe test <target>` flags

| Flag | Effect |
|---|---|
| `--update` | Rewrite `expected/<channel>.ndjson` from canonicalised actual on every run. Mutually exclusive with `--update-if-missing`. |
| `--update-if-missing` | Write `expected/<channel>.ndjson` only when the file doesn't exist. Safe in CI. |
| `--cache <use\|refresh\|bypass\|off>` | CLI cache override; wins over `test.yaml` `cache:`. |

Loading env from a file is a global flag, not test-specific:

```sh
dpe --env-file .env test my-pipeline:main:llm
dpe --env-file .env --env-file .env.local test my-pipeline   # merge two; first wins
```

`--env-file` rules:
- Path **must exist** — missing file is a hard error.
- A var **already in the process environment is never overridden**.
- `test.yaml` `env:` block still wins for `${VAR}` interpolation inside `settings_override` and for the spawned tool's environment.

---

## Common patterns

### Smoke-test every variant

```sh
dpe test my-pipeline                 # bulk over all variants
```

Skip-list and `test_exclusive` apply, so `gate / checkpoint / toggle / dedup / llm` etc. don't run.

### Pin one stage's behaviour

```sh
mkdir -p my-pipeline/tests/main/normalise/case-empty/{input,expected}
echo '' > my-pipeline/tests/main/normalise/case-empty/input/seed.ndjson
dpe test my-pipeline:main:normalise:case-empty --update-if-missing
```

Confirms the stage emits nothing on empty input. Commit the empty `expected/data.ndjson` and the case is locked in.

### Parameterise a case via env

```yaml
# tests/main/llm/case-mock/test.yaml
env:
  ANTHROPIC_API_KEY: "mock-key"
  MOCK_RESPONSE: "fixture/canned-response.json"
settings_override:
  endpoint: "${MOCK_RESPONSE}"   # ${VAR} interpolation honours test.yaml env
```

The tool sees both env vars in its process environment, AND `settings_override` is interpolated against `test.yaml` env first (process env is the fallback).

### Override one setting without redefining the rest

```yaml
# tests/main/scan/case-strict/test.yaml
settings_override:
  hash: blake2b   # variant defaults to hash: none — override only this
```

Deep merge: every key not set in `settings_override` inherits from the variant.

### Scrub volatile paths from output

```yaml
compare:
  global:
    scrub_paths:
      - { from: "<run_dir>",                  to: "<run>" }
      - { from: "msgbatch_[A-Za-z0-9]+",      to: "msgbatch_<ID>" }
      - { from: "\\d{4}-\\d{2}-\\d{2}T[^\"]+", to: "<TS>" }
```

Applied to BOTH expected and actual after canonicalisation, so the committed snapshot doesn't mention the user's homedir.

### Fuzzy-match LLM output

```yaml
compare:
  files:
    - path: "output/summary.md"
      mode: "fuzzy"
      threshold_pct: 5
```

LLM output's never byte-stable; fuzzy mode tolerates up to N% mismatching lines.

### JSON Schema validation

```yaml
compare:
  files:
    - path: "output/report.json"
      mode: "schema"
      schema: "expected/output/report.schema.json"
```

`schema:` path is relative to the case dir. The actual file must be valid JSON and validate against the schema.

### Cross-channel assertion script

Beyond what declarative compare can express:

```yaml
assert:
  engine: "python"
  script: "assert.py"
```

```python
# assert.py — runs after declarative steps pass
import json, os, sys

with open(os.environ["DPE_ACTUAL_DATA"]) as f:
    rows = [json.loads(l) for l in f if l.strip()]
assert len(rows) > 0, "no data envelopes produced"
assert all("page_count" in r["v"] for r in rows), "missing page_count"
sys.exit(0)
```

### Snapshot-update workflow

```sh
# After an intentional change to the tool's output:
dpe test my-pipeline --update           # regenerate every snapshot
git diff -- 'my-pipeline/tests/'        # review what changed
git add my-pipeline/tests/
git commit -m "test: update snapshots after <reason>"
```

---

## CI integration

```sh
# Validate variants parse + reference real tools / inputs
dpe check --all my-pipeline || exit 1

# Run snapshot tests
dpe test my-pipeline || exit 1

# Print coverage as informational output
dpe coverage my-pipeline
```

Exit codes:
- `dpe test` — 0 on all-pass / all-skipped, 1 on any FAIL, 2 on any ERROR (invocation problem).
- `dpe coverage` — always 0; never gates.

---

## Troubleshooting

**"variant 'X' not found"** — the variant name in the target must match a file in `variants/`. `dpe coverage <pipeline>` lists what the runner can see.

**"stage 'Y' not in variant"** — the test directory exists but the variant config no longer declares that stage. In bulk runs the runner silently skips these; explicitly named cases error out.

**"channel produced but not declared"** — the tool emitted on a strict channel (`data` / `meta` / `errors`) that you didn't declare in `compare.channels` and didn't commit `expected/<channel>.ndjson` for. Either:
- Declare it: `compare.channels: ["data", "meta"]`.
- Commit a reference: `expected/<channel>.ndjson` (empty file = "must be silent").
- Confirm the tool shouldn't be emitting on that channel and fix the tool.

**Diff shows envelope `id` differences** — `id` and `src` are stripped by default. If you see them in the diff, the input file probably has different envelopes (different `v` content), not the same envelopes with different ids.

**Tests pass locally but fail in CI** — most common: `test_exclusive: true` is missing on a tool that needs a host-only resource, so CI runs it with no API key. Either set `test_exclusive` in the tool's `meta.json` or inject the missing env via `test.yaml`'s `env` block.

**`test exceeded Nms timeout`** — the spawned tool didn't finish within the case's wall-clock cap (default 60_000 ms). Raise it for the case (`timeout_ms: 600000` in `test.yaml`) or for one phase (`phases[].timeout_ms`). Long live-API stages (LLMs, batch APIs) routinely need 5-30 minutes.

**Stage uses files but the test says "input not found"** — the stage runs in `tests/<variant>/<stage>/<case>/.run/` so any `$input` / `$storage` path it references must exist there OR be absolute. Ephemerals are gitignored; either commit a fixture under `tests/<variant>/<stage>/<case>/fixtures/` or have the case use absolute paths to a shared fixture.

**"assert engine 'X' not found on PATH"** — install the engine (python / bun / node) or pick a different one in `assert.engine:`. The runner uses `which::which()` to resolve; check `which python` (or `where python` on Windows) returns a path.

**Phase 2 expected `output/foo.txt` but actual is empty** — phase 1's tool didn't write that file; phase 2 inherits the same `$output` so phase 2 sees what phase 1 produced. If `output/foo.txt` is meant to come from phase 1, that's a phase-1 bug to fix first.

---

## See also

- [CLI reference / test](cli.md#test) and [coverage](cli.md#coverage) — flag details
- [Authoring a tool](authoring-a-tool.md) — language-native tool tests (cargo / pytest / bun)
- [Path prefixes](path-prefixes.md) — how `$input` / `$session` etc. resolve under `.run/`
- [Configuration](configuration.md) — runner config knobs that affect test runs (spawn timeouts, trace size limits, etc.)
- [Caching](caching.md) — how `ctx.cached(...)` interacts with the per-case `cache:` setting

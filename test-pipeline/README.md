# test-pipeline

Regression suite. Every variant here exercises one or more of the 7 standard tools shipped with DPE.

## Layout

```
test-pipeline/
├── standard/                       # the regression pipeline
│   ├── pipeline.toml
│   ├── variants/
│   │   ├── 00-read-write.yaml       # read-file-stream → write-file-stream
│   │   ├── 01-scan-write.yaml       # scan-fs → write-file-stream
│   │   ├── 02-read-normalize-write.yaml  # read → normalize (uppercase) → write
│   │   ├── 03-gate-checkpoint.yaml  # gate + checkpoint coordination
│   │   └── 04-write-hashed.yaml     # write-file-stream-hashed dedup
│   ├── configs/
│   │   └── normalize/uppercase.yaml  # trivial normalize rulebook
│   ├── data/
│   │   ├── input/_seed.ndjson        # synthesised seed envelopes (no private data)
│   │   └── output/                   # runtime output (gitignored)
│   └── tools/                        # (empty — no pipeline-local overrides)
└── run-all.sh                        # runs every variant + reports pass/fail
```

## Running

```bash
# From the monorepo root after `cargo build --release -p dpe`:
./test-pipeline/run-all.sh

# Or with an explicit binary:
DPE_BIN=/opt/dpe/bin/dpe ./test-pipeline/run-all.sh

# One-off for a single variant:
dpe run test-pipeline/standard:02-read-normalize-write \
  -i test-pipeline/standard/data/input \
  -o test-pipeline/standard/data/output
```

Expected result: every variant exits 0, outputs land in `standard/data/output/`.

## What's being tested

| Variant | Tools exercised |
|---|---|
| 00-read-write | read-file-stream → write-file-stream |
| 01-scan-write | scan-fs → write-file-stream |
| 02-read-normalize-write | read-file-stream → normalize → write-file-stream |
| 03-gate-checkpoint | read → gate → checkpoint → write |
| 04-write-hashed | read → write-file-stream-hashed |

Every envelope in `data/input/_seed.ndjson` is synthesised — zero private data. Safe to commit to a public repo.

## CI integration

The full Phase 1 acceptance script runs `run-all.sh` as one of its steps. For CI, run:

```bash
cargo build --release --workspace
./test-pipeline/run-all.sh
```

Exit code 0 = all variants pass.

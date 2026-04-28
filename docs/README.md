# DPE — Data Processing Engine

High-throughput streaming data processor. A long-running Rust **runner** spawns **tools** (any language) as child processes, routes NDJSON envelopes between them over OS pipes, and coordinates the whole pipeline from a declarative YAML/JSON graph.

---

## Five-minute tour

A pipeline is a folder with one or more **variants**:

```
my-pipeline/
├── tools/               # pipeline-local tools (optional; override global tools)
├── configs/             # prompts, dictionaries, anything tools read
├── storage/             # caches that survive across runs
├── temp/                # intermediaries (gates, checkpoints, spools)
├── sessions/<id>/       # per-run artefacts (auto-created)
└── variants/
    └── main.yaml
```

A variant declares the DAG:

```yaml
pipeline: my-pipeline
variant: main
stages:
  scan:
    tool: scan-fs
    settings: { include: "*.txt", hash: blake2b }
    input: $input
  read:
    tool: read-file-stream
    settings: { format: lines }
    input: scan
  sink:
    tool: write-file-stream
    settings: { default_file: "$output/_index.ndjson", format: ndjson }
    input: read
```

Then run it:

```sh
dpe run my-pipeline:main -i input/ -o output/
```

That's it. Each stage is a separate OS process; the runner wires stdout→stdin between them; backpressure comes free from the kernel.

---

## Start here

| Doc | Read if you want to… |
|---|---|
| [Installing](installing.md) | get `dpe` + standard tools on your machine (npm / script / Docker) |
| [Configuration](configuration.md) | `config.toml` schema + resolution order |
| [Concepts](concepts.md) | envelopes, stages, DAG topology, the runner's job |
| [Writing pipelines](writing-pipelines.md) | author YAML variants: linear, fan-in, route, filter, replicas, dedup |
| [Expressions](expressions.md) | route / filter / condition expressions in the DSL |
| [Path prefixes](path-prefixes.md) | `$input` / `$output` / `$configs` / `$storage` / `$temp` / `$session` |
| [CLI reference](cli.md) | `dpe init / run / check / tools / install / config / logs / monitor` |
| [Session artefacts](sessions.md) | what lands in `sessions/<id>/`: trace, errors, journal, stages.json |
| [Tools overview](tools/README.md) | standard-tool catalogue + tool contract |
| [Frameworks](frameworks.md) | write your own tool in Rust / Python / TypeScript |
| [Authoring a tool](authoring-a-tool.md) | one-command scaffold + headless build from a `spec.yaml` |
| [Docker](docker.md) | `dpe-base` + `dpe-dev` images + client pipeline patterns |
| [Examples](examples/README.md) | worked variants walked through end-to-end |

---

## What's in this monorepo

| Component | Path | Purpose |
|---|---|---|
| **Runner + CLI** | `runner/` | `dpe` binary — spawns tools, wires pipes, traces, controls sessions |
| **Dev CLI** | `dpe-dev/` | Tool authoring: `scaffold / build / test / verify / setup` |
| Rust framework | `frameworks/rust/` | SDK for writing tools in Rust (`combycode-dpe` crate) |
| TypeScript framework | `frameworks/ts/` | SDK for Bun/TS (`@combycode/dpe-framework-ts`) |
| Python framework | `frameworks/python/` | SDK for Python (`combycode-dpe` package) |
| [scan-fs](tools/scan-fs.md) | `tools/scan-fs/` | filesystem scanner (files / dirs / both) |
| [read-file-stream](tools/read-file-stream.md) | `tools/read-file-stream/` | stream text-file rows (NDJSON / lines / CSV) |
| [write-file-stream](tools/write-file-stream.md) | `tools/write-file-stream/` | write envelopes to files, LRU-bounded |
| write-file-stream-hashed | `tools/write-file-stream-hashed/` | same with per-file content dedup |
| [normalize](tools/normalize.md) | `tools/normalize/` | row-level normaliser (dict / parse / rename / compute / template / require) |
| [gate](tools/gate.md) | `tools/gate/` | stateful pass-through that publishes progress |
| [checkpoint](tools/checkpoint.md) | `tools/checkpoint/` | spool-then-release on gate(s) met |
| Built-ins | in-runner | [`route`](tools/builtins.md#route) / [`filter`](tools/builtins.md#filter) / [`dedup`](tools/builtins.md#dedup) / [`group-by`](tools/builtins.md#group-by) |
| dev-workspace-template | `dev-workspace-template/` | Claude skill pack + fixtures — embedded into dpe-dev |
| Docker | `docker/` | Multi-stage Dockerfile: `dpe-base` + `dpe-dev` images |
| Test pipeline | `test-pipeline/` | Regression suite — runs every standard tool against synthesised inputs |
| Catalogue | `catalog.json` | Manifest of known tools (standard + custom) |

**Custom tools** (mongo-upsert, mongo-find, xlsx-extract, read-tables, classify, llm, doc-converter) ship as separate repos and install via `dpe install <name>`.

---

## Design rules (read these before arguing with the code)

1. **Each tool is an independent program.** Any language. Receives settings as one JSON argument on argv[1]. Reads NDJSON envelopes line-by-line from stdin. Writes NDJSON to stdout. Writes typed events to stderr. Tools never self-exit — the runner owns their lifecycle.
2. **Envelopes are the contract.** `{"t":"d","id":"...","src":"...","v":{...}}` for data; `{"t":"m","v":{...}}` for meta. Only `v` is business-meaningful; `id` / `src` chain the provenance.
3. **Filesystem-first coordination.** Gates, checkpoints, dedup indices — all live on disk. The runner observes files; it doesn't own state across tools.
4. **Framework does the merged trace event.** On every `ctx.output()`, the framework emits one `{type:"trace",id,src,labels}` to stderr. Runner just appends to `$session/trace/trace.N.ndjson`. No sniffing of stdout.
5. **IPC is cross-platform local sockets, never TCP.** Named pipe on Windows, UDS on Unix, via `interprocess`. Runner writes `$session/control.addr`; CLI reads it to connect.
6. **DAG is acyclic.** If you need iteration, loop *inside* a tool. Cycles in the DAG are explicitly rejected.

---

## License

DPE is licensed under **AGPL-3.0-or-later**. See [LICENSE](../LICENSE) for the full text. Commercial licensing is available — contact CombyCode for details.

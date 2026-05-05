# Caching expensive tool work

`ctx.cached(namespace, key, produce)` lets a tool skip expensive work
(LLM calls, large parses, network round-trips) when the same input has
been processed before. All three frameworks ship the same shape; the
on-disk format is identical so a cache built by a Python tool is read
by a Rust or TS tool with the same key.

## When to use it

- Per-envelope work whose result depends only on (file content +
  settings) — PDF→markdown, DOCX→PDF, LLM API calls, big spreadsheet
  parses.
- Operations where the cost is real (API $$ or seconds of CPU) and
  the result is deterministic given the inputs.

## When NOT to use it

- Stateful work (each envelope changes a counter / DB state).
- Operations whose result depends on wall time (current weather,
  random sampling).
- Outputs too large to fit comfortably in JSON files (multi-MB blobs —
  store those as side files in `$storage` keyed by hash, and cache
  only the **path** in `ctx.cached`).
- Trivially cheap work (don't pay the JSON-serialize round-trip to
  cache something that takes less time than the read+write).

## Storage layout

```
$DPE_STORAGE/
└── <namespace>/
    └── <hash>.json              ← serialized produce() return value
```

- `$DPE_STORAGE` resolves to `<pipeline>/storage` by default, or
  whatever `--storage-dir` was set to. Cleared by `dpe run --clear
  storage`. Survives across runs unless explicitly cleared.
- `<namespace>` — caller-chosen bucket name. Convention: same as the
  tool name (e.g. `doc-converter`, `llm`, `xlsx`). One namespace per
  tool keeps caches independent.
- `<hash>` — first 32 hex chars of blake2b over canonical-JSON of `key`.
  Same key in the same namespace → same file → guaranteed hit on
  re-run.

## Cache modes

The runner exports `DPE_CACHE_MODE` to every spawned tool. Driven by
`dpe run --cache <mode>` (or `[cache].default_mode` in config.toml).
Tools using `ctx.cached` honor it automatically.

| Mode | Read | Write | Use case |
|---|---|---|---|
| `use` (default) | yes | on miss | Normal runs — skip work that's already done |
| `refresh` | no | always | Force regeneration. The next `use` run picks up the new value |
| `bypass` | no | no | One-off skip — produce fresh, don't touch cache |
| `off` | no | no | Same as bypass |

## Producing the key

The key is anything JSON-serializable; the framework hashes it
canonically (sorted keys, compact). What you put in determines what
counts as "same input":

```ts
// TS
const result = await ctx.cached(
  "doc-converter",
  {
    file_hash: await ctx.hashFile(v.path),    // file CONTENTS, not path
    settings_hash: stableHash(settings),       // tool-relevant settings
    page: pageIdx,                             // per-page caching
    model: settings.provider.model,            // model id is part of identity
    tool_version: TOOL_VERSION,                // bump to invalidate everyone
  },
  () => provider.convertPage(...),
);
ctx.output(result);
```

```python
# Python
result = ctx.cached(
    "doc-converter",
    {
        "file_hash": ctx.hash_file(v["path"]),
        "settings": settings,
        "page": page_idx,
    },
    lambda: provider.convert_page(...),
)
ctx.output(result)
```

```rust
// Rust
let key = json!({
    "file_hash": ctx.hash_file(&v["path"].as_str().unwrap_or("")),
    "settings": settings,
    "page": page_idx,
});
let result: Value = ctx.cached("doc-converter", &key, || {
    Ok(provider.convert_page(...))
})?;
ctx.output(result, None, None);
```

### Cache-key gotchas

- **Use file content hash, not file path.** Two files with the same
  path but different content (rebuilt artifact, regenerated PDF)
  must produce different cache entries.
- **Include tool version when output schema changes.** A `gen-messages`
  v2 with a new word emits different output for the same `n` than v1.
  Bump a `version` field in the key to invalidate.
- **Don't include things that don't affect output.** A timestamp, a
  request id, an unused setting field — these should NOT be in the
  key, otherwise nothing ever hits.
- **Don't include the whole settings blob if only a subset matters.**
  E.g. `provider.api_key` doesn't affect the OUTPUT (just authn);
  exclude it. Otherwise rotating keys invalidates everything.

## Failure modes

`ctx.cached` is designed to never make things worse than no-cache:

| Situation | Behavior |
|---|---|
| `$DPE_STORAGE` not set | Cache silently disabled, every call produces |
| Cache file missing | Treated as miss; produce + write |
| Cache file unreadable (permissions, etc.) | Logged as warn, treated as miss |
| Cache file unparseable JSON | Logged as warn, treated as miss, overwritten on next produce |
| Producer raises | Propagates to caller; cache is NOT written |
| Storage write fails (disk full, etc.) | Logged as warn, caller still gets the produced value |

A failed producer never writes a poisoned entry. A corrupted file is
self-healing on the next refresh.

## Inspecting / clearing the cache

```sh
# See what's there
ls -la $pipeline_dir/storage/<namespace>/

# Force regeneration of one specific cache
rm $pipeline_dir/storage/<namespace>/<hash>.json

# Wipe all caches for a tool
rm -rf $pipeline_dir/storage/<namespace>/

# Wipe everything (next run rebuilds all caches)
dpe run my-pipeline:main --clear storage  ...

# Force a regeneration without wiping (useful when key derivation changed)
dpe run my-pipeline:main --cache refresh  ...
```

## Adoption checklist for tool authors

When adding caching to an expensive tool:

1. Pick a stable namespace (the tool's name).
2. Identify the key — what set of inputs uniquely determines the
   output? File hash, relevant settings, version markers.
3. Wrap the expensive call: `ctx.cached(namespace, key, () => work())`.
4. Test cache-hit + cache-miss paths. The framework's own test suites
   (`frameworks/{ts,python,rust}/tests/...cache...`) are the
   reference template.
5. Document the key derivation in the tool's README so users understand
   what triggers regeneration.

## See also

- [CLI reference](cli.md) — `--cache` flag, `--clear storage`
- [Configuration](configuration.md) — `[cache].default_mode`
- [Frameworks](frameworks.md) — `ctx.cached` method signatures
- [Path prefixes](path-prefixes.md) — `$storage` semantics

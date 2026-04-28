# Expression DSL

Small purpose-built language used by built-in `route` and `filter` stages. ~350 LOC Rust evaluator — not a general scripting host. Compiles at stage startup (caught by `dpe check`), runs per envelope in the hot path.

## Scope

Each expression sees two top-level bindings:

- `env` — the whole envelope (`env.t`, `env.id`, `env.src`, `env.v`)
- `v` — convenience alias for `env.v`

```
env.t == "d"                           # only data envelopes
v.word_count >= 3 && v.kind == "text"  # on the payload
```

## Literals

| Kind | Example |
|---|---|
| number | `3`, `3.14`, `-1` |
| string | `"text"` or `'text'` (either quote) |
| boolean | `true`, `false` |
| null | `null` |
| array | `[1, 2, 3]`, `["a", "b"]` |

## Path access

Dotted paths on `env` / `v` / any subobject:

```
v.a.b.c
env.id
v.items[0]                  # array index
```

Missing fields evaluate to `null` (never throw). Null propagates through comparisons: `v.missing > 3` → `false` (the node counts this as an eval error under `on_error: fail` only; default is silent).

## Operators

### Comparison

| Op | Types |
|---|---|
| `==` `!=` | any (deep-equal for objects / arrays) |
| `<` `<=` `>` `>=` | number vs number, string vs string |

### Logical

```
a && b      # short-circuit AND
a || b      # short-circuit OR
!a          # NOT
```

Truthiness matches JavaScript-ish rules:
- `null` → false
- `false` → false
- `0` → false
- `""` → false
- `[]` → false
- `{}` → false
- everything else → true

### Grouping

```
(a || b) && !c
```

## Built-in helpers

These are callable like functions:

| Helper | Returns | Example |
|---|---|---|
| `empty(x)` | `true` iff x is null, "", [], or {} | `empty(v.items)` |
| `starts_with(s, prefix)` | bool | `starts_with(v.name, "test_")` |
| `ends_with(s, suffix)` | bool | `ends_with(v.filename, ".pdf")` |
| `contains(s, needle)` | bool | `contains(v.title, "summary")` |
| `normalize(s)` | lowercase + trim | `normalize(v.vendor) == "acme"` |

(Reserved built-ins you'll see used: `includes`, `all`, `any` — check the source for the current exact list.)

## Expression locations

### Route channels

```yaml
router:
  tool: route
  routes:
    priority:    "env.v.class.className == 'priority'"
    large_value: "v.amount > 100"
    default:     "true"
```

Evaluated **in declaration order**; **first truthy wins**. If nothing matches, the envelope is dropped (or passed to all channels when `on_error: pass`).

### Filter predicates

```yaml
keep-rich:
  tool: filter
  expression: "v.word_count > 10 && !empty(v.text)"
  on_false: drop          # drop | emit-meta | emit-stderr
```

### Dedup key paths

These aren't expressions; they're bare path strings inside a list:

```yaml
dedup:
  key: ["v.hash"]            # single path
  key: ["v.id", "v.date"]     # composite, joined with '|' then hashed
```

## Gotchas

- **String comparison is byte-wise**, not locale-aware. Normalize first if you need case/accent insensitivity.
- **Numbers** — JSON numbers only. No BigInt. Precision follows f64 rules.
- **No variable assignment**. Expressions are pure — they compute one value.
- **No regex** today. Use `contains` / `starts_with` / `ends_with` or multiple route channels.
- **No timestamps / dates** built in. If you need them, encode durations/epochs in numbers upstream.

## Validating at check time

```sh
dpe check my-pipeline:main
```

Compiles every route expression and every filter predicate. Errors out with the stage id + channel name + specific parse error, *before* running anything.

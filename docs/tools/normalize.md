# normalize

Row-level universal normaliser. Applies a declarative **rulebook** of ops against each envelope: dictionary replacement, type coercion, header renaming, shape transforms, dynamic path templating, require-field guards. Supports **profile dispatch** so different envelope types get different rulebooks in one stage.

Repo: `dpe-tool-normalize/` (Rust). Tool name: `normalize`.

## When to use

- Heterogeneous source data (different spreadsheet templates, multiple data sources, mixed unit conventions, dirty headers)
- One stage that handles multiple record types side-by-side, each with its own rules
- Cleaning header names, parsing locale-specific number formats, normalising units, unwrapping spreadsheet formula cells

## Settings

```yaml
normalize:
  tool: normalize
  settings:
    rulebook:     "$configs/normalize/master.rules.yaml"
    on_unmatched: passthrough           # passthrough | drop | error
    on_error:     trace                  # trace | null | passthrough | drop | error | quarantine
  input: upstream
```

### Top-level rulebook

Two forms — flat rules for single-shape input, **profiles** for heterogeneous input:

```yaml
# form 1: flat rules
rules:
  - op: trim,   path: v.name
  - op: parse_number, path: v.amount

# form 2: profiles (first-match-wins)
profiles:
  - when: "v.format == 'titled'"
    use:  "titled.rules.yaml"         # separate file (resolved relative to master)
  - when: "v.format == 'flat'"
    rules:                              # inline
      - op: trim, path: v.row
      - op: rename, path: v.row, map: {amount: Amount}
  - when: "true"                        # catch-all
    rules: []
```

`on_unmatched` controls what happens when no profile matches. Default `passthrough` lets envelopes through untouched (with a trace label); `drop` silently drops; `error` routes to stderr error log.

### Rule shape

Every rule has:
```yaml
- op:       trim                   # operation name (required)
  path:     v.row.name             # target path (default "v")
  on_error: drop                   # per-rule override of default
  <op-specific params>
```

## Operations

### Value ops (polymorphic — applies to scalar or every element of array/object)

| Op | Purpose |
|---|---|
| `trim` | Strip leading/trailing whitespace |
| `collapse_whitespace` | `"a  b\tc"` → `"a b c"` |
| `case` (`to: lower|upper|title|sentence`) | Change case |
| `null_if` (`values: [...]`) | Replace matching values with null |
| `slugify` | ASCII, dash-separated |
| `normalize_unicode` | NFC |
| `replace` (`pattern`, `with`, `regex: bool`) | Literal or regex substitution |
| `dict` (`map`, `default`) | Lookup-replace; see Dict section |
| `parse_number` (`decimal`, `thousand`, `parens_negative`, `percent`, `strip`) | String → Number |
| `round` (`decimals`) | Decimal rounding |
| `scale` (`factor`) | Multiply by factor |
| `clamp` (`min`, `max`) | Bound |
| `abs` | Absolute value |
| `parse_date` (`formats`, `assume_tz`, `convert_tz`, `output`) | String → date/datetime, with TZ conversion |
| `parse_bool` (`truthy`, `falsy`) | String → bool with configurable vocab |
| `normalize_currency` (`overrides`, `fallback`) | `"€"` → `"EUR"` |
| `split_amount_currency` (`target_amount`, `target_currency`, …) | `"1 234,56 €"` → `(1234.56, "EUR")` |

### Key ops (object-only)

| Op | Purpose |
|---|---|
| `rename` (`map`) | Change keys via explicit map |
| `whitelist` (`keys`) / `blacklist` (`keys`) | Keep / drop keys (supports `*` globs) |
| `prefix_keys` (`value`) / `suffix_keys` (`value`) | Prepend / append to all keys |

### Shape ops

| Op | Purpose |
|---|---|
| `drop_fields` / `keep_fields` (`fields`) | Remove / retain fields by name or glob |
| `add_field` (`field`, `value`) | Insert literal value |
| `split_field` (`field`, `separator`, `into`, `regex`) | `"John Smith"` → `{first:"John", last:"Smith"}` |
| `join_fields` (`fields`, `separator`, `into`) | Reverse of split |
| `coalesce` (`fields`, `into`) | First non-null wins |
| `to_object` (`keys`, `values`, `target`, `on_duplicate`) | Zip `columns[]` + `row[]` → `{col: val}`. Duplicate modes: `array` / `suffix` / `first` / `last` / `max` / `sum` / `error` |
| `unwrap_formulas` | Detect Excel `[value, "=formula"]` cells, take value |

### Compute / template / require

| Op | Purpose |
|---|---|
| `compute` (`expression`, `target`) | Evaluate expression DSL (like filter/route), write result to target |
| `template` (`template`, `from`, `target`) | `"{a}/{b}.ndjson"` + path-lookup map → rendered string |
| `require` (`fields`) | If any field null/empty on object at `path` → drop envelope + trace |

## Dict — lookup tables

```yaml
- op: dict
  path: v.columns
  map:                                  # inline
    "Сумма":         "amount"
    "Дата":          "date"
    "/^Fee \\d+$/":  "fee"              # regex key (slashes around pattern)
    "/^Column (\\d+)$/": "col_$1"       # capture group in replacement
  default: passthrough                  # passthrough | null | drop | "<literal>"
```

Or load from file:
```yaml
map: "$configs/dicts/categories.csv"     # .yaml / .json / .csv / .tsv
```

CSV format: two columns `key,value`, no header required. Best for 100k+ entries.

Default handling: `passthrough` keeps input, `null` replaces with null, `drop` removes the envelope, any string is treated as literal replacement.

Literal keys checked first (O(1) HashMap), regex fallback in declaration order.

## Template — dynamic path building

```yaml
- op: template
  template: "{stream}/{day}.ndjson"
  from:
    stream: v.doc_kind
    day:    v.row.date
  target:   v.file                      # → {stream}/{day}.ndjson written here
```

Placeholder substitution with string coercion; missing paths render as empty string. `{{` / `}}` escape literal braces.

Typical pattern: build `v.file` then feed to `write-file-stream` for per-envelope dynamic output paths:

```yaml
split-into-daily:
  tool: normalize
  settings:
    rulebook: "$configs/dispatch.rules.yaml"
  input: upstream
sink:
  tool: write-file-stream              # uses v.file per envelope, falls back to default_file
  settings: { default_file: "$output/unmapped.ndjson", format: ndjson }
  input: split-into-daily
```

## Compute — expression DSL

```yaml
- op: compute
  expression: "v.row.amount * (1 + v.row.vat_rate)"
  target:     v.row.total_with_vat
```

The expression DSL supports: paths (`v.a.b`), number/string/bool/null literals, `== != < <= > >=`, `&& || !`, arrays `[a, b]`, function calls (`lower`, `normalize`, `includes`, `startsWith`, `endsWith`, `length`, `empty`, `contains`, `matches`). See [expressions.md](../expressions.md).

## On-error policy

Each rule's error is handled according to:

| `on_error` | Behaviour |
|---|---|
| `trace` (default) | Write value=null, emit trace event, continue |
| `null` | Write value=null, continue silently |
| `passthrough` | Leave value unchanged, continue |
| `drop` | Drop envelope entirely |
| `error` | Emit stderr error event + drop |
| `quarantine` | Route to error sink + drop |

## Worked example — reconciliation + transactions pipeline

One normalize stage handles both file types via profiles:

```yaml
# $configs/normalize/master.rules.yaml
profiles:
  - when: "v.format == 'titled'"
    use:  "titled.rules.yaml"
  - when: "v.format == 'flat'"
    use:  "flat.rules.yaml"
```

```yaml
# $configs/normalize/titled.rules.yaml — Cyrillic headers → canonical
rules:
  - path: v.columns
    op: dict
    map:
      "Дата":             "date"
      "Оборот, EUR":      "turnover"
      "/^Вознаграждение.*/": "fee"
    default: passthrough
  - op: to_object
    keys: v.columns
    values: v.row
    target: v.row
    on_duplicate: suffix
  - path: v.row
    op: unwrap_formulas
  - path: v.row
    op: keep_fields
    fields: [date, turnover, fee, payout, debt]
  - path: v.row.date
    op: parse_date
    formats: ["%Y-%m-%d"]
    output: date
  - path: v.row
    op: require
    fields: [date]
  - path: v
    op: add_field
    field: doc_kind
    value: reconciliation
```

```yaml
# flat.rules.yaml — English headers → canonical, datetime + currency
rules:
  - path: v.columns
    op: dict
    map:
      "commission": "fee"
      "Amount":     "amount"
    default: passthrough
  - op: to_object
    keys: v.columns
    values: v.row
    target: v.row
    on_duplicate: error
  - path: v.row.date
    op: parse_date
    formats: ["%Y-%m-%d %H:%M:%S"]
    output: iso
  - path: v.row.currency
    op: normalize_currency
  - path: v.row
    op: require
    fields: [txn_id, date, amount, currency]
  - path: v
    op: add_field
    field: doc_kind
    value: transactions
```

Same stage, zero route splitting, different rulebooks per envelope type.

## Full reference

The full set of operations and settings shapes is exercised by **432 unit tests** in the crate. See `dpe-tool-normalize/src/` — each op module is self-contained and tested.

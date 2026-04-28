# normalize

DPE standard tool — row-level universal normaliser. Applies a declarative **rulebook** of ops to each envelope: dictionary replacement, type coercion, header renaming, shape transforms, dynamic path templating, require-field guards. Supports **profile dispatch** so different envelope types get different rulebooks in one stage.

## When to use

- Heterogeneous source data (mixed spreadsheet templates, multiple sources, messy headers, locale-specific number formats)
- One stage that handles multiple record types side-by-side, each with its own rules
- Cleaning, parsing, unit conversion, formula-cell unwrapping

## Settings

```yaml
normalize:
  tool: normalize
  settings:
    rulebook:     "$configs/normalize/master.rules.yaml"
    on_unmatched: passthrough     # passthrough | drop | error
    on_error:     trace           # trace | null | passthrough | drop | error | quarantine
```

The rulebook is a YAML file describing rules (flat or by profile). See full docs for the rule grammar (`dict`, `parse`, `rename`, `compute`, `template`, `require`, etc.).

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/normalize.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.

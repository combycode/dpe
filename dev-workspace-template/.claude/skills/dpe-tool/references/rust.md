# Rust runtime — DPE tool framework reference

Crate: `combycode-dpe` (path dep from the scaffolded `Cargo.toml`).

## Minimal tool (what the scaffold gives you)

```rust
use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

fn process_input(v: Value, settings: &Value, ctx: &mut Context) {
    // v is env.v (serde_json::Value); settings is argv[1] parsed.
    ctx.output(v, None, None);
}

fn main() {
    dpe_run! {
        input: process_input,
    };
}
```

## Context API

| Method | Purpose |
|---|---|
| `ctx.output(v, id, src)` | Emit data envelope. `id`/`src` are `Option<&str>`; `None` → inherit from input. |
| `ctx.meta(v)` | Emit meta envelope. |
| `ctx.error(v, err)` | Emit error event to stderr (envelope is NOT forwarded to stdout). |
| `ctx.log(msg, level)` | Log event. `level` = `"info"`, `"warn"`, `"error"`. |
| `ctx.trace(k, v)` | Attach label to the next output envelope's trace event. |
| `ctx.stats(obj)` | Emit stats event (numeric only; framework forwards to StatsCollector). |
| `ctx.emit(queue, v, id, src)` | Fire to internal queue (requires a `queues` param in `dpe_run!`). |
| `ctx.memory` | Shared typed accumulators across invocations. |

## Settings handling

Settings is `serde_json::Value`. Either access fields ad-hoc:

```rust
let marker = settings.get("marker").and_then(|v| v.as_str()).unwrap_or("");
```

Or deserialize into a typed struct (preferred for anything non-trivial):

```rust
#[derive(Deserialize)]
struct Settings {
    #[serde(default)] marker: String,
}

static SETTINGS: OnceLock<Settings> = OnceLock::new();

fn get_settings(v: &Value) -> &'static Settings {
    SETTINGS.get_or_init(|| serde_json::from_value(v.clone()).unwrap_or_default())
}

fn process_input(v: Value, settings: &Value, ctx: &mut Context) {
    let s = get_settings(settings);
    // use s.marker …
}
```

`OnceLock` ensures settings are parsed exactly once.

## Envelope value access

`v: Value` is `serde_json::Value`. Typical reads:

```rust
let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
let amount = v.get("amount").and_then(|x| x.as_f64()).unwrap_or(0.0);
```

Mutation:

```rust
let mut out = v.clone();  // or: take ownership if last use
if let Some(obj) = out.as_object_mut() {
    obj.insert("text".into(), Value::String(text.to_uppercase()));
}
ctx.output(out, None, None);
```

## Tests

Unit tests in `tests/` or `src/` (under `#[cfg(test)]`). Call `process_input` directly with a mock context isn't possible — the framework's Context constructor isn't public. Instead, test pure helpers (extract logic to standalone functions, test those) AND rely on verify cases for integration.

Example structure:

```rust
// src/main.rs
fn uppercase_text(input: &str, marker: &str) -> String {
    format!("{}{}", marker, input.to_uppercase())
}

fn process_input(v: Value, settings: &Value, ctx: &mut Context) {
    let s = get_settings(settings);
    let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
    let out = json!({ "text": uppercase_text(text, &s.marker) });
    ctx.output(out, None, None);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn plain() { assert_eq!(uppercase_text("hello", ""), "HELLO"); }
    #[test] fn with_marker() { assert_eq!(uppercase_text("hi", "UP:"), "UP:HI"); }
}
```

Run: `cargo test` (or `dpe-dev test .`).

## Build output

Release binary lives at `target/release/<tool-name>.exe` (Windows) or `target/release/<tool-name>` (Unix). `meta.json`'s `entry` field points at this.

## Common mistakes

- **Don't call `println!`** — it goes to stdout, mixing with real output. Use `ctx.log`.
- **Don't call `eprintln!`** — same reason, mixes with typed events. Use `ctx.log(msg, "error")`.
- **Don't return early without calling `ctx.output` or `ctx.error`** — the envelope vanishes silently, which is almost never what you want. Be explicit.

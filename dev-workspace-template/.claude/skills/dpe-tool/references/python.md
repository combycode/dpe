# Python runtime — DPE tool framework reference

Package: `combycode-dpe` (file:// dep from scaffolded `pyproject.toml`).

## Minimal tool (what the scaffold gives you)

```python
import dpe

def process_input(v, settings, ctx):
    ctx.output(v)

if __name__ == "__main__":
    dpe.run()
```

`dpe.run()` discovers functions named `process_input` and `process_<queue_name>` in the calling module automatically.

## Context API

| Method | Purpose |
|---|---|
| `ctx.output(v, *, id=None, src=None)` | Emit data envelope. id/src default to input's. |
| `ctx.meta(v)` | Emit meta envelope. |
| `ctx.error(v, err)` | Emit error to stderr (NOT forwarded to stdout). |
| `ctx.log(msg, level="info")` | Log event. Levels: info / warn / error. |
| `ctx.trace(key, value)` | Attach label to next output's trace event. |
| `ctx.stats(obj)` | Emit stats event (dict). |
| `ctx.emit(queue, v, *, id=None, src=None)` | Fire to internal queue (requires matching `process_<queue>` function). |
| `ctx.memory` | Shared typed accumulators across invocations. |

## Queues (optional)

```python
import dpe

def process_input(v, settings, ctx):
    ctx.emit("validate", v)

def process_validate(v, settings, ctx):
    if v.get("amount", 0) > 0:
        ctx.output(v)
    else:
        ctx.error(v, "non-positive amount")

if __name__ == "__main__":
    dpe.run()
```

## Settings handling

Settings is a `dict` (parsed JSON). Read at module level or per-call:

```python
import dpe

_settings_cache = None

def _cfg(settings):
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = {
            "marker": str(settings.get("marker", "")),
        }
    return _settings_cache

def process_input(v, settings, ctx):
    c = _cfg(settings)
    text = str(v.get("text", ""))
    ctx.output({**v, "text": c["marker"] + text.upper()})

if __name__ == "__main__":
    dpe.run()
```

## Tests — `pytest`

Place in `tests/test_<name>.py`:

```python
from <pkg_name>.main import uppercase_text  # extract pure helper

def test_uppercase_plain():
    assert uppercase_text("hello", "") == "HELLO"

def test_uppercase_with_marker():
    assert uppercase_text("hi", "UP:") == "UP:HI"
```

The scaffolded `pyproject.toml` has `pythonpath = ["src"]` under `[tool.pytest.ini_options]` so `from <pkg>.main import ...` works.

Package name = tool name with hyphens → underscores (e.g. `uppercase-text` → `uppercase_text`).

Run: `pytest` (or `dpe-dev test .`).

## Tool entry

`meta.json` points at `src/<pkg>/main.py`. Run via `python src/<pkg>/main.py` (or `dpe-dev build .` to do the editable-install first, then `python -m <pkg>.main` works too).

## Common mistakes

- **Don't use `print()`** — stdout mixes with real output. Use `ctx.log`.
- **Don't use `sys.stderr.write`** — stderr mixes with typed events. Use `ctx.log(msg, "error")`.
- **Don't forget `if __name__ == "__main__": dpe.run()`** — without it, the module imports but never processes.
- **Don't call `sys.exit()` mid-processing** — framework owns lifecycle. On EOF it drains queues and exits cleanly.
- **Don't raise in `process_input`** — framework catches exceptions and emits error events, but the envelope is lost silently. Better to catch explicitly and call `ctx.error(v, str(e))`.

## Package structure

After scaffold for `my-tool`:

```
my-tool/
  pyproject.toml
  meta.json
  src/
    my_tool/
      __init__.py
      main.py
  tests/
    test_basic.py
  verify/
    case-basic/
```

Keep logic under `src/my_tool/`. Tests go in `tests/` (not inside the package).

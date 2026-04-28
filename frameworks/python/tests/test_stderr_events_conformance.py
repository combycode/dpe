"""Conformance test: every stderr event the Python framework emits must
validate against ``runner/schemas/stderr-events.schema.json``. Cross-checked
with the Rust and TS conformance tests.

Strategy: capture stderr by redirecting ``sys.stderr`` to a string buffer for
the duration of each emitter call, then validate every line against the
schema. The framework's ``_envelope.write_*`` helpers flush after each write,
so a buffer suffices — no subprocess required.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import jsonschema
import pytest

from dpe import _envelope as env

SCHEMA_PATH = (
    Path(__file__).resolve().parents[3] / "runner" / "schemas" / "stderr-events.schema.json"
)


@pytest.fixture(scope="module")
def validator() -> jsonschema.Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(schema)


def capture_stderr(emit) -> list[dict]:
    """Run ``emit`` with sys.stderr redirected to a buffer, return parsed lines."""
    buf = io.StringIO()
    real = sys.stderr
    sys.stderr = buf
    try:
        emit()
    finally:
        sys.stderr = real
    out = []
    for line in buf.getvalue().splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def test_log_event_validates(validator):
    events = capture_stderr(lambda: env.write_log("hello world", "info"))
    assert len(events) == 1
    validator.validate(events[0])
    assert events[0]["type"] == "log"
    assert events[0]["level"] == "info"
    assert events[0]["msg"] == "hello world"


@pytest.mark.parametrize("level", ["debug", "info", "warn", "error"])
def test_log_event_each_level_validates(validator, level):
    events = capture_stderr(lambda: env.write_log("m", level))
    validator.validate(events[0])


def test_log_event_with_extra_fields_validates(validator):
    events = capture_stderr(lambda: env.write_log("m", "info", {"stage": "convert", "rows": 10}))
    validator.validate(events[0])
    assert events[0]["stage"] == "convert"
    assert events[0]["rows"] == 10


def test_error_event_validates(validator):
    events = capture_stderr(
        lambda: env.write_error({"k": "v"}, "boom", "id1", "src1")
    )
    assert len(events) == 1
    validator.validate(events[0])
    ev = events[0]
    assert ev["type"] == "error"
    assert ev["error"] == "boom"
    assert ev["input"] == {"k": "v"}
    assert ev["id"] == "id1"
    assert ev["src"] == "src1"


def test_trace_event_validates(validator):
    events = capture_stderr(
        lambda: env.write_trace("id1", "src1", {"stage": "convert", "tool": "doc-converter"})
    )
    assert len(events) == 1
    validator.validate(events[0])
    assert events[0]["type"] == "trace"


def test_trace_event_with_empty_labels_validates(validator):
    events = capture_stderr(lambda: env.write_trace("id1", "src1", {}))
    validator.validate(events[0])


def test_stats_event_validates(validator):
    events = capture_stderr(
        lambda: env.write_stats({"rows_in": 100, "rows_out": 95, "skipped": 5})
    )
    assert len(events) == 1
    validator.validate(events[0])
    assert events[0]["type"] == "stats"
    assert events[0]["rows_in"] == 100


def test_stats_event_with_no_extra_fields_validates(validator):
    events = capture_stderr(lambda: env.write_stats({}))
    validator.validate(events[0])


def test_schema_rejects_malformed_events(validator):
    """Sanity check the schema actually rejects bad shapes — guards against
    a permissive schema that would let drift slip through."""
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "log", "msg": "missing level"})
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "error", "error": "x"})
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "mystery"})

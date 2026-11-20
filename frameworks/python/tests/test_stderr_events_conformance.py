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


def test_error_extra_fields_and_bad_input_validate(validator):
    class QuotaError(Exception):
        pass

    class HttpError(Exception):
        code = 429

    def emit():
        extra = {"code": "E_BAD", "retryable": False, "details": {"n": 1}}
        env.write_error({"k": 1}, ValueError("bad"), "id1", "src1", extra)
        env.write_error({"k": 2}, HttpError("rate limited"), "id2", "src2")
        env.write_error({"k": 3}, QuotaError("over"), "id3", "src3")
        env.write_error({"k": 4}, Exception("plain"), "id4", "src4")
        env.write_error({"k": 5}, "a string", "id5", "src5", {"retryable": "yes"})
        env.write_bad_input("{not json" + "x" * 5000 + "\n", "invalid JSON")

    events = capture_stderr(emit)
    assert len(events) == 6
    for e in events:
        validator.validate(e)
    first = events[0]
    assert (first["code"], first["retryable"], first["details"]) == ("E_BAD", False, {"n": 1})
    assert events[1]["code"] == "429"          # from err.code
    assert events[2]["code"] == "QuotaError"   # from the class name
    assert "code" not in events[3]             # plain Exception: no code
    assert "code" not in events[4] and "retryable" not in events[4]
    assert len(events[5]["raw"]) == env.RAW_LINE_LIMIT
    assert "input" not in events[5]


def test_parse_input_line_classifies():
    assert env.parse_input_line("  \n") == ("empty", None)
    assert env.parse_input_line('{"t":"d","id":"a","src":"s","v":1}')[0] == "envelope"
    assert env.parse_input_line("{not json")[0] == "bad"
    assert env.parse_input_line("[1, 2]") == ("bad", "not a JSON object")


def test_schema_rejects_malformed_events(validator):
    """Sanity check the schema actually rejects bad shapes — guards against
    a permissive schema that would let drift slip through."""
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "log", "msg": "missing level"})
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "error", "error": "x"})
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "mystery"})


def test_bom_and_raising_code_property():
    assert env.parse_input_line(chr(0xFEFF) + '{"t":"d","id":"a","src":"s","v":1}')[0] == "envelope"

    class Weird(Exception):
        @property
        def code(self):
            raise RuntimeError("no code here")

    events = capture_stderr(lambda: env.write_error({}, Weird("w"), "id", "src"))
    assert events[0]["code"] == "Weird"   # falls back to the class name


def test_schema_rejects_bad_new_fields(validator):
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "error", "error": "x", "input": 1, "id": "a", "src": "b",
                            "retryable": "yes"})
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"type": "error", "error": "x", "raw": "y" * 4097})

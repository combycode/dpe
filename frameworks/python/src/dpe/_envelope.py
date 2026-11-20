"""NDJSON envelope handling — parse, create, hash utilities."""

import hashlib
import json
import sys
from typing import Any


def hash_string(key: str) -> str:
    """Hash a string. Returns 16-char hex (8 bytes blake2b)."""
    return hashlib.blake2b(key.encode("utf-8"), digest_size=8).hexdigest()


def hash_file(filepath: str, algorithm: str = "blake2b", chunk_size: int = 65536) -> str | None:
    """Hash file content in chunks. Returns hex string or None on error.

    Args:
        filepath: Path to file
        algorithm: 'blake2b' (default) or 'xxhash' (requires xxhash package)
        chunk_size: Read buffer size (default 64KB)
    """
    # Type: hashlib.blake2b OR xxhash.xxh3_64. Both share .update(bytes)
    # and .hexdigest() so we treat as Any to avoid a structural-protocol
    # gymnastic for two short-circuit branches.
    h: Any
    try:
        if algorithm == "xxhash":
            try:
                # xxhash is an optional runtime dep (see fallback). Need
                # both codes here: `import-not-found` fires when xxhash isn't
                # installed (CI), `unused-ignore` fires when it IS installed
                # (some dev machines). Listing both keeps mypy happy in both
                # environments without disabling the warning globally.
                import xxhash  # type: ignore[import-not-found,unused-ignore]
                h = xxhash.xxh3_64()
            except ImportError:
                h = hashlib.blake2b(digest_size=16)
        else:
            h = hashlib.blake2b(digest_size=16)

        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return h.hexdigest()
    except (PermissionError, FileNotFoundError, OSError):
        return None


def compute_id(src: str, stage: str, v) -> str:
    """Compute envelope id from src + stage + v content."""
    v_str = json.dumps(v, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    key = f"{src}|{stage}|{v_str}"
    return hash_string(key)


def parse_envelope(line: str) -> dict | None:
    """Parse a single NDJSON line into envelope dict. Returns None on parse error."""
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def write_data(v, id: str, src: str):
    """Write a data envelope to stdout."""
    _write_stdout({"t": "d", "id": id, "src": src, "v": v})


def write_meta(v: dict):
    """Write a metadata envelope to stdout."""
    _write_stdout({"t": "m", "v": v})


def write_log(msg: str, level: str, extra: dict | None = None):
    """Write structured log to stderr."""
    record: dict = {"type": "log", "level": level, "msg": msg}
    if extra:
        record.update(extra)
    _write_stderr(record)


RAW_LINE_LIMIT = 4096
"""Longest start of an unparseable input line kept in its error event."""


def parse_input_line(line: str) -> tuple[str, Any]:
    """Classify one stdin line for the runtime's input loop.

    Returns ("empty", None), ("envelope", dict) or ("bad", reason) - "bad" when the line is
    not valid JSON or not a JSON object (such a line used to be skipped, or crashed the loop).
    """
    line = line.strip()
    if not line:
        return "empty", None
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as e:
        return "bad", f"invalid JSON ({e})"
    if not isinstance(obj, dict):
        return "bad", "not a JSON object"
    return "envelope", obj


def _error_code(err) -> str | None:
    """`err.code` when it is a string / number, else the exception's class name.

    A plain `Exception` gives no code.
    """
    code = getattr(err, "code", None)
    if isinstance(code, (str, int, float)) and not isinstance(code, bool):
        return str(code)
    if isinstance(err, BaseException) and type(err) is not Exception:
        return type(err).__name__
    return None


def write_error(v, err, id: str, src: str, extra: dict | None = None):
    """Write error to stderr with original input preserved.

    `extra` may carry `code`, `retryable` and `details`; the runner puts them on the stage's
    `errors` port. `code` defaults to `err.code`, else the exception's class name.
    """
    record: dict = {
        "type": "error",
        "error": str(err),
        "input": v,
        "id": id,
        "src": src,
    }
    extra = extra or {}
    code = str(extra["code"]) if extra.get("code") is not None else _error_code(err)
    if code is not None:
        record["code"] = code
    if isinstance(extra.get("retryable"), bool):
        record["retryable"] = extra["retryable"]
    if "details" in extra:
        record["details"] = extra["details"]
    _write_stderr(record)


def write_bad_input(line: str, reason: str):
    """An input line that is not an envelope: reported instead of being skipped silently."""
    _write_stderr({
        "type": "error",
        "error": f"unparseable input line: {reason}",
        "raw": line.rstrip("\r\n")[:RAW_LINE_LIMIT],
    })


def write_trace(id: str, src: str, labels: dict, channel: str | None = None):
    """Write merged trace event to stderr. Called by ctx before each output.

    `channel` distinguishes counter buckets at the runner side:
        - "data" from ctx.output  -> rows_out
        - "meta" from ctx.meta    -> meta
    None is written without a `channel` key (back-compat: runner treats
    unset as data).
    """
    record: dict = {"type": "trace", "id": id, "src": src, "labels": labels}
    if channel is not None:
        record["channel"] = channel
    _write_stderr(record)


def write_input(id: str, src: str):
    """Write input event to stderr. Emitted by the runtime per envelope read
    from stdin, BEFORE the processor runs. Lets the runner count rows_in
    for stages that never call ctx.output() (pass-through, terminal sinks).
    """
    _write_stderr({"type": "input", "id": id, "src": src})


def write_stats(data: dict):
    """Write a stats event to stderr."""
    record = {"type": "stats"}
    record.update(data)
    _write_stderr(record)


def _write_stdout(obj: dict):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _write_stderr(obj: dict):
    sys.stderr.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stderr.flush()

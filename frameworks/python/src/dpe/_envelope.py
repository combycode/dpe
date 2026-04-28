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


def write_error(v, err, id: str, src: str):
    """Write error to stderr with original input preserved."""
    record = {
        "type": "error",
        "error": str(err),
        "input": v,
        "id": id,
        "src": src,
    }
    _write_stderr(record)


def write_trace(id: str, src: str, labels: dict):
    """Write merged trace event to stderr. Called by ctx before each output."""
    _write_stderr({"type": "trace", "id": id, "src": src, "labels": labels})


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

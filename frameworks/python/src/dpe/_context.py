"""Context object — passed to every processor invocation."""

from collections.abc import Callable
from typing import Any, TypeVar

from dpe._accumulators import Memory
from dpe._cache import cached_impl
from dpe._envelope import (
    hash_file,
    hash_string,
    write_data,
    write_error,
    write_log,
    write_meta,
    write_stats,
    write_trace,
)
from dpe._paths import EnvPaths

T = TypeVar("T")


class Context:
    """Processing context created per invocation.

    Provides framework API: output, emit, drain, meta, log, error, trace, stats, hash.
    Each processor call gets its own ctx with current id/src.
    Memory is shared across all processors in the tool.
    """

    __slots__ = ("id", "src", "memory", "_runtime", "_paths", "_labels")

    def __init__(self, id: str, src: str, memory: Memory, runtime,
                 paths: EnvPaths | None = None):
        self.id = id
        self.src = src
        self.memory = memory
        self._runtime = runtime
        self._paths = paths if paths is not None else EnvPaths()
        # Labels accumulated by ctx.trace(k, v). Flushed as one merged
        # {type:"trace"} stderr event before each ctx.output(), then cleared.
        self._labels: dict = {}

    def trace(self, key: str, value):
        """Attach a label to this invocation's next output envelope.

        Call as many times as needed between ctx.output() calls. The
        accumulated labels are flushed once (merged) as a {type:"trace"}
        event to stderr just before ctx.output() writes the envelope,
        then cleared.
        """
        self._labels[key] = value

    def stats(self, **kwargs):
        """Emit a stats event to stderr. Example: ctx.stats(rows=1000, rps=250)."""
        if kwargs:
            write_stats(kwargs)

    def output(self, v, *, id: str | None = None, src: str | None = None):
        """Emit data record to stdout.

        Flushes accumulated trace labels as a merged trace event first
        (channel="data" -> counts as rows_out at the runner), then writes
        the envelope, then clears the label bag.
        Absolute paths in v are reverse-tokenized to $token/... form.
        """
        out_id = id if id is not None else self.id
        out_src = src if src is not None else self.src
        # Emit trace even with empty labels -- the chain row itself is the value.
        write_trace(out_id, out_src, self._labels, channel="data")
        self._labels = {}
        write_data(self._paths.tokenize_value(v), out_id, out_src)

    def emit(self, queue: str, v, *, id: str | None = None, src: str | None = None):
        """Emit to internal named queue. Processed by process_<queue>().

        Args:
            queue: Queue name (maps to process_<queue> function)
            v: Payload data
            id: Override id. Defaults to current ctx.id.
            src: Override src. Defaults to current ctx.src.
        """
        item_id = id if id is not None else self.id
        item_src = src if src is not None else self.src
        self._runtime.enqueue(queue, v, item_id, item_src)

    def drain(self):
        """Block until all queued items are processed.

        After drain returns, all accumulators reflect completed processing.
        Can be called multiple times within one processor call.
        """
        self._runtime.drain_queue()

    def meta(self, v: dict):
        """Emit metadata record to stdout.

        Also emits a {type:"trace", channel:"meta"} stderr event so the
        runner can increment its per-stage `meta` counter. Inherits
        ctx id/src. Absolute paths in v are reverse-tokenized to $token/... form.
        """
        write_trace(self.id, self.src, {}, channel="meta")
        write_meta(self._paths.tokenize_value(v))

    def log(self, msg: str, *, level: str = "info", **extra):
        """Write structured log to stderr."""
        write_log(msg, level, extra if extra else None)

    def error(self, v, err):
        """Write error to stderr with original input preserved."""
        write_error(v, err, self.id, self.src)

    def hash(self, key: str) -> str:
        """Hash a string. Returns 16-char hex."""
        return hash_string(key)

    def hash_file(self, filepath: str, algorithm: str = "blake2b") -> str | None:
        """Hash file content in chunks. Returns hex string or None on error."""
        return hash_file(filepath, algorithm)

    def cached(
        self,
        namespace: str,
        key: Any,
        produce: Callable[[], T],
    ) -> T:
        """Cache the result of `produce()` under
        `$DPE_STORAGE/<namespace>/<hash>.json`.

        Honors the runner's `DPE_CACHE_MODE` env:
          - use      (default) -- read cache if present, else produce + write
          - refresh  -- always produce, overwrite cache
          - bypass   -- produce, skip both read and write
          - off      -- same as bypass

        `key` is canonical-JSON-hashed (blake2b, first 32 hex chars).
        Compose it from whatever determines output equivalence.

        If `$DPE_STORAGE` isn't set (e.g. tool invoked outside a
        pipeline), cache is silently disabled — every call produces.

        Producer errors propagate; failed runs do NOT poison the cache.

        Example:
            result = ctx.cached(
                "doc-converter",
                {"file_hash": ctx.hash_file(v["path"]),
                 "settings": settings,
                 "page": page_idx},
                lambda: provider.convert_page(...),
            )
            ctx.output(result)
        """
        return cached_impl(namespace, key, produce)

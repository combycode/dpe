"""Context object — passed to every processor invocation."""

from dpe._accumulators import Memory
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


class Context:
    """Processing context created per invocation.

    Provides framework API: output, emit, drain, meta, log, error, trace, stats, hash.
    Each processor call gets its own ctx with current id/src.
    Memory is shared across all processors in the tool.
    """

    __slots__ = ("id", "src", "memory", "_runtime", "_labels")

    def __init__(self, id: str, src: str, memory: Memory, runtime):
        self.id = id
        self.src = src
        self.memory = memory
        self._runtime = runtime
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

        Flushes accumulated trace labels as a merged trace event first,
        then writes the envelope, then clears the label bag.
        """
        out_id = id if id is not None else self.id
        out_src = src if src is not None else self.src
        # Emit trace even with empty labels — the chain row itself is the value.
        write_trace(out_id, out_src, self._labels)
        self._labels = {}
        write_data(v, out_id, out_src)

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
        """Emit metadata record to stdout."""
        write_meta(v)

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

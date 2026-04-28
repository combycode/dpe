"""Runtime — main loop, discovery, queue management, signal handling."""

import inspect
import json
import signal
import sys
from collections import deque
from collections.abc import Callable

from dpe._accumulators import Memory
from dpe._context import Context
from dpe._envelope import parse_envelope, write_log


class Runtime:
    """Core runtime engine. Manages stdin loop, queue, and processor dispatch."""

    def __init__(self):
        self.settings: dict = {}
        self.memory = Memory()
        self.input_fn: Callable | None = None
        self.processors: dict[str, Callable] = {}
        self._queue: deque = deque()
        self._shutdown = False

    def enqueue(self, queue_name: str, v, id: str, src: str):
        """Add item to internal queue."""
        self._queue.append((queue_name, v, id, src))

    def drain_queue(self):
        """Process all queued items until empty. Queue processors may emit more items."""
        max_iterations = 100_000
        iterations = 0

        while self._queue and iterations < max_iterations:
            name, v, item_id, item_src = self._queue.popleft()

            proc = self.processors.get(name)
            if proc is None:
                write_log(f"No processor for queue '{name}', dropping item", "warn")
                continue

            ctx = Context(item_id, item_src, self.memory, self)
            try:
                proc(v, self.settings, ctx)
            except Exception as e:
                ctx.error(v, e)

            iterations += 1

        if iterations >= max_iterations:
            write_log(f"Queue drain hit safety limit ({max_iterations})", "error")

    def create_ctx(self, id: str, src: str) -> Context:
        return Context(id, src, self.memory, self)

    def _handle_signal(self, signum, frame):
        self._shutdown = True

    def run(self, caller_module):
        """Main entry point. Discovers processors, reads stdin, dispatches."""
        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Discover processors
        self.input_fn, self.processors = _discover_processors(caller_module)
        if self.input_fn is None:
            write_log("No process_input function found in tool module", "error")
            sys.exit(1)

        # Parse settings
        self.settings = _parse_settings()

        # Main stdin loop
        for line in sys.stdin:
            if self._shutdown:
                break

            envelope = parse_envelope(line)
            if envelope is None:
                continue

            # Skip non-data lines
            if envelope.get("t") != "d":
                continue

            id = envelope.get("id", "")
            src = envelope.get("src", "")
            v = envelope.get("v", {})

            ctx = self.create_ctx(id, src)
            try:
                self.input_fn(v, self.settings, ctx)
            except Exception as e:
                ctx.error(v, e)

            # Auto-drain if queue has items (process_input didn't call drain)
            if self._queue:
                self.drain_queue()

        # stdin EOF or SIGTERM: drain remaining queue
        if self._queue:
            self.drain_queue()


def _discover_processors(module) -> tuple[Callable | None, dict[str, Callable]]:
    """Find process_input and process_<name> functions in the tool module."""
    input_fn: Callable | None = None
    queue_fns: dict[str, Callable] = {}

    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if not name.startswith("process_"):
            continue
        # Only discover functions defined in the module itself, not imports
        if getattr(obj, "__module__", None) != module.__name__:
            continue

        suffix = name[len("process_"):]
        if suffix == "input":
            input_fn = obj
        else:
            queue_fns[suffix] = obj

    return input_fn, queue_fns


def _parse_settings() -> dict:
    """Parse settings from argv[1] JSON string."""
    if len(sys.argv) < 2:
        return {}
    try:
        return json.loads(sys.argv[1])
    except (json.JSONDecodeError, IndexError):
        write_log("Failed to parse settings from argv[1], using empty dict", "warn")
        return {}

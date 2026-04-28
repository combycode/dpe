"""
DPE — Data Processing Engine framework.

Build streaming pipeline tools with minimal boilerplate.

Usage:
    import dpe

    def process_input(v, settings, ctx):
        ctx.output({"result": transform(v)})
        ctx.emit("validate", {"data": v})

    def process_validate(v, settings, ctx):
        if ok(v):
            ctx.output(v)
        else:
            ctx.error(v, "validation failed")

    if __name__ == "__main__":
        dpe.run()
"""

import inspect

from dpe._accumulators import (
    Average,
    BitMask,
    Boolean,
    Buffer,
    Map,
    Memory,
    MinMax,
    Number,
    Set,
    Trigger,
)
from dpe._context import Context
from dpe._envelope import hash_file, hash_string
from dpe._runtime import Runtime


def run():
    """Start the tool processing loop.

    Discovers process_input and process_<queue> functions in the calling module,
    parses settings from argv[1], reads stdin line by line, and dispatches.

    Call this at the bottom of your tool:
        if __name__ == "__main__":
            dpe.run()
    """
    frame = inspect.stack()[1]
    caller_module = inspect.getmodule(frame[0])
    if caller_module is None:
        raise RuntimeError("Cannot discover caller module")

    runtime = Runtime()
    runtime.run(caller_module)


__all__ = [
    "run",
    "hash_string",
    "hash_file",
    "Context",
    "Memory",
    "Number",
    "Average",
    "MinMax",
    "Set",
    "Map",
    "Buffer",
    "Boolean",
    "BitMask",
    "Trigger",
]

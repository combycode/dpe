"""Cache helper used by Context.cached(). Mirrors the TS framework's
cache.ts behavior — same DPE_CACHE_MODE semantics, same on-disk layout
($DPE_STORAGE/<namespace>/<hash>.json), same canonical-JSON key
hashing.

Failure modes (cache-disabling, NOT errors propagated to user):
  - DPE_STORAGE not set     -> cache disabled, every call produces
  - cache file unreadable   -> treat as miss, log warn
  - cache file unparseable  -> treat as miss, log warn
  - producer raises         -> re-raise to caller (no cache write)
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from typing import Any, TypeVar

from dpe._envelope import write_log

T = TypeVar("T")

CACHE_MODES = ("use", "refresh", "bypass", "off")


def read_cache_mode() -> str:
    """Read DPE_CACHE_MODE; default to "use". Unrecognized -> "use"."""
    v = os.getenv("DPE_CACHE_MODE", "use")
    return v if v in CACHE_MODES else "use"


def cache_path(namespace: str, key: Any) -> str | None:
    """Compute the on-disk path for (namespace, key). Returns None when
    DPE_STORAGE isn't set -- caller treats that as cache disabled.
    """
    storage = os.getenv("DPE_STORAGE")
    if not storage:
        return None
    canonical = json.dumps(key, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    h = hashlib.blake2b(canonical.encode("utf-8")).hexdigest()[:32]
    return os.path.join(storage, namespace, f"{h}.json")


def cached_impl(
    namespace: str,
    key: Any,
    produce: Callable[[], T],
) -> T:
    """Read-or-produce cache around `produce`. Honors the four cache
    modes. Returns the produced or cached value."""
    mode = read_cache_mode()
    path = cache_path(namespace, key)

    can_read = path is not None and mode in ("use", "refresh")
    can_write = path is not None and mode not in ("bypass", "off")
    will_read = can_read and mode != "refresh"

    if will_read and path is not None and os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                value = json.load(f)
            write_log(f"cached: hit ({namespace})", "debug")
            return value
        except (OSError, json.JSONDecodeError) as e:
            write_log(f"cached: read failed ({namespace}) — {e}", "warn")
            # Fall through to produce.

    result = produce()

    if can_write and path is not None:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
        except OSError as e:
            write_log(f"cached: write failed ({namespace}) — {e}", "warn")
            # Don't fail the caller — they got their value.

    return result

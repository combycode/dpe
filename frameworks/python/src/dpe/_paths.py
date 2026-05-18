"""EnvPaths — resolves $token path prefixes in envelope v on input;
reverse-tokenizes absolute paths back to $token form on output.

Input side (runtime, before processor): $token/subpath -> absolute path
Output side (context.output / context.meta): absolute path -> $token/subpath

Both sides are a no-op when the DPE_* env vars are not set.
"""

import os
from typing import Any

_TOKEN_MAP = [
    ("input",   "DPE_INPUT"),
    ("output",  "DPE_OUTPUT"),
    ("configs", "DPE_CONFIGS"),
    ("storage", "DPE_STORAGE"),
    ("temp",    "DPE_TEMP"),
    ("session", "DPE_SESSION"),
]


class EnvPaths:
    """Resolved env prefix map.  Build once at tool startup."""

    __slots__ = ("_entries",)

    def __init__(self, pairs: list[tuple[str, str]] | None = None):
        """
        If *pairs* is None the standard DPE_* env vars are read.
        Pass explicit pairs for tests: ``[("input", "/abs/input"), ...]``.

        Prefer the named factories :meth:`from_env` and :meth:`from_pairs`
        -- they're consistent with the Rust SDK (``from_env()`` /
        ``from_pairs(&[...])``) and read more clearly at call sites. The
        polymorphic constructor is kept for backwards compat.
        """
        if pairs is None:
            raw = []
            for token, env_var in _TOKEN_MAP:
                val = os.environ.get(env_var, "")
                if val:
                    raw.append((token, val.replace("\\", "/")))
        else:
            raw = [(t, a.replace("\\", "/")) for t, a in pairs]
        # Sort longest-abs-first for greedy tokenization.
        self._entries: list[tuple[str, str]] = sorted(
            raw, key=lambda e: len(e[1]), reverse=True
        )

    @classmethod
    def from_env(cls) -> "EnvPaths":
        """Read the standard DPE_* env vars; missing/empty vars are skipped."""
        return cls()

    @classmethod
    def from_pairs(cls, pairs: list[tuple[str, str]]) -> "EnvPaths":
        """Construct from explicit (token, abs_path) pairs -- useful for tests."""
        return cls(pairs)

    def is_empty(self) -> bool:
        return not self._entries

    def resolve_value(self, v: Any) -> Any:
        """Walk v, resolving $token[/subpath] strings to absolute paths."""
        if self.is_empty():
            return v
        return self._walk(v, self._resolve_str)

    def tokenize_value(self, v: Any) -> Any:
        """Walk v, tokenizing absolute paths back to $token[/subpath] form."""
        if self.is_empty():
            return v
        return self._walk(v, self._tokenize_str)

    def _walk(self, v: Any, f) -> Any:
        if isinstance(v, str):
            return f(v)
        if isinstance(v, list):
            return [self._walk(x, f) for x in v]
        if isinstance(v, dict):
            return {k: self._walk(val, f) for k, val in v.items()}
        return v

    def _resolve_str(self, s: str) -> str:
        if not s.startswith("$"):
            return s
        slash = s.find("/")
        if slash < 0:
            name, tail = s[1:], ""
        else:
            name, tail = s[1:slash], s[slash:]
        for token, abs_path in self._entries:
            if token == name:
                return abs_path + tail if tail else abs_path
        return s

    def _tokenize_str(self, s: str) -> str:
        normalized = s.replace("\\", "/")
        for token, abs_path in self._entries:
            if normalized.startswith(abs_path):
                rest = normalized[len(abs_path):]
                if not rest:
                    return f"${token}"
                if rest.startswith("/"):
                    return f"${token}{rest}"
                # rest doesn't start with "/" -- partial component match, skip
        return s

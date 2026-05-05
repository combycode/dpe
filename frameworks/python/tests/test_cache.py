"""Tests for the cache helper. Mirror coverage of the TS suite."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from dpe._cache import cache_path, cached_impl, read_cache_mode


@pytest.fixture
def storage_dir(monkeypatch):
    """Set DPE_STORAGE to a tempdir for the test, clear DPE_CACHE_MODE."""
    with tempfile.TemporaryDirectory(prefix="dpe-cache-test-") as d:
        monkeypatch.setenv("DPE_STORAGE", d)
        monkeypatch.delenv("DPE_CACHE_MODE", raising=False)
        yield d


# ─── read_cache_mode ─────────────────────────────────────────────────────


def test_read_cache_mode_default(monkeypatch):
    monkeypatch.delenv("DPE_CACHE_MODE", raising=False)
    assert read_cache_mode() == "use"


@pytest.mark.parametrize("m", ["use", "refresh", "bypass", "off"])
def test_read_cache_mode_recognized(monkeypatch, m):
    monkeypatch.setenv("DPE_CACHE_MODE", m)
    assert read_cache_mode() == m


def test_read_cache_mode_garbage(monkeypatch):
    monkeypatch.setenv("DPE_CACHE_MODE", "nonsense")
    assert read_cache_mode() == "use"


# ─── cache_path ──────────────────────────────────────────────────────────


def test_cache_path_none_when_no_storage(monkeypatch):
    monkeypatch.delenv("DPE_STORAGE", raising=False)
    assert cache_path("ns", {"k": 1}) is None


def test_cache_path_key_order_stable(storage_dir):
    a = cache_path("ns", {"k": 1, "m": 2})
    b = cache_path("ns", {"m": 2, "k": 1})
    assert a == b


def test_cache_path_different_keys(storage_dir):
    assert cache_path("ns", {"k": 1}) != cache_path("ns", {"k": 2})


def test_cache_path_different_namespaces(storage_dir):
    assert cache_path("ns1", {"k": 1}) != cache_path("ns2", {"k": 1})


# ─── cached_impl ─────────────────────────────────────────────────────────


def test_miss_calls_produce_writes_file(storage_dir):
    calls = [0]

    def produce():
        calls[0] += 1
        return {"hello": "world"}

    result = cached_impl("ns", {"k": 1}, produce)
    assert result == {"hello": "world"}
    assert calls[0] == 1
    assert Path(cache_path("ns", {"k": 1})).is_file()


def test_hit_skips_produce(storage_dir):
    # Prime via miss to create the dir.
    cached_impl("ns", {"k": 1}, lambda: {"initial": True})
    # Overwrite with our seeded value.
    Path(cache_path("ns", {"k": 1})).write_text(json.dumps({"cached": True}))

    calls = [0]

    def produce():
        calls[0] += 1
        return {"fresh": True}

    result = cached_impl("ns", {"k": 1}, produce)
    assert result == {"cached": True}
    assert calls[0] == 0


def test_refresh_always_produces_overwrites(storage_dir, monkeypatch):
    cached_impl("ns", {"k": 1}, lambda: {"first": True})
    monkeypatch.setenv("DPE_CACHE_MODE", "refresh")

    calls = [0]

    def produce():
        calls[0] += 1
        return {"fresh": True}

    result = cached_impl("ns", {"k": 1}, produce)
    assert result == {"fresh": True}
    assert calls[0] == 1
    on_disk = json.loads(Path(cache_path("ns", {"k": 1})).read_text())
    assert on_disk == {"fresh": True}


def test_bypass_no_read_no_write(storage_dir, monkeypatch):
    cached_impl("ns", {"k": 1}, lambda: {"initial": True})
    monkeypatch.setenv("DPE_CACHE_MODE", "bypass")

    result = cached_impl("ns", {"k": 1}, lambda: {"fresh": True})
    assert result == {"fresh": True}
    on_disk = json.loads(Path(cache_path("ns", {"k": 1})).read_text())
    # bypass does NOT write — original cache content unchanged.
    assert on_disk == {"initial": True}


def test_off_same_as_bypass(storage_dir, monkeypatch):
    cached_impl("ns", {"k": 1}, lambda: {"initial": True})
    monkeypatch.setenv("DPE_CACHE_MODE", "off")
    result = cached_impl("ns", {"k": 1}, lambda: {"fresh": True})
    assert result == {"fresh": True}
    on_disk = json.loads(Path(cache_path("ns", {"k": 1})).read_text())
    assert on_disk == {"initial": True}


def test_no_storage_disables_cache(monkeypatch):
    monkeypatch.delenv("DPE_STORAGE", raising=False)
    monkeypatch.delenv("DPE_CACHE_MODE", raising=False)

    calls = [0]

    def produce():
        calls[0] += 1
        return {"n": calls[0]}

    r1 = cached_impl("ns", {"k": 1}, produce)
    r2 = cached_impl("ns", {"k": 1}, produce)
    assert r1 == {"n": 1}
    assert r2 == {"n": 2}


def test_producer_error_propagates_no_write(storage_dir):
    attempts = [0]

    def boom():
        attempts[0] += 1
        raise RuntimeError("kaboom")

    with pytest.raises(RuntimeError, match="kaboom"):
        cached_impl("ns", {"k": 1}, boom)
    assert attempts[0] == 1
    assert not Path(cache_path("ns", {"k": 1})).is_file()


def test_malformed_cache_treated_as_miss(storage_dir):
    cached_impl("ns", {"k": 1}, lambda: {"ok": True})
    Path(cache_path("ns", {"k": 1})).write_text("this is not json")

    calls = [0]

    def produce():
        calls[0] += 1
        return {"recovered": True}

    result = cached_impl("ns", {"k": 1}, produce)
    assert result == {"recovered": True}
    assert calls[0] == 1

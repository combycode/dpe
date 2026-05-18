"""Tests for EnvPaths -- $token resolve/tokenize round-trip."""

from dpe._paths import EnvPaths


def paths():
    return EnvPaths([
        ("input", "/abs/input"),
        ("output", "/abs/output"),
        ("storage", "/abs/storage"),
        # Intentionally shorter abs -- tests longest-match logic.
        ("session", "/abs/storage/session"),
    ])


# --- resolve_value ---

class TestResolveValue:
    def test_top_level_token(self):
        assert paths().resolve_value("$input") == "/abs/input"

    def test_token_with_subpath(self):
        assert paths().resolve_value("$input/data/file.csv") == "/abs/input/data/file.csv"

    def test_unknown_token_unchanged(self):
        assert paths().resolve_value("$set/field") == "$set/field"
        assert paths().resolve_value("$bogus") == "$bogus"

    def test_recurses_into_dict(self):
        v = {"path": "$input/a.csv", "n": 42, "nested": {"p": "$output/b"}}
        r = paths().resolve_value(v)
        assert r["path"] == "/abs/input/a.csv"
        assert r["n"] == 42
        assert r["nested"]["p"] == "/abs/output/b"

    def test_recurses_into_list(self):
        v = ["$input/x", "$output/y", "plain"]
        r = paths().resolve_value(v)
        assert r == ["/abs/input/x", "/abs/output/y", "plain"]

    def test_no_env_is_noop(self):
        empty = EnvPaths([])
        v = {"p": "$input/foo"}
        assert empty.resolve_value(v) == v

    def test_non_string_types_unchanged(self):
        v = {"n": 42, "b": True, "nil": None, "p": "$input"}
        r = paths().resolve_value(v)
        assert r["n"] == 42
        assert r["b"] is True
        assert r["nil"] is None
        assert r["p"] == "/abs/input"


# --- tokenize_value ---

class TestTokenizeValue:
    def test_exact_prefix(self):
        assert paths().tokenize_value("/abs/input") == "$input"

    def test_prefix_with_subpath(self):
        assert paths().tokenize_value("/abs/output/results/out.csv") == "$output/results/out.csv"

    def test_partial_component_not_replaced(self):
        # "/abs/inputXYZ" must NOT match "$input"
        assert paths().tokenize_value("/abs/inputXYZ") == "/abs/inputXYZ"

    def test_longest_prefix_wins(self):
        # "/abs/storage/session/..." should match "$session", not "$storage"
        assert paths().tokenize_value("/abs/storage/session/data.json") == "$session/data.json"

    def test_non_path_unchanged(self):
        assert paths().tokenize_value("hello world") == "hello world"
        assert paths().tokenize_value(42) == 42
        assert paths().tokenize_value(True) is True
        assert paths().tokenize_value(None) is None

    def test_recurses_into_nested(self):
        v = {"a": {"b": "/abs/input/x"}, "arr": ["/abs/output/y"]}
        r = paths().tokenize_value(v)
        assert r["a"]["b"] == "$input/x"
        assert r["arr"][0] == "$output/y"


# --- round-trip ---

class TestRoundTrip:
    def test_resolve_then_tokenize(self):
        p = EnvPaths([("input", "/data/in"), ("output", "/data/out")])
        original = {"src": "$input/file.csv", "dst": "$output/result.csv"}
        resolved = p.resolve_value(original)
        assert resolved == {"src": "/data/in/file.csv", "dst": "/data/out/result.csv"}
        assert p.tokenize_value(resolved) == original

    def test_windows_backslash_in_abs_normalised(self):
        p = EnvPaths([("data", r"C:\Data\proj")])
        assert p.resolve_value("$data/sub") == "C:/Data/proj/sub"


# --- from env vars ---

class TestFromEnv:
    def test_reads_dpe_input(self, monkeypatch):
        monkeypatch.setenv("DPE_INPUT", "/mnt/input")
        monkeypatch.delenv("DPE_OUTPUT", raising=False)
        p = EnvPaths()
        assert p.resolve_value("$input/file.csv") == "/mnt/input/file.csv"
        assert p.resolve_value("$output/x") == "$output/x"  # not set

    def test_missing_vars_empty(self, monkeypatch):
        for v in ["DPE_INPUT", "DPE_OUTPUT", "DPE_CONFIGS",
                  "DPE_STORAGE", "DPE_TEMP", "DPE_SESSION"]:
            monkeypatch.delenv(v, raising=False)
        p = EnvPaths()
        assert p.is_empty()
        assert p.resolve_value("$input/foo") == "$input/foo"


# --- named factories (mirror Rust SDK) ---

class TestNamedFactories:
    def test_from_env_reads_dpe_vars(self, monkeypatch):
        monkeypatch.setenv("DPE_INPUT", "/mnt/x")
        monkeypatch.delenv("DPE_OUTPUT", raising=False)
        p = EnvPaths.from_env()
        assert p.resolve_value("$input/file") == "/mnt/x/file"

    def test_from_pairs_bypasses_env(self, monkeypatch):
        monkeypatch.setenv("DPE_INPUT", "/ignored")
        p = EnvPaths.from_pairs([("input", "/explicit")])
        assert p.resolve_value("$input/file") == "/explicit/file"

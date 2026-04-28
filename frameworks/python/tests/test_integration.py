"""Integration tests — full tool lifecycle via subprocess."""

import json
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path


def run_tool_code(code: str, settings: dict, stdin_lines: list[dict],
                  timeout: int = 30) -> tuple[list[dict], list[dict], list[dict]]:
    """Write tool code to temp file, run it, return (data, meta, stderr_records)."""
    stdin_data = "\n".join(json.dumps(r) for r in stdin_lines) + "\n" if stdin_lines else ""

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        delete=False,
        dir=tempfile.gettempdir(),
        encoding="utf-8",
    ) as f:
        f.write(textwrap.dedent(code))
        f.flush()
        tool_path = f.name

    result = subprocess.run(
        [sys.executable, tool_path, json.dumps(settings)],
        input=stdin_data,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    Path(tool_path).unlink(missing_ok=True)

    data, meta = [], []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        record = json.loads(line)
        if record.get("t") == "d":
            data.append(record)
        elif record.get("t") == "m":
            meta.append(record)

    stderr_records = []
    for line in result.stderr.strip().split("\n"):
        if not line:
            continue
        try:
            stderr_records.append(json.loads(line))
        except json.JSONDecodeError:
            pass

    return data, meta, stderr_records


def inp(v: dict, id: str = "test-id", src: str = "test-src") -> dict:
    return {"t": "d", "id": id, "src": src, "v": v}


class TestBasicIO:
    def test_passthrough(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"name": "alice"})])

        assert len(data) == 1
        assert data[0]["v"]["name"] == "alice"
        assert data[0]["id"] == "test-id"
        assert data[0]["src"] == "test-src"

    def test_multiple_inputs(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output({"n": v["n"] * 2})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"n": 1}), inp({"n": 2}), inp({"n": 3})])

        assert len(data) == 3
        assert [r["v"]["n"] for r in data] == [2, 4, 6]

    def test_fan_out(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                for item in v["items"]:
                    ctx.output({"item": item})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"items": ["a", "b", "c"]})])

        assert len(data) == 3
        assert [r["v"]["item"] for r in data] == ["a", "b", "c"]

    def test_filter(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                if v.get("keep"):
                    ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"keep": True}), inp({"keep": False}), inp({"keep": True})])

        assert len(data) == 2

    def test_custom_id_src(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output(v, id="custom-id", src="custom-src")

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert data[0]["id"] == "custom-id"
        assert data[0]["src"] == "custom-src"

    def test_empty_stdin(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [])

        assert len(data) == 0

    def test_meta_lines_skipped(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [{"t": "m", "v": {"ignore": True}}, inp({"real": "data"})])

        assert len(data) == 1
        assert data[0]["v"]["real"] == "data"


class TestSettings:
    def test_settings_passed(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output({"result": v["n"] * settings["mult"]})

            if __name__ == "__main__":
                dpe.run()
        """, {"mult": 10}, [inp({"n": 5})])

        assert data[0]["v"]["result"] == 50

    def test_empty_settings(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output({"empty": len(settings) == 0})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert data[0]["v"]["empty"] is True


class TestQueues:
    def test_emit_to_queue(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.emit("double", {"n": v["n"]})

            def process_double(v, settings, ctx):
                ctx.output({"n": v["n"] * 2})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"n": 5})])

        assert len(data) == 1
        assert data[0]["v"]["n"] == 10

    def test_queue_chain(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.emit("step1", v)

            def process_step1(v, settings, ctx):
                ctx.emit("step2", {"n": v["n"] + 1})

            def process_step2(v, settings, ctx):
                ctx.output({"n": v["n"] * 10})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"n": 1})])

        assert data[0]["v"]["n"] == 20  # (1+1)*10

    def test_queue_inherits_context(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.emit("out", v)

            def process_out(v, settings, ctx):
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1}, id="row-42", src="file:5")])

        assert data[0]["id"] == "row-42"
        assert data[0]["src"] == "file:5"

    def test_queue_custom_id(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.emit("out", v, id="new-id", src="new-src")

            def process_out(v, settings, ctx):
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert data[0]["id"] == "new-id"
        assert data[0]["src"] == "new-src"

    def test_fan_out_queue(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                for i in range(v["count"]):
                    ctx.emit("handle", {"i": i})

            def process_handle(v, settings, ctx):
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"count": 3})])

        assert len(data) == 3
        assert [r["v"]["i"] for r in data] == [0, 1, 2]


class TestDrain:
    def test_drain_before_meta(self):
        data, meta, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.memory.number("total", 0)
                for i in range(v["count"]):
                    ctx.emit("count", {"i": i})
                ctx.drain()
                ctx.meta({"total": ctx.memory.number("total").value})

            def process_count(v, settings, ctx):
                ctx.memory.number("total").inc()
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"count": 5})])

        assert len(data) == 5
        assert len(meta) == 1
        assert meta[0]["v"]["total"] == 5

    def test_multiple_drains(self):
        data, meta, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                # Phase 1
                ctx.emit("phase1", {"val": 10})
                ctx.drain()
                p1 = ctx.memory.number("sum").value

                # Phase 2
                ctx.emit("phase2", {"val": 20})
                ctx.drain()
                p2 = ctx.memory.number("sum").value

                ctx.meta({"after_p1": p1, "after_p2": p2})

            def process_phase1(v, settings, ctx):
                ctx.memory.number("sum").inc(v["val"])

            def process_phase2(v, settings, ctx):
                ctx.memory.number("sum").inc(v["val"])

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert meta[0]["v"]["after_p1"] == 10
        assert meta[0]["v"]["after_p2"] == 30

    def test_auto_drain(self):
        """Queue auto-drains after process_input if not manually drained."""
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.emit("handle", v)
                # no drain() call - framework should auto-drain

            def process_handle(v, settings, ctx):
                ctx.output({"handled": True})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert len(data) == 1
        assert data[0]["v"]["handled"] is True


class TestAccumulatorsInContext:
    def test_shared_memory(self):
        data, meta, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.memory.number("count", 0)
                ctx.memory.set("seen")
                for item in v["items"]:
                    ctx.emit("process", {"item": item})
                ctx.drain()
                ctx.meta({
                    "count": ctx.memory.number("count").value,
                    "unique": ctx.memory.set("seen").size,
                })

            def process_process(v, settings, ctx):
                ctx.memory.number("count").inc()
                ctx.memory.set("seen").add(v["item"])
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"items": ["a", "b", "a", "c"]})])

        assert len(data) == 4
        assert meta[0]["v"]["count"] == 4
        assert meta[0]["v"]["unique"] == 3

    def test_trigger_in_context(self):
        data, meta, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.memory.number("done", 0)
                ctx.memory.number("expected", 0)
                ctx.memory.trigger("complete",
                    lambda m: m.number("done").value >= m.number("expected").value > 0)

                items = v["items"]
                ctx.memory.number("expected").inc(len(items))
                for item in items:
                    ctx.emit("handle", {"item": item})
                ctx.drain()
                ctx.meta({"complete": ctx.memory.trigger("complete").check()})

            def process_handle(v, settings, ctx):
                ctx.memory.number("done").inc()
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"items": [1, 2, 3]})])

        assert meta[0]["v"]["complete"] is True


class TestErrorHandling:
    def test_error_to_stderr(self):
        _, _, stderr = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.error(v, "bad data")

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"bad": True})])

        errors = [r for r in stderr if r.get("type") == "error"]
        assert len(errors) == 1
        assert errors[0]["error"] == "bad data"
        assert errors[0]["input"] == {"bad": True}

    def test_exception_caught(self):
        data, _, stderr = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                if v.get("crash"):
                    raise ValueError("boom")
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"crash": True}), inp({"value": "ok"})])

        assert len(data) == 1
        assert data[0]["v"]["value"] == "ok"
        errors = [r for r in stderr if r.get("type") == "error"]
        assert len(errors) == 1

    def test_queue_exception_caught(self):
        data, _, stderr = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.emit("fail", v)
                ctx.output({"survived": True})

            def process_fail(v, settings, ctx):
                raise RuntimeError("queue crash")

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert len(data) == 1
        assert data[0]["v"]["survived"] is True
        errors = [r for r in stderr if r.get("type") == "error"]
        assert len(errors) == 1


class TestLogging:
    def test_log_to_stderr(self):
        _, _, stderr = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.log("hello", level="info")
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        logs = [r for r in stderr if r.get("type") == "log"]
        assert len(logs) == 1
        assert logs[0]["msg"] == "hello"
        assert logs[0]["level"] == "info"


class TestMeta:
    def test_custom_meta(self):
        _, meta, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.meta({"custom": "stats"})
                ctx.output(v)

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert any(m["v"].get("custom") == "stats" for m in meta)


class TestHash:
    def test_deterministic(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                id1 = ctx.hash("test:path")
                id2 = ctx.hash("test:path")
                ctx.output({"same": id1 == id2, "id": id1})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert data[0]["v"]["same"] is True
        assert len(data[0]["v"]["id"]) == 16

    def test_different_keys(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output({"a": ctx.hash("key_a"), "b": ctx.hash("key_b")})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1})])

        assert data[0]["v"]["a"] != data[0]["v"]["b"]


class TestContext:
    def test_ctx_id_src(self):
        data, _, _ = run_tool_code("""
            import dpe

            def process_input(v, settings, ctx):
                ctx.output({"id": ctx.id, "src": ctx.src})

            if __name__ == "__main__":
                dpe.run()
        """, {}, [inp({"x": 1}, id="my-id", src="my-src")])

        assert data[0]["v"]["id"] == "my-id"
        assert data[0]["v"]["src"] == "my-src"

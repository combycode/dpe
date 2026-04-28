"""Tests for typed accumulators."""

import pytest

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


class TestNumber:
    def test_initial_value(self):
        n = Number(10)
        assert n.value == 10

    def test_default_initial(self):
        n = Number()
        assert n.value == 0

    def test_inc(self):
        n = Number(0)
        assert n.inc() == 1
        assert n.inc() == 2
        assert n.inc(5) == 7

    def test_dec(self):
        n = Number(10)
        assert n.dec() == 9
        assert n.dec(3) == 6

    def test_inc_returns_new_value(self):
        n = Number(0)
        result = n.inc(42)
        assert result == 42
        assert n.value == 42


class TestAverage:
    def test_empty(self):
        a = Average()
        assert a.value == 0.0
        assert a.count == 0

    def test_single_value(self):
        a = Average()
        a.add(10)
        assert a.value == 10.0
        assert a.count == 1

    def test_multiple_values(self):
        a = Average()
        a.add(10)
        a.add(20)
        a.add(30)
        assert a.value == 20.0
        assert a.count == 3

    def test_precision(self):
        a = Average()
        a.add(1)
        a.add(2)
        assert a.value == 1.5


class TestMinMax:
    def test_empty(self):
        mm = MinMax()
        assert mm.min is None
        assert mm.max is None
        assert mm.count == 0

    def test_single_value(self):
        mm = MinMax()
        mm.add(5)
        assert mm.min == 5
        assert mm.max == 5
        assert mm.count == 1

    def test_multiple_values(self):
        mm = MinMax()
        for v in [3, 7, 1, 9, 5]:
            mm.add(v)
        assert mm.min == 1
        assert mm.max == 9
        assert mm.count == 5

    def test_negative_values(self):
        mm = MinMax()
        mm.add(-5)
        mm.add(-1)
        mm.add(-10)
        assert mm.min == -10
        assert mm.max == -1


class TestSet:
    def test_empty(self):
        s = Set()
        assert s.size == 0

    def test_add_and_has(self):
        s = Set()
        s.add("a")
        assert s.has("a")
        assert not s.has("b")

    def test_uniqueness(self):
        s = Set()
        s.add("a")
        s.add("a")
        s.add("a")
        assert s.size == 1

    def test_delete(self):
        s = Set()
        s.add("a")
        s.delete("a")
        assert not s.has("a")
        assert s.size == 0

    def test_delete_nonexistent(self):
        s = Set()
        s.delete("nothing")  # should not raise

    def test_values(self):
        s = Set()
        s.add("b")
        s.add("a")
        assert sorted(s.values) == ["a", "b"]


class TestMap:
    def test_empty(self):
        m = Map()
        assert m.size == 0
        assert m.get("key") is None

    def test_set_get(self):
        m = Map()
        m.set("k", "v")
        assert m.get("k") == "v"
        assert m.has("k")

    def test_default_value(self):
        m = Map()
        assert m.get("missing", "default") == "default"

    def test_delete(self):
        m = Map()
        m.set("k", "v")
        m.delete("k")
        assert not m.has("k")
        assert m.size == 0

    def test_delete_nonexistent(self):
        m = Map()
        m.delete("nothing")  # should not raise

    def test_keys(self):
        m = Map()
        m.set("b", 2)
        m.set("a", 1)
        assert sorted(m.keys) == ["a", "b"]

    def test_overwrite(self):
        m = Map()
        m.set("k", 1)
        m.set("k", 2)
        assert m.get("k") == 2
        assert m.size == 1


class TestBuffer:
    def test_empty(self):
        b = Buffer()
        assert b.size == 0
        assert b.items == []

    def test_push(self):
        b = Buffer()
        b.push("a")
        b.push("b")
        assert b.items == ["a", "b"]
        assert b.size == 2

    def test_limit(self):
        b = Buffer(limit=3)
        for i in range(5):
            b.push(i)
        assert b.items == [2, 3, 4]
        assert b.size == 3

    def test_clear(self):
        b = Buffer()
        b.push("x")
        b.clear()
        assert b.size == 0

    def test_unlimited(self):
        b = Buffer()
        for i in range(1000):
            b.push(i)
        assert b.size == 1000


class TestBoolean:
    def test_default_false(self):
        b = Boolean()
        assert b.value is False

    def test_initial_true(self):
        b = Boolean(True)
        assert b.value is True

    def test_set(self):
        b = Boolean()
        b.set(True)
        assert b.value is True
        b.set(False)
        assert b.value is False

    def test_toggle_no_arg(self):
        b = Boolean(False)
        b.toggle()
        assert b.value is True
        b.toggle()
        assert b.value is False

    def test_toggle_with_value(self):
        b = Boolean(False)
        b.toggle(True)
        assert b.value is True
        b.toggle(True)
        assert b.value is True


class TestBitMask:
    def test_initial_all_false(self):
        bm = BitMask(["a", "b", "c"])
        assert not bm.get("a")
        assert not bm.any()
        assert not bm.all()

    def test_set_and_get(self):
        bm = BitMask(["a", "b"])
        bm.set("a")
        assert bm.get("a")
        assert not bm.get("b")

    def test_all(self):
        bm = BitMask(["x", "y"])
        bm.set("x")
        assert not bm.all()
        bm.set("y")
        assert bm.all()

    def test_any(self):
        bm = BitMask(["x", "y"])
        assert not bm.any()
        bm.set("x")
        assert bm.any()

    def test_clear(self):
        bm = BitMask(["a"])
        bm.set("a")
        bm.clear("a")
        assert not bm.get("a")

    def test_unknown_flag_raises(self):
        bm = BitMask(["a"])
        with pytest.raises(KeyError):
            bm.set("unknown")
        with pytest.raises(KeyError):
            bm.get("unknown")
        with pytest.raises(KeyError):
            bm.clear("unknown")


class TestTrigger:
    def test_simple_condition(self):
        mem = Memory()
        mem.number("count", 0)
        mem.trigger("ready", lambda m: m.number("count").value >= 3)

        assert not mem.trigger("ready").check()
        mem.number("count").inc()
        mem.number("count").inc()
        assert not mem.trigger("ready").check()
        mem.number("count").inc()
        assert mem.trigger("ready").check()

    def test_complex_condition(self):
        mem = Memory()
        mem.number("done", 0)
        mem.number("expected", 0)
        mem.boolean("started", False)

        mem.trigger("complete", lambda m: (
            m.boolean("started").value
            and m.number("done").value >= m.number("expected").value
            and m.number("expected").value > 0
        ))

        assert not mem.trigger("complete").check()
        mem.boolean("started").set(True)
        mem.number("expected").inc(5)
        assert not mem.trigger("complete").check()
        for _ in range(5):
            mem.number("done").inc()
        assert mem.trigger("complete").check()

    def test_trigger_without_condition_raises(self):
        mem = Memory()
        with pytest.raises(ValueError):
            mem.trigger("nonexistent")


class TestMemory:
    def test_create_and_reuse(self):
        mem = Memory()
        n1 = mem.number("count")
        n2 = mem.number("count")
        assert n1 is n2

    def test_different_names(self):
        mem = Memory()
        a = mem.number("a")
        b = mem.number("b")
        assert a is not b

    def test_type_mismatch_raises(self):
        mem = Memory()
        mem.number("x")
        with pytest.raises(TypeError):
            mem.average("x")

    def test_get_existing(self):
        mem = Memory()
        mem.number("count", 42)
        assert mem.get("count").value == 42

    def test_get_nonexistent(self):
        mem = Memory()
        assert mem.get("nothing") is None

    def test_delete(self):
        mem = Memory()
        mem.number("x")
        mem.delete("x")
        assert not mem.has("x")

    def test_has(self):
        mem = Memory()
        assert not mem.has("x")
        mem.number("x")
        assert mem.has("x")

    def test_all_accumulator_types(self):
        mem = Memory()
        assert isinstance(mem.number("n"), Number)
        assert isinstance(mem.average("a"), Average)
        assert isinstance(mem.minmax("mm"), MinMax)
        assert isinstance(mem.set("s"), Set)
        assert isinstance(mem.map("m"), Map)
        assert isinstance(mem.buffer("b"), Buffer)
        assert isinstance(mem.boolean("bool"), Boolean)
        assert isinstance(mem.bitmask("bm", ["x"]), BitMask)
        assert isinstance(mem.trigger("t", lambda m: True), Trigger)

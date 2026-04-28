"""Typed accumulators for shared memory across processors."""

from collections import deque


class Number:
    """Numeric accumulator with increment/decrement."""

    __slots__ = ("_value",)

    def __init__(self, initial: float = 0):
        self._value = initial

    def inc(self, delta: float = 1) -> float:
        self._value += delta
        return self._value

    def dec(self, delta: float = 1) -> float:
        self._value -= delta
        return self._value

    @property
    def value(self) -> float:
        return self._value


class Average:
    """Running average without storing all values."""

    __slots__ = ("_sum", "_count")

    def __init__(self):
        self._sum = 0.0
        self._count = 0

    def add(self, value: float):
        self._sum += value
        self._count += 1

    @property
    def value(self) -> float:
        if self._count == 0:
            return 0.0
        return self._sum / self._count

    @property
    def count(self) -> int:
        return self._count


class MinMax:
    """Track minimum and maximum values."""

    __slots__ = ("_min", "_max", "_count")

    def __init__(self):
        self._min = None
        self._max = None
        self._count = 0

    def add(self, value: float):
        if self._min is None or value < self._min:
            self._min = value
        if self._max is None or value > self._max:
            self._max = value
        self._count += 1

    @property
    def min(self) -> float | None:
        return self._min

    @property
    def max(self) -> float | None:
        return self._max

    @property
    def count(self) -> int:
        return self._count


class Set:
    """Unique value collection."""

    __slots__ = ("_data",)

    def __init__(self):
        self._data = set()

    def add(self, value):
        self._data.add(value)

    def has(self, value) -> bool:
        return value in self._data

    def delete(self, value):
        self._data.discard(value)

    @property
    def size(self) -> int:
        return len(self._data)

    @property
    def values(self) -> list:
        return list(self._data)


class Map:
    """Key-value lookup. Transient storage, not for accumulation."""

    __slots__ = ("_data",)

    def __init__(self):
        self._data = {}

    def set(self, key: str, value):
        self._data[key] = value

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def has(self, key: str) -> bool:
        return key in self._data

    def delete(self, key: str):
        self._data.pop(key, None)

    @property
    def keys(self) -> list:
        return list(self._data.keys())

    @property
    def size(self) -> int:
        return len(self._data)


class Buffer:
    """Ordered collection with optional size limit (sliding window)."""

    __slots__ = ("_data", "_limit")

    def __init__(self, limit: int | None = None):
        self._data: deque = deque(maxlen=limit)
        self._limit = limit

    def push(self, item):
        self._data.append(item)

    @property
    def items(self) -> list:
        return list(self._data)

    @property
    def size(self) -> int:
        return len(self._data)

    def clear(self):
        self._data.clear()


class Boolean:
    """Simple flag."""

    __slots__ = ("_value",)

    def __init__(self, initial: bool = False):
        self._value = initial

    def set(self, value: bool = True):
        self._value = value

    def toggle(self, value: bool | None = None):
        if value is not None:
            self._value = value
        else:
            self._value = not self._value

    @property
    def value(self) -> bool:
        return self._value


class BitMask:
    """Named flag set for tracking multiple boolean states."""

    __slots__ = ("_flags",)

    def __init__(self, names: list[str]):
        self._flags = {name: False for name in names}

    def set(self, name: str):
        if name not in self._flags:
            raise KeyError(f"Unknown flag: {name}")
        self._flags[name] = True

    def get(self, name: str) -> bool:
        if name not in self._flags:
            raise KeyError(f"Unknown flag: {name}")
        return self._flags[name]

    def clear(self, name: str):
        if name not in self._flags:
            raise KeyError(f"Unknown flag: {name}")
        self._flags[name] = False

    def all(self) -> bool:
        return all(self._flags.values())

    def any(self) -> bool:
        return any(self._flags.values())


class Trigger:
    """Condition checker across accumulators. Condition is a callable(memory) → bool."""

    __slots__ = ("_condition", "_memory_ref")

    def __init__(self, condition, memory):
        self._condition = condition
        self._memory_ref = memory

    def check(self) -> bool:
        return self._condition(self._memory_ref)


class Memory:
    """Shared memory — typed accumulator registry.

    Accumulators are created on first access by name.
    Same name always returns the same instance.
    """

    def __init__(self):
        self._store: dict = {}

    def number(self, name: str, initial: float = 0) -> Number:
        return self._get_or_create(name, Number, initial)

    def average(self, name: str) -> Average:
        return self._get_or_create(name, Average)

    def minmax(self, name: str) -> MinMax:
        return self._get_or_create(name, MinMax)

    def set(self, name: str) -> Set:
        return self._get_or_create(name, Set)

    def map(self, name: str) -> Map:
        return self._get_or_create(name, Map)

    def buffer(self, name: str, limit: int | None = None) -> Buffer:
        return self._get_or_create(name, Buffer, limit)

    def boolean(self, name: str, initial: bool = False) -> Boolean:
        return self._get_or_create(name, Boolean, initial)

    def bitmask(self, name: str, flags: list[str]) -> BitMask:
        return self._get_or_create(name, BitMask, flags)

    def trigger(self, name: str, condition=None) -> Trigger:
        if name in self._store:
            return self._store[name]
        if condition is None:
            raise ValueError(f"Trigger '{name}' does not exist and no condition provided")
        t = Trigger(condition, self)
        self._store[name] = t
        return t

    def get(self, name: str):
        """Get accumulator by name. Returns None if not found."""
        return self._store.get(name)

    def delete(self, name: str):
        """Remove accumulator by name."""
        self._store.pop(name, None)

    def has(self, name: str) -> bool:
        return name in self._store

    def _get_or_create(self, name, cls, *args):
        existing = self._store.get(name)
        if existing is not None:
            if not isinstance(existing, cls):
                raise TypeError(
                    f"Accumulator '{name}' exists as {type(existing).__name__}, "
                    f"not {cls.__name__}"
                )
            return existing
        instance = cls(*args)
        self._store[name] = instance
        return instance

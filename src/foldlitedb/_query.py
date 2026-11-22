"""Portable JSON ordering for Python-owned callback query results.

Canonical JSON remains the equality key. This tuple is only the query order;
changing it must never reorder ingestion or the contributions in an ordered fold.
"""
from __future__ import annotations

from typing import Any

QUERY_FORMAT = "foldlite-query-v1"


def key_order(value: Any):
    if value is None:
        return (0,)
    if value is False:
        return (1,)
    if value is True:
        return (2,)
    t = type(value)
    if t is int or t is float:
        return (3, value)
    if t is str:
        return (4, value)
    if t is list:
        return (5, tuple(key_order(item) for item in value))
    if t is dict:
        return (6, tuple((name, key_order(value[name])) for name in sorted(value)))
    if isinstance(value, (int, float)):
        return (3, value)
    if isinstance(value, str):
        return (4, value)
    if isinstance(value, list):
        return (5, tuple(key_order(item) for item in value))
    return (6, tuple((name, key_order(value[name])) for name in sorted(value)))


class CompiledSelector:
    """Precomputed query selector bounds so ``key_order`` runs once per candidate key."""

    __slots__ = (
        "kind",
        "key_order_val",
        "str_prefix",
        "list_prefix_len",
        "list_prefix_orders",
        "start_order",
        "inclusive_start",
        "end_order",
        "inclusive_end",
    )

    def __init__(self, selector: dict[str, Any]):
        self.key_order_val = None
        self.str_prefix: str | None = None
        self.list_prefix_len = 0
        self.list_prefix_orders: tuple[Any, ...] = ()
        self.start_order = None
        self.inclusive_start = bool(selector.get("inclusive_start", True))
        self.end_order = None
        self.inclusive_end = bool(selector.get("inclusive_end", True))

        if "key" in selector:
            self.kind = "key"
            self.key_order_val = key_order(selector["key"])
        elif "prefix" in selector:
            prefix = selector["prefix"]
            if isinstance(prefix, str):
                self.kind = "str_prefix"
                self.str_prefix = prefix
            else:
                self.kind = "list_prefix"
                self.list_prefix_len = len(prefix)
                self.list_prefix_orders = tuple(key_order(item) for item in prefix)
        else:
            self.kind = "range"
            if "start_key" in selector:
                self.start_order = key_order(selector["start_key"])
            if "end_key" in selector:
                self.end_order = key_order(selector["end_key"])

    def matches(self, value: Any, order: Any = None) -> bool:
        kind = self.kind
        if kind == "key":
            if order is None:
                order = key_order(value)
            return order == self.key_order_val
        if kind == "str_prefix":
            return isinstance(value, str) and value.startswith(self.str_prefix)
        if kind == "list_prefix":
            if not isinstance(value, list) or len(value) < self.list_prefix_len:
                return False
            if order is not None:
                return order[1][:self.list_prefix_len] == self.list_prefix_orders
            return tuple(key_order(item) for item in value[:self.list_prefix_len]) == self.list_prefix_orders
        if self.start_order is None and self.end_order is None:
            return True
        if order is None:
            order = key_order(value)
        if self.start_order is not None:
            if order < self.start_order or (order == self.start_order and not self.inclusive_start):
                return False
        if self.end_order is not None:
            if order > self.end_order or (order == self.end_order and not self.inclusive_end):
                return False
        return True


def compile_selector(selector: dict[str, Any]) -> CompiledSelector:
    return CompiledSelector(selector)


def matches_selector(value: Any, selector: dict[str, Any]) -> bool:
    return CompiledSelector(selector).matches(value)

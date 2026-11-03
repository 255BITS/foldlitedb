"""Portable JSON ordering for Python-owned callback query results.

Canonical JSON remains the equality key. This tuple is only the query order;
changing it must never reorder ingestion or the contributions in an ordered fold.
"""

QUERY_FORMAT = "foldlite-query-v1"


def key_order(value):
    if value is None:
        return (0,)
    if value is False:
        return (1,)
    if value is True:
        return (2,)
    if isinstance(value, (int, float)):
        return (3, value)
    if isinstance(value, str):
        return (4, value)
    if isinstance(value, list):
        return (5, tuple(key_order(item) for item in value))
    return (6, tuple((name, key_order(value[name])) for name in sorted(value)))


def matches_selector(value, selector):
    order = key_order(value)
    if "key" in selector:
        return order == key_order(selector["key"])
    if "prefix" in selector:
        prefix = selector["prefix"]
        if isinstance(prefix, str):
            return isinstance(value, str) and value.startswith(prefix)
        return (isinstance(value, list) and len(value) >= len(prefix)
                and key_order(value[:len(prefix)]) == key_order(prefix))
    if "start_key" in selector:
        start = key_order(selector["start_key"])
        if order < start or (order == start and not selector["inclusive_start"]):
            return False
    if "end_key" in selector:
        end = key_order(selector["end_key"])
        if order > end or (order == end and not selector["inclusive_end"]):
            return False
    return True

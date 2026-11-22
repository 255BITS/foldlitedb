"""Regression tests for fast JSON validation/copying, single-pass ingest_many, and callback query fast paths."""
from enum import IntEnum, StrEnum
import hashlib
import math

import pytest

from foldlitedb import (
    CursorExpired,
    Database,
    DatabaseError,
    Document,
    DocumentConflict,
    OrderedFold,
    SourceOrderError,
    View,
)
from foldlitedb._query import compile_selector, key_order, matches_selector
from foldlitedb.database import _RegisteredView, _copy, _encode
from foldlitedb.projection import json_bytes
from foldlitedb.streaming import sse
from foldlitedb.types import _json


class _SampleIntEnum(IntEnum):
    ONE = 1


class _SampleStrEnum(StrEnum):
    KEY = "enum_key"


def _nest_list(inner, levels):
    value = inner
    for _ in range(levels):
        value = [value]
    return value


def _nest_dict(inner, levels):
    value = inner
    for _ in range(levels):
        value = {"k": value}
    return value


def test_fast_copy_primitives_and_types():
    assert _copy(None) is None
    assert _copy(True) is True
    assert _copy(False) is False
    assert type(_copy(True)) is bool
    assert type(_copy(False)) is bool
    assert type(_copy(0)) is int
    assert type(_copy(1)) is int
    assert _copy(0) == 0
    assert _copy(1.0) == 1.0 and type(_copy(1.0)) is float

    neg_zero = _copy(-0.0)
    assert neg_zero == 0.0 and math.copysign(1.0, neg_zero) == -1.0

    # Exact IEEE-754 integers at and above 2**53.
    for exact in (-(2**53), 2**53, 2**54, -(2**54), 10**20):
        copied = _copy(exact)
        assert copied == exact and type(copied) is int

    # ASCII and non-ASCII valid UTF-8 strings.
    for text in ("", "ascii", "héllo ☃ 😀", "\x00\n\"\\"):
        assert _copy(text) == text and type(_copy(text)) is str

    # Tuples normalize to plain lists; subclasses normalize to plain builtins.
    assert _copy((1, (2, 3))) == [1, [2, 3]]
    assert type(_copy((1, 2))) is list
    assert type(_copy(_SampleIntEnum.ONE)) is int
    assert type(_copy(_SampleStrEnum.KEY)) is str
    sub_dict = _copy({_SampleStrEnum.KEY: (_SampleIntEnum.ONE, True)})
    assert sub_dict == {"enum_key": [1, True]}
    assert type(next(iter(sub_dict.keys()))) is str
    assert type(sub_dict["enum_key"]) is list
    assert type(sub_dict["enum_key"][0]) is int
    assert type(sub_dict["enum_key"][1]) is bool


def test_fast_copy_deep_mutation_isolation():
    original = {"a": [1, {"b": [2, 3]}], "t": ({"c": 4},)}
    cloned = _copy(original)
    assert cloned == {"a": [1, {"b": [2, 3]}], "t": [{"c": 4}]}

    cloned["a"][1]["b"].append(99)
    cloned["t"][0]["c"] = 999
    cloned["new"] = True

    assert original == {"a": [1, {"b": [2, 3]}], "t": ({"c": 4},)}


@pytest.mark.parametrize(
    "invalid",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        [float("nan")],
        {"x": float("-inf")},
        2**53 + 1,
        -(2**53 + 1),
        10**100 + 1,
        10**400,
        -(10**400),
        "\ud800",
        "\udfff",
        "bad\ud83dstring",
        {"\ud800": 1},
        {_SampleStrEnum.KEY: "\ud800"},
        {1: "int key"},
        {True: "bool key"},
        {None: "none key"},
        {(1, 2): "tuple key"},
        {"nested": {1: "bad"}},
        [{"nested": {False: "bad"}}],
        object(),
        b"bytes",
        {1, 2},
    ],
)
def test_fast_copy_and_encode_reject_invalid_portable_json(invalid):
    with pytest.raises(DatabaseError):
        _copy(invalid)
    with pytest.raises(DatabaseError):
        _encode(invalid)
    with pytest.raises(DatabaseError):
        _encode(invalid, sort_keys=False)


def test_fast_copy_and_encode_depth_boundaries_and_cycles():
    # 64 nested containers around a scalar reaches depth 64 (allowed).
    depth_64_list = _nest_list(0, 64)
    depth_64_dict = _nest_dict("ok", 64)
    assert _copy(depth_64_list) == depth_64_list
    assert _copy(depth_64_dict) == depth_64_dict
    assert _encode(depth_64_list)
    assert _encode(depth_64_dict)

    # Empty container at depth 64 has no children at depth 65 (allowed).
    empty_at_64 = _nest_list([], 64)
    assert _copy(empty_at_64) == empty_at_64
    assert _encode(empty_at_64)

    # 65 nested containers around a scalar reaches depth 65 (rejected).
    depth_65_list = _nest_list(0, 65)
    depth_65_dict = _nest_dict("too deep", 65)
    for deep in (depth_65_list, depth_65_dict, _nest_list([], 65)):
        with pytest.raises(DatabaseError, match="nesting"):
            _copy(deep)
        with pytest.raises(DatabaseError, match="nesting"):
            _encode(deep)

    # Cyclic containers are rejected cleanly via depth guard.
    cyclic_list: list = []
    cyclic_list.append(cyclic_list)
    with pytest.raises(DatabaseError, match="nesting"):
        _copy(cyclic_list)
    with pytest.raises(DatabaseError, match="nesting"):
        _encode(cyclic_list)

    cyclic_dict: dict = {}
    cyclic_dict["self"] = cyclic_dict
    with pytest.raises(DatabaseError, match="nesting"):
        _copy(cyclic_dict)
    with pytest.raises(DatabaseError, match="nesting"):
        _encode(cyclic_dict)


def test_ingest_many_single_pass_snapshots_and_enforces_depth_64():
    payload = {"nested": [0]}

    def gen():
        for i in range(5):
            payload["nested"][0] = i
            yield Document(f"d-{i}", payload)

    view = View(
        "sum",
        version="1",
        map=lambda doc: [("total", doc.payload["nested"][0])] if isinstance(doc.payload, dict) else [],
        reducer=OrderedFold(0, lambda s, v: s + v, version="1"),
    )
    with Database(":memory:", view) as db:
        results = list(db.ingest_many(gen(), batch_size=2))
        assert [r.inserted for r in results] == [2, 2, 1]
        assert [d.payload["nested"][0] for d in db.documents()] == [0, 1, 2, 3, 4]
        assert db.query(key="total").rows[0].result == 10

        # Depth 64 payload succeeds; depth 65 payload fails without committing.
        deep_ok = _nest_list(1, 64)
        deep_bad = _nest_list(1, 65)
        ok_res = list(db.ingest_many([Document("deep-ok", deep_ok)], batch_size=1))
        assert [r.inserted for r in ok_res] == [1]
        with pytest.raises(DatabaseError, match="nesting"):
            list(db.ingest_many([Document("deep-bad", deep_bad)], batch_size=1))
        assert db.get("deep-bad") is None


def test_registered_view_caches_descriptor_signature():
    view = View(
        "sigview",
        version="1",
        map=lambda d: [(d.payload["k"], d.payload["v"])],
        reducer=OrderedFold(0, lambda s, v: s + v, version="1"),
        references={"scale": 2},
    )
    with Database(":memory:", view) as db:
        entry = db._views["sigview"]
        assert isinstance(entry, _RegisteredView)
        expected = hashlib.sha256(_json(["sigview", entry.descriptor]).encode()).hexdigest()
        assert entry.signature == expected
        assert entry.copy().signature == expected
        cursor = db.query().cursor
        assert cursor.endswith("~" + expected)


def test_callback_query_selector_fast_paths_and_compiled_selector():
    keys = [
        None,
        False,
        True,
        -5,
        0,
        2,
        10,
        "",
        "a",
        "ab",
        "abc",
        "b",
        [],
        [0],
        [0, "x"],
        [False],
        [{"a": 1, "b": 2}, 1],
        [{"b": 2, "a": 1}, 2],
        {"k": 1},
    ]
    view = View(
        "mixed",
        version="1",
        map=lambda d: [(k, {"seen": 1}) for k in d.payload["keys"]],
        reducer=OrderedFold(
            {"count": 0},
            lambda s, v: {"count": s["count"] + v["seen"]},
            version="1",
        ),
    )
    with Database(":memory:", view) as db:
        db.add({"keys": list(reversed(keys))})

        # Exact key O(1) fast path: present, absent, numeric equivalence, type distinction.
        assert db.query(key=2).rows[0].key == 2
        assert db.query(key=2.0).rows[0].key == 2
        assert db.query(key=False).rows[0].key is False
        assert db.query(key=0).rows[0].key == 0 and type(db.query(key=0).rows[0].key) is int
        assert db.query(key="missing").rows == ()
        assert db.query(key="missing").complete is True

        # Mutation isolation on returned rows.
        row = db.query(key=2).rows[0]
        row.state["count"] = 999
        row.result["count"] = 888
        assert db.query(key=2).rows[0].state == {"count": 1}
        assert db.query(key=2).rows[0].result == {"count": 1}

        # String prefix fast path (non-empty and empty) with pagination.
        assert [r.key for r in db.rows(prefix="ab", limit=1)] == ["ab", "abc"]
        assert [r.key for r in db.rows(prefix="", limit=2)] == ["", "a", "ab", "abc", "b"]

        # List prefix fast path (empty, scalar, structured object canonical equality).
        assert [r.key for r in db.rows(prefix=[0.0], limit=1)] == [[0], [0, "x"]]
        assert [r.key for r in db.rows(prefix=[False], limit=1)] == [[False]]
        assert [r.key for r in db.rows(prefix=[{"a": 1.0, "b": 2}], limit=1)] == [
            [{"a": 1, "b": 2}, 1],
            [{"b": 2, "a": 1}, 2],
        ]
        assert [r.key for r in db.rows(prefix=[], limit=2)] == [
            [],
            [False],
            [0],
            [0, "x"],
            [{"a": 1, "b": 2}, 1],
            [{"b": 2, "a": 1}, 2],
        ]

        # Range fast path with exclusive/inclusive bounds and pagination.
        assert [r.key for r in db.rows(start_key=0, end_key=10, inclusive_start=False, inclusive_end=False, limit=1)] == [2]
        assert [r.key for r in db.rows(start_key=False, end_key=0, limit=1)] == [False, True, -5, 0]

    # Direct unit test of CompiledSelector parity with matches_selector.
    for selector in (
        {"key": 2.0, "inclusive_start": True, "inclusive_end": True},
        {"prefix": "ab", "inclusive_start": True, "inclusive_end": True},
        {"prefix": [0.0], "inclusive_start": True, "inclusive_end": True},
        {"start_key": -5, "end_key": "b", "inclusive_start": False, "inclusive_end": True},
        {"inclusive_start": True, "inclusive_end": True},
    ):
        compiled = compile_selector(selector)
        for sample in keys:
            order = key_order(sample)
            expected = matches_selector(sample, selector)
            assert compiled.matches(sample) is expected
            assert compiled.matches(sample, order) is expected


def test_unified_canonical_json_preserves_error_types():
    assert json_bytes({"b": 1, "a": "☃"}) == b'{"a":"\\u2603","b":1}'
    with pytest.raises(ValueError) as proj_exc:
        json_bytes(float("nan"))
    assert type(proj_exc.value) is ValueError

    assert sse("msg", {"b": 1, "a": "☃"}) == b'event: msg\ndata: {"a":"\\u2603","b":1}\n\n'
    with pytest.raises(ValueError) as sse_exc:
        sse("msg", float("inf"))
    assert type(sse_exc.value) is ValueError

    assert _json({"b": 1, "a": "☃"}) == '{"a":"☃","b":1}'
    with pytest.raises(DatabaseError):
        _json({1: "bad"})


def test_pyo3_direct_bridge_types_errors_and_cursor_fast_path(monkeypatch):
    with Database(":memory:") as db:
        # Lightweight cursor matches stats()["cursor"] and updates on ingest.
        c0 = db._cursor()
        assert c0 == db.stats()["cursor"]

        # Types round-trip cleanly: bool vs int, exact large ints, floats, tuples, enums, Unicode.
        doc = db.add(
            {
                "metric": "m",
                "value": 10,
                "flags": (True, False, None, 0, 1, -0.0),
                "big_exact": [2**53, -(2**53), 10**20],
                _SampleStrEnum.KEY: _SampleIntEnum.ONE,
                "unicode": "café ☃ 🚀",
            },
            id="typed-1",
            source="s",
            position=5,
        )
        assert db._cursor() == db.stats()["cursor"]
        assert db._cursor() != c0

        fetched = db.get("typed-1", source="s")
        assert fetched == doc
        assert type(fetched.payload["flags"][0]) is bool and fetched.payload["flags"][0] is True
        assert type(fetched.payload["flags"][1]) is bool and fetched.payload["flags"][1] is False
        assert fetched.payload["flags"][2] is None
        assert type(fetched.payload["flags"][3]) is int and fetched.payload["flags"][3] == 0
        assert type(fetched.payload["flags"][4]) is int and fetched.payload["flags"][4] == 1
        assert fetched.payload["big_exact"] == [float(2**53), float(-(2**53)), float(10**20)]
        assert fetched.payload["enum_key"] == 1
        assert fetched.payload["unicode"] == "café ☃ 🚀"

        # Structured error classes preserved across direct execute / execute_batch.
        with pytest.raises(DocumentConflict):
            db.add({"metric": "m", "value": 999}, id="typed-1", source="s", position=5)
        with pytest.raises(SourceOrderError):
            db.add({"metric": "m", "value": 1}, id="typed-2", source="s", position=3)
        with pytest.raises(CursorExpired):
            db.changes(since="foreign:deadbeef:1")

        # Invalid inputs rejected directly in PyO3 converter before engine lock.
        for bad in (
            float("nan"),
            float("inf"),
            float("-inf"),
            2**53 + 1,
            -(2**53 + 1),
            10**100 + 1,
            10**400,
            -(10**400),
            "\ud800",
            {"\udfff": 1},
            {1: "non-str-key"},
            {True: "bool-key"},
            b"bytes",
            object(),
        ):
            with pytest.raises(DatabaseError):
                db.add(bad)
            with pytest.raises(DatabaseError):
                db.query(key=bad)
            with pytest.raises(DatabaseError):
                db.preview(bad)

        # Depth 64 allowed vs depth 65 rejected on add, preview, and query.
        deep_ok = _nest_list(1, 64)
        deep_bad = _nest_list(1, 65)
        assert db.add(deep_ok, id="deep-ok").payload == deep_ok
        assert db.preview(deep_ok) == []
        assert db.query(key=deep_ok).rows == ()
        for fn in (db.add, db.preview, lambda v: db.query(key=v)):
            with pytest.raises(DatabaseError, match="nesting"):
                fn(deep_bad)

        # Cyclic containers rejected cleanly without RecursionError.
        cyc_list: list = []
        cyc_list.append(cyc_list)
        cyc_dict: dict = {}
        cyc_dict["self"] = cyc_dict
        for cyc in (cyc_list, cyc_dict):
            with pytest.raises(DatabaseError, match="nesting"):
                db.add(cyc)
            with pytest.raises(DatabaseError, match="nesting"):
                db.query(key=cyc)

        # add_many snapshots mutable payloads at yield time and stops after 1024 items.
        reused = {"metric": "snap", "value": 0}

        def mutating_payloads():
            for i in range(4):
                reused["value"] = i
                yield reused

        docs = db.add_many(mutating_payloads())
        assert [d.payload["value"] for d in docs] == [0, 1, 2, 3]

        yielded = 0

        def too_many():
            nonlocal yielded
            for i in range(2000):
                yielded += 1
                yield {"metric": "x", "value": i}

        with pytest.raises(DatabaseError, match="1024"):
            db.add_many(too_many())
        assert yielded == 1025

        # execute_json remains 100% backwards-compatible on the native engine.
        raw_stats = db._call_json('{"op":"stats"}')
        assert raw_stats["cursor"] == db._cursor()

    # Closed engine reports DatabaseError from _cursor() and _call().
    with pytest.raises(DatabaseError, match="closed"):
        db._cursor()

    # Callback query uses lightweight _cursor() without calling stats().
    view = View(
        "fast_cb",
        version="1",
        map=lambda d: [(d.payload["k"], d.payload["v"])],
        reducer=OrderedFold(0, lambda s, v: s + v, version="1"),
    )
    with Database(":memory:", view) as cb_db:
        cb_db.add({"k": "a", "v": 7})
        monkeypatch.setattr(cb_db, "stats", lambda: (_ for _ in ()).throw(AssertionError("stats() must not be called")))
        snap = cb_db.query(key="a")
        assert len(snap.rows) == 1 and snap.rows[0].result == 7

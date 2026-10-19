"""Atomic payload batches, bounded snapshot iteration, and numeric shortcuts."""
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from foldlitedb import (
    Count, CursorExpired, Database, DatabaseError, Envelope, Mean, OrderedFold,
    RustReducer, Sum, View,
)


def collecting(name="values", *, version="1", mapping=None):
    if mapping is None:
        mapping = lambda document: [("all", document.payload)]
    return View(name, version, mapping,
                OrderedFold([], lambda state, value: state + [value], version="1"))


def test_bulk_snapshots_generator_payloads_and_returns_ordered_metadata(tmp_path):
    payload = {"values": []}

    def payloads():
        for value in range(3):
            payload["values"][:] = [value]
            yield payload

    def mutating(document):
        document.payload["values"].append(999)
        return []

    with Database(tmp_path / "db") as database:
        database.register(collecting("mutating", mapping=mutating))
        database.register(collecting())
        database.add({"values": [-1]}, source="stream", position=10)
        documents = database.add_many(payloads(), source="stream")
        assert isinstance(documents, tuple)
        assert [document.position for document in documents] == [11, 12, 13]
        assert all(document.source == "stream" for document in documents)
        assert len({document.id for document in documents}) == 3
        expected = [{"values": [value]} for value in range(3)]
        assert [document.payload for document in documents] == expected
        assert payload == {"values": [2]}
        assert list(database.rows("values"))[0].result == [{"values": [-1]}, *expected]
        documents[0].payload["values"].append(7)
        assert database.get(documents[0].id, source="stream").payload == expected[0]
    with Database(tmp_path / "db") as reopened:
        assert reopened.get(documents[-1].id, source="stream").payload == expected[-1]


def test_bulk_is_eager_atomic_accepts_json_arrays_and_coalesces_changes():
    with Database(":memory:") as database:
        database.register(collecting())
        before = database.query("values")
        payloads = [None, False, 2, "text", [1, 2], {"enabled": True}]
        documents = database.add_many(iter(payloads))
        # No iteration over the returned tuple is needed to commit the batch.
        assert database.stats()["documents"] == len(payloads)
        assert [document.payload for document in documents] == payloads
        updates = database.changes("values", since=before.cursor)
        assert len(updates.rows) == 1
        assert updates.rows[0].result == payloads
        assert database.ingest(documents).replayed == len(payloads)
        assert database.add_many([]) == ()
        assert database.stats()["documents"] == len(payloads)


def test_bulk_input_failure_does_not_map_or_commit_a_prefix():
    mapped = []

    def payloads():
        yield 1
        raise RuntimeError("source failed")

    def mapping(document):
        mapped.append(document.id)
        return []

    with Database(":memory:") as database:
        database.register(collecting(mapping=mapping))
        for payloads_to_add, exception in [(payloads(), RuntimeError), ([1, float("inf")], DatabaseError)]:
            with pytest.raises(exception):
                database.add_many(payloads_to_add)
            assert mapped == []
            assert database.checkpoint() is None
            assert database.stats()["documents"] == 0


def test_bulk_mapper_failure_rolls_back_earlier_documents_and_view_writes():
    def mapping(document):
        if document.payload == "fail":
            raise RuntimeError("map failed")
        yield "all", document.payload

    with Database(":memory:") as database:
        database.register(collecting())
        database.register(collecting("failure", mapping=mapping))
        database.add("original")
        before = database.query("values")
        with pytest.raises(RuntimeError, match="map failed"):
            database.add_many(["first", "fail"])
        assert database.query("values") == before
        assert database.changes("values", since=before.cursor).rows == ()
        assert database.checkpoint() == 0
        assert database.stats()["documents"] == 1
        assert [document.position for document in database.add_many(["retry", "ok"])] == [1, 2]


def test_bulk_bounds_stop_input_and_do_not_reserve_positions():
    visited = []

    def payloads():
        for value in range(1000000):
            visited.append(value)
            yield value

    with Database(":memory:", max_batch=2) as database:
        with pytest.raises(DatabaseError, match="max_batch"):
            database.add_many(payloads())
        assert len(visited) == 3
        assert database.checkpoint() is None
    for options, payloads_to_add, message in [
        ({"max_batch_bytes": 60}, [None, None], "max_batch_bytes"),
        ({"max_value_bytes": 8}, [None, "too large"], "max_value_bytes"),
    ]:
        with Database(":memory:", **options) as database:
            with pytest.raises(DatabaseError, match=message):
                database.add_many(payloads_to_add)
            assert database.stats()["documents"] == 0
            assert database.add(None).position == 0
    with Database(":memory:") as database:
        with pytest.raises(DatabaseError):
            database.add_many([], source="")
        database.add(None, position=2**53 - 2)
        with pytest.raises(DatabaseError):
            database.add_many([1, 2])
        assert database.checkpoint() == 2**53 - 2
        assert database.stats()["documents"] == 1
        assert database.add(1).position == 2**53 - 1


def test_bulk_positions_remain_contiguous_across_concurrent_connections(tmp_path):
    with Database(tmp_path / "db") as first, Database(tmp_path / "db") as second:
        barrier = threading.Barrier(2)

        def append(database):
            barrier.wait()
            return [database.add_many(range(5)) for _ in range(4)]

        with ThreadPoolExecutor(max_workers=2) as executor:
            batches = [batch for worker in executor.map(append, [first, second]) for batch in worker]
        for batch in batches:
            assert [document.position for document in batch] == list(range(batch[0].position, batch[0].position + 5))
        documents = [document for batch in batches for document in batch]
        assert sorted(document.position for document in documents) == list(range(40))
        assert len({document.id for document in documents}) == 40
        assert first.stats()["documents"] == 40


def test_rows_is_lazy_pins_first_iteration_and_releases_transactions(tmp_path, monkeypatch):
    def mapping(document):
        yield document.payload["key"], document.payload["value"]

    view = View("totals", "1", mapping, OrderedFold(0, lambda state, value: state + value, version="1"))
    with Database(tmp_path / "db") as database:
        database.register(view)
        calls = []
        query = database.query

        def tracked(*args, **kwargs):
            calls.append(kwargs)
            return query(*args, **kwargs)

        monkeypatch.setattr(database, "query", tracked)
        iterator = database.rows("totals", limit=1)
        assert calls == []
        database.add_many([{"key": key, "value": value} for key, value in [("a", 1), ("b", 2), ("c", 3)]])
        first = next(iterator)
        assert (first.key, first.result) == ("a", 1)
        assert not database._connection.in_transaction
        # A writer using the same instance must not wait on a paused iterator.
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(database.add_many, [{"key": "b", "value": 10}, {"key": "aa", "value": 20}]).result(timeout=5)
        assert [(row.key, row.result) for row in iterator] == [("b", 2), ("c", 3)]
        assert not database._connection.in_transaction
        assert len(calls) == 3
        assert calls[1]["cursor"] == calls[2]["cursor"]
        assert [(row.key, row.result) for row in database.rows("totals")] == [("a", 1), ("aa", 20), ("b", 12), ("c", 3)]


def test_rows_follows_byte_limited_pages_and_exact_structured_filters(monkeypatch):
    mapping = lambda document: [(document.payload["key"], document.payload["value"])]
    view = View("latest", "1", mapping, OrderedFold(None, lambda _, value: value, version="1"))
    with Database(":memory:", max_page_bytes=1200) as database:
        database.register(view)
        keys = [None, "x", ["group", {"enabled": True}]]
        database.add_many([{"key": key, "value": "x" * 400} for key in keys])
        page_sizes = []
        query = database.query

        def tracked(*args, **kwargs):
            page = query(*args, **kwargs)
            page_sizes.append(len(page.rows))
            return page

        monkeypatch.setattr(database, "query", tracked)
        assert len(list(database.rows("latest", limit=100))) == 3
        assert page_sizes == [1, 1, 1]
        for key in keys:
            assert [row.key for row in database.rows("latest", key=key)] == [key]
        assert list(database.rows("latest", key="missing")) == []


def test_rows_propagates_revision_expiry_instead_of_switching_snapshots():
    with Database(":memory:") as database:
        mapping = lambda document: [(document.payload, 1)]
        database.register(collecting(mapping=mapping))
        assert list(database.rows("values")) == []
        database.add_many(["a", "b"])
        iterator = database.rows("values", limit=1)
        assert next(iterator).key == "a"
        database.register(collecting(version="2", mapping=mapping))
        with pytest.raises(CursorExpired):
            next(iterator)
        with pytest.raises(DatabaseError, match="limit"):
            next(database.rows("values", limit=0))


@pytest.mark.parametrize("shortcut,name,expected", [
    (Mean, "mean/v1", {"count": 2, "value": 3.0}),
    (Sum, "sum/v1", {"count": 2, "value": 6.0}),
    (Count, "count/v1", {"count": 2, "value": 2}),
    (Envelope, "envelope/v1", None),
])
def test_numeric_shortcuts_preserve_descriptors_results_and_view_revisions(shortcut, name, expected):
    reducer = shortcut()
    assert reducer.descriptor == RustReducer(name).descriptor
    assert shortcut(positioned=True).descriptor == RustReducer(name, positioned=True).descriptor
    pytest.importorskip("wasmtime")
    mapping = lambda document: [("all", document.payload)]
    with Database(":memory:") as database:
        revision = database.register(View("numeric", "1", mapping, reducer))
        database.add_many([2, None, 4])
        snapshot = database.query("numeric")
        if expected is not None:
            assert snapshot.rows[0].result == expected
        else:
            result = snapshot.rows[0].result
            assert (result["first"]["value"], result["min"]["value"], result["max"]["value"], result["last"]["value"]) == (2, 2, 4, 4)
        assert database.register(View("numeric", "1", mapping, RustReducer(name))) == revision
        assert database.query("numeric") == snapshot
        for invalid in (True, "3", {"value": 3}):
            with pytest.raises(DatabaseError, match="numbers or null"):
                database.add_many([1, invalid])
            assert database.query("numeric") == snapshot
            assert database.stats()["documents"] == 3

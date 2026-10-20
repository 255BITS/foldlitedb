"""Bounded native batches and stable, lazy snapshot iteration."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from foldlitedb import CursorExpired, Database, DatabaseError, Document


def test_bulk_bounds_stop_input_and_do_not_reserve_positions():
    visited = []
    def payloads():
        for value in range(1000000):
            visited.append(value)
            yield value
    with Database(":memory:") as database:
        with pytest.raises(DatabaseError, match="max_batch"):
            database.add_many(payloads())
        assert len(visited) == 1025
        assert database.stats()["documents"] == 0
        assert database.add(None).position == 0
        with pytest.raises(DatabaseError):
            database.add_many([], source="")


def test_ingest_many_is_lazy_and_prior_batches_remain_committed_on_failure():
    def documents():
        for index in range(3):
            yield Document(str(index), {"metric": "x", "value": index}, index)
        raise RuntimeError("input stopped")
    with Database(":memory:") as database:
        batches = database.ingest_many(documents(), batch_size=2)
        assert database.stats()["documents"] == 0
        assert next(batches).inserted == 2
        with pytest.raises(RuntimeError, match="input stopped"):
            next(batches)
        assert [document.position for document in database.documents()] == [0, 1]
        assert next(database.rows()).result["sum"] == 1


def test_ingest_many_snapshots_each_yielded_document_payload():
    payload = {"metric": "x", "value": 0}
    def documents():
        for value in range(4):
            payload["value"] = value
            yield Document(str(value), payload, value)
    with Database(":memory:") as database:
        assert [batch.inserted for batch in database.ingest_many(documents(), batch_size=2)] == [2, 2]
        assert [item.payload["value"] for item in database.documents()] == [0, 1, 2, 3]
        assert next(database.rows()).result["sum"] == 6


def test_rows_pin_at_first_iteration_and_release_the_native_lock_between_pages(tmp_path, monkeypatch):
    with Database(tmp_path / "documents.log") as database:
        calls = []
        query = database.query
        def tracked(**kwargs):
            calls.append(kwargs)
            return query(**kwargs)
        monkeypatch.setattr(database, "query", tracked)
        iterator = database.rows(limit=1)
        assert calls == []
        database.add_many([{"metric": key, "value": value} for key, value in [("a", 1), ("b", 2), ("c", 3)]])
        assert next(iterator).key == "a"
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(database.add_many, [{"metric": "b", "value": 10}, {"metric": "aa", "value": 20}]).result(timeout=5)
        assert [(row.key, row.result["sum"]) for row in iterator] == [("b", 2), ("c", 3)]
        assert len(calls) == 3
        assert calls[1]["cursor"] == calls[2]["cursor"]
        assert [(row.key, row.result["sum"]) for row in database.rows()] == [("a", 1), ("aa", 20), ("b", 12), ("c", 3)]


def test_rebuild_invalidates_a_paused_snapshot_iterator():
    with Database(":memory:") as database:
        database.add_many([{"metric": key, "value": 1} for key in ("a", "b")])
        iterator = database.rows(limit=1)
        assert next(iterator).key == "a"
        database.rebuild()
        with pytest.raises(CursorExpired):
            next(iterator)
        with pytest.raises(DatabaseError, match="limit"):
            next(database.rows(limit=0))


def test_close_reports_checkpoint_failure_and_always_releases_the_handle(monkeypatch):
    database = Database(":memory:")
    def failure(engine, request):
        raise DatabaseError("cannot write cache checkpoint")
    monkeypatch.setattr(database, "_execute_json", failure)
    with pytest.raises(DatabaseError, match="cannot write"):
        database.close()
    assert database._engine is None
    database.close()


def test_concurrent_closes_are_idempotent(tmp_path):
    path = tmp_path / "documents.log"
    database = Database(path)
    database.add({"metric": "x", "value": 2})
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _: database.close(), range(8)))
    with Database(path) as reopened:
        assert reopened.stats()["cache"]["status"] == "loaded"
        assert next(reopened.rows()).result["sum"] == 2

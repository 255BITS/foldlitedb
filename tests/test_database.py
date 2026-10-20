"""The installed native API executes the same portable maps and reducers as JS."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json

import pytest

from foldlitedb import Database, DatabaseError, Document, View, portable_assets


def results(database, **options):
    return {row.key: row.result for row in database.rows(**options)}


def test_native_map_reduce_reopen_changes_and_rebuild(tmp_path):
    path = tmp_path / "metrics.db"
    with Database(path) as database:
        initial = database.query()
        retained = database.add_many([
            {"metric": "loss", "value": 4}, {"metric": "loss", "value": 8},
            {"metric": "精度", "value": 0.75},
        ])
        assert [document.position for document in retained] == [0, 1, 2]
        assert results(database)["loss"] == {"count": 2, "sum": 12, "mean": 6, "min": 4, "max": 8}
        updates = database.changes(since=initial.cursor)
        assert {row.key for row in updates.rows} == {"loss", "精度"}
        assert updates.complete
        assert database.stats()["module_sha256"] == View.bundled().sha256
    with Database(path) as database:
        assert database.ingest(retained).replayed == 3
        assert list(database.documents(limit=1)) == list(retained)
        expected = results(database)
    with Database(tmp_path / "rebuilt.db") as rebuilt:
        assert rebuilt.ingest(retained).inserted == 3
        assert results(rebuilt) == expected


def test_named_and_positioned_replays_conflicts_and_atomic_map_failure(tmp_path):
    with Database(tmp_path / "metrics.db") as database:
        document = database.add({"metric": "loss", "value": 4}, id="first", position=10)
        assert database.add(document.payload, id="first") == document
        assert database.add(document.payload, position=10) == document
        before = database.query()
        with pytest.raises(DatabaseError):
            database.add({"metric": "loss", "value": 7}, id="first")
        with pytest.raises(DatabaseError):
            database.add_many([{"metric": "loss", "value": 1e308}, {"metric": "loss", "value": 1e308}])
        assert database.query() == before
        assert database.stats()["documents"] == 1
        assert database.add({"metric": "loss", "value": 2}).position == 11


def test_snapshot_pages_and_change_pages_survive_new_writes(tmp_path):
    with Database(tmp_path / "metrics.db") as database:
        database.add_many([{"metric": key, "value": 1} for key in ("a", "b", "c")])
        iterator = database.rows(limit=1)
        assert next(iterator).key == "a"
        before = database.query()
        database.add_many([{"metric": "b", "value": 3}, {"metric": "d", "value": 4}])
        assert [(row.key, row.result["count"]) for row in iterator] == [("b", 1), ("c", 1)]
        page = database.changes(since=before.cursor, limit=1)
        changed = list(page.rows)
        while not page.complete:
            page = database.changes(since=page.cursor, limit=1)
            changed.extend(page.rows)
        assert {row.key for row in changed} == {"b", "d"}


def test_modules_rebuild_the_cache_and_cursors_are_bound_to_their_generation(tmp_path):
    path = tmp_path / "first.log"
    with Database(path) as first, Database(tmp_path / "second.log") as second:
        first.add({"metric": "loss", "value": 4, "category": "books", "id": "a", "title": "A"})
        cursor = first.query().cursor
        with pytest.raises(DatabaseError, match="(?i)cursor"):
            second.changes(since=cursor)
    with Database(path, View.bundled("catalogue")) as catalogue:
        assert results(catalogue) == {"books": {"count": 1, "items": {"a": "A"}}}
        assert catalogue.stats()["cache"]["status"] == "module-mismatch"
        with pytest.raises(DatabaseError, match="(?i)cursor"):
            catalogue.changes(since=cursor)
    with Database(path) as metrics:
        assert results(metrics)["loss"]["sum"] == 4
        assert metrics.stats()["documents"] == 1
    with pytest.raises(DatabaseError, match="digest"):
        View(View.bundled().module, sha256="0" * 64)


def test_input_generator_is_snapshotted_and_failure_commits_nothing():
    payload = {"metric": "loss", "value": 0}

    def payloads():
        for number in range(3):
            payload["value"] = number
            yield payload

    with Database(":memory:") as database:
        documents = database.add_many(payloads())
        assert [document.payload["value"] for document in documents] == [0, 1, 2]
        before = database.stats()["documents"]

        def fails():
            yield {"metric": "loss", "value": 10}
            raise RuntimeError("source failed")

        with pytest.raises(RuntimeError, match="source failed"):
            database.add_many(fails())
        assert database.stats()["documents"] == before
        for bad in (float("nan"), float("inf"), {1: "nonstring key"}, 2**53 + 1, 10**100 + 1,
                    "\ud800", {"\ud800": "invalid Unicode"}):
            with pytest.raises(DatabaseError):
                database.add(bad)
        with pytest.raises(DatabaseError, match="max_batch"):
            database.add_many({"metric": "x", "value": 1} for _ in range(1025))


def test_one_native_owner_serializes_threaded_appends(tmp_path):
    path = tmp_path / "shared.log"
    with Database(path) as database:
        def append(_):
            return database.add_many([{"metric": "loss", "value": 1}] * 5)
        with ThreadPoolExecutor(max_workers=2) as executor:
            documents = [document for batch in executor.map(append, range(2)) for document in batch]
        assert sorted(document.position for document in documents) == list(range(10))
        assert results(database)["loss"]["count"] == 10
        with pytest.raises(DatabaseError, match="lock|owner|open"):
            Database(path)
    with Database(path) as reopened:
        assert reopened.stats()["documents"] == 10


def test_browser_export_shape_is_accepted_without_identity_changes(tmp_path):
    exported = json.loads('[{"id":"web-1","source":"browser","position":0,"payload":{"metric":"loss","value":4}}]')
    with Database(tmp_path / "imported.db") as database:
        database.ingest(Document(**record) for record in exported)
        assert [asdict(document) for document in database.documents()] == exported
        assert portable_assets().joinpath("client.js").is_file()
        assert portable_assets().joinpath("worker.js").is_file()


def test_document_names_are_scoped_to_their_source():
    with Database(":memory:") as database:
        first = database.add({"metric": "loss", "value": 4}, id="same", source="one")
        second = database.add({"metric": "loss", "value": 8}, id="same", source="two")
        assert first.position == second.position == 0
        assert database.add(first.payload, id="same", source="one") == first
        assert database.add(second.payload, id="same", source="two") == second
        assert results(database)["loss"]["count"] == 2


def test_structured_catalogue_fold_and_reserved_unicode_keys(tmp_path):
    with Database(tmp_path / "catalogue.db", View.bundled("catalogue")) as database:
        documents = database.add_many([
            {"category": "图书", "id": "__proto__", "title": "First"},
            {"category": "图书", "id": "line\n\"\\", "title": "Second 😀"},
            {"category": "图书", "id": "__proto__", "title": "Revised"},
        ])
        result = results(database)["图书"]
        assert result == {"count": 2, "items": {"__proto__": "Revised", "line\n\"\\": "Second 😀"}}
        database.add({"category": "图书", "id": "__proto__", "deleted": True})
        assert results(database)["图书"]["items"] == {"line\n\"\\": "Second 😀"}
        assert len(list(database.documents())) == 4


def test_zero_emission_documents_advance_coverage_and_replay_once():
    with Database(":memory:") as database:
        before = database.query()
        documents = database.add_many([None, {"ignored": "document"}, {"metric": "x", "value": None}])
        assert list(database.rows()) == []
        after = database.changes(since=before.cursor)
        assert after.complete and after.rows == () and after.cursor != before.cursor
        assert database.ingest(documents).replayed == 3
        assert database.stats()["documents"] == 3


def test_floating_point_payloads_and_results_roundtrip_exactly(tmp_path):
    values = [-9.281557522556183e-188, -1.7486753975874687e180, 1e18 + 128, 5e-324]
    path = tmp_path / "numeric.db"
    with Database(path) as database:
        documents = database.add_many({"metric": str(index), "value": value}
                                      for index, value in enumerate(values))
        assert [document.payload["value"] for document in documents] == values
        assert [row.result["sum"] for row in database.rows()] == values
        assert [document.payload["value"] for document in database.documents()] == values
        assert database.ingest(documents).replayed == len(values)
    with Database(path) as reopened:
        assert [document.payload["value"] for document in reopened.documents()] == values


def test_large_resolved_documents_can_be_replayed_in_bounded_batches(tmp_path):
    with Database(tmp_path / "source.db") as source:
        documents = [source.add({"metric": "x", "value": index, "padding": "x" * 100000})
                     for index in range(3)]
        expected = results(source)
    with Database(tmp_path / "rebuilt.db") as rebuilt:
        batches = list(rebuilt.ingest_many(documents, batch_size=1))
        assert [batch.inserted for batch in batches] == [1, 1, 1]
        assert results(rebuilt) == expected
        with pytest.raises(DatabaseError, match="batch_size"):
            list(rebuilt.ingest_many([], batch_size=0))


def test_close_releases_engine_and_rejects_operations():
    database = Database(":memory:")
    database.close()
    with pytest.raises(DatabaseError, match="closed"):
        database.query()

"""Durability, replay, ordering and bounded snapshot/change-feed contracts."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys
import threading
import tracemalloc

import pytest

from foldlitedb.database import (
    CursorExpired, Database, DatabaseError, Document, DocumentConflict,
    GenerationMismatch, OrderedFold, RustReducer, SourceOrderError, View,
)


def sums(name="totals", version="1", *, references=None):
    return View(name, version, lambda d: [(d.payload["key"], d.payload["value"])],
                OrderedFold(0, lambda a, b: a + b, version="sum-1"),
                references or {})


def doc(position, key="x", value=1, *, source="default", identifier=None):
    return Document(str(position) if identifier is None else identifier,
                    {"key": key, "value": value}, position, source)


def results(database, name="totals", **kwargs):
    return [(row.key, row.result) for row in database.query(name, **kwargs).rows]


def test_reopen_replay_and_source_scoped_identity(tmp_path):
    path = tmp_path / "db.sqlite"
    with Database(path) as database:
        database.register(sums())
        assert database.checkpoint() is None
        first = database.ingest([doc(10), doc(40, value=2)])
        assert (first.inserted, first.replayed, first.sequence) == (2, 0, 2)
        assert database.checkpoint() == 40  # Gaps represent offsets, not lost records.
    with Database(path) as database:
        database.register(sums())
        replay = database.ingest([doc(10), doc(40, value=2), doc(50, value=4)])
        assert (replay.inserted, replay.replayed) == (1, 2)
        # The same IDs and source positions in a different stream are distinct.
        database.ingest([doc(10, value=8, source="other")])
        assert database.checkpoint("other") == 10
        assert results(database) == [("x", 15)]
        assert database.stats()["documents"] == 4


@pytest.mark.parametrize("conflicting,exception", [
    (doc(0, value=9), DocumentConflict),
    (doc(1, identifier="0"), DocumentConflict),
    (doc(0, identifier="replacement"), DocumentConflict),
    (doc(2, identifier="unseen"), SourceOrderError),
])
def test_conflicts_rollback_whole_batch(tmp_path, conflicting, exception):
    with Database(tmp_path / "db") as database:
        database.register(sums())
        database.ingest([doc(0), doc(4)])
        original = database.query("totals")
        with pytest.raises(exception):
            database.ingest([doc(5), conflicting])
        assert database.checkpoint() == 4
        assert database.get("5") is None
        assert database.query("totals") == original


def test_mapper_failure_rolls_back_documents_state_checkpoints_and_changes(tmp_path):
    fail = {"enabled": False}

    def mapping(document):
        # Force an actual state write before the next document fails.
        for _ in range(1024):
            yield "x", 1
        if fail["enabled"]:
            raise RuntimeError("mapper failure")

    view = View("totals", "1", mapping, OrderedFold(0, lambda a, b: a + b, version="1"))
    with Database(tmp_path / "db") as database:
        database.register(view)
        database.ingest([doc(0)])
        snapshot = database.query("totals")
        fail["enabled"] = True
        with pytest.raises(RuntimeError, match="mapper failure"):
            database.ingest([doc(1)])
        assert database.checkpoint() == 0
        assert database.stats()["documents"] == 1
        assert database.changes("totals", since=snapshot.cursor).rows == ()
        fail["enabled"] = False
        database.ingest([doc(1)])
        assert results(database) == [("x", 2048)]


def test_process_exit_during_transaction_is_recoverable(tmp_path):
    path = tmp_path / "db"
    with Database(path) as database:
        database.register(sums())
        database.ingest([doc(0)])
    script = """
import os, sys
from foldlitedb.database import Database, Document, OrderedFold, View
def mapping(document):
    if document.position == 2:
        os._exit(23)
    for _ in range(1024):
        yield 'x', 1
db = Database(sys.argv[1])
db.register(View('crash', '1', mapping, OrderedFold(0, lambda a,b:a+b, version='1')))
db.ingest([Document('1', {}, 1), Document('2', {}, 2)])
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    completed = subprocess.run([sys.executable, "-c", script, str(path)], env=environment)
    assert completed.returncode == 23
    with Database(path) as database:
        database.register(sums())
        assert database.checkpoint() == 0
        assert results(database) == [("x", 1)]
        assert database.get("1") is None
        database.ingest([doc(1), doc(2)])
        assert results(database) == [("x", 3)]


def test_disposable_database_rebuild_matches_live_ordered_folds(tmp_path):
    documents = [
        Document("a0", {"item": "a", "open": True}, 0),
        Document("b0", {"item": "b", "open": True}, 1),
        Document("a1", {"item": "a", "open": False}, 2),
        Document("a2", {"item": "a", "open": True}, 3),
    ]
    view = View("current", "1", lambda d: [(d.payload["item"], d.payload)],
                OrderedFold(None, lambda _, value: value, version="latest-1"))
    with Database(tmp_path / "live") as live, Database(tmp_path / "rebuilt") as rebuilt:
        live.register(view)
        for document in documents:
            live.ingest([document])
        # Ingest retained documents first; registering a view builds its index.
        rebuilt.ingest(documents)
        rebuilt.register(view)
        expected = [("a", {"item": "a", "open": True}), ("b", {"item": "b", "open": True})]
        assert results(live, "current") == expected == results(rebuilt, "current")


def test_view_reference_and_code_revision_rebuild_and_invalidate_cursors(tmp_path):
    with Database(tmp_path / "db") as database:
        database.register(sums(references={"taxonomy": "v1"}))
        database.ingest([doc(0), doc(1)])
        previous = database.query("totals")
        # Reusing the same definition preserves cursors and does no work.
        database.register(sums(references={"taxonomy": "v1"}))
        assert database.query("totals").cursor == previous.cursor
        view = View("totals", "2", lambda d: [(d.payload["key"], d.payload["value"] * 2)],
                    OrderedFold(0, lambda a, b: a + b, version="sum-1"), {"taxonomy": "v2"})
        database.register(view)
        assert results(database) == [("x", 4)]
        with pytest.raises(CursorExpired):
            database.changes("totals", since=previous.cursor)
        revision = database.query("totals")
        database.register(View("totals", "2", view.map, view.reducer, {"taxonomy": "v3"}))
        with pytest.raises(CursorExpired):
            database.query("totals", cursor=revision.cursor)


def test_generation_requires_explicit_reset_and_invalidates_other_instances(tmp_path):
    path = tmp_path / "db"
    with Database(path, generation="a") as original:
        original.register(sums())
        original.ingest([doc(0)])
        before = original.query("totals")
        with pytest.raises(GenerationMismatch):
            Database(path, generation="b")
        with Database(path, generation="b", reset=True) as replacement:
            replacement.register(sums())
            assert replacement.checkpoint() is None
            assert results(replacement) == []
            with pytest.raises(CursorExpired):
                replacement.changes("totals", since=before.cursor)
            with pytest.raises(CursorExpired):
                original.ingest([doc(1)])


def test_snapshot_pages_are_stable_while_other_connection_appends(tmp_path):
    path = tmp_path / "db"
    with Database(path) as reader, Database(path) as writer:
        reader.register(sums())
        writer.register(sums())
        writer.ingest([doc(0, "a", 1), doc(1, "b", 2), doc(2, "c", 3)])
        first = reader.query("totals", limit=1)
        assert [(r.key, r.result) for r in first.rows] == [("a", 1)]
        writer.ingest([doc(3, "b", 10), doc(4, "aa", 20), doc(5, "d", 30)])
        second = reader.query("totals", limit=1, cursor=first.cursor, after=first.after)
        third = reader.query("totals", limit=1, cursor=first.cursor, after=second.after)
        assert [(r.key, r.result) for r in second.rows + third.rows] == [("b", 2), ("c", 3)]
        assert third.complete
        updates = reader.changes("totals", since=first.cursor, limit=1)
        seen = []
        while True:
            seen.extend((r.key, r.result) for r in updates.rows)
            if updates.complete:
                break
            updates = reader.changes("totals", since=updates.cursor, limit=1)
        assert seen == [("aa", 20), ("b", 12), ("d", 30)]
        assert reader.changes("totals", since=updates.cursor).rows == ()


def test_zero_emission_batches_advance_resume_cursor(tmp_path):
    with Database(tmp_path / "db") as database:
        database.register(View("empty", "1", lambda _: [], OrderedFold(0, lambda a, b: a + b, version="1")))
        before = database.query("empty")
        database.ingest([doc(10)])
        after = database.changes("empty", since=before.cursor)
        assert after.complete and not after.rows and before.cursor != after.cursor


def test_unattached_views_catch_up_on_register_and_next_ingest(tmp_path):
    path = tmp_path / "db"
    with Database(path) as reader, Database(path) as writer:
        reader.register(sums())
        writer.ingest([doc(0)])  # A writer can retain documents without callbacks.
        assert results(reader) == [("x", 1)]  # Reader catches up its registered callback.
        reader.ingest([doc(1)])
        assert results(reader) == [("x", 2)]
        writer.ingest([doc(2)])
        reader.register(sums())
        assert results(reader) == [("x", 3)]


def test_pagination_tokens_bound_to_snapshot_and_filter(tmp_path):
    with Database(tmp_path / "db") as database:
        database.register(sums())
        database.ingest([doc(0, "a"), doc(1, "b")])
        first = database.query("totals", limit=1)
        with pytest.raises(DatabaseError, match="both cursor and after"):
            database.query("totals", after=first.after)
        database.ingest([doc(2, "c")])
        latest = database.query("totals")
        with pytest.raises(DatabaseError, match="different snapshot or filter"):
            database.query("totals", cursor=latest.cursor, after=first.after)
        with pytest.raises(DatabaseError, match="different snapshot or filter"):
            database.query("totals", key="b", cursor=first.cursor, after=first.after)
        for token in ("!", "", "e30", "bnVsbA"):
            with pytest.raises(DatabaseError):
                database.changes("totals", since=token)


def test_compaction_preserves_current_and_retained_snapshot(tmp_path):
    with Database(tmp_path / "db") as database:
        database.register(sums())
        old = database.query("totals")
        for position in range(10):
            database.ingest([doc(position, "a"), doc(position, "b", source="second")])
        retained = database.query("totals", limit=1)
        database.ingest([doc(10, "a", 100)])
        database.compact("totals", before=retained.cursor)
        assert database.stats()["history"] == 3
        assert results(database) == [("a", 110), ("b", 10)]
        assert results(database, cursor=retained.cursor) == [("a", 10), ("b", 10)]
        assert [(r.key, r.result) for r in database.changes("totals", since=retained.cursor).rows] == [("a", 110)]
        with pytest.raises(CursorExpired):
            database.changes("totals", since=old.cursor)


def test_mutating_map_cannot_change_cached_or_other_view_inputs(tmp_path):
    def mutating(document):
        document.payload["value"] = 999
        return []

    payload = {"key": "x", "value": 1}
    with Database(tmp_path / "db") as database:
        database.register(View("mutating", "1", mutating, OrderedFold(0, lambda a, b: a, version="1")))
        database.register(sums())
        database.ingest([Document("a", payload, 0)])
        assert payload["value"] == 1
        assert results(database) == [("x", 1)]
        cached = database.get("a")
        cached.payload["value"] = 42
        assert database.get("a").payload["value"] == 1


def test_mutating_reference_descriptor_requires_registration(tmp_path):
    references = {"taxonomy": "1"}
    with Database(tmp_path / "db") as database:
        database.register(sums(references=references))
        references["taxonomy"] = "2"
        with pytest.raises(DatabaseError, match="configuration changed"):
            database.ingest([doc(0)])
        assert database.checkpoint() is None
        database.register(sums(references=references))
        database.ingest([doc(0)])


def test_structured_keys_and_values_and_json_validation(tmp_path):
    fold = OrderedFold([], lambda state, value: state + [value], version="1")
    view = View("structured", "1", lambda d: [(d.payload["key"], d.payload["value"])], fold)
    with Database(tmp_path / "db") as database:
        database.register(view)
        key = ["catalogue", {"class": "α"}]
        database.ingest([Document("a", {"key": key, "value": {"enabled": True, "tags": ["x"]}}, 0)])
        assert results(database, "structured", key=key) == [(key, [{"enabled": True, "tags": ["x"]}])]
        for bad in (float("nan"), float("inf"), {1: "aliased"}, {"x": object()}):
            with pytest.raises(DatabaseError):
                database.ingest([Document("bad", bad, 1)])
        assert database.checkpoint() == 0


def test_bounds_rollback_and_stop_unbounded_map(tmp_path):
    visited = []

    def unbounded(_):
        for index in range(1000000):
            visited.append(index)
            yield "x", 1

    with Database(tmp_path / "db", max_batch=2, max_value_bytes=128,
                  max_emissions_per_document=3) as database:
        database.register(View("totals", "1", unbounded, OrderedFold(0, lambda a, b: a + b, version="1")))
        with pytest.raises(DatabaseError, match="max_batch"):
            database.ingest([doc(0), doc(1), doc(2)])
        with pytest.raises(DatabaseError, match="max_value_bytes"):
            database.ingest([Document("x", "a" * 129, 0)])
        with pytest.raises(DatabaseError, match="max_emissions"):
            database.ingest([doc(0)])
        assert len(visited) == 4
        assert database.stats()["documents"] == 0
    with Database(tmp_path / "keys", max_key_bytes=4) as database:
        database.register(sums())
        with pytest.raises(DatabaseError, match="max_key_bytes"):
            database.ingest([doc(0, "large")])
    with Database(tmp_path / "state", max_value_bytes=128) as database:
        database.register(View("growth", "1", lambda _: [("x", "a")],
                               OrderedFold("", lambda a, b: a + "x" * 129, version="1")))
        with pytest.raises(DatabaseError, match="reducer state"):
            database.ingest([doc(0)])
        assert database.stats()["documents"] == 0


def test_pending_map_values_have_a_byte_budget(tmp_path):
    # Small source documents can emit much larger contributions. Input batch
    # limits alone must not permit gigabytes of transient Python buffering.
    payload = "x" * 300000
    with Database(tmp_path / "db", max_batch_bytes=600000) as database:
        database.register(View("latest", "1", lambda d: [(d.id, payload)],
                               OrderedFold(None, lambda _, value: value, version="1")))
        tracemalloc.start()
        try:
            database.ingest([Document(str(i), {}, i) for i in range(40)])
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert peak < 8 * 1024 * 1024
        assert database.stats()["states"] == 40


def test_shared_connection_thread_writes_preserve_atomic_results(tmp_path):
    with Database(tmp_path / "db") as database:
        database.register(sums())
        barrier = threading.Barrier(4)

        def write(source):
            barrier.wait()
            for position in range(10):
                database.ingest([doc(position, source=source)])

        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(write, ["a", "b", "c", "d"]))
        assert results(database) == [("x", 40)]
        assert database.stats()["documents"] == 40


def test_ingest_many_is_bounded_and_reports_committed_batches(tmp_path):
    with Database(tmp_path / "db", max_batch=3) as database:
        database.register(sums())
        batches = list(database.ingest_many((doc(i) for i in range(8)), batch_size=3))
        assert [batch.inserted for batch in batches] == [3, 3, 2]
        assert results(database) == [("x", 8)]


@pytest.mark.parametrize("name,expected", [("mean/v1", 2), ("sum/v1", 6), ("count/v1", 3)])
def test_rust_builtins_rebuild_and_live_equivalence(tmp_path, name, expected):
    pytest.importorskip("wasmtime")
    view = View("numeric", "1", lambda d: [("x", d.payload)], RustReducer(name))
    documents = [Document(str(i), value, i) for i, value in enumerate([1, None, 2, 3])]
    with Database(tmp_path / "live") as live, Database(tmp_path / "rebuilt") as rebuilt:
        live.register(view)
        live.ingest(documents[:2])
        live.ingest(documents[2:])
        rebuilt.ingest(documents)
        rebuilt.register(view)
        assert live.query("numeric").rows[0].state == rebuilt.query("numeric").rows[0].state
        finalized = live.query("numeric").rows[0].result
        assert finalized["value"] == expected


def test_positioned_rust_adapter_preserves_caller_positions(tmp_path):
    pytest.importorskip("wasmtime")
    reducer = RustReducer("envelope/v1", positioned=True)
    view = View("numeric", "1", lambda d: [("x", d.payload)], reducer)
    values = [{"value": 2, "position": [50, "a"]}, {"value": 1, "position": [40, "b"]}]
    with Database(tmp_path / "db") as database:
        database.register(view)
        database.ingest([Document(str(i), value, i) for i, value in enumerate(values)])
        state = database.query("numeric").rows[0].state
        assert state == reducer.runtime.add(reducer.runtime.identity("envelope/v1"), values)


def test_positioned_rust_adapter_splits_by_actual_encoded_request_bytes(tmp_path):
    pytest.importorskip("wasmtime")
    reducer = RustReducer("envelope/v1", positioned=True)
    # ASCII control characters expand sixfold in JSON at the WASM boundary.
    values = [{"value": index, "position": [index, "\x01" * 128]} for index in range(1024)]
    with Database(tmp_path / "db") as database:
        database.register(View("numeric", "1", lambda d: [("x", d.payload)], reducer))
        database.ingest([Document(str(i), value, i) for i, value in enumerate(values)])
        state = database.query("numeric").rows[0].state
        expected = reducer.runtime.identity("envelope/v1")
        for offset in range(0, len(values), 64):
            expected = reducer.runtime.add(expected, values[offset:offset + 64])
        assert state == expected


def test_query_and_change_pages_obey_byte_budget(tmp_path):
    with Database(tmp_path / "db", max_page_bytes=1200) as database:
        database.register(View("latest", "1", lambda d: [(d.id, d.payload)],
                               OrderedFold(None, lambda _, value: value, version="1")))
        empty = database.query("latest")
        database.ingest([Document(str(i), "x" * 400, i) for i in range(3)])
        first = database.query("latest", limit=100)
        assert len(first.rows) == 1 and not first.complete
        second = database.query("latest", cursor=first.cursor, after=first.after)
        assert len(second.rows) == 1 and not second.complete
        changes = database.changes("latest", since=empty.cursor)
        assert len(changes.rows) == 1 and not changes.complete
        remaining = database.changes("latest", since=changes.cursor)
        assert len(remaining.rows) == 1 and not remaining.complete


def test_cached_document_reads_detect_another_instances_reset(tmp_path):
    path = tmp_path / "db"
    with Database(path) as old:
        old.ingest([doc(0)])
        with Database(path, reset=True):
            for read in (lambda: old.get("0"), old.checkpoint, old.stats):
                with pytest.raises(CursorExpired):
                    read()


@pytest.mark.parametrize("timeout", [-1, True, None, "2", float("nan"), float("inf"), 2147483.648])
def test_invalid_lock_timeouts_are_rejected_before_opening(timeout):
    with pytest.raises(DatabaseError, match="timeout must be finite"):
        Database(":memory:", timeout=timeout)


@pytest.mark.parametrize("timeout", [0, 0.125, 2147483.647])
def test_supported_lock_timeout_boundaries(timeout):
    with Database(":memory:", timeout=timeout) as database:
        assert database.stats()["documents"] == 0

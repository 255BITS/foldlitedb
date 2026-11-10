"""Callback projections share native acceptance, commit, and replay boundaries."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys

import pytest

from foldlitedb import Database, DatabaseError, Document, OrderedFold, RustReducer, View


def total_view(*, version="1", mapper=None, reducer=None, references=None):
    return View("totals", version=version,
                map=mapper or (lambda d: [(d.payload.get("key", "total"), d.payload["value"])]),
                reducer=reducer or OrderedFold(0, lambda state, value: state + value, version=version),
                references=references)


def total(db):
    return db.query("totals").rows[0].result


def test_all_append_paths_fold_only_resolved_new_documents():
    with Database(":memory:", total_view()) as db:
        first = db.add({"value": 1}, id="one")
        second, third = db.add_many([{"value": 2}, {"value": 3}])
        retry = db.ingest([first, second, Document("four", {"value": 4}), first])
        assert (retry.inserted, retry.replayed) == (1, 3)
        assert total(db) == 10
        assert [batch.inserted for batch in db.ingest_many([
            third, Document("five", {"value": 5}), Document("five", {"value": 5})
        ], batch_size=3)] == [1]
        assert total(db) == 15
        assert db.query() == db.query("totals")
        assert list(db.rows()) == list(db.query().rows)
        assert [document.position for document in db.documents()] == list(range(5))


def test_callback_rust_reducer_receives_resolved_ids_and_positions():
    view = total_view(reducer=RustReducer("sum/v1"))
    with Database(":memory:", view) as db:
        db.ingest([Document("one", {"value": 3}), Document("one", {"value": 3}),
                   Document("two", {"value": 4})])
        assert total(db) == {"count": 2, "value": 7}


@pytest.mark.parametrize("failure", ["map", "add", "finalize", "json"])
def test_failed_callbacks_commit_no_documents_or_partial_groups(tmp_path, failure):
    def mapper(doc):
        if doc.payload.get("fail") and failure == "map":
            raise RuntimeError("map failed")
        return [(doc.payload["key"], doc.payload["value"])]

    def step(state, value):
        state["values"].append(value)
        if value == 9 and failure == "add":
            raise RuntimeError("add failed")
        return state

    def finalize(state):
        if 9 in state["values"]:
            if failure == "finalize":
                raise RuntimeError("finalize failed")
            if failure == "json":
                return float("inf")
        return state

    view = total_view(mapper=mapper,
                      reducer=OrderedFold({"values": []}, step, finalize=finalize, version="1"))
    path = tmp_path / "atomic.fold"
    with Database(path, view) as db:
        db.add({"key": "a", "value": 1})
        before, stats = db.query(), db.stats()
        with pytest.raises((RuntimeError, DatabaseError)):
            db.add_many([{"key": "a", "value": 2}, {"key": "z", "value": 9, "fail": True}])
        assert db.query() == before
        assert db.stats() == stats
        assert db.add({"key": "a", "value": 3}).position == 1
        assert db.query(key="a").rows[0].state == {"values": [1, 3]}
    with Database(path, view) as reopened:
        assert len(list(reopened.documents())) == 2
        assert reopened.query(key="a").rows[0].state == {"values": [1, 3]}


def test_later_callback_failure_leaves_all_views_and_source_unchanged():
    with Database(":memory:", total_view()) as db:
        other = View("other", version="1", map=lambda d: [("other", d.payload["value"])],
                     reducer=OrderedFold(0, lambda s, v: (_ for _ in ()).throw(RuntimeError("later")), version="1"))
        db.register(other)
        with pytest.raises(RuntimeError, match="later"):
            db.add({"value": 1})
        assert db.query().rows == db.query("other").rows == ()
        assert db.stats()["documents"] == 0


def test_callback_cache_is_disposable_and_rebuild_preserves_log(tmp_path):
    path = tmp_path / "replay.fold"
    with Database(path, total_view()) as db:
        db.add_many([{"value": 1}, {"value": 2}])
        db.add({"value": 3})
        expected = db.query().rows
        assert db.stats()["documents"] == 3  # no aggregate documents in the source log
    source = path.read_bytes()
    Path(str(path) + ".cache").unlink()
    with Database(path, total_view()) as db:
        assert db.query().rows == expected
        old_cursor = db.query().cursor
        db.rebuild()
        assert db.query().rows == expected
        assert db.query().cursor != old_cursor
    assert path.read_bytes() == source


def test_callback_replays_only_original_uncached_suffix_after_process_exit(tmp_path):
    path = tmp_path / "crash.fold"
    with Database(path, total_view()) as db:
        db.add_many([{"value": 1}, {"value": 2}])
    program = '''
import os, sys
from foldlitedb import Database, View, OrderedFold
view=View("totals",version="1",map=lambda d:[("total",d.payload["value"])],
          reducer=OrderedFold(0,lambda s,v:s+v,version="1"))
db=Database(sys.argv[1],view)
db.add_many([{"value":3},{"value":4}])
os._exit(0)
'''
    subprocess.run([sys.executable, "-I", "-c", program, str(path)], check=True)
    mapped = []
    def track(doc):
        mapped.append(doc.payload["value"])
        return [("total", doc.payload["value"])]
    with Database(path, total_view(mapper=track)) as db:
        assert total(db) == 10
        assert mapped == [3, 4]
        assert db.ingest(list(db.documents())).inserted == 0
        assert total(db) == 10


def test_callback_view_descriptor_change_rebuilds_retained_prefix(tmp_path):
    path = tmp_path / "descriptor.fold"
    with Database(path, total_view()) as db:
        db.add_many([{"value": 2}, {"value": 3}])
    with Database(path, total_view(version="2", mapper=lambda d: [("total", 2 * d.payload["value"])])) as db:
        assert total(db) == 10
        db.add({"value": 4})
        assert total(db) == 18


def test_callback_paging_pins_snapshot_and_returns_independent_json():
    view = total_view(reducer=OrderedFold([], lambda s, v: s + [v], version="1"))
    with Database(":memory:", view) as db:
        db.add_many([{"key": key, "value": 1} for key in ("a", "b", "c")])
        page = db.query(limit=1)
        assert not page.complete and page.after is not None
        page.rows[0].state.append(999)
        page.rows[0].result.append(888)
        db.add({"key": "b", "value": 2})
        suffix = db.query(limit=10, cursor=page.cursor, after=page.after)
        assert [(row.key, row.result) for row in suffix.rows] == [("b", [1]), ("c", [1])]
        assert db.query(key="a").rows[0].result == [1]
        with pytest.raises(DatabaseError, match="different callback snapshot"):
            db.query(key="b", cursor=page.cursor, after=page.after)
        with pytest.raises(DatabaseError, match="change feed"):
            db.changes(since=page.cursor)
        with pytest.raises(DatabaseError, match="resolved Document"):
            db.preview({"value": 1})
    with pytest.raises(DatabaseError, match="closed"):
        db.query()


def test_callback_threaded_appends_are_serialized():
    with Database(":memory:", total_view()) as db:
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(lambda _: db.add({"value": 1}), range(40)))
        assert total(db) == 40
        assert len(list(db.documents())) == 40


def test_callback_reentrant_mutation_fails_before_any_commit():
    with Database(":memory:") as db:
        db.register(total_view(mapper=lambda d: [("total", db.add(1))]))
        with pytest.raises(DatabaseError, match="must not mutate"):
            db.add({"value": 1})
        assert db.stats()["documents"] == 0


def test_ingest_consumption_is_bounded_and_payloads_snapshot_per_yield():
    visited = []
    def too_many():
        for index in range(5000):
            visited.append(index)
            yield Document(str(index), None)
    with Database(":memory:") as db:
        with pytest.raises(DatabaseError, match="max_batch"):
            db.ingest(too_many())
        assert len(visited) == 1025
        assert db.stats()["documents"] == 0
        payload = {"value": 0}
        def reused():
            for index in range(3):
                payload["value"] = index
                yield Document(str(index), payload)
        db.ingest(reused())
        assert [doc.payload["value"] for doc in db.documents()] == [0, 1, 2]


def test_close_surfaces_callback_checkpoint_errors_and_releases_owner(tmp_path, monkeypatch):
    path = tmp_path / "close.fold"
    db = Database(path, total_view())
    db.add({"value": 2})
    def fail():
        raise DatabaseError("callback checkpoint failed")
    monkeypatch.setattr(db, "_flush_view_checkpoints", fail)
    with pytest.raises(DatabaseError, match="callback checkpoint failed"):
        db.close()
    with Database(path, total_view()) as reopened:
        assert total(reopened) == 2


def test_callback_does_not_run_metrics_reducer():
    with Database(":memory:", total_view()) as db:
        db.add_many([{"metric": "x", "value": 1e308}, {"metric": "x", "value": -1e308}])
        assert total(db) == 0


def test_prepared_native_transaction_is_invalidated_by_mutation():
    with Database(":memory:") as db:
        staged = db._call({"op": "prepare_ingest", "documents": [{"id": "one", "payload": None}]})
        db.add(None, id="two")
        with pytest.raises(DatabaseError, match="prepared"):
            db._call({"op": "commit_prepared", "token": staged["token"]})
        assert [doc.id for doc in db.documents()] == ["two"]


def test_callback_cursors_cannot_be_reused_with_a_different_named_view():
    with Database(":memory:", total_view()) as db:
        db.register(View("other", version="1", map=lambda d: [("z", d.payload["value"])],
                         reducer=OrderedFold(0, lambda s, v: s + v, version="1")))
        db.add_many([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
        first = db.query("totals", limit=1)
        with pytest.raises(DatabaseError, match="different view descriptor"):
            db.query("other", cursor=first.cursor, after=first.after)


def test_historical_query_cannot_clear_an_outer_callback_mutation_guard():
    snapshot = None
    def mapper(doc):
        if doc.payload["value"] == 3:
            db.query(cursor=snapshot.cursor)
            db.add({"value": 999})
        return [("total", doc.payload["value"])]
    with Database(":memory:", total_view(mapper=mapper)) as db:
        db.add({"value": 1})
        snapshot = db.query()
        db.add({"value": 2})
        with pytest.raises(DatabaseError, match="must not mutate"):
            db.add({"value": 3})
        assert total(db) == 3
        assert [doc.payload["value"] for doc in db.documents()] == [1, 2]


def test_callback_maps_receive_independent_payload_copies():
    def mutate(doc):
        doc.payload["value"] = 99
        return [("total", doc.payload["value"])]
    with Database(":memory:", total_view(mapper=mutate)) as db:
        db.register(View("other", version="1", map=lambda d: [("other", d.payload["value"])],
                         reducer=OrderedFold(0, lambda s, v: s + v, version="1")))
        db.add({"value": 2})
        assert total(db) == 99
        assert db.query("other").rows[0].result == 2
        assert next(db.documents()).payload == {"value": 2}


def test_failed_open_and_invalid_callback_view_never_clobber_log_or_cache(tmp_path, monkeypatch):
    from foldlitedb import GenerationMismatch

    path = tmp_path / "safe_open.fold"
    cache = tmp_path / "safe_open.fold.cache"
    with Database(path, total_view(), generation="gen-1") as db:
        db.add({"value": 10})
    original_log = path.read_bytes()
    original_cache = cache.read_bytes()

    # Invalid callback View with reset=True must fail BEFORE touching the log.
    bad_view = View("totals", version="1", map="not-callable", reducer="not-reducer")
    with pytest.raises(DatabaseError, match="callable"):
        Database(path, bad_view, reset=True)
    assert path.read_bytes() == original_log
    assert cache.read_bytes() == original_cache

    # Generation mismatch must close without overwriting the valid cache.
    with pytest.raises(GenerationMismatch):
        Database(path, total_view(), generation="gen-2")
    assert cache.read_bytes() == original_cache

    # Failing callback replay during open must close without overwriting the valid cache.
    def boom(_doc):
        raise RuntimeError("boom during open")

    with pytest.raises(RuntimeError, match="boom during open"):
        Database(path, total_view(version="2", mapper=boom))
    assert cache.read_bytes() == original_cache

    # Opening an existing non-empty un-tagged database with generation= must fail.
    untagged = tmp_path / "untagged.fold"
    with Database(untagged) as db:
        db.add({"metric": "x", "value": 1})
    with pytest.raises(GenerationMismatch):
        Database(untagged, generation="gen-1")

    # View.module is read-only and callback View ignores same-named files in cwd.
    monkeypatch.chdir(tmp_path)
    (tmp_path / "collision_name").write_bytes(b"not wasm")
    v = View("collision_name", version="1")
    assert v.is_callback
    with pytest.raises(AttributeError):
        v.module = b"mutated"


def test_lazy_finalization_and_group_reduction_enforce_reentrancy_and_rebuild_writes_cache_once(tmp_path):
    path = tmp_path / "rebuild_once.fold"
    cache = tmp_path / "rebuild_once.fold.cache"
    with Database(path, total_view()) as db:
        db.add({"value": 4})
        db.add({"value": 6})
        stats = db.rebuild()
        assert stats["cache"]["bytes"] == cache.stat().st_size

    # Re-entrancy guard blocks mutation inside lazy finalization and _reduce_view_group.
    db_ref = None
    view = View(
        "lazy",
        version="1",
        map=lambda d: [("k", d.payload["value"])],
        reducer=OrderedFold(
            0,
            lambda s, v: s + v,
            version="1",
            finalize=lambda s: db_ref.add({"value": 99}) if s == 10 else s,
        ),
    )
    with Database(":memory:", view) as db:
        db_ref = db
        db._reduce_view_group("lazy", None, "k", [10], 1)
        with pytest.raises(DatabaseError, match="must not mutate"):
            db.query("lazy")
        with pytest.raises(DatabaseError, match="must not mutate"):
            db.checkpoint()
        db._views["lazy"].states.clear()


def test_explicit_view_query_disambiguation_and_rust_reducer_trap_recovery():
    from foldlitedb import CursorExpired, DocumentConflict
    from foldlitedb.reducers import Reducer

    with Database(":memory:", View("by_key_primary", version="1",
                                   map=lambda d: [(d.payload["k"], d.payload["value"])],
                                   reducer=OrderedFold(0, lambda s, v: s + v, version="1"))) as db:
        db.register(View("by_key", version="1", map=lambda d: [(d.payload["k"], d.payload["value"])],
                         reducer=OrderedFold(0, lambda s, v: s + v, version="1")))
        db.add_many([{"k": "a", "value": 3}, {"k": "by_key", "value": 7}])
        assert [r.result for r in db.rows(view="by_key")] == [3, 7]
        assert db.query("by_key", key="a").rows[0].result == 3
        assert db.query(view="by_key", key="a").rows[0].result == 3
        # key="by_key" on primary view never collides with secondary view named "by_key".
        assert db.query(key="by_key").rows[0].result == 7
        assert [r.result for r in db.rows("by_key", key="by_key")] == [7]
        with pytest.raises(DatabaseError, match="Cannot specify both positional view name and keyword view="):
            db.query("by_key", view="by_key")
        with pytest.raises(DatabaseError, match="Unknown registered callback view"):
            db.query("unknown_view", key="a")
        with pytest.raises(DatabaseError, match="Unknown registered callback view"):
            db.query("a")
        with pytest.raises(DatabaseError, match="Unknown registered callback view"):
            db.query(view="unknown_view")

        # Guest Wasm view error messages containing "conflict" or "cursor" are DatabaseError,
        # not DocumentConflict or CursorExpired.
        class DummyEngine:
            def execute_json(self, _req):
                raise ValueError("View rejected operation: simulated conflict and cursor text")

        with pytest.raises(DatabaseError) as exc_info:
            db._execute_json(DummyEngine(), "{}")
        assert not isinstance(exc_info.value, (DocumentConflict, CursorExpired))

    # RustReducer with implicitly owned runtime recovers after a trap.
    reducer = RustReducer("sum/v1")
    reducer._runtime = Reducer(fuel=10)
    with pytest.raises(DatabaseError):
        reducer.identity()
    assert reducer.identity()["count"] == 0


def test_callback_state_exceeding_8_mib_checkpoints_reopens_warm_and_rebuilds(tmp_path):
    """Callback checkpoints exceeding the 8 MiB per-frame log limit persist and reopen cleanly."""
    chunk = "z" * 200_000
    large_view = View(
        "large_state",
        version="1",
        map=lambda d: [(d.payload["group"], chunk)],
        reducer=OrderedFold("", lambda s, v: v, version="1"),
    )
    path = tmp_path / "large_cb.fold"
    with Database(path, large_view) as db:
        # 50 groups * 200 KB > 10 MiB (> 8 MiB per-frame log limit), committed incrementally.
        for i in range(50):
            db.add({"group": f"g-{i:03d}"}, id=f"doc-{i:03d}")
        # Subsequent tiny append only sends delta for g-000, not 10 MiB!
        db.add({"group": "g-000"}, id="doc-extra")
        cp = db.checkpoint()
        assert cp["cache"]["bytes"] > 9 * 1024 * 1024
        assert db.get("doc-010").position == 10
        assert db.get(position=10).id == "doc-010"
        assert db.get(position=9999) is None
        with pytest.raises(DatabaseError, match="either id or position"):
            db.get()
        with pytest.raises(DatabaseError, match="either id or position"):
            db.get("doc-010", position=10)

    with Database(path, large_view) as reopened:
        assert reopened.stats()["cache"]["status"] == "loaded"
        assert reopened.query(key="g-049").rows[0].result == chunk
        rebuilt = reopened.rebuild()
        assert rebuilt["cache"]["bytes"] > 9 * 1024 * 1024
        assert reopened.query(key="g-000").rows[0].result == chunk

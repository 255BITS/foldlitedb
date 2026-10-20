"""Named imports and read-only mapping diagnostics for application developers."""
import math

import pytest

from foldlitedb import (
    CursorExpired, Database, DatabaseError, Document, DocumentConflict,
    SourceOrderError, View,
)


RECOVERY = "Obtain a fresh snapshot with db.query() and replace the previous results before resuming changes."


def test_named_batches_resolve_positions_per_source_and_replay_after_progress(tmp_path):
    path = tmp_path / "named.fold"
    first = Document("delivery-1", {"metric": "x", "value": 1}, source="a")
    second = Document("delivery-1", {"metric": "x", "value": 2}, source="b")
    third = Document("delivery-2", {"metric": "x", "value": 3}, source="a")
    assert first.position is None
    with Database(path) as db:
        batch = [first, second, third, first]
        inserted = db.ingest(batch)
        assert (inserted.inserted, inserted.replayed) == (3, 1)
        db.ingest([Document("delivery-3", {"metric": "x", "value": 4}, source="a")])
    # Raw log snapshots require releasing the native file lock on Windows.
    log = path.read_bytes()
    with Database(path) as db:
        before = db.stats()
        replay = db.ingest(batch)
        assert (replay.inserted, replay.replayed) == (0, 4)
        assert db.stats() == before
        retained = list(db.documents())
        assert [(doc.source, doc.position) for doc in retained] == [
            ("a", 0), ("b", 0), ("a", 1), ("a", 2),
        ]
        assert all(type(doc.position) is int for doc in retained)
        assert first.position is None  # Caller input remains an unresolved request.
        assert next(db.rows()).result["sum"] == 10
    assert path.read_bytes() == log
    with Database(path) as db:
        assert list(db.documents()) == retained
        assert db.ingest(batch).replayed == 4
        db.rebuild()
        assert list(db.documents()) == retained


def test_named_batch_conflicts_are_atomic_and_explicit_positions_keep_semantics(tmp_path):
    path = tmp_path / "atomic.fold"
    with Database(path) as db:
        db.ingest([Document("old", 1, 10, "a")])
    log = path.read_bytes()
    with Database(path) as db:
        before = db.stats()
        with pytest.raises(DocumentConflict):
            db.ingest([
                Document("new", 2, source="a"),
                Document("new", 3, source="b"),
                Document("old", 999, source="a"),
            ])
        assert db.stats() == before
    assert path.read_bytes() == log
    with Database(path) as db:
        assert db.ingest([Document("new", 2, source="a"), Document("new", 3, source="b")]).inserted == 2
        assert [(doc.source, doc.position) for doc in db.documents()] == [("a", 10), ("a", 11), ("b", 0)]
        assert db.ingest([Document("old", 1, 10, "a")]).replayed == 1
        with pytest.raises(DocumentConflict):
            db.ingest([Document("old", 1, 12, "a")])
        with pytest.raises(SourceOrderError):
            db.ingest([Document("late", 5, 5, "a")])
        assert db.add(4, source="a").position == 12


def test_ingest_many_snapshots_mutable_payloads_with_omitted_positions():
    payload = {"metric": "x", "value": 0}

    def events():
        for value in range(5):
            payload["value"] = value
            yield Document(str(value), payload, source="a" if value % 2 == 0 else "b")

    with Database(":memory:") as db:
        assert [batch.inserted for batch in db.ingest_many(events(), batch_size=2)] == [2, 2, 1]
        retained = list(db.documents())
        assert [doc.payload["value"] for doc in retained] == list(range(5))
        assert [doc.position for doc in retained] == [0, 0, 1, 1, 2]
        assert db.stats()["checkpoints"] == {"a": 2, "b": 1}
        assert next(db.rows()).result["sum"] == 10
        assert [batch.replayed for batch in db.ingest_many(events(), batch_size=2)] == [2, 2, 1]
        assert list(db.documents()) == retained


def test_preview_distinguishes_malformed_catalogue_events_without_storage_writes(tmp_path):
    path = tmp_path / "preview.fold"
    cache_path = tmp_path / "preview.fold.cache"
    valid = {"category": "books", "id": "product-a", "title": "Original"}
    with Database(path, View.bundled("catalogue")) as db:
        db.checkpoint()
    log, cache = path.read_bytes(), cache_path.read_bytes()
    with Database(path, View.bundled("catalogue")) as db:
        before = db.stats()
        mtimes = path.stat().st_mtime_ns, cache_path.stat().st_mtime_ns
        expected = [{"key": "books", "value": {"id": "product-a", "title": "Original", "deleted": False}}]
        assert db.preview(valid) == expected
        assert db.preview(dict(category="books", id="product-a", name="Wrong field")) == []
        assert db.preview(None) == []
        changed_reply = db.preview(valid)
        changed_reply[0]["value"]["title"] = "Mutated reply"
        assert db.preview(valid) == expected
        assert db.stats() == before
        assert list(db.documents()) == []
        assert cache_path.read_bytes() == cache
        assert (path.stat().st_mtime_ns, cache_path.stat().st_mtime_ns) == mtimes
    assert path.read_bytes() == log
    with Database(path, View.bundled("catalogue")) as db:
        assert db.add(valid, id="actual-delivery").position == 0


@pytest.mark.parametrize("value", [-0.0, -9.281557522556183e-188, 1e18 + 128, 10**20, 5e-324])
def test_preview_uses_deterministic_ieee_mapping(value):
    with Database(":memory:") as db:
        payload = {"metric": "x", "value": value}
        emission = db.preview(payload)
        assert emission == db.preview(payload)
        mapped = float(emission[0]["value"])
        expected = 0.0 if value == 0 else float(value)
        assert mapped.hex() == expected.hex()
        assert math.copysign(1, mapped) == math.copysign(1, expected)
        assert db.stats()["documents"] == db.stats()["sequence"] == 0
        db.add(payload)
        assert float(next(db.rows()).result["sum"]).hex() == expected.hex()


@pytest.mark.parametrize("payload", [float("nan"), float("inf"), 2**53 + 1, {1: "invalid key"}, "\ud800"])
def test_invalid_preview_inputs_do_not_change_progress(payload):
    with Database(":memory:") as db:
        before = db.stats()
        with pytest.raises(DatabaseError):
            db.preview(payload)
        assert db.stats() == before
        assert db.add(None, id="not-reserved").position == 0


def test_preview_does_not_promise_reducer_success_or_batch_fit():
    with Database(":memory:") as db:
        db.add({"metric": "huge", "value": 1e308})
        before = db.stats()
        assert db.preview({"metric": "huge", "value": 1e308}) == [{"key": "huge", "value": 1e308}]
        with pytest.raises(DatabaseError):
            db.add({"metric": "huge", "value": 1e308})
        assert db.stats() == before
        payload = {"metric": "x", "value": 1, "padding": "x" * 150000}
        assert db.preview(payload) == [{"key": "x", "value": 1}]
        with pytest.raises(DatabaseError, match="Wasm request exceeds the 262144-byte ABI buffer") as error:
            db.ingest([Document("one", payload), Document("two", payload)])
        assert "smaller batches" in str(error.value) and "ingest_many" in str(error.value)
        assert db.stats() == before
        assert [result.inserted for result in db.ingest_many([Document("one", payload), Document("two", payload)], batch_size=1)] == [1, 1]


def test_batch_bound_errors_suggest_an_ingest_many_recovery():
    with Database(":memory:") as db:
        with pytest.raises(DatabaseError, match="max_batch.*1024") as error:
            db.ingest(Document(str(index), None) for index in range(1025))
        assert "smaller batches" in str(error.value) and "ingest_many" in str(error.value)
        assert db.stats()["documents"] == 0
        assert db.add(None).position == 0


def test_expired_cursors_have_recovery_guidance_and_closed_preview_is_clear():
    db = Database(":memory:")
    cursor = db.query().cursor
    db.rebuild()
    with pytest.raises(CursorExpired, match="different database, view module, or cache generation") as error:
        db.changes(since=cursor)
    assert str(error.value).endswith(RECOVERY)
    with pytest.raises(DatabaseError, match="limit") as error:
        db.query(limit=0)
    assert RECOVERY not in str(error.value)
    db.close()
    with pytest.raises(DatabaseError, match="Database is closed"):
        db.preview(None)

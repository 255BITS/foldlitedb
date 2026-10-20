"""Immutable source identities and atomic native log appends."""
from dataclasses import FrozenInstanceError

import pytest

from foldlitedb import Database, DatabaseError, DocumentConflict, SourceOrderError


def test_plain_adds_are_distinct_and_documents_are_independent_copies(tmp_path):
    path = tmp_path / "documents.log"
    payload = {"metric": "loss", "value": 4, "nested": [1]}
    with Database(path) as database:
        first = database.add(payload)
        second = database.add(payload)
        assert first.id != second.id
        assert (first.position, second.position, first.source) == (0, 1, "default")
        with pytest.raises(FrozenInstanceError):
            first.id = "changed"
        payload["nested"].append(2)
        first.payload["nested"].append(3)
        persisted = list(database.documents())
        assert [item.payload["nested"] for item in persisted] == [[1], [1]]
        persisted[0].payload["value"] = 999
        assert next(database.rows()).result["sum"] == 8
    with Database(path) as reopened:
        assert [item.payload["value"] for item in reopened.documents()] == [4, 4]
        assert reopened.add(None).position == 2


def test_named_replays_never_append_or_mutate_the_original_log(tmp_path):
    path = tmp_path / "documents.log"
    with Database(path) as database:
        first = database.add({"metric": "x", "value": 4}, id="measurement/α", position=10)
        original = path.read_bytes()
        before = database.query()
        assert database.add(first.payload, id=first.id) == first
        assert database.add(first.payload, position=10) == first
        assert path.read_bytes() == original
        assert database.query() == before
        for kwargs, payload in [
            ({"id": first.id}, {"metric": "x", "value": 9}),
            ({"id": first.id, "position": 11}, first.payload),
            ({"id": "other", "position": first.position}, first.payload),
        ]:
            with pytest.raises(DocumentConflict):
                database.add(payload, **kwargs)
        assert path.read_bytes() == original
        assert database.stats()["documents"] == 1


def test_source_order_and_names_are_source_scoped():
    with Database(":memory:") as database:
        first = database.add("first", source="log", position=10)
        second = database.add("second", source="other", position=0)
        third = database.add("third", source="log")
        assert (first.position, second.position, third.position) == (10, 0, 11)
        assert database.add("first", source="log", position=10) == first
        with pytest.raises(DocumentConflict):
            database.add("changed", source="log", position=10)
        with pytest.raises(SourceOrderError):
            database.add("late", source="log", position=5)
        assert database.add("fourth", id="named", source="log").position == 12
        assert database.add("fifth", id="named", source="other").position == 1
        assert database.stats()["checkpoints"] == {"log": 12, "other": 1}


@pytest.mark.parametrize("kwargs", [
    {"id": ""}, {"id": 1}, {"id": "λ" * 2049},
    {"source": ""}, {"source": None},
    {"position": -1}, {"position": True}, {"position": 1.5},
    {"position": "1"}, {"position": 2**53},
])
def test_invalid_metadata_does_not_advance_the_log(kwargs):
    with Database(":memory:") as database:
        before = database.query()
        with pytest.raises(DatabaseError):
            database.add({}, **kwargs)
        assert database.query() == before
        assert database.stats()["documents"] == 0
        assert database.add(None).position == 0


@pytest.mark.parametrize("payload", [float("nan"), float("inf"), {1: "alias"}, object()])
def test_invalid_json_does_not_reserve_the_identity(payload):
    with Database(":memory:") as database:
        with pytest.raises(DatabaseError):
            database.add(payload, id="retry")
        assert database.add(None, id="retry").position == 0


def test_position_exhaustion_preserves_the_last_replayable_document():
    with Database(":memory:") as database:
        last = database.add(None, id="last", position=2**53 - 1)
        with pytest.raises(DatabaseError):
            database.add(None, id="overflow")
        assert database.add(None, id="last") == last
        assert database.add(None, position=last.position) == last
        assert database.stats()["documents"] == 1


def test_failed_fold_never_appends_a_partial_batch(tmp_path):
    path = tmp_path / "documents.log"
    with Database(path) as database:
        database.add({"metric": "x", "value": 2})
        original = path.read_bytes()
        before = database.query()
        with pytest.raises(DatabaseError):
            database.add_many([{"metric": "x", "value": 1e308}] * 2)
        assert path.read_bytes() == original
        assert database.query() == before
        assert database.add({"metric": "x", "value": 3}).position == 1
        assert path.read_bytes().startswith(original)

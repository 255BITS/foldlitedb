"""Plain JSON appends and optional source-scoped replay identities."""
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from foldlitedb import (
    Database, DatabaseError, Document, DocumentConflict, OrderedFold,
    RustReducer, SourceOrderError, View,
)


def collected(name="values", mapping=lambda document: [("all", document.payload)]):
    return View(name, "1", mapping,
                OrderedFold([], lambda state, value: state + [value], version="1"))


def values(database, name="values"):
    return database.query(name).rows[0].result


def test_plain_adds_are_distinct_and_persist_resolved_documents(tmp_path):
    path = tmp_path / "db"
    payload = {"metric": "loss", "value": 4}
    with Database(path) as database:
        database.register(collected())
        first = database.add(payload)
        second = database.add(payload)
        assert isinstance(first, Document)
        assert first.id and second.id and first.id != second.id
        assert (first.position, second.position, first.source) == (0, 1, "default")
        assert values(database) == [payload, payload]
    with Database(path) as database:
        database.register(collected())
        assert database.get(first.id) == first
        assert database.get(second.id) == second
        assert database.add(payload).position == 2
        assert values(database) == [payload] * 3


def test_optional_names_replay_without_remapping_after_reopen(tmp_path):
    calls = []

    def mapping(document):
        calls.append(document.id)
        yield "all", document.payload

    path = tmp_path / "db"
    with Database(path) as database:
        database.register(collected(mapping=mapping))
        first = database.add({"value": 4}, id="measurement/α")
        database.add({"value": 8}, position=40)
    with Database(path) as database:
        database.register(collected(mapping=mapping))
        before = database.query("values")
        assert database.add({"value": 4}, id="measurement/α") == first
        assert len(calls) == 2
        assert database.query("values") == before
        assert database.checkpoint() == 40
        for kwargs, payload in [
            ({"id": first.id}, {"value": 9}),
            ({"id": first.id, "position": 41}, first.payload),
            ({"id": "other", "position": first.position}, first.payload),
        ]:
            with pytest.raises(DocumentConflict):
                database.add(payload, **kwargs)
        assert database.stats()["documents"] == 2


def test_source_positions_supply_identity_and_keep_source_order(tmp_path):
    with Database(tmp_path / "db") as database:
        database.register(collected())
        first = database.add("first", source="log", position=10)
        second = database.add("second", source="other", position=0)
        third = database.add("third", source="log")
        assert (first.position, second.position, third.position) == (10, 0, 11)
        snapshot = database.query("values")
        assert database.add("first", source="log", position=10) == first
        assert database.query("values") == snapshot
        with pytest.raises(DocumentConflict):
            database.add("changed", source="log", position=10)
        with pytest.raises(SourceOrderError):
            database.add("late", source="log", position=5)
        # Caller-supplied names are also independent within each source.
        assert database.add("fourth", id="named", source="log").position == 12
        assert database.add("fifth", id="named", source="other").position == 1
        assert values(database) == ["first", "second", "third", "fourth", "fifth"]
        assert database.checkpoint("default") is None


def test_add_accepts_json_values_and_isolates_mutable_payloads(tmp_path):
    inputs = [None, False, 0, 1.25, "text", [1, {"nested": True}], {"value": [2]}]

    def mutating(document):
        if isinstance(document.payload, dict):
            document.payload["value"].append(999)
        return []

    with Database(tmp_path / "db") as database:
        database.register(collected("mutating", mutating))
        database.register(collected())
        documents = [database.add(payload) for payload in inputs]
        assert [document.payload for document in documents] == inputs
        assert values(database) == inputs
        inputs[-1]["value"].append(3)
        documents[-1].payload["value"].append(4)
        assert database.get(documents[-1].id).payload == {"value": [2]}
        assert values(database)[-1] == {"value": [2]}


def test_add_failure_rolls_back_identity_position_views_and_changes(tmp_path):
    fail = {"enabled": False}

    def mapping(document):
        if fail["enabled"]:
            raise RuntimeError("map failed")
        return []

    with Database(tmp_path / "db") as database:
        database.register(collected())
        database.register(collected("failure", mapping))
        database.add("first")
        before = database.query("values")
        fail["enabled"] = True
        with pytest.raises(RuntimeError, match="map failed"):
            database.add("second", id="retry")
        assert database.get("retry") is None
        assert database.checkpoint() == 0
        assert database.stats()["documents"] == 1
        assert database.changes("values", since=before.cursor).rows == ()
        fail["enabled"] = False
        assert database.add("second", id="retry").position == 1
        assert values(database) == ["first", "second"]


def test_auto_positions_serialize_across_separate_connections(tmp_path):
    path = tmp_path / "db"
    with Database(path) as first, Database(path) as second:
        first.register(collected())
        barrier = threading.Barrier(2)

        def append(database):
            barrier.wait()
            return [database.add(1) for _ in range(20)]

        with ThreadPoolExecutor(max_workers=2) as executor:
            batches = list(executor.map(append, [first, second]))
        documents = [document for batch in batches for document in batch]
        assert sorted(document.position for document in documents) == list(range(40))
        assert len({document.id for document in documents}) == 40
        assert first.checkpoint() == second.checkpoint() == 39
        assert values(first) == [1] * 40
        assert first.stats()["documents"] == 40


@pytest.mark.parametrize("kwargs", [
    {"id": ""}, {"id": 1}, {"id": "λ" * 2049},
    {"source": ""}, {"source": None},
    {"position": -1}, {"position": True}, {"position": 1.5},
    {"position": "1"}, {"position": 2**53},
])
def test_invalid_add_metadata_does_not_advance_progress(kwargs):
    with Database(":memory:") as database:
        with pytest.raises(DatabaseError):
            database.add({}, **kwargs)
        assert database.checkpoint() is None
        assert database.stats()["documents"] == 0


@pytest.mark.parametrize("payload", [float("nan"), float("inf"), {1: "alias"}, object()])
def test_invalid_add_payloads_do_not_reserve_names(payload):
    with Database(":memory:") as database:
        with pytest.raises(DatabaseError):
            database.add(payload, id="retry")
        assert database.add(None, id="retry").position == 0


def test_add_obeys_payload_limits_and_position_exhaustion(tmp_path):
    with Database(tmp_path / "small", max_value_bytes=8) as database:
        with pytest.raises(DatabaseError, match="max_value_bytes"):
            database.add("too large", id="retry")
        assert database.add(0, id="retry").position == 0
    with Database(tmp_path / "metadata", max_batch_bytes=33) as database:
        with pytest.raises(DatabaseError, match="max_batch_bytes"):
            database.add(None)
        assert database.checkpoint() is None
        assert database.stats()["documents"] == 0
        assert database.add(None, id="x", source="s").position == 0
    with Database(tmp_path / "positions") as database:
        last = database.add(None, id="last", position=2**53 - 1)
        with pytest.raises(DatabaseError):
            database.add(None, id="overflow")
        assert database.get("overflow") is None
        assert database.checkpoint() == last.position
        assert database.add(None, id="last") == last
        assert database.add(None, position=last.position) == last
        assert database.stats()["documents"] == 1


def test_retained_add_documents_regenerate_metadata_and_ingestion_order(tmp_path):
    def metadata(document):
        yield "all", [document.id, document.source, document.position, document.payload]

    view = collected(mapping=metadata)
    with Database(tmp_path / "live") as live:
        live.register(view)
        retained = [live.add("first"), live.add("second", id="named", source="other"),
                    live.add("third", position=40)]
        expected = values(live)
    with Database(tmp_path / "rebuilt") as rebuilt:
        assert rebuilt.ingest(retained).inserted == 3
        rebuilt.register(view)
        assert values(rebuilt) == expected
        replay = rebuilt.ingest(retained)
        assert (replay.inserted, replay.replayed) == (0, 3)


def test_retained_generated_ids_reproduce_envelope_ties_exactly(tmp_path):
    pytest.importorskip("wasmtime")
    view = View("envelope", "1", lambda document: [("all", document.payload)] * 2,
                RustReducer("envelope/v1"))
    with Database(tmp_path / "live") as live, Database(tmp_path / "rebuilt") as rebuilt:
        live.register(view)
        retained = [live.add(4, source="a", position=10),
                    live.add(4, source="b", position=10), live.add(2, source="a")]
        expected = live.query("envelope").rows[0]
        rebuilt.ingest(retained)
        rebuilt.register(view)
        actual = rebuilt.query("envelope").rows[0]
        assert actual.state == expected.state
        assert actual.result == expected.result

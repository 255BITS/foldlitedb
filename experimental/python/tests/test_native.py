"""Supported-subset conformance through the installed Python/native boundary."""
from concurrent.futures import ThreadPoolExecutor
import json
import subprocess
import sys
import threading
import time

import pytest

from foldlitedb import Database, DatabaseError, Document, RustReducer, View
from foldlitedb_experimental import NativeDatabase, NativeView


def mapped_value(document, work):
    value = document.payload["value"]
    if work:
        accumulator = document.position ^ 0x9e3779b9
        accumulator &= 0xffffffff
        for iteration in range(work):
            accumulator = (accumulator * 1664525 + 1013904223) & 0xffffffff
            accumulator = (((accumulator << 13) | (accumulator >> 19)) & 0xffffffff) ^ iteration
        if value is not None:
            value += accumulator & 0xffff
    return value


def definition(reducer="mean/v1", work=0, emissions=1):
    return NativeView("metrics", "key", "value", reducer=reducer,
                      work=work, emissions=emissions)


def reference(spec):
    def mapping(document):
        if not spec.emissions:
            return
        value = mapped_value(document, spec.work)
        for _ in range(spec.emissions):
            yield document.payload["key"], value
    return View(spec.name, spec.version, mapping, RustReducer(spec.reducer))


def documents():
    values = [1e16, 1.0, -1e16, 1.0, None, 4, -3, 1e-200, 9]
    return [Document(f"record-{index}", {"key": [index % 3, "group"], "value": value},
                     index * 10, "test") for index, value in enumerate(values)]


@pytest.mark.parametrize("backend", ["sqlite", "log"])
@pytest.mark.parametrize("reducer", ["mean/v1", "sum/v1", "count/v1", "envelope/v1"])
@pytest.mark.parametrize("workers", [1, 4])
def test_native_results_reopen_replay_and_regeneration_match_reference(tmp_path, backend, reducer, workers):
    spec = definition(reducer)
    retained = documents()
    with Database(tmp_path / "reference") as baseline:
        baseline.register(reference(spec))
        baseline.ingest(retained[:4])
        baseline.ingest(retained[4:])
        expected = tuple(baseline.rows(spec.name, limit=2))
    path = tmp_path / backend
    with NativeDatabase(path, spec, backend=backend, workers=workers) as database:
        assert database.ingest(retained[:4]).inserted == 4
        assert database.ingest(retained[4:]).inserted == 5
        assert tuple(database.rows(limit=2)) == expected
        assert database.checkpoint("test") == 80
        assert database.get("record-3", source="test") == retained[3]
        replay = database.ingest(retained)
        assert (replay.inserted, replay.replayed, replay.sequence) == (0, 9, 9)
        assert tuple(database.rows()) == expected
    with NativeDatabase(path, spec, backend=backend, workers=workers) as reopened:
        assert tuple(reopened.rows()) == expected
        assert reopened.ingest(retained).replayed == 9
    with NativeDatabase(tmp_path / "regenerated", spec, backend=backend, workers=workers) as rebuilt:
        rebuilt.ingest(retained)
        # Batch boundaries change history sequences, but preserve mathematical states.
        assert [(row.key, row.state, row.result) for row in rebuilt.rows()] == [
            (row.key, row.state, row.result) for row in expected]


@pytest.mark.parametrize("backend", ["sqlite", "log"])
def test_optional_metadata_and_immutable_returned_documents(tmp_path, backend):
    spec = definition("sum/v1")
    path = tmp_path / backend
    payload = {"key": "a", "value": 4, "extra": []}
    with NativeDatabase(path, spec, backend=backend, workers=2) as database:
        saved = database.add(payload, id="named")
        assert saved.position == 0
        payload["extra"].append(1)
        assert database.get("named").payload["extra"] == []
        saved.payload["extra"].append(2)
        assert database.get("named").payload["extra"] == []
        original = database.get("named")
        database.add({"key": "a", "value": 8}, position=40)
        assert database.add(original.payload, id="named") == original
        assert database.add(original.payload, position=0) == original
        assert database.add({"key": "b", "value": 2}, source="other").position == 0
        assert database.add({"key": "a", "value": 1}).position == 41
        assert database.stats()["documents"] == 4
        with pytest.raises(DatabaseError):
            database.add({"key": "a", "value": 999}, id="named")


@pytest.mark.parametrize("backend", ["sqlite", "log"])
def test_snapshot_and_change_pagination_follow_committed_batches(tmp_path, backend):
    with NativeDatabase(tmp_path / backend, definition("sum/v1"), backend=backend, workers=4) as database:
        database.add_many([{"key": key, "value": 1} for key in ["a", "b", "c"]])
        snapshot = database.query(limit=1)
        database.add_many([{"key": "a", "value": 2}, {"key": "b", "value": 3},
                           {"key": "a", "value": 4}, {"key": "d", "value": 5}])
        original = list(snapshot.rows)
        page = snapshot
        while not page.complete:
            page = database.query(cursor=snapshot.cursor, after=page.after, limit=1)
            original.extend(page.rows)
        assert [(row.key, row.result["value"]) for row in original] == [("a", 1), ("b", 1), ("c", 1)]
        cursor, changes = snapshot.cursor, []
        while True:
            page = database.changes(since=cursor, limit=1)
            changes.extend(page.rows)
            cursor = page.cursor
            if page.complete:
                break
        assert [(row.key, row.result["value"]) for row in changes] == [("a", 7), ("b", 4), ("d", 5)]
        assert not database.changes(since=cursor).rows
        assert database.query(key="a").rows[0].result["value"] == 7


@pytest.mark.parametrize("backend", ["sqlite", "log"])
def test_invalid_batch_rolls_back_documents_states_history_and_checkpoint(tmp_path, backend):
    with NativeDatabase(tmp_path / backend, definition(), backend=backend, workers=4) as database:
        database.add({"key": "a", "value": 1}, id="first")
        before = database.query()
        with pytest.raises(DatabaseError):
            database.add_many([{"key": "a", "value": 2}, {"key": "bad"}])
        assert database.query() == before
        assert database.checkpoint() == 0
        assert database.stats()["documents"] == 1
        assert not database.changes(since=before.cursor).rows
        assert database.add({"key": "a", "value": 3}, id="retry").position == 1
        with pytest.raises(DatabaseError):
            database.ingest([Document("tail", {"key": "a", "value": 2}, 2),
                             Document("first", {"key": "a", "value": 99}, 0)])
        assert database.get("tail") is None
        assert database.checkpoint() == 1


@pytest.mark.parametrize("backend", ["sqlite", "log"])
@pytest.mark.parametrize("emissions", [0, 3])
def test_native_synthetic_map_and_multiple_emissions_match_reference(tmp_path, backend, emissions):
    spec = definition("mean/v1", work=111, emissions=emissions)
    retained = documents()
    with Database(tmp_path / "reference") as baseline:
        baseline.register(reference(spec))
        baseline.ingest(retained)
        expected = tuple(baseline.rows(spec.name))
    with NativeDatabase(tmp_path / backend, spec, backend=backend, workers=4) as database:
        database.ingest(retained)
        assert tuple(database.rows()) == expected
        assert database.checkpoint("test") == 80


def test_backends_and_worker_counts_share_identical_history_and_states(tmp_path):
    collected = []
    spec = definition("envelope/v1", work=50, emissions=2)
    for backend in ["sqlite", "log"]:
        for workers in [1, 4]:
            with NativeDatabase(tmp_path / f"{backend}-{workers}", spec, backend=backend, workers=workers) as database:
                database.ingest(documents()[:4])
                first = database.query()
                database.ingest(documents()[4:])
                collected.append((tuple(database.rows()), database.changes(since=first.cursor).rows))
    assert all(result == collected[0] for result in collected)


@pytest.mark.parametrize("backend", ["sqlite", "log"])
def test_large_mapped_floats_preserve_state_across_json_and_native_batches(tmp_path, backend):
    # These states exposed a one-ULP serde_json transport roundtrip defect.
    spec = definition(work=10000)
    retained = [Document(f"document-{index}", {"key": 0, "value": 1e16}, index, "benchmark")
                for index in range(0, 2048, 128)]
    with Database(tmp_path / "reference") as baseline, NativeDatabase(
            tmp_path / backend, spec, backend=backend, workers=4) as database:
        baseline.register(reference(spec))
        for start in range(0, len(retained), 4):
            batch = retained[start:start + 4]
            baseline.ingest(batch)
            database.ingest(batch)
            assert tuple(database.rows()) == tuple(baseline.rows(spec.name))


@pytest.mark.parametrize("backend", ["sqlite", "log"])
def test_mutable_generator_values_are_captured_before_resume(tmp_path, backend):
    def payloads():
        payload = {"key": "a", "value": 0}
        for number in range(3):
            payload["value"] = number
            yield payload

    def records():
        payload = {"key": "b", "value": 0}
        for number in range(3):
            payload["value"] = number
            yield Document(str(number), payload, number, "second")

    with NativeDatabase(tmp_path / backend, definition("sum/v1"), backend=backend, workers=2) as database:
        saved = database.add_many(payloads())
        assert [document.payload["value"] for document in saved] == [0, 1, 2]
        database.ingest(records())
        assert [(row.key, row.result["value"]) for row in database.rows()] == [("a", 3), ("b", 3)]


def test_native_float_key_order_and_snapshots_are_deterministic_across_backends(tmp_path):
    # Native serde_json lexical keys have a different exponent spelling than
    # Python json. Verify deterministic native behavior without claiming parity.
    keys = [1e20, 1e-100, ["float", 1e20], ["float", 1e-100],
            {"float": 1e20}, {"float": 1e-100}, None]
    retained = [Document(str(index), {"key": key, "value": index + 1}, index)
                for index, key in enumerate(keys)]
    results = []
    for backend in ["sqlite", "log"]:
        for workers in [1, 4]:
            with NativeDatabase(tmp_path / f"{backend}-{workers}", definition("sum/v1"),
                                backend=backend, workers=workers) as database:
                database.ingest(retained)
                first = database.query(limit=2)
                for index, key in enumerate(keys):
                    assert database.query(key=key).rows[0].result["value"] == index + 1
                database.add({"key": keys[0], "value": 99})
                snapshot_rows = list(first.rows)
                page = first
                while not page.complete:
                    page = database.query(cursor=first.cursor, after=page.after, limit=2)
                    snapshot_rows.extend(page.rows)
                results.append(snapshot_rows)
    assert all(rows == results[0] for rows in results)


def test_nonfloat_structured_and_escaped_keys_match_default_order(tmp_path):
    keys = ["line\n", "line\\n", "\u0001", "α", ["x", {"control": "\b"}],
            {"nested": [3, True, None]}, -3, 2, None]
    retained = [Document(f"name-{index}/α", {"key": key, "value": index}, index, "源")
                for index, key in enumerate(keys)]
    spec = definition("envelope/v1")
    with Database(tmp_path / "reference") as baseline, NativeDatabase(
            tmp_path / "native", spec, backend="log", workers=4) as database:
        baseline.register(reference(spec))
        baseline.ingest(retained)
        database.ingest(retained)
        assert tuple(database.rows(limit=2)) == tuple(baseline.rows(spec.name, limit=2))


@pytest.mark.parametrize("backend", ["sqlite", "log"])
def test_reopen_rejects_changed_definition(tmp_path, backend):
    path = tmp_path / backend
    with NativeDatabase(path, definition(), backend=backend, workers=1) as database:
        database.add({"key": "a", "value": 1})
    with pytest.raises(DatabaseError):
        NativeDatabase(path, definition("count/v1"), backend=backend, workers=1)


def test_closed_database_rejects_calls(tmp_path):
    database = NativeDatabase(tmp_path / "db", definition(), workers=1)
    database.close()
    with pytest.raises(DatabaseError, match="closed"):
        database.query()


def test_separate_native_databases_can_run_from_python_threads(tmp_path):
    spec = definition(work=100)
    barrier = threading.Barrier(2)

    def run(index):
        with NativeDatabase(tmp_path / str(index), spec, backend="log", workers=2) as database:
            barrier.wait()
            return database.ingest(documents()), tuple(database.rows())
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(run, range(2)))
    assert results[0] == results[1]


def test_native_map_uses_worker_threads_and_detaches_from_python(tmp_path):
    spec = definition(work=200000)
    retained = [Document(str(index), {"key": index % 4, "value": 1}, index)
                for index in range(256)]
    stop = threading.Event()
    samples = []

    def sample_python_progress():
        while not stop.wait(0.001):
            samples.append(time.perf_counter())

    with NativeDatabase(tmp_path / "parallel", spec, backend="log", workers=4) as database:
        sampler = threading.Thread(target=sample_python_progress)
        sampler.start()
        try:
            started = time.perf_counter()
            database.ingest(retained)
            finished = time.perf_counter()
        finally:
            stop.set()
            sampler.join()
        assert any(started < timestamp < finished for timestamp in samples)
        telemetry = database.stats()["last_ingest"]
        assert 1 < telemetry["map_workers_observed"] <= 4
        assert telemetry["map_seconds"] > 0


def test_persisted_native_database_reopens_in_fresh_python_process(tmp_path):
    path = tmp_path / "log"
    with NativeDatabase(path, definition(), backend="log", workers=2) as database:
        database.ingest(documents())
        expected = [(row.key, row.result) for row in database.rows()]
    script = """import json,sys
from foldlitedb_experimental import NativeDatabase,NativeView
with NativeDatabase(sys.argv[1], NativeView('metrics','key','value'),backend='log',workers=1) as db:
    print(json.dumps([(row.key,row.result) for row in db.rows()]))
"""
    result = subprocess.run([sys.executable, "-c", script, str(path)], check=True,
                            text=True, capture_output=True)
    assert json.loads(result.stdout) == [[key, value] for key, value in expected]

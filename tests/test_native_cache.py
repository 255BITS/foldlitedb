"""Checkpoints are disposable; immutable log batches are the authority."""
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys

import pytest

from foldlitedb import CursorExpired, Database, DatabaseError


def test_native_api_has_no_sqlite_runtime_dependency():
    program = '''
import importlib.abc
import sys
class NoSQLite(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in ("sqlite3", "_sqlite3") or fullname.startswith("sqlite3."):
            raise AssertionError("SQLite must not be imported")
sys.meta_path.insert(0, NoSQLite())
from foldlitedb import Database
with Database(":memory:") as database:
    database.add({"metric": "x", "value": 2})
    database.rebuild()
    assert next(database.rows()).result["sum"] == 2
'''
    subprocess.run([sys.executable, "-c", program], check=True)


def content(database):
    return [(row.key, row.result, row.state, row.sequence) for row in database.rows(limit=1)]


def test_rebuild_preserves_original_documents_and_log_bytes(tmp_path):
    path = tmp_path / "documents.log"
    with Database(path) as database:
        documents = database.add_many([
            {"metric": "x", "value": 0.1}, {"metric": "x", "value": 0.2},
        ])
        documents += (database.add({"metric": "x", "value": -0.1}, source="other"),)
        expected = content(database)
    # Read bytes only after releasing the database's Windows file lock.
    original = path.read_bytes()
    with Database(path) as database:
        cursor = database.query().cursor
        generation = database.stats()["generation"]
        checkpoint = database.checkpoint()
        assert checkpoint["checkpointed"] is True
        assert Path(str(path) + ".cache").is_file()
    assert path.read_bytes() == original
    with Database(path) as database:
        rebuilt = database.rebuild()
        assert rebuilt["rebuilt"] is True
        assert rebuilt["generation"] != generation
        assert list(database.documents()) == list(documents)
        assert content(database) == expected
        with pytest.raises(CursorExpired):
            database.changes(since=cursor)
    assert path.read_bytes() == original
    with Database(path) as database:
        assert database.ingest(documents).replayed == len(documents)
    assert path.read_bytes() == original


@pytest.mark.parametrize("damage", ["delete", "corrupt"])
def test_missing_or_corrupt_checkpoint_replays_without_mutating_the_log(tmp_path, damage):
    path = tmp_path / "documents.log"
    cache = Path(str(path) + ".cache")
    with Database(path) as database:
        retained = database.add_many([{"metric": "a", "value": 2}, {"metric": "b", "value": 4}])
        expected = content(database)
        cursor = database.query().cursor
    original = path.read_bytes()
    if damage == "delete":
        cache.unlink()
    else:
        cache.write_bytes(b"discardable corrupt cache")
    with Database(path) as rebuilt:
        stats = rebuilt.stats()
        assert stats["cache"]["status"] == ("missing" if damage == "delete" else "invalid")
        assert stats["replay"]["documents"] == 2
        assert list(rebuilt.documents()) == list(retained)
        assert content(rebuilt) == expected
        with pytest.raises(CursorExpired):
            rebuilt.changes(since=cursor)
    assert path.read_bytes() == original
    with Database(path) as reopened:
        assert reopened.stats()["cache"]["status"] == "loaded"
        assert reopened.stats()["replay"]["documents"] == 0
    assert path.read_bytes() == original


def test_good_checkpoint_preserves_cursors_and_replays_only_durable_suffix(tmp_path):
    path = tmp_path / "documents.log"
    with Database(path) as database:
        database.add({"metric": "x", "value": 2})
        cursor = database.query().cursor
    program = '''
import os
from foldlitedb import Database
database = Database(PATH)
database.add({"metric": "x", "value": 4})
os._exit(0)
'''.replace("PATH", repr(str(path)))
    subprocess.run([sys.executable, "-c", program], check=True)
    with Database(path) as reopened:
        stats = reopened.stats()
        assert stats["cache"]["status"] == "loaded"
        assert stats["replay"]["documents"] == 1
        assert stats["documents"] == 2
        assert next(reopened.rows()).result["sum"] == 6
        changes = reopened.changes(since=cursor)
        assert changes.complete and changes.rows[0].result["sum"] == 6


def test_incomplete_unacknowledged_tail_is_recovered(tmp_path):
    path = tmp_path / "documents.log"
    with Database(path) as database:
        database.add({"metric": "x", "value": 2})
    original = path.read_bytes()
    with path.open("ab") as handle:
        handle.write(b"BA")
    with Database(path) as recovered:
        assert recovered.stats()["log"]["recovered_tail_bytes"] == 2
        assert recovered.stats()["documents"] == 1
        assert next(recovered.rows()).result["sum"] == 2
    assert path.read_bytes() == original


def test_authoritative_log_corruption_is_reported_even_with_a_valid_cache(tmp_path):
    path = tmp_path / "documents.log"
    with Database(path) as database:
        database.add({"metric": "x", "value": 2})
    raw = bytearray(path.read_bytes())
    raw[-1] ^= 1
    path.write_bytes(raw)
    with pytest.raises(DatabaseError, match="checksum|corrupt|hash|invalid"):
        Database(path)
    assert path.read_bytes() == raw


def test_imported_document_payloads_survive_browser_double_parsing(tmp_path):
    records = json.loads(
        '[{"id":"web-1","source":"browser","position":0,'
        '"payload":{"metric":"x","value":1000000000000000100}}]',
        parse_int=float,
    )
    from foldlitedb import Document
    records = [Document(row["id"], row["payload"], int(row["position"]), row["source"]) for row in records]
    with Database(tmp_path / "documents.log") as database:
        database.ingest(records)
        before = [asdict(row) for row in database.documents()]
        database.rebuild()
        assert [asdict(row) for row in database.documents()] == before
        assert next(database.rows()).result["sum"] == 1e18 + 128


BOUNDARY_INTEGERS = [
    2**53 - 1, -(2**53 - 1), 2**53, -(2**53), 2**53 + 1, -(2**53 + 1), 2**53 + 2,
    2**63 - 1, -(2**63), -(2**63) + 1, 2**64 - 1,
]


def _python_accepts(value):
    return int(float(value)) == value


@pytest.mark.parametrize("value", BOUNDARY_INTEGERS)
def test_native_host_applies_the_python_exact_integer_rule(tmp_path, value):
    path = tmp_path / "exact.log"
    with Database(path) as database:
        # Python validation agrees with the native rule at every boundary.
        if _python_accepts(value):
            database.add({"metric": "python", "value": value}, id="python")
        else:
            with pytest.raises(DatabaseError, match="exactly representable"):
                database.add({"metric": "python", "value": value}, id="python")
        # Raw JSON bypasses Python validation and reaches the Rust host directly.
        raw = ('{"op":"ingest","documents":[{"id":"native","payload":'
               '{"metric":"native","nested":[%d]}}]}' % value)
        log = path.read_bytes()
        if _python_accepts(value):
            stored = database._call_json(raw)["documents"][0]["payload"]["nested"][0]
            assert stored == float(value)
        else:
            with pytest.raises(DatabaseError, match=str(value)):
                database._call_json(raw)
            assert path.read_bytes() == log
        before = [asdict(row) for row in database.documents()]
    with Database(path) as reopened:
        assert [asdict(row) for row in reopened.documents()] == before


def test_writer_health_is_reported_and_reopen_recovers(tmp_path):
    path = tmp_path / "documents.log"
    database = Database(path)
    database.add({"metric": "x", "value": 2}, id="one")
    assert database.stats()["log"]["writer_poisoned"] is False
    with path.open("ab") as handle:
        handle.write(b"B")
    with pytest.raises(DatabaseError, match="uncertain"):
        database.add({"metric": "x", "value": 3}, id="two")
    assert database.stats()["log"]["writer_poisoned"] is True
    assert next(database.rows()).result["sum"] == 2
    with pytest.raises(DatabaseError, match="poisoned"):
        database.close()
    with Database(path) as reopened:
        assert reopened.stats()["log"]["writer_poisoned"] is False
        reopened.add({"metric": "x", "value": 3}, id="two")
        assert reopened.add({"metric": "x", "value": 3}, id="two").position == 1
        assert next(reopened.rows()).result["sum"] == 5

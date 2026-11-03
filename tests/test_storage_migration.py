"""Public API compatibility with an actual log produced by the 0.1.4 wheel."""
import json
from pathlib import Path

import pytest

from foldlitedb import Database, DatabaseError


FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "legacy_log_v1.json").read_text())


def test_released_v1_log_is_readable_and_migrates_without_changing_source(tmp_path):
    source = tmp_path / "old.fold"
    destination = tmp_path / "new.fold"
    original = bytes.fromhex(FIXTURE["hex"])
    source.write_bytes(original)
    with Database(source) as legacy:
        assert legacy.stats()["log"]["format"] == 1
        assert legacy.stats()["log"]["read_only"] is True
        assert next(legacy.rows()).result["sum"] == FIXTURE["expected_sum"]
        with pytest.raises(DatabaseError, match="read-only"):
            legacy.add({"metric": "x", "value": 1})
        with pytest.raises(DatabaseError, match="locked|open"):
            Database.migrate(source, destination)
    Database.migrate(source, destination)
    assert source.read_bytes() == original
    with Database(destination) as migrated:
        assert migrated.stats()["log"]["format"] == 2
        assert migrated.stats()["log"]["read_only"] is False
        assert migrated.stats()["log"]["batches"] == 2
        assert [doc.position for doc in migrated.documents()] == FIXTURE["expected_positions"]
        assert next(migrated.rows()).result["sum"] == FIXTURE["expected_sum"]
        migrated.add({"metric": "x", "value": 7}, id="d", source="fixture")
        assert next(migrated.rows()).result["sum"] == 17
        migrated.rebuild()
        assert next(migrated.rows()).result["sum"] == 17
    destination_bytes = destination.read_bytes()
    with pytest.raises(DatabaseError, match="exists"):
        Database.migrate(source, destination)
    assert destination.read_bytes() == destination_bytes
    assert source.read_bytes() == original

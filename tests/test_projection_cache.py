"""Persisted projection jobs resume exact cursor coverage and can be discarded."""
import asyncio
from pathlib import Path
import time
import pytest

from foldlitedb.database import Database, DatabaseError, OrderedFold, RustReducer, View
from foldlitedb.projection import materialize_jsonl_view
from foldlitedb.reducers import Reducer


def ordered_fold_view():
    return View(
        "totals",
        version="1",
        map=lambda document: [
            (
                document.payload["document"]["key"],
                document.payload["document"]["value"],
            )
        ],
        reducer=OrderedFold(0, lambda state, value: state + value, version="1"),
    )


def materialize(
    path,
    documents,
    reads,
    *,
    generation="one",
    max_groups=10,
    invalid=False,
    timeout=180,
    target_pos=None,
):
    target = len(documents) if target_pos is None else target_pos

    def read_page(cursor, limit=256):
        reads.append(cursor)
        if invalid:
            raise ValueError("Consumed document boundary changed")
        rows = [row for row in documents if row["n"] > int(cursor or 0)][:limit]
        return rows, [str(row["n"]) for row in rows], {}

    return materialize_jsonl_view(
        path,
        generation=generation,
        view=ordered_fold_view(),
        read_page=read_page,
        target_cursor=str(target),
        target_position=target,
        document_position=lambda document: document["n"],
        cursor_position=lambda cursor: int(cursor or 0),
        max_groups=max_groups,
        timeout=timeout,
    )


def test_reopen_ingests_only_suffix_and_discard_regenerates(tmp_path):
    path = tmp_path / "views.fold"
    documents = [{"n": n, "key": "total", "value": n} for n in range(1, 4)]
    first = asyncio.run(materialize(path, documents, []))
    assert first[0].state == 6
    reads = []
    reopened = asyncio.run(materialize(path, documents, reads))
    assert reopened[0].state == 6 and None not in reads
    documents.append({"n": 4, "key": "total", "value": 4})
    reads = []
    suffix = asyncio.run(materialize(path, documents, reads))
    assert suffix[0].state == 10 and None not in reads
    path.unlink()
    regenerated = asyncio.run(materialize(path, documents, []))
    assert regenerated[0].state == suffix[0].state


def test_saved_source_boundary_must_validate_before_cached_result(tmp_path):
    path = tmp_path / "views.fold"
    documents = [{"n": 1, "key": "total", "value": 3}]
    asyncio.run(materialize(path, documents, []))
    with pytest.raises(ValueError, match="boundary changed"):
        asyncio.run(materialize(path, documents, [], invalid=True))


def test_generation_change_regenerates_and_group_limit_is_explicit(tmp_path):
    path = tmp_path / "views.fold"
    documents = [{"n": 1, "key": "total", "value": 3}]
    asyncio.run(materialize(path, documents, []))
    documents[0]["value"] = 7
    changed = asyncio.run(materialize(path, documents, [], generation="two"))
    assert changed[0].state == 7
    documents.append({"n": 2, "key": "other", "value": 9})
    with pytest.raises(ValueError, match="exceeds 1 groups"):
        asyncio.run(materialize(path, documents, [], generation="two", max_groups=1))


def test_materialize_view_creates_native_trio(tmp_path):
    path = tmp_path / "trio.fold"
    documents = [{"n": n, "key": "total", "value": n} for n in range(1, 4)]
    first = asyncio.run(materialize(path, documents, []))
    assert first[0].state == 6
    assert path.is_file()
    assert Path(str(path) + ".cache").is_file()


def test_o1_source_checkpoint_watermark_lookup(tmp_path):
    path = tmp_path / "watermark.fold"
    documents = [{"n": n, "key": "total", "value": n} for n in range(1, 4)]
    asyncio.run(materialize(path, documents, []))
    with Database(path) as db:
        stats = db.stats()
        assert "checkpoints" in stats
        assert stats["checkpoints"].get("projection") == 3
        assert db.checkpoint("projection") == 3


def test_corrupted_cache_recovery_without_data_loss(tmp_path):
    path = tmp_path / "recover.fold"
    documents = [{"n": n, "key": "total", "value": n} for n in range(1, 4)]
    asyncio.run(materialize(path, documents, []))
    cache_path = Path(str(path) + ".cache")
    assert cache_path.is_file()
    cache_path.write_bytes(b"CORRUPT_BYTES_FOR_TEST")
    reads = []
    reopened = asyncio.run(materialize(path, documents, reads))
    assert reopened[0].state == 6


def test_checkpoint_ahead_of_snapshot_safety(tmp_path):
    path = tmp_path / "ahead.fold"
    documents = [{"n": n, "key": "total", "value": n} for n in range(1, 6)]
    asyncio.run(materialize(path, documents, []))
    with pytest.raises(ValueError, match="ahead of the requested snapshot"):
        asyncio.run(materialize(path, documents[:3], [], target_pos=3))


def test_timeout_budget_enforcement(tmp_path):
    path = tmp_path / "timeout.fold"
    documents = [{"n": n, "key": "total", "value": n} for n in range(1, 4)]
    with pytest.raises(ValueError, match="Historical reduction exceeded its time budget"):
        asyncio.run(materialize(path, documents, [], timeout=-1))


def test_rust_reducer_adapter_batching_and_byte_caps():
    reducer = Reducer()
    rust_reducer = RustReducer("envelope/v1", runtime=reducer, positioned=True)
    identity = rust_reducer.identity()
    points = [{"value": i, "position": [i, f"doc-{i}"]} for i in range(2500)]
    state = rust_reducer.add(identity, points)
    final = rust_reducer.finalize(state)
    assert final["count"] == 2500
    assert final["first"]["value"] == 0
    assert final["last"]["value"] == 2499
    oversized = [{"value": 1, "position": [1, "x" * 300_000]}]
    with pytest.raises(DatabaseError, match="exceeds the reducer request byte budget"):
        rust_reducer.add(identity, oversized)

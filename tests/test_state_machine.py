"""A deterministic external oracle checks interacting commit/replay operations."""
from collections import defaultdict
from pathlib import Path
import random

import pytest

from foldlitedb import CursorExpired, Database, Document, DocumentConflict, OrderedFold, View


def summarize(values):
    return {"count": len(values), "sum": sum(values), "min": min(values),
            "max": max(values), "mean": sum(values) / len(values)}


def expected_rows(documents):
    groups = defaultdict(list)
    for document in documents:
        if "metric" in document.payload:
            groups[document.payload["metric"]].append(document.payload["value"])
    return {key: summarize(values) for key, values in groups.items()}


def snapshot_rows(database, cursor=None):
    page = database.query(cursor=cursor, limit=2)
    rows = list(page.rows)
    while not page.complete:
        page = database.query(cursor=page.cursor, after=page.after, limit=2)
        rows.extend(page.rows)
    return {row.key: row.result for row in rows}


@pytest.mark.parametrize("seed", [173, 991])
@pytest.mark.parametrize("kind", ["wasm", "callback"])
def test_seeded_commit_replay_state_machine_matches_external_oracle(tmp_path, seed, kind):
    rng = random.Random(seed)
    view = View.bundled() if kind == "wasm" else View(
        "observations", version="1",
        map=lambda doc: [(doc.payload["metric"], doc.payload["value"])]
        if "metric" in doc.payload else [],
        # Retain contributions in callback state; the oracle independently
        # recomputes grouped results from accepted source documents.
        reducer=OrderedFold([], lambda state, value: state + [value],
                            version="1", finalize=summarize),
    )
    path = tmp_path / "state-machine.fold"
    database = Database(path, view)
    accepted = []
    snapshots = []

    def fresh(count):
        positions = {source: max((d.position for d in accepted if d.source == source), default=-1)
                     for source in ("east", "west")}
        documents = []
        for _ in range(count):
            source = rng.choice(("east", "west"))
            positions[source] += rng.randint(1, 3)
            position = positions[source]
            payload = {"metric": rng.choice(("a", "b", "c", "d")), "value": rng.randint(-20, 20)}
            if rng.randrange(5) == 0:
                payload = {"ignored": True}
            documents.append(Document(str(position), payload, position, source))
        return documents

    operations = ["append", "mixed", "retry", "conflict", "checkpoint", "warm",
                  "append", "cold", "mixed", "rebuild", "retry", "conflict", "warm"]
    rng.shuffle(operations)
    try:
        for operation in ["append", *operations]:
            old_cursor = database.query().cursor
            if operation in ("append", "mixed"):
                new = fresh(rng.randint(1, 3))
                retries = [rng.choice(accepted)] if operation == "mixed" else []
                batch = [*retries, *new, *retries]
                outcome = database.ingest(batch)
                assert (outcome.inserted, outcome.replayed) == (len(new), 2 * len(retries))
                accepted.extend(new)
            elif operation == "retry":
                retries = [rng.choice(accepted) for _ in range(3)]
                outcome = database.ingest(retries)
                assert (outcome.inserted, outcome.replayed) == (0, len(retries))
                assert database.query().cursor == old_cursor
            elif operation == "conflict":
                retained = rng.choice(accepted)
                changed = Document(retained.id, {"conflicting": True}, retained.position, retained.source)
                before = database.stats()
                with pytest.raises(DocumentConflict):
                    database.ingest([*fresh(1), changed])
                assert database.stats() == before
                assert database.query().cursor == old_cursor
            elif operation == "checkpoint":
                before = database.stats()["log"]
                database.checkpoint()
                assert database.stats()["log"] == before
            elif operation in ("warm", "cold"):
                database.close()
                source = path.read_bytes()  # release Windows ownership first
                if operation == "cold":
                    Path(str(path) + ".cache").unlink()
                database = Database(path, view)
                if operation == "warm":
                    assert database.query().cursor == old_cursor
                # Verify preservation through an additional normal close/open;
                # no inspection relies on access to an actively locked log.
                database.close()
                assert path.read_bytes() == source
                database = Database(path, view)
            elif operation == "rebuild":
                database.rebuild()

            if operation in ("cold", "rebuild"):
                with pytest.raises(CursorExpired):
                    database.query(cursor=old_cursor)
                snapshots.clear()
            assert list(database.documents(limit=2)) == accepted
            assert snapshot_rows(database) == expected_rows(accepted)
            if snapshots:
                cursor, expected = rng.choice(snapshots)
                assert snapshot_rows(database, cursor) == expected
            snapshots.append((database.query().cursor, expected_rows(accepted)))
    finally:
        database.close()

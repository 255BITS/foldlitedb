"""Run with ``python examples/catalogue.py`` after installing Foldlite DB.

Immutable observations produce changing catalogue lifecycle state. The retained
documents and declared taxonomy are enough to recreate the disposable database.
No numerical reducer dependency is needed for this example.
"""
from __future__ import annotations

import json
from pathlib import Path
import tempfile

from foldlitedb import Database, OrderedFold, View


TAXONOMY = {"bug": "issue", "feature": "request"}
OBSERVATIONS = [
    {"record": "a", "type": "bug", "status": "open"},
    {"record": "b", "type": "feature", "status": "open"},
    {"record": "a", "type": "bug", "status": "closed"},
    {"record": "a", "type": "bug", "status": "open"},
]


def catalogue(document):
    value = document.payload
    yield value["record"], {"category": TAXONOMY[value["type"]], "status": value["status"]}


def view():
    return View("catalogue", "1", catalogue,
                OrderedFold(None, lambda previous, current: current, version="latest-1"),
                references={"taxonomy": "catalogue-taxonomy-1"})


def snapshot_rows(database):
    return {row.key: row.result for row in database.rows("catalogue", limit=2)}


def main():
    with tempfile.TemporaryDirectory(prefix="foldlite-catalogue-") as directory:
        path = Path(directory) / "catalogue.db"
        with Database(path, generation="catalogue-input-1") as database:
            database.register(view())
            documents = database.add_many(OBSERVATIONS[:2])
            snapshot = database.query("catalogue")
            documents += database.add_many(OBSERVATIONS[2:])
            updates = database.changes("catalogue", since=snapshot.cursor)
            assert [(row.key, row.result["status"]) for row in updates.rows] == [("a", "open")]
            live = snapshot_rows(database)

        # A separate cache stands in for deleting and recreating the database.
        # Retain resolved identities and positions outside the disposable cache.
        with Database(Path(directory) / "regenerated.db", generation="catalogue-input-1") as database:
            for batch in database.ingest_many(documents, batch_size=2):
                assert batch.inserted == 2
            database.register(view())
            assert snapshot_rows(database) == live
        print(json.dumps(live, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

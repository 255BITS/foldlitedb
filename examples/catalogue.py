"""A retry-safe offline catalogue built from immutable lifecycle observations."""
import json
from pathlib import Path
import tempfile

from foldlitedb import Database, Document, View


def category_items(database, category):
    return next((row.result["items"] for row in database.rows(key=category)), {})


def current_catalogue(database):
    return {row.key: row.result["items"] for row in database.rows() if row.result["count"]}


def main():
    with tempfile.TemporaryDirectory(prefix="foldlite-catalogue-") as directory:
        path = Path(directory) / "catalogue.log"
        view = View.bundled("catalogue")
        with Database(path, view) as database:
            # Preview runs the map without storing a fact. An adapter that leaves
            # the supplier's `name` field unmapped would emit no catalogue item.
            incoming = {"category": "gear", "id": "mug", "name": "Trail Mug"}
            assert database.preview(incoming) == []
            product = {"category": incoming["category"], "id": incoming["id"],
                       "title": incoming["name"]}
            assert database.preview(product)[0]["key"] == "gear"

            # The outer ID names a delivery; payload.id names the product.
            # Positions are allocated automatically within this atomic batch.
            created = [
                Document("delivery-1", {"category": "books", "id": "a", "title": "First edition"}, source="shop"),
                Document("delivery-2", {"category": "books", "id": "b", "title": "Another book"}, source="shop"),
                Document("delivery-3", product, source="shop"),
            ]
            assert database.ingest(created).inserted == 3
            snapshot = database.query()
            updated = [
                Document("delivery-4", {"category": "books", "id": "a", "title": "Revised edition"}, source="shop"),
                Document("delivery-5", {"category": "books", "id": "b", "deleted": True}, source="shop"),
                Document("delivery-6", {"category": "gear", "id": "mug", "deleted": True}, source="shop"),
            ]
            assert database.ingest(updated).inserted == 3
            assert database.changes(since=snapshot.cursor).rows[0].result == {
                "count": 1, "items": {"a": "Revised edition"},
            }
            # A delayed retry keeps its old position and cannot undo revision.
            assert database.ingest(created).replayed == 3
            assert category_items(database, "books") == {"a": "Revised edition"}

            # Last-item withdrawal leaves a zero-count category row; an absent
            # category has no row. Both expose an empty product collection.
            assert database.query(key="gear").rows[0].result == {"count": 0, "items": {}}
            assert database.query(key="missing").rows == ()
            assert category_items(database, "gear") == category_items(database, "missing") == {}
            assert current_catalogue(database) == {"books": {"a": "Revised edition"}}
            assert len(list(database.documents())) == 6

            expected = [(row.key, row.result) for row in database.rows()]
            original_log = path.read_bytes()
            database.rebuild()
            assert [(row.key, row.result) for row in database.rows()] == expected
            assert path.read_bytes() == original_log

        # Delete only the disposable checkpoint; the immutable log is retained.
        Path(str(path) + ".cache").unlink()
        with Database(path, view) as rebuilt:
            assert rebuilt.stats()["replay"]["documents"] == 6
            assert [(row.key, row.result) for row in rebuilt.rows()] == expected
            assert path.read_bytes() == original_log
        print(json.dumps(expected, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

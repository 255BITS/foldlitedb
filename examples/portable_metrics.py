"""The same AssemblyScript view runs in the native engine and a browser worker."""
from dataclasses import asdict
import json
from pathlib import Path
import tempfile

from foldlitedb import Database, View


def main():
    view = View.bundled("metrics")
    with tempfile.TemporaryDirectory(prefix="foldlite-metrics-") as directory:
        path = Path(directory) / "metrics.log"
        with Database(path, view) as database:
            documents = database.add_many([
                {"metric": "loss", "value": 4},
                {"metric": "loss", "value": 8},
            ])
            snapshot = database.query()
            documents += (database.add({"metric": "loss", "value": 3}),)
            assert database.changes(since=snapshot.cursor).rows[0].result["mean"] == 5
            expected = [(row.key, row.result) for row in database.rows()]
            database.checkpoint()
            database.rebuild()
            assert [(row.key, row.result) for row in database.rows()] == expected

        # Caches are safe to discard. The log holds every resolved immutable doc.
        Path(str(path) + ".cache").unlink()
        with Database(path, view) as rebuilt:
            assert [(row.key, row.result) for row in rebuilt.rows()] == expected
            assert list(rebuilt.documents()) == list(documents)
        print(json.dumps({"module_sha256": view.sha256, "rows": expected,
                          "documents": [asdict(document) for document in documents]}, indent=2))


if __name__ == "__main__":
    main()

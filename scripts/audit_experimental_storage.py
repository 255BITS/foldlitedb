"""Record active SQLite/WAL/SHM versus closed file sizes on the native benchmark fixture."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

from benchmark_experimental import ROOT, fixture, reference
from foldlitedb import Database, RustReducer
from foldlitedb_experimental import NativeDatabase, NativeView


def files(directory):
    return {path.name: path.stat().st_size for path in Path(directory).iterdir() if path.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--storage-root", type=Path,
                        default=ROOT / "experimental/engine/target/benchmarks")
    arguments = parser.parse_args()
    arguments.storage_root.mkdir(parents=True, exist_ok=True)
    spec = NativeView("metrics", "key", "value")
    results = []
    for shape in ["uniform", "hot", "unique"]:
        retained = list(fixture(4096, shape, 128))
        suffix = list(fixture(8, shape, 128, start=len(retained)))
        for engine in ["python-sqlite", "rust-sqlite", "rust-log"]:
            with tempfile.TemporaryDirectory(prefix="foldlite-storage-audit-",
                                             dir=arguments.storage_root) as directory:
                path = Path(directory) / "database"
                if engine == "python-sqlite":
                    database = Database(path)
                    database.register(reference(spec, RustReducer(spec.reducer)))
                else:
                    database = NativeDatabase(path, spec, backend=engine.removeprefix("rust-"), workers=1)
                for offset in range(0, len(retained), 512):
                    database.ingest(retained[offset:offset + 512])
                for document in suffix:
                    database.ingest([document])
                rows = (tuple(database.rows(spec.name, limit=4096)) if engine == "python-sqlite"
                        else tuple(database.rows(limit=4096)))
                stats = database.stats()
                active = files(directory)
                database.close()
                closed = files(directory)
                results.append({
                    "engine": engine, "shape": shape, "map": "cheap", "work": 0,
                    "documents": 4096, "live_documents": 8, "batch_size": 512,
                    "result_rows": len(rows), "stats": stats,
                    "active_files_bytes": active, "active_total_bytes": sum(active.values()),
                    "closed_files_bytes": closed, "closed_total_bytes": sum(closed.values()),
                })
    mount = subprocess.run(["findmnt", "-T", str(arguments.storage_root.resolve()),
                            "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"],
                           check=True, capture_output=True, text=True)
    report = {
        "scope": "Storage accounting probe; no performance timing or ranking claims",
        "storage_root": str(arguments.storage_root.resolve()),
        "storage_mount": json.loads(mount.stdout),
        "durability": {"python-sqlite": "WAL, synchronous FULL",
                       "rust-sqlite": "WAL, synchronous FULL",
                       "rust-log": "sync_all file and parent directory before acknowledgement"},
        "note": "Logical file sizes before close include active WAL/SHM; closed sizes reflect SQLite checkpoint/cleanup. Native backends journal identical records and rebuild in-memory indexes on reopen.",
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "results": results,
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps([{"engine": result["engine"], "shape": result["shape"],
                       "active_bytes": result["active_total_bytes"],
                       "closed_bytes": result["closed_total_bytes"]} for result in results], indent=2))


if __name__ == "__main__":
    main()

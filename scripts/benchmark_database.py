"""Measure immutable-log ingestion, rebuild, cached reopen, and suffix replay.

Run against the filesystem you mean to measure with --storage-root. Each commit
is durable; no page-cache eviction or timing-based acceptance threshold is used.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

from foldlitedb import Database, Document, View


ROOT = Path(__file__).resolve().parents[1]


def records(start, stop, keys):
    for position in range(start, stop):
        yield Document(str(position), {"metric": str(position % keys), "value": position % 1000},
                       position, "benchmark")


def ingest(database, start, stop, keys, batch_size):
    for position in range(start, stop, batch_size):
        database.ingest(records(position, min(position + batch_size, stop), keys))


def rows(database):
    return [asdict(row) for row in database.rows(limit=1024)]


def measure(function):
    started = time.perf_counter()
    value = function()
    return value, time.perf_counter() - started


def run(directory, *, count, keys, batch_size, live_count):
    path = directory / "metrics.fold"
    database = Database(path)
    _, ingest_seconds = measure(lambda: ingest(database, 0, count, keys, batch_size))
    expected = rows(database)
    assert len(expected) == min(count, keys)
    _, checkpoint_seconds = measure(database.checkpoint)
    database.close()

    database, warm_seconds = measure(lambda: Database(path))
    warm_stats = database.stats()
    assert warm_stats["cache"]["status"] == "loaded"
    assert warm_stats["replay"]["documents"] == 0
    assert rows(database) == expected
    database.close()

    # Simulate a writer ending after a durable commit, before cache checkpoint.
    program = """
import os, sys
from foldlitedb import Database, Document
path, start, stop, keys, size = sys.argv[1:]
start, stop, keys, size = map(int, (start, stop, keys, size))
db = Database(path)
for begin in range(start, stop, size):
    db.ingest(Document(str(p), {'metric':str(p % keys),'value':p % 1000}, p, 'benchmark')
              for p in range(begin, min(begin + size, stop)))
os._exit(0)
"""
    subprocess.run([sys.executable, "-c", program, str(path), str(count),
                    str(count + live_count), str(keys), str(batch_size)], check=True)
    database, suffix_seconds = measure(lambda: Database(path))
    suffix_stats = database.stats()
    assert suffix_stats["cache"]["status"] == "loaded"
    assert suffix_stats["replay"]["documents"] == live_count
    expected = rows(database)
    assert len(list(database.documents())) == count + live_count
    database.close()

    # Caches are accelerators: deleting one cannot alter retained input bytes.
    log_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    Path(str(path) + ".cache").unlink()
    database, cold_seconds = measure(lambda: Database(path))
    cold_stats = database.stats()
    assert cold_stats["cache"]["status"] == "missing"
    assert cold_stats["replay"]["documents"] == count + live_count
    assert rows(database) == expected
    assert hashlib.sha256(path.read_bytes()).hexdigest() == log_hash
    old_cursor = database.query().cursor
    _, rebuild_seconds = measure(database.rebuild)
    assert rows(database) == expected
    assert database.query().cursor != old_cursor
    assert hashlib.sha256(path.read_bytes()).hexdigest() == log_hash
    database.close()
    return {
        "seconds": {"durable_ingest": ingest_seconds, "checkpoint": checkpoint_seconds,
                    "cached_reopen": warm_seconds, "uncached_suffix_reopen": suffix_seconds,
                    "missing_cache_rebuild": cold_seconds, "explicit_rebuild": rebuild_seconds},
        "documents_per_second": {"durable_ingest": count / ingest_seconds,
                                 "missing_cache_rebuild": (count + live_count) / cold_seconds},
        "cached_reopen_stats": warm_stats, "suffix_reopen_stats": suffix_stats,
        "missing_cache_stats": cold_stats, "verified_rows": len(expected),
        "immutable_log_sha256": log_hash,
        "storage_bytes": {p.name: p.stat().st_size for p in directory.iterdir() if p.is_file()},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--documents", type=int, default=10000)
    parser.add_argument("--keys", type=int, default=16)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--live-documents", type=int, default=20)
    parser.add_argument("--storage-root", type=Path)
    parser.add_argument("--output", type=Path, default=Path("native-benchmark.json"))
    options = parser.parse_args()
    if options.documents < 1 or options.keys < 1 or not 1 <= options.batch_size <= 1024 or options.live_documents < 1:
        parser.error("documents, keys and live-documents must be positive; batch-size must be 1..1024")
    if options.storage_root:
        options.storage_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="foldlitedb-benchmark-", dir=options.storage_root) as temporary:
        directory = Path(temporary)
        try:
            mount = json.loads(subprocess.check_output(
                ["findmnt", "-T", str(directory), "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"], text=True))
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
            mount = None
        report = {
            "schema": "foldlitedb-native-cache-benchmark/v1",
            "runtime": {"python": platform.python_version(), "platform": platform.platform(),
                        "machine": platform.machine(), "storage_path": str(directory), "mount": mount},
            "settings": {"documents": options.documents, "keys": options.keys,
                         "batch_size": options.batch_size, "uncached_suffix_documents": options.live_documents},
            "view_sha256": View.bundled().sha256,
            "source_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                              for name in ("native/engine/src/lib.rs", "native/engine/src/storage.rs",
                                           "native/engine/src/runtime.rs", "native/Cargo.lock",
                                           "src/foldlitedb/database.py", "scripts/benchmark_database.py")},
            "method": "Single run with filesystem cache retained. Warm open verifies all log bytes but executes only uncached Wasm batches. Timings include Python/native transport. No timing threshold or cross-engine claim.",
            "results": run(directory, count=options.documents, keys=options.keys,
                           batch_size=options.batch_size, live_count=options.live_documents),
        }
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"output": str(options.output), **report["results"]["seconds"]}, indent=2))


if __name__ == "__main__":
    main()

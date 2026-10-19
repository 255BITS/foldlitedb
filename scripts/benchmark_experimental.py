"""Measure the supported numeric subset through Python and both native backends.

Build/install experimental/python first. Each trial runs in a fresh subprocess.
The native SQLite and log engines share projection code and in-memory indexes;
the existing Python engine also persists SQL state/history indexes. Consequently
the Python comparison includes architecture changes, not an isolated language
comparison. Native worker-count comparisons isolate parallelism within one core.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time

import foldlitedb_experimental
from foldlitedb import Database, Document, RustReducer, View
from foldlitedb import database as baseline_database, reducers as baseline_reducers
from foldlitedb_experimental import NativeDatabase, NativeView
from foldlitedb_experimental import _native


ROOT = Path(__file__).resolve().parents[1]


def synthetic(value, position, rounds):
    if not rounds:
        return value
    accumulator = (position ^ 0x9e3779b9) & 0xffffffff
    for iteration in range(rounds):
        accumulator = (accumulator * 1664525 + 1013904223) & 0xffffffff
        accumulator = (((accumulator << 13) | (accumulator >> 19)) & 0xffffffff) ^ iteration
    return value + (accumulator & 0xffff) if value is not None else None


def reference(spec, reducer):
    def mapping(document):
        if not spec.emissions:
            return
        value = synthetic(document.payload[spec.value_field], document.position, spec.work)
        for _ in range(spec.emissions):
            yield document.payload[spec.key_field], value
    return View(spec.name, spec.version, mapping, reducer)


def fixture(count, shape, keys, start=0):
    for position in range(start, start + count):
        if shape == "unique":
            key = position
        elif shape == "hot":
            key = 0 if position % 10 else 1 + position % max(1, keys - 1)
        else:
            key = position % keys
        # Include cancellation and mixed magnitudes rather than only exact integer sums.
        values = [1e16, 1.0, -1e16, 1e-100, -3.25, 7.5, None, 2.0]
        yield Document(f"document-{position}", {"key": key, "value": values[position % len(values)]},
                       position, "benchmark")


def encode_documents(documents):
    return [{"id": d.id, "payload": d.payload, "position": d.position, "source": d.source}
            for d in documents]


def percentile(values, fraction):
    return sorted(values)[min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))]


def peak_rss():
    try:
        status = Path("/proc/self/status").read_text()
        return next(int(line.split()[1]) * 1024 for line in status.splitlines()
                    if line.startswith("VmHWM:"))
    except (OSError, StopIteration):
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return value if platform.system() == "Darwin" else value * 1024


def trial(configuration):
    spec = NativeView("metrics", "key", "value", reducer=configuration["reducer"],
                      work=configuration["work"])
    engine = configuration["engine"]
    build_started = time.perf_counter()
    retained = list(fixture(configuration["documents"], configuration["shape"], configuration["keys"]))
    suffix = list(fixture(configuration["live_documents"], configuration["shape"],
                          configuration["keys"], start=len(retained)))
    input_build_seconds = time.perf_counter() - build_started
    fixture_json = json.dumps(encode_documents(retained + suffix), sort_keys=True,
                              separators=(",", ":"), allow_nan=False)
    reducer = RustReducer(spec.reducer)
    warmup_started = time.perf_counter()
    if engine == "python-sqlite":
        reducer.identity()  # Exclude one-time Wasmtime compilation from steady-state timings.
    warmup_seconds = time.perf_counter() - warmup_started
    with tempfile.TemporaryDirectory(prefix="foldlite-native-benchmark-",
                                     dir=configuration.get("storage_root")) as directory:
        path = Path(directory) / "database"

        def open_database():
            if engine == "python-sqlite":
                database = Database(path)
                database.register(reference(spec, reducer))
                return database
            return NativeDatabase(path, spec, backend=engine.removeprefix("rust-"),
                                  workers=configuration["workers"])

        def query(database, **kwargs):
            return database.query(spec.name, **kwargs) if engine == "python-sqlite" else database.query(**kwargs)

        def rows(database):
            return tuple(database.rows(spec.name, limit=4096)) if engine == "python-sqlite" else tuple(database.rows(limit=4096))

        def changes(database, cursor):
            result = []
            while True:
                page = (database.changes(spec.name, since=cursor, limit=4096)
                        if engine == "python-sqlite" else database.changes(since=cursor, limit=4096))
                result.extend(page.rows)
                cursor = page.cursor
                if page.complete:
                    return result

        started = time.perf_counter()
        database = open_database()
        setup_seconds = time.perf_counter() - started
        ingest_seconds = 0.0
        ingest_cpu_seconds = 0.0
        phases = {key: 0.0 for key in ("map_seconds", "group_seconds", "reduce_seconds", "commit_seconds")}
        observed = 0
        batch_size = configuration["batch_size"]
        inserted = 0
        for offset in range(0, len(retained), batch_size):
            batch = retained[offset:offset + batch_size]
            cpu_started = time.process_time()
            started = time.perf_counter()
            result = database.ingest(batch)
            ingest_seconds += time.perf_counter() - started
            ingest_cpu_seconds += time.process_time() - cpu_started
            inserted += result.inserted
            if engine != "python-sqlite":
                telemetry = database.stats().get("last_ingest", {})
                for key in phases:
                    phases[key] += telemetry.get(key, 0.0)
                observed = max(observed, telemetry.get("map_workers_observed", 0))
        started = time.perf_counter()
        initial_rows = rows(database)
        query_seconds = time.perf_counter() - started
        before = query(database)
        latencies = []
        for document in suffix:
            started = time.perf_counter()
            database.ingest([document])
            query(database, key=document.payload[spec.key_field], limit=1)
            latencies.append(time.perf_counter() - started)
        started = time.perf_counter()
        updates = changes(database, before.cursor)
        change_query_seconds = time.perf_counter() - started
        expected = rows(database)
        started = time.perf_counter()
        replayed = 0
        for offset in range(0, len(retained), batch_size):
            replayed += database.ingest(retained[offset:offset + batch_size]).replayed
        replay_seconds = time.perf_counter() - started
        assert rows(database) == expected
        before_close_stats = database.stats()
        started = time.perf_counter()
        database.close()
        close_seconds = time.perf_counter() - started
        disk_bytes = sum(file.stat().st_size for file in Path(directory).rglob("*") if file.is_file())
        started = time.perf_counter()
        database = open_database()
        reopened = rows(database)
        reopen_query_seconds = time.perf_counter() - started
        assert reopened == expected
        database.close()
        normalized = [{"key": row.key, "state": row.state, "result": row.result,
                       "sequence": row.sequence} for row in expected]
        row_json = json.dumps(normalized, sort_keys=True, allow_nan=False, separators=(",", ":"))
        return {
            **configuration,
            "fixture_sha256": hashlib.sha256(fixture_json.encode()).hexdigest(),
            "result_sha256": hashlib.sha256(row_json.encode()).hexdigest(),
            "result_rows": len(expected),
            "initial_result_rows": len(initial_rows),
            "reduced_nonnull_contributions": sum(row.result["count"] for row in expected),
            "inserted_documents": inserted,
            "replayed_documents": replayed,
            "change_rows": len(updates),
            "input_build_seconds": input_build_seconds,
            "runtime_warmup_seconds": warmup_seconds,
            "open_seconds": setup_seconds,
            "ingest_seconds": ingest_seconds,
            "ingest_cpu_seconds": ingest_cpu_seconds,
            "average_ingest_cpu_cores": ingest_cpu_seconds / ingest_seconds,
            "ingest_documents_per_second": len(retained) / ingest_seconds,
            "first_full_query_seconds": query_seconds,
            "ingest_to_full_query_seconds": ingest_seconds + query_seconds,
            "live_document_to_key_query_seconds": {
                "median": statistics.median(latencies),
                "p95": percentile(latencies, .95), "p99": percentile(latencies, .99),
                "samples": latencies},
            "change_query_seconds": change_query_seconds,
            "replay_seconds": replay_seconds,
            "close_seconds": close_seconds,
            "reopen_and_full_query_seconds": reopen_query_seconds,
            "database_bytes": disk_bytes,
            "peak_process_rss_bytes": peak_rss(),
            "native_ingest_phases": phases if engine != "python-sqlite" else None,
            "native_map_workers_observed": observed if engine != "python-sqlite" else None,
            "native_storage_info": {key: value for key, value in before_close_stats.items()
                                    if key not in {"last_ingest"}},
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--documents", type=int, default=4096)
    parser.add_argument("--keys", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=512)
    parser.add_argument("--live-documents", type=int, default=8)
    parser.add_argument("--workers", default="1,2,4,8,12,24")
    parser.add_argument("--synthetic-work", type=int, default=2000)
    parser.add_argument("--shapes", default="uniform,hot,unique")
    parser.add_argument("--maps", default="cheap,synthetic")
    parser.add_argument("--reducer", default="mean/v1")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--storage-root", type=Path,
                        default=ROOT / "experimental/engine/target/benchmarks",
                        help="temporary database parent; choose a real filesystem for durable I/O measurements")
    parser.add_argument("--trial-json", help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.trial_json:
        print(json.dumps(trial(json.loads(arguments.trial_json)), allow_nan=False))
        return
    if arguments.output is None:
        parser.error("--output is required")
    arguments.storage_root.mkdir(parents=True, exist_ok=True)
    compiled_sources = {
        "experimental/engine/src/lib.rs": _native.ENGINE_SOURCE_SHA256,
        "experimental/engine/src/storage.rs": _native.STORAGE_SOURCE_SHA256,
        "experimental/python/src/lib.rs": _native.BINDING_SOURCE_SHA256,
        "reducers/core/src/kernel.rs": _native.KERNEL_SOURCE_SHA256,
        "reducers/core/Cargo.toml": _native.KERNEL_MANIFEST_SHA256,
    }
    for relative, expected in compiled_sources.items():
        actual = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
        if actual != expected:
            raise RuntimeError(f"Installed native wheel is stale for {relative}; rebuild before measuring")
    source_wrapper = ROOT / "experimental/python/python/foldlitedb_experimental/__init__.py"
    if source_wrapper.read_bytes() != Path(foldlitedb_experimental.__file__).read_bytes():
        raise RuntimeError("Installed native Python wrapper is stale; rebuild before measuring")
    workers = [int(value) for value in arguments.workers.split(",")]
    if min(arguments.documents, arguments.keys, arguments.batch_size, arguments.live_documents,
           arguments.repeats, *workers) < 1 or max(workers) > 64 or arguments.batch_size > 1024:
        parser.error("positive sizes, batch<=1024 and workers1..64 required")
    results = []
    for shape in arguments.shapes.split(","):
        if shape not in {"uniform", "hot", "unique"}:
            parser.error("unknown shape")
        for map_name in arguments.maps.split(","):
            if map_name not in {"cheap", "synthetic"}:
                parser.error("unknown map")
            rounds = arguments.synthetic_work if map_name == "synthetic" else 0
            cases = [("python-sqlite", 1)] + [(backend, count) for backend in ["rust-sqlite", "rust-log"]
                                             for count in workers]
            for repeat in range(arguments.repeats):
                expected = None
                for engine, count in cases:
                    configuration = {
                        "documents": arguments.documents, "keys": arguments.keys,
                        "batch_size": arguments.batch_size, "live_documents": arguments.live_documents,
                        "shape": shape, "map": map_name, "work": rounds,
                        "reducer": arguments.reducer, "engine": engine, "workers": count,
                        "repeat": repeat,
                        "storage_root": str(arguments.storage_root.resolve()),
                    }
                    started = time.perf_counter()
                    process = subprocess.run([sys.executable, str(Path(__file__).resolve()),
                                              "--trial-json", json.dumps(configuration)],
                                             check=True, capture_output=True, text=True)
                    result = json.loads(process.stdout)
                    result["subprocess_wall_seconds"] = time.perf_counter() - started
                    identity = (result["fixture_sha256"], result["result_sha256"], result["result_rows"],
                                result["inserted_documents"], result["reduced_nonnull_contributions"])
                    if expected is None:
                        expected = identity
                    if identity != expected:
                        raise AssertionError(f"Result mismatch: {configuration}: {identity} != {expected}")
                    results.append(result)
                    print(f"{shape}/{map_name} {engine} workers={count}: "
                          f"{result['ingest_documents_per_second']:.0f} docs/s, "
                          f"{result['result_rows']} rows", flush=True)
                    # Preserve completed measurements even if a later trial fails.
                    arguments.output.parent.mkdir(parents=True, exist_ok=True)
                    arguments.output.write_text(json.dumps({"complete": False, "trials": results}, indent=2) + "\n")
    provenance_paths = [Path(__file__).resolve(), ROOT / "experimental/python/src/lib.rs",
                        ROOT / "experimental/python/build.rs",
                        ROOT / "experimental/python/python/foldlitedb_experimental/__init__.py",
                        ROOT / "experimental/engine/src/lib.rs", ROOT / "experimental/engine/src/storage.rs",
                        ROOT / "experimental/engine/Cargo.lock", ROOT / "experimental/python/Cargo.lock"]
    provenance_paths += [ROOT / "reducers/core/Cargo.toml", ROOT / "reducers/core/src/kernel.rs",
                         ROOT / "reducers/core/Cargo.lock", ROOT / "experimental/engine/Cargo.toml",
                         ROOT / "experimental/python/Cargo.toml", ROOT / "experimental/python/pyproject.toml",
                         ROOT / "src/foldlitedb/reducers/assets/reducer.json",
                         ROOT / "src/foldlitedb/reducers/assets/reducer.wasm"]
    provenance_paths += [ROOT / "src/foldlitedb/database.py",
                         ROOT / "src/foldlitedb/reducers/__init__.py"]
    provenance = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in provenance_paths if path.exists()}
    try:
        mount = subprocess.run(["findmnt", "-T", str(arguments.storage_root.resolve()),
                                "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"],
                               check=True, capture_output=True, text=True)
        storage_mount = json.loads(mount.stdout)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
        storage_mount = None
    report = {
        "complete": True,
        "scope": "experimental single declarative numeric view; identical documents, maps, ordered per-key reductions, transaction boundaries and durable acknowledgements",
        "architecture": {
            "python-sqlite": "existing Python callbacks + WASM reducers + SQL document/state/history indexes; WAL synchronous FULL",
            "rust-sqlite": "native field map + typed kernel + in-memory indexes; identical commit records journaled in SQLite, reconstructed on reopen",
            "rust-log": "same native computation/indexes; checksummed append-only commit records with durable sync, reconstructed on reopen",
        },
        "limitations": [
            "Python versus Rust comparison changes data layout and storage algorithm as well as language and reducer ABI",
            "Native worker comparisons use the same core and backend; they measure parallel document map/emit plus ordered per-key reduction",
            "Native document map/emit is parallel; grouping and per-key reduction are serial in this prototype",
            "Synthetic map performs artificial integer mixing and changes contributions; it is explicitly not an application performance claim",
            "Ingestion is measured after runtime warm-up; setup, subprocess lifetime and reopen/query costs are recorded separately",
            "Ingestion timings sum batch calls identically across engines; native stats telemetry is collected outside those timed calls",
            "No operating-system cache eviction is performed; first reads and reopen reads may be served from OS page cache",
            "Native journal backends reconstruct in-memory document/state/history indexes on reopen; no production compaction or bounded persistent history",
            "Expanded emissions are buffered for the whole bounded input batch; high fanout and large keys can consume substantial memory",
            "Native serde_json lexical key ordering differs from Python json for exponent-form floating keys, including floats inside structured keys",
            "Fixtures include mixed magnitudes and cancellation; matching per-key input order is required for exact state equality",
        ],
        "runtime": {
            "python": sys.version, "platform": platform.platform(), "cpu_count": os.cpu_count(),
            "affinity_cpu_count": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            "python_sqlite": __import__("sqlite3").sqlite_version,
            "wasmtime": importlib.metadata.version("wasmtime"),
            "rustc": subprocess.run(["rustc", "--version"], check=True, capture_output=True, text=True).stdout.strip(),
            "maturin": importlib.metadata.version("maturin"),
            "storage_root": str(arguments.storage_root.resolve()),
            "storage_mount": storage_mount,
            "baseline_database_module": baseline_database.__file__,
            "baseline_reducers_module": baseline_reducers.__file__,
            "baseline_actual_wasm_sha256": baseline_reducers.descriptor()["sha256"],
            "baseline_database_source_sha256": hashlib.sha256(Path(baseline_database.__file__).read_bytes()).hexdigest(),
            "baseline_reducers_source_sha256": hashlib.sha256(Path(baseline_reducers.__file__).read_bytes()).hexdigest(),
        },
        "provenance_sha256": provenance,
        "installed_extension": {
            "binary_sha256": hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),
            "engine_source_sha256": _native.ENGINE_SOURCE_SHA256,
            "storage_source_sha256": _native.STORAGE_SOURCE_SHA256,
            "binding_source_sha256": _native.BINDING_SOURCE_SHA256,
            "kernel_source_sha256": _native.KERNEL_SOURCE_SHA256,
            "kernel_manifest_sha256": _native.KERNEL_MANIFEST_SHA256,
            "python_wrapper_sha256": hashlib.sha256(Path(foldlitedb_experimental.__file__).read_bytes()).hexdigest(),
        },
        "trials": results,
    }
    arguments.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()

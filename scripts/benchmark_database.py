"""Deterministic rebuild, reopen and live-query measurements; no seed variants.

Run explicitly: python scripts/benchmark_database.py --documents 100000 --output report.json
Timings include Python document construction, mapping, reduction and transactions.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
from pathlib import Path
import statistics
import subprocess
import tempfile
import time

from foldlitedb.database import Database, Document, RustReducer, View


def mapping(document):
    yield document.payload['series'], document.payload['value']


def view():
    return View('metrics', '1', mapping, RustReducer('mean/v1'))


def documents(start, stop, keys):
    for position in range(start, stop):
        yield Document(str(position), {'series': position % keys, 'value': position % 1000},
                       position=position, source='benchmark')


def ingest_range(database, start, stop, keys, batch_size):
    for position in range(start, stop, batch_size):
        database.ingest(documents(position, min(position + batch_size, stop), keys))


def all_rows(database):
    snapshot = database.query('metrics', limit=4096)
    rows = [(row.key, row.result, row.state) for row in snapshot.rows]
    while snapshot.after is not None:
        snapshot = database.query('metrics', limit=4096, cursor=snapshot.cursor,
                                  after=snapshot.after)
        rows.extend((row.key, row.result, row.state) for row in snapshot.rows)
    return rows


def percentile(values, fraction):
    return sorted(values)[min(len(values) - 1, int((len(values) - 1) * fraction))]


def storage_info(root):
    try:
        mount = subprocess.run(
            ['findmnt', '-T', str(root.resolve()), '-J', '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS'],
            check=True, capture_output=True, text=True)
        mount = json.loads(mount.stdout)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
        mount = None
    return {'path': str(root.resolve()), 'mount': mount}


def run(root, count, keys, batch_size, live_count):
    path = root / 'metrics.db'
    database = Database(path)
    database.register(view())
    started = time.perf_counter()
    ingest_range(database, 1, count + 1, keys, batch_size)
    rebuild_seconds = time.perf_counter() - started
    started = time.perf_counter()
    rows = all_rows(database)
    first_query_ms = (time.perf_counter() - started) * 1000
    assert len(rows) == min(count, keys)
    database.close()

    started = time.perf_counter()
    database = Database(path)
    database.register(view())
    assert all_rows(database) == rows
    reopen_query_ms = (time.perf_counter() - started) * 1000

    latency, key_latency = [], []
    for position in range(count + 1, count + live_count + 1):
        started = time.perf_counter()
        database.ingest(documents(position, position + 1, keys))
        database.query('metrics', key=position % keys, limit=1)
        key_latency.append((time.perf_counter() - started) * 1000)
        all_rows(database)
        latency.append((time.perf_counter() - started) * 1000)
    expected = all_rows(database)
    database.close()
    database_bytes = path.stat().st_size

    regenerated = Database(root / 'regenerated.db')
    regenerated.register(view())
    started = time.perf_counter()
    ingest_range(regenerated, 1, count + live_count + 1, keys, batch_size)
    regeneration_seconds = time.perf_counter() - started
    assert all_rows(regenerated) == expected, 'Full regeneration differs from live ingestion'
    regenerated.close()

    memory_method = None
    try:
        # ru_maxrss may retain the launcher parent's pre-exec high water on
        # Linux. /proc measures this executable's actual memory high water.
        status = Path('/proc/self/status').read_text()
        peak_rss_bytes = next(int(line.split()[1]) * 1024 for line in status.splitlines()
                              if line.startswith('VmHWM:'))
        memory_method = 'Linux /proc/self/status VmHWM'
    except (OSError, StopIteration):
        peak_rss_bytes = None
    if peak_rss_bytes is None:
        try:
            import resource
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            peak_rss_bytes = rss if platform.system() == 'Darwin' else rss * 1024
            memory_method = 'resource.getrusage(RUSAGE_SELF).ru_maxrss'
        except ImportError:
            peak_rss_bytes = None
    return {
        'workload': {'documents': count, 'keys': keys, 'batch_size': batch_size,
                     'live_documents': live_count, 'reducer': 'mean/v1',
                     'payload': 'one integer series key and one scalar per document'},
        'runtime': {'python': platform.python_version(), 'platform': platform.platform(),
                    'machine': platform.machine(), 'sqlite': __import__('sqlite3').sqlite_version},
        'storage': storage_info(root),
        'rebuild_seconds': rebuild_seconds,
        'rebuild_documents_per_second': count / rebuild_seconds,
        'first_query_ms': first_query_ms,
        'reopen_and_query_ms': reopen_query_ms,
        'live_document_to_key_query_ms': {
            'median': statistics.median(key_latency),
            'p95': percentile(key_latency, .95), 'p99': percentile(key_latency, .99)},
        'live_document_to_full_query_ms': {
            'median': statistics.median(latency),
            'p95': percentile(latency, .95), 'p99': percentile(latency, .99)},
        'regeneration_seconds': regeneration_seconds,
        'regeneration_matches_live': True,
        'database_bytes': database_bytes,
        'database_bytes_per_document': database_bytes / (count + live_count),
        'peak_process_rss_bytes': peak_rss_bytes,
        'memory_measurement': memory_method,
        'benchmark_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--documents', type=int, default=100000)
    parser.add_argument('--keys', type=int, default=128)
    parser.add_argument('--batch-size', type=int, default=1024)
    parser.add_argument('--live-documents', type=int, default=100)
    parser.add_argument('--storage-root', type=Path,
                        help='temporary database parent; defaults to the system temporary directory')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if min(args.documents, args.keys, args.batch_size, args.live_documents) < 1:
        parser.error('All workload sizes must be positive')
    if args.storage_root is not None:
        args.storage_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='foldlitedb-benchmark-', dir=args.storage_root) as temporary:
        result = run(Path(temporary), args.documents, args.keys, args.batch_size,
                     args.live_documents)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))

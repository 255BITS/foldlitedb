"""Import telemetry, follow changes, and recover a consumer after cache loss.

Run with: python examples/telemetry.py
The temporary directory holds a real persistent log and a consumer checkpoint.
"""
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import tempfile

from foldlitedb import CursorExpired, Database, Document, View

METRICS = ("cpu.percent", "memory.mib", "request.latency_ms", "temperature.c", "voltage.v")


def reading(position):
    sample = position // len(METRICS)
    values = (20 + sample % 61, 1024 + (sample % 128) * 8,
              5 + (sample % 80) * 0.25, 18 + (sample % 50) * 0.25,
              3 + (sample % 8) * 0.125)
    payload = {"metric": METRICS[position % len(METRICS)],
               "value": values[position % len(METRICS)], "sample_seconds": position}
    # Preserve upstream offsets when available. Stable IDs make retries safe.
    return Document(f"gateway-07:{position}", payload, position, "gateway-07")


def oracle(records):
    values = defaultdict(list)
    for record in records:
        values[record.payload["metric"]].append(record.payload["value"])
    return {key: {"count": len(items), "sum": sum(items), "min": min(items),
                  "max": max(items), "mean": sum(items) / len(items)}
            for key, items in values.items()}


def snapshot(db):
    page = db.query(limit=2)
    summaries, cursor = {}, page.cursor
    while True:
        summaries.update((row.key, row.result) for row in page.rows)
        if page.complete:
            return summaries, cursor
        page = db.query(limit=2, cursor=page.cursor, after=page.after)


def poll(db, summaries, cursor):
    try:
        while True:
            page = db.changes(since=cursor, limit=2)
            # Change rows are complete postimages: replace a metric's result.
            summaries.update((row.key, row.result) for row in page.rows)
            cursor = page.cursor
            if page.complete:
                return summaries, cursor
    except CursorExpired:
        # Replace the entire application state, including keys no longer present.
        return snapshot(db)


def main():
    history = [reading(position) for position in range(2500)]
    live = [reading(position) for position in range(2500, 2513)]
    accepted = list(history)
    view = View.bundled("metrics")
    with tempfile.TemporaryDirectory(prefix="foldlite-telemetry-") as directory:
        path = Path(directory) / "telemetry.fold"
        consumer_path = Path(directory) / "consumer.json"
        with Database(path, view) as db:
            # ingest_many is lazy: iteration performs separate committed batches.
            imports = list(db.ingest_many(history, batch_size=100))
            assert len(imports) == 25 and sum(r.inserted for r in imports) == 2500
            before = db.stats()["sequence"]
            retry = db.ingest(history[1200:1300])
            assert retry.inserted == 0 and retry.replayed == 100
            assert db.stats()["sequence"] == before
            summaries, cursor = snapshot(db)
            assert summaries == oracle(accepted)
            for start in range(0, 12, 4):
                batch = live[start:start + 4]
                db.ingest(batch)
                accepted.extend(batch)
                summaries, cursor = poll(db, summaries, cursor)
                assert summaries == oracle(accepted)
            # Persist the cursor together with the state it describes.
            consumer_path.write_text(json.dumps({"summaries": summaries, "cursor": cursor}))

        with Database(path, view) as db:
            saved = json.loads(consumer_path.read_text())
            summaries, cursor = saved["summaries"], saved["cursor"]
            assert db.stats()["cache"]["status"] == "loaded"
            assert db.changes(since=cursor).rows == ()  # No duplicate changes.
            db.ingest(live[12:])
            accepted.extend(live[12:])
            summaries, cursor = poll(db, summaries, cursor)
            assert summaries == oracle(accepted)
            assert list(db.documents()) == accepted
            consumer_path.write_text(json.dumps({"summaries": summaries, "cursor": cursor}))

        log_digest = hashlib.sha256(path.read_bytes()).hexdigest()
        Path(str(path) + ".cache").unlink()  # Delete only the derived cache.
        with Database(path, view) as db:
            assert db.stats()["cache"]["status"] == "missing"
            assert db.stats()["replay"]["documents"] == 2513
            saved = json.loads(consumer_path.read_text())
            summaries, cursor = saved["summaries"], saved["cursor"]
            previous = summaries
            summaries, cursor = poll(db, summaries, cursor)
            assert summaries is not previous  # CursorExpired caused replacement.
            assert summaries == oracle(accepted)
            assert list(db.documents()) == accepted
            assert db.changes(since=cursor).rows == ()
            consumer_path.write_text(json.dumps({"summaries": summaries, "cursor": cursor}))
        assert hashlib.sha256(path.read_bytes()).hexdigest() == log_digest
        print(json.dumps({"readings": len(accepted), "summaries": summaries}, indent=2))


if __name__ == "__main__":
    main()

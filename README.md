# Foldlite DB

Embedded realtime views over immutable documents.

Foldlite DB builds versioned map/reduce views in a local, disposable database.
Applications retain their source documents and view definitions; derived storage
can be deleted and regenerated. HyperGAN is the first integrated consumer, and a
catalogue example exercises structured documents and ordered lifecycle folds.

The core uses Python's SQLite binding. The optional Rust/WebAssembly reducers
share one mathematical implementation between Python and browser workers.

```sh
python -m pip install '.[reducers]'
```

Installation uses the bundled WASM artifact and needs no Rust compiler. The base
package has no external runtime dependencies; numeric Rust reducers require the
`reducers` extra. Python ordered folds can run with the base package.

## A realtime metric

```python
from foldlitedb import Database, Document, RustReducer, View

def measurements(document):
    yield document.payload["metric"], document.payload["value"]

view = View("metrics", "1", measurements, RustReducer("mean/v1"))
with Database("metrics.db", generation="experiment-1") as db:
    db.register(view)
    db.ingest([
        Document("measurement-1", {"metric": "loss", "value": 4}, position=1),
        Document("measurement-2", {"metric": "loss", "value": 8}, position=2),
    ])
    snapshot = db.query("metrics")
    assert snapshot.rows[0].result == {"count": 2, "value": 6.0}

    db.ingest([Document("measurement-3", {"metric": "loss", "value": 3}, position=3)])
    updates = db.changes("metrics", since=snapshot.cursor)
    assert updates.rows[0].result == {"count": 3, "value": 5.0}
```

Retain the documents outside `metrics.db`. Deleting the database and replaying
those documents recreates its view results. Reopening an existing database and
registering the same view reuses its checkpoint. A different view version,
reducer identity, or declared reference version rebuilds that view atomically.

## Structured lifecycle state

```python
from foldlitedb import OrderedFold

def statuses(document):
    yield document.payload["record"], document.payload["status"]

lifecycle = View("status", "1", statuses,
                 OrderedFold(None, lambda previous, status: status, version="1"))
```

Documents remain immutable while their derived lifecycle state changes. Ordered
folds run in persisted ingestion order. Source positions may have gaps and must
increase for previously unseen documents within each source. Ordering several
sources by a domain timestamp is application policy; supplying documents in a
different order can change an ordered fold's result.

## Replay, pagination, and bounds

IDs and positions are scoped to a source. An identical replay performs no map
work; conflicting reuse of an ID or position fails. An ingestion batch commits
its documents, source checkpoints, view states, and changes together. Use
`for result in db.ingest_many(documents, batch_size=1024): ...` to consume a larger
iterable with bounded memory and separate atomic batches. This method is a lazy
iterator; iterating commits its batches.

`query` returns a stable snapshot. Continue its pages with **both** its `cursor`
and `after` fields, then pass the snapshot cursor to `changes`. Change pages carry
their own continuation cursor. Updates within one ingestion transaction are
coalesced per key; pages contain resulting key states. An empty map still advances
progress. Cursor validity is tied to the database generation and view revision.
`compact` can discard old change history; the caller chooses the retention boundary
and older cursors then expire explicitly.

Keys use canonical JSON lexical ordering. Ingestion, payloads, keys, emissions,
reducer state, and result pages have explicit limits. Total disk usage grows with
retained documents, keys, and change history. Large historical view registrations
hold a write transaction while rebuilding; readers can continue on their SQLite
snapshots. Source documents and reference-input contents remain the application's
responsibility.

Each database instance registers the view callbacks it owns. Queries and change
polls automatically catch those views up if another instance has ingested source
documents without those callbacks. `refresh(name)` explicitly performs the same
catch-up. A failing callback rolls back that view's catch-up and reports the error.

Map and ordered-fold code is trusted Python. A version is an explicit promise
about its behavior and captured inputs; changing a callable requires changing its
version or declared references. Rust provides `mean/v1`, `envelope/v1`, `sum/v1`,
and `count/v1`, with portable, versioned states and bounded execution. Arbitrary
floating-point merge trees need not produce bitwise-identical results. Browser
portability applies to the Rust reducers; Python maps and folds run in Python.

## Development and verification

```sh
python -m pip install '.[dev,browser]'
python -m pytest
python -m playwright install chromium
python -m pytest tests/browser
python scripts/build_reducer.py --check
cd reducers/core
cargo test --locked
```

An explicit deterministic benchmark measures the complete database path, including
mapping, Rust reduction, transactions, reopening, live queries, and regeneration:

```sh
python scripts/benchmark_database.py --documents 100000 --output /tmp/foldlite.json
```

The version is an initial implementation with HyperGAN as its first consumer.
See [DESIGN.md](DESIGN.md) for the architecture and acceptance contract, and
[reducers/core/README.md](reducers/core/README.md) for the reducer ABI and build.

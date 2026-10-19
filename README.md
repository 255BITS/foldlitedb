# Foldlite DB

Embedded realtime views over immutable documents.

This checkout contains an unreleased API update. The examples below require the
[reviewed development source](https://github.com/255BITS/foldlitedb/tree/api-native-prototype-2026-10-19);
a published package or the default remote branch may differ.

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

The examples start with a fresh `metrics.db` file.

```python
from foldlitedb import Database, Mean, View

def measurements(document):
    yield document.payload["metric"], document.payload["value"]

view = View("metrics", "1", measurements, Mean())
with Database("metrics.db", generation="experiment-1") as db:
    db.register(view)
    documents = db.add_many([
        {"metric": "loss", "value": 4},
        {"metric": "loss", "value": 8},
    ])
    assert [(row.key, row.result) for row in db.rows("metrics")] == [
        ("loss", {"count": 2, "value": 6.0}),
    ]
```

`add(payload, *, id=None, source="default", position=None)` accepts any finite
JSON value and returns a `Document` with its resolved ID, source, position, and
payload. IDs and positions are assigned automatically when omitted. Without a
name or position, each call appends a distinct document, even when the payload is
identical. `add_many(payloads, *, source="default")` eagerly appends one atomic
batch and returns a tuple of resolved documents in input order. Each iterable
member is one JSON payload, including arrays. It obeys `max_batch` (default 1024),
`max_batch_bytes`, and the per-payload limit; an error rolls back the whole batch.
Repeating an unnamed batch appends another batch.

`rows("metrics")` iterates the view in bounded pages. Its first iteration pins a
stable snapshot, and paused iteration holds no database transaction. Use
`rows("metrics", key="loss")` to read one group; `limit` sets the page size.

Retain the returned `documents` outside `metrics.db`.
Deleting the database and replaying them in the same order with
`db.ingest(retained_documents)` recreates its view results, including deterministic
numeric tie breaking. Re-adding only the payloads assigns new identities.
Reopening an existing database and registering the same view reuses its
checkpoint. A different view version, reducer identity, or declared reference
version rebuilds that view atomically. `Mean()`, `Sum()`, `Count()`, and
`Envelope()` select the bundled numeric reducers. They accept numeric map
contributions and ignore nulls; `Count()` counts non-null numeric contributions.
`RustReducer("mean/v1")` and the other explicit reducer IDs remain available.

## Live updates

Use a snapshot cursor when you need changes after a particular read:

```python
with Database("metrics.db", generation="experiment-1") as db:
    db.register(view)
    snapshot = db.query("metrics")
    third = db.add({"metric": "loss", "value": 3})
    updates = db.changes("metrics", since=snapshot.cursor)
    assert updates.rows[0].result == {"count": 3, "value": 5.0}
```

## Optional names and replay

Give a document a unique name when a retry should refer to the same document:

```python
with Database("metrics.db", generation="experiment-1") as db:
    db.register(view)
    saved = db.add({"metric": "loss", "value": 4}, id="measurement-1")
    assert db.add(saved.payload, id="measurement-1") == saved
    assert db.get("measurement-1") == saved
```

An identical named add does no map work, even after later documents arrive.
Changing the payload under an existing name raises `DocumentConflict`; documents
are immutable. Names are scoped to a source, which defaults to `"default"`.
An unnamed retry creates another document unless it supplies an existing source
position or replays the returned `Document` through `ingest`.

## Structured lifecycle state

```python
from foldlitedb import OrderedFold

def statuses(document):
    yield document.payload["record"], document.payload["status"]

lifecycle = View("status", "1", statuses,
                 OrderedFold(None, lambda previous, status: status, version="1"))
```

Documents remain immutable while their derived lifecycle state changes. Ordered
folds run in persisted ingestion order. Ordering several sources by a domain
timestamp is application policy; supplying documents in a different order can
change an ordered fold's result.

## External streams and batches

For an external log, supply its offset explicitly:

```python
with Database("metrics.db", generation="experiment-1") as db:
    db.register(view)
    saved = db.add({"metric": "loss", "value": 4}, source="training.log", position=4096)
```

Replaying the same payload at that source and position returns the original
document without counting it again. Source positions may have gaps and must
increase for previously unseen documents within each source. They are integers
from `0` to `2**53 - 1`; automatic positions start at zero and follow the source's
current checkpoint. Position assignment and view updates commit together, even
with several database connections. Avoid mixing automatic positions with an
external log's offsets unless they share the same progression.

`Document(id, payload, position, source="default")` and `db.ingest(documents)`
remain available for explicit source records and atomic batches. IDs and positions
are scoped to a source. An identical replay performs no map work; conflicting
reuse of an ID or position fails. An ingestion batch commits its documents, source
checkpoints, view states, and changes together. Use
`for result in db.ingest_many(documents, batch_size=1024): ...` to consume a larger
iterable with bounded memory and separate atomic batches. This method is a lazy
iterator; iterating commits its batches.

## Pagination and bounds

`rows` handles snapshot pagination for ordinary reads. Iterators follow the same
cursor expiry rules as explicit queries: a view rebuild or compaction can expire
a paused iterator. Use `query` when you need snapshot cursors, explicit pages,
or a continuation to retain outside the process.

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

## Documentation site

Read the [development documentation](https://255bits.github.io/foldlitedb/).

The repository-native site lives in `docs/` and builds without frontend runtime
dependencies. Validate its links and executable examples locally:

```sh
python scripts/build_docs.py
python scripts/check_docs.py --execute --browser
python -m http.server 8000 --directory _site
```

The browser check uses the optional Playwright installation shown above. An
authenticated repository administrator can publish the checked static files with
`python scripts/publish_docs.py`. It updates only generated files on the isolated
`gh-pages` branch and configures GitHub Pages branch hosting; it preserves source
branches and unrelated files on an existing recognized docs branch.

An explicit deterministic benchmark measures the complete database path, including
mapping, Rust reduction, transactions, reopening, live queries, and regeneration:

```sh
mkdir -p .benchmark-storage
python scripts/benchmark_database.py --documents 100000 \
    --storage-root .benchmark-storage --output /tmp/foldlite.json
```

Choose a storage directory on the device you intend to measure. Temporary
directories can reside in memory; the output report records the selected storage.

The optional [native experiment](https://255bits.github.io/foldlitedb/experiments.html)
supports one numeric field projection with SQLite or a durable custom log. Rust
mapping/emission can use multiple cores; grouping and reduction remain serial.
Both backends replay their full journal into memory on reopen. This prototype
does not support arbitrary Python maps, ordered folds, multiple views, or
compaction, and its data layout differs from the production Python engine.

The [primary report](reports/experimental-native-2026-10-19.json) contains 156
fresh-process trials on NVMe/ext4, with two repeats per configuration. All matched
fixtures and result hashes agree. Uniform synthetic mapping reached about 2×
end-to-end throughput relative to one native worker; cheap maps showed no
consistent multicore benefit. SQLite and the custom log were close in this
fixture. These local measurements do not establish production guarantees.

A separate [30-trial payload probe](reports/experimental-payloads-2026-10-19.json)
varies unused padding from zero to 1 KiB and 16 KiB per document. Larger payloads
reduce throughput and increase whole-process memory across this small fixture;
JSON, copying, retained storage, and durability costs change together. This does
not isolate one cause or model every nested payload shape.

The version is an initial implementation with HyperGAN as its first consumer.
See [DESIGN.md](DESIGN.md) for the architecture and acceptance contract, and
[reducers/core/README.md](reducers/core/README.md) for the reducer ABI and build.

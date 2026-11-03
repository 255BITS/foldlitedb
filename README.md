# Foldlite DB

**Immutable documents. Rebuildable live views. Rust and WebAssembly.**

Foldlite DB is a CouchDB-inspired map/reduce engine built around a simple rule:
keep immutable documents, derive everything else. The Rust engine appends accepted
document batches to a durable log. Indexes, source checkpoints, group states, and
change history form a disposable cache that can be reconstructed from that log.
One native engine owns document acceptance, source order, commits, and replay,
and is exposed to Python. Storage needs no database server or external database
library. Python callbacks can derive local views through that same commit path.

Write a view in AssemblyScript and compile it to WebAssembly. The same module
maps and reduces documents in Rust or entirely in a browser worker. Try the
[browser playground](https://255bits.github.io/foldlitedb/playground.html) or read
the [view authoring guide](docs/portable.html).

Read the [changelog](CHANGELOG.md) for the consolidated release history and the
[upgrade guide](https://255bits.github.io/foldlitedb/upgrading.html) before moving
an existing database to 0.3.2.

## Install

Use Python 3.11+ to install the 0.3.2 release:

```sh
python -m pip install foldlitedb==0.3.2
```

Native wheels are provided for Linux x86-64 and ARM64, macOS Intel and Apple
Silicon, and Windows x86-64. Installing a matching wheel needs neither Rust nor
Node. The [hosted documentation](https://255bits.github.io/foldlitedb/) describes
the API and bundled portable views.

### Build from this checkout

Building from source requires Python 3.11+, Rust/Cargo 1.90+, and a platform C
compiler and linker. From the repository root:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
python examples/portable_metrics.py
```

On Windows, activate with `.venv\Scripts\activate` and use an MSVC build
environment. Node.js is unnecessary for installation and bundled views. To make
a wheel, run `python -m pip wheel . --no-deps --wheel-dir dist`. Install that
wheel on a matching platform with `python -m pip install /path/to/the-built.whl`;
installing a built wheel needs neither Rust nor Node. See the
[release guide](docs/releasing.html) for PyPI Trusted Publisher setup and the
0.3.2 publication workflow.

## Append documents, read a live view

Start with a fresh `metrics.fold` file:

```python
from foldlitedb import Database, View

view = View.bundled("metrics")
with Database("metrics.fold", view) as db:
    documents = db.add_many([
        {"metric": "loss", "value": 4},
        {"metric": "loss", "value": 8},
    ])
    assert list(db.rows())[0].result == {
        "count": 2, "sum": 12, "min": 4, "max": 8, "mean": 6,
    }
    snapshot = db.query()
    documents += (db.add({"metric": "loss", "value": 3}),)
    assert db.changes(since=snapshot.cursor).rows[0].result["mean"] == 5
    retained = tuple(db.documents())
    db.checkpoint()
```

Rust executes both map and reduce through a bounded Wasm interpreter. `metrics`
groups finite numbers by a string `metric`; `catalogue` demonstrates an ordered
fold over structured events. Each database holds one view module. `Database` and
`View` are the primary Python names; `PortableDatabase` and `PortableView` are
aliases for the same types.

Try the [telemetry collector](examples/telemetry.py) for bulk import, retries, and
a persistent changes consumer, or the [catalogue example](examples/catalogue.py)
for an ordered item lifecycle. If a payload produces no rows, inspect its map
output with [`db.preview(payload)`](docs/api.html#preview).

Every unnamed append creates a new observation. Give an observation an ID when
identical retries should count once. IDs and positions are scoped to a source;
conflicting reuse raises an error. Documents cannot be updated or deleted. Append
new observations to represent changes, then let the view fold their history.

`add_many` commits one atomic batch of at most 1024 documents. Its complete Wasm
map request must fit 256 KiB, so larger payloads need smaller batches.
`rows()` follows a stable snapshot in bounded pages; `query()` and
`changes(since=...)` expose continuation cursors. See the
[Python API](docs/api.html#database) for signatures and limits.

## Query ranges and prefixes

Select an exact `key`, an ascending range with `start_key`/`end_key`, or a string
or array `prefix`. Bounds are inclusive unless `inclusive_start=False` or
`inclusive_end=False`. The selectors work in portable Wasm and Python callback
views, and the browser uses the same option names and ordering.

```python
with Database(":memory:") as db:
    db.add_many([
        {"metric": "temperature.c", "value": 21},
        {"metric": "temperature.f", "value": 70},
        {"metric": "voltage", "value": 12},
    ])
    assert [row.key for row in db.rows(prefix="temperature.")] == [
        "temperature.c", "temperature.f",
    ]
    assert [row.key for row in db.rows(
        start_key="temperature.c", end_key="voltage", inclusive_end=False,
    )] == ["temperature.c", "temperature.f"]
```

Keys sort by JSON type, with numbers in numeric order and compound keys compared
element by element. Strings use Unicode codepoint order, rather than CouchDB's
locale-sensitive ordering. An array prefix such as `["sales", "alice"]` selects
all keys beginning with those complete elements. These queries return existing
per-key results; they do not combine a selected range into a new reduction.
Continue explicit pages with the same selector and both returned `cursor` and
`after`; page size may change. See the [range query reference](docs/api.html#ranges).

## A log you keep; a cache you can discard

For a persistent database, `metrics.fold` contains the immutable document log.
`metrics.fold.cache` is an optional checkpoint; `metrics.fold.lock` coordinates
one active owner. A batch is acknowledged after its log frame is synced. The
engine validates and computes a batch before appending it, so a map or reducer
failure does not commit a prefix.

`checkpoint()` writes an atomic cache snapshot; closing cleanly does the same.
On reopen, the engine verifies the log and matching cache prefix, then runs Wasm
only for batches after the checkpoint. Missing, corrupt, or incompatible caches
are rebuilt from the documents. `rebuild()` explicitly recomputes the view and
starts a new cursor generation. Neither operation changes the source documents.

The document log is independent of the view module. Opening it with different
module bytes rebuilds the cache in place; retain the exact module if you need to
reproduce its previous results. Close the database before backing up the log and
view artifacts; Windows locks prevent other handles from reading an active log.
The cache is replaceable; the only copy of a document log is not.

Version 0.3.2 creates `FOLDLOG2` logs with persisted predecessor hashes. Existing
`FOLDLOG1` logs open for reading and export; migrate into a new filename before
appending:

```python
Database.migrate("old.fold", "migrated.fold")
```

Close the source first. Migration verifies and preserves its original batch
boundaries, leaves the source unchanged, and refuses an existing destination.
Keep the source backup until the migrated log and its view have been verified.
The [step-by-step upgrade guide](https://255bits.github.io/foldlitedb/upgrading.html)
covers backup, verification, application cutover, and rollback.
Filenames ending in `.cache` or `.lock`, or containing `.cache.`, are reserved
for native sidecars. `reset=True` explicitly discards a database only after
obtaining its ownership locks; it refuses a live owner and validates the module
before changing data. An I/O failure during reset is reported and may leave its
outcome uncertain.

Rebuild uses the original batch order. It scans the log linearly and runs each
uncached batch through the same map/reduce path as live ingestion. A valid cache
avoids repeated Wasm work, but reopening still verifies the complete log. Current
indexes and history grow in memory with the data. Cache snapshots are limited to
1 GiB of encoded checkpoint data; a checkpoint or close error reports a failure
to save that accelerator, while committed log data remains intact. There is no
claim of constant memory or measured rebuild throughput.

To copy retained records into a fresh database:

```python
with Database("copy.fold", view) as copied:
    copied.ingest(retained)
    assert list(copied.rows())[0].result["mean"] == 5
    before = [(r.key, r.state, r.result) for r in copied.rows()]
    copied.rebuild()
    assert [(r.key, r.state, r.result) for r in copied.rows()] == before
```

Use `ingest_many(records, batch_size=100)` for larger archives. **Iterate its
results to perform the import**: each yielded result commits one separate bounded
batch, and earlier batches remain committed if a later batch fails. See the
[retry-safe source import](docs/api.html#bulk-import) for constructing records with
upstream positions or stable IDs alone. Preserve record order; preserve original
batch boundaries too if you need identical engine sequence values.

## Author a view, run it on a website

For Python-only computation, `View(name, version=..., map=..., reducer=...)` and
`OrderedFold` use the same native document acceptance and durable commit model.
Callbacks receive resolved `Document` values; identical retries never map twice.
All maps, reducer updates, and finalizers finish before the batch commits. Their
descriptor-bound caches are disposable and recover by replaying original source
batches after reopen or cache loss. Changes to callback code or reference inputs
require a new view/reducer version or references. Callback code is trusted Python
and does not have Wasm's fuel or memory isolation. See the
[callback API](docs/api.html#callbacks) for snapshots and supported operations.

Source names beginning with `_` are reserved for metadata. They are retained,
exported and replayed, and advance source progress, but do not contribute to
views in either host.

The [AssemblyScript SDK](views/README.md) provides JSON helpers and the Wasm ABI.
Implement `map`, `identity`, `add`, and `finalize`; implement `merge` only for
states that can be combined correctly. Inputs and state are explicit, and views
must be deterministic. An ordered fold must give equivalent state when the same
contributions are split across successive batches.

Rebuild the two bundled examples with a Node.js runtime matching `^22.23.4 || ^24.22.1 || >=26.12.1`:

```sh
npm ci --prefix views
npm run build --prefix views
npm test --prefix views
npm run check --prefix views
```

The build writes modules and SHA-256 manifests into
`src/foldlitedb/portable_assets/`. It builds the named `metrics` and `catalogue`
examples plus an internal document-only module used by callback views. Compile a custom view and generate its manifest with:

```sh
npm run build:view --prefix views -- assembly/examples/my-view.ts --out-dir .build
```

Both paths are relative to `views/`. The
[authoring guide](docs/portable.html#authoring) includes a complete custom view
and the build command to generate its Wasm and digest manifest.

`foldlitedb.portable_assets()` exposes compiled modules plus `client.js`,
`worker.js`, and `host.js`. Copy them to a static site and import
`BrowserDatabase` from `client.js`. The [browser example](docs/portable.html#browser)
covers appends, snapshots, changes, export, and replay verification. JavaScript
owns in-memory grouping and storage in the browser worker; the Rust log engine
is not compiled into this browser build.

Browser state is lost on reload or close. Export resolved documents first. The
preview bounds documents, groups, history, and request sizes. Persistent browser
storage, log/history compaction, and multiple views per native database remain
future work. JSON numbers use IEEE-754 double precision; integers a double
cannot represent exactly are rejected, so use strings for exact
arbitrary-precision identifiers.

## Development and documentation

```sh
python -m pip install '.[dev,browser]'
python -m playwright install chromium
python -m pytest
python -m pytest tests/browser
cargo test --locked --manifest-path native/Cargo.toml -p foldlitedb-portable-engine
npm ci --prefix views
npm test --prefix views
npm run check --prefix views
python scripts/build_docs.py
python scripts/check_docs.py --execute --browser
python -m http.server 8000 --directory _site
```

The docs checker executes annotated Python examples in fresh temporary
directories and checks links, search, and desktop/mobile rendering in Chromium.
Open `http://localhost:8000` for the built docs and playground. An authorized
maintainer can publish checked generated docs with
`python scripts/publish_docs.py`; building alone does not publish them.

Measure the native path on your target filesystem with:

```sh
python scripts/benchmark_database.py --documents 10000 --keys 16 --batch-size 100 --live-documents 20 --output native-benchmark.json
```

Use `--storage-root /path/to/filesystem` to select the filesystem being measured.
The report compares durable ingestion, checkpointing, cached reopen, suffix
replay, and full reconstruction, while checking unchanged log bytes and matching
results. Local measurements describe that fixture and host.

See [DESIGN.md](DESIGN.md) for durability, replay, cache validation, and execution
contracts.

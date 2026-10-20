# Foldlite DB design

Foldlite DB derives live views from immutable documents. Its computation model is
inspired by CouchDB: map each document into keyed contributions, then reduce each
key. Its persistence model is a Rust append-only document log with a disposable
cache. Python exposes that engine directly. One compiled WebAssembly module
contains the full map and reducer, and runs unchanged in the native host and a
browser worker.

## Separate facts from derived state

The document log is the persistent source for this engine. Accepted documents are
immutable. An identical replay is a no-op; conflicting reuse of an identity or
source position is an error. Changes in a domain are new documents, rather than
mutations of old observations.

Document indexes, source checkpoints, group states, finalized results, and change
history are derived from those facts and the exact view module. They can be
reconstructed. A cache is useful only while its dependencies can be verified:
format version, document-log identity, committed prefix and digest, and compiled
module digest. Missing, corrupt, or incompatible cache data cannot silently
become authoritative.

Applications must back up the document log and computation artifacts, or retain
an external authoritative document stream with stable identities and order.
Deleting a cache is safe; deleting the only copy of the log is data loss. A
reference version is insufficient to reproduce a lost reference snapshot; any
reference data needed by a deterministic view must remain reproducible.

## Native durability

`native/engine` owns storage and Wasm execution. The path passed to `Database`
is a log file; `.cache` and `.lock` siblings hold an optional cache checkpoint and
an operating-system lock. One active owner may open a persistent log. `:memory:`
uses the same computation without persistent files or durable acknowledgements.

The log has an identity and checksummed, hash-linked frames containing accepted
document batches. The source format does not contain a view-module dependency.
An ingestion operation validates identities, source order, limits, mapping, and
reduction before writing. The engine appends the complete frame and syncs it
before acknowledging the batch, then exposes its already-computed derived state.
A failed validation or view operation cannot commit a batch prefix. An I/O error
can leave commit outcome uncertain, as with any durable append; reopen and retry
using explicit document identities rather than assuming a failed acknowledgement
means absence.

Opening verifies the complete log. An incomplete final frame can be discarded
as an interrupted append. A complete frame with corrupt checksums or invalid
content is an error; the engine does not skip committed source data. Frame size
is bounded to 8 MiB. The ordinary map-request limit is smaller and often limits a
batch first.

`checkpoint()` writes an atomic derived snapshot, and graceful `close()` also
checkpoints. Checkpoints are not written after every ingestion. Encoded cache
snapshots are limited to 1 GiB. Checkpoint errors, including on close, are reported;
they do not undo already-synced document batches. A failed close still releases
the native handle. A valid snapshot
must match the log identity, exact module digest, and verified prefix. The host
loads it and replays only the original suffix batches through Wasm. A missing,
invalid, or different-module cache triggers a full rebuild from the log.

A checkpoint is a performance hint: failure or absence must not change the
meaning of accepted documents. Its checksum and verified log prefix detect
accidental corruption and stale files; they are not an authenticity guarantee
against an adversary who can rewrite local files and checksums.

## Rebuild as the ordinary computation path

Live ingestion and replay use the same map, grouping, ordered fold, and finalizer.
Rebuild preserves accepted document order and original batch boundaries. That
also preserves batch-dependent engine sequence allocation; a separate export and
re-import with different boundaries need only preserve view state and results.

`rebuild()` discards derived state, replays the log, creates a new cursor
generation, and checkpoints it. Opening the same log with a changed module does
the same reconstruction automatically. Neither operation rewrites documents.
An unchanged valid cache preserves its generation across reopen; cursor consumers
must obtain a new snapshot after reconstruction.

Rebuild cost is a linear read of the log plus Wasm work for uncached documents
and their emissions. A valid cache avoids Wasm execution for its verified prefix,
but opening still reads that prefix to validate it. There is no unmeasured
throughput promise. Current documents, indexes, grouped states, and change history
grow in host memory; bounded per-call Wasm memory is not a total-memory bound.
Compaction, incremental checkpoint formats, and multiple native views remain
future work rather than implicit capabilities.

## Identity and order

The public input is a `Document(id, payload, position, source)`. Payloads are
finite JSON. `add` can generate IDs and source positions; `add_many` resolves one
atomic batch. Generated IDs are stable only after resolution, so an unnamed
retry creates another observation. External sources can provide their own IDs
and monotonically increasing positions with gaps. Positions start at zero when
assigned automatically and are scoped to a source.

An identical named append reuses its original position even after later records
arrive. A supplied source position can also identify a replay without a supplied
ID. Reusing an ID or position with different content fails. Previously unseen
positions behind the source checkpoint fail. Source progress and document
acceptance are one logical commit.

Per-key reduction follows ingestion order, including within a batch. Source
positions do not sort documents across sources, and timestamps do not reorder a
fold. Applications choose domain order before ingestion. Maps receive payloads;
identity, deduplication, positions, and transaction boundaries belong to the host.
Any domain metadata a view needs belongs in its explicit payload or declared
reproducible module data.

## Portable computation

A view exports `descriptor`, `map`, `identity`, `add`, `finalize`, and optionally
`merge` through ABI v1. AssemblyScript is the first SDK; the ABI does not require
that source language. A map returns zero or more JSON key/value contributions.
The host partitions by canonical keys and supplies ordered contributions to the
module's reducer.

State is explicit. Fresh Wasm instances prevent module globals from carrying
state between requests. Views must not depend on clocks, randomness, mutable
external data, or request boundaries. Splitting one ordered contribution sequence
across successive `add` calls must produce equivalent state. Implement `merge`
only when independently covered compatible states can be combined correctly;
an ordered lifecycle fold need not be mergeable.

The exact module SHA-256 binds cached state to computation, state schema, compiler,
and SDK runtime. Human-readable view and state versions explain changes but do
not override a changed digest. Preserve the previous module to reproduce its
previous result. Opening the same documents with another module is a new derived
view and invalidates old cursors.

Modules have no host imports. ABI requests and responses are limited to 256 KiB,
linear memory to 16 MiB, and native execution to a fuel budget. Ingestion is
bounded to 1024 documents and 4096 emissions per batch. JSON nesting is bounded
to 64 levels. Python releases the GIL while the native engine runs. These limits
isolate individual operations; they do not bound accumulated host storage.

JSON numbers follow finite IEEE-754 double semantics in both hosts. Python
rejects integers that cannot be represented exactly as doubles. Source positions
and engine sequences stay within JavaScript's nonnegative safe integer range.
Canonical object keys and numeric formatting agree across hosts. Floating-point
addition is not associative, so arbitrary merge trees can change rounding.

## Reads and cursor generations

A snapshot identifies a database generation, module digest, and committed
sequence. Bounded pages retain a stable prefix while ingestion continues. Change
pages return complete resulting key states after a cursor, coalesced per key
within each batch. Zero-emission documents still advance progress. Sequence is
not a document count: inserted documents and changed group states allocate it.

`rows()` and `documents()` pin their prefix on first iteration and release the
engine between pages. No lock spans a yield into application code. The persistent
file's ownership lock is retained for the database lifetime. Cursors are local
continuation markers, not access credentials. Rebuild or module change starts a
new generation and explicitly expires old cursors. Current history is retained;
there is no compaction or reader-retention policy yet.

## Python packaging and the browser

`Database` and `View` are the primary Python types; `PortableDatabase` and
`PortableView` are exact aliases. The package has one native storage and
computation path. `native/python` provides PyO3 bindings, and maturin builds the
extension into wheels. Source builds need Python 3.11+, Rust/Cargo 1.90+, and a C
toolchain. Bundled modules need no Node; authoring uses Node 20+.

The browser worker runs the same module bytes, with JavaScript owning grouping
and in-memory storage. It does not run the Rust log engine and has no persistent
browser storage yet. Export and replay resolved documents to preserve work.
Worker deadlines, retained-byte limits, and document/group/history caps bound
this preview's requests and retained data. A timeout discards its memory.

## Acceptance and evidence

Correctness requires immutable-identity conflicts, source-order enforcement,
atomic batches, interrupted-tail recovery, complete-corruption rejection,
single-owner locking, checkpoint validation, suffix replay, module replacement,
and explicit rebuild. Rebuild and live execution must agree on structured keys,
ordered state, and results. Snapshot/change continuity survives a valid cache
reopen and fails explicitly after a new generation. Native and actual browser
execution must agree for the same module and supported numerical inputs.

Installation checks exercise built wheels without local compilers, and source
distributions with build prerequisites. PyPI publication remains pending. Future
performance evidence must record workload, fixture, batch size, documents,
emissions, key cardinality, host/runtime, durability, log/cache bytes, peak memory,
rebuild time, reopen validation time, and document-to-query latency. Run `python scripts/benchmark_database.py --documents 10000 --keys 16
--batch-size 100 --live-documents 20 --output native-benchmark.json` to produce a
reproducible local report; `--storage-root` selects the measured filesystem. The
design promises reconstructibility and a clear cost model, rather than an
unmeasured speedup.

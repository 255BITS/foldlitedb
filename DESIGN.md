# Foldlite DB design

Foldlite DB is an embedded engine for realtime views over immutable documents.
CouchDB's views inspire its computation model; SQLite inspires its deployment
model and supplies its initial local persistence implementation.

## Authority and regeneration

Applications own the authoritative source documents. Foldlite DB stores a derived
copy, indexes, reduction states, and checkpoints. Retained documents, exact view
definitions, reducer versions, and declared reference-input versions must suffice
to regenerate query results. A database file is disposable. Deleting it does not
delete the application source. Reference contents must remain reproducible at the
declared version; an identifier alone cannot recreate a lost taxonomy or snapshot.

The canonical public term for an input is **document**. Applications can append a
finite JSON payload with `add(payload)`, optionally supplying a unique `id`, a
`source`, or a `position`. The returned `Document` always has a stable identity,
a source identity, a monotonically increasing source position, and the payload.
The default source is `"default"`. Omitted identities are generated; omitted
positions start at zero or follow that source's committed checkpoint. Assignment
and ingestion share one transaction, including across database connections.
`add_many(payloads, source=...)` eagerly appends one bounded atomic payload batch
and returns resolved documents in input order. Each iterable member is a payload;
JSON arrays remain whole documents. Retrying an unnamed batch appends new records.

Every append without a name or position creates a distinct document. An identical
named append reuses the original position and does no map work. A supplied source
and position can also identify an identical replay without a caller-supplied name. A conflicting
reuse of an identity or position is an error. Identities and positions are scoped
to a source. Existing `Document` records and atomic `ingest` batches retain their
explicit replay contract. New source positions must increase; gaps are allowed.
Source order and domain ordering such as a measurement timestamp are distinct.

Automatic metadata is a convenience, not a transfer of document authority. For
exact regeneration, applications must retain the returned documents outside the
derived database and replay them in their original ingestion order. Raw payloads
alone do not preserve generated identities or numeric envelope tie breaking.
External logs may continue to supply their own stable identities and positions.

## Computation

A versioned map inspects a document and emits zero or more structured key/value
contributions. Views partition contributions by canonical keys. Reducers declare
their accepted input and state contracts. Numeric reducers use a shared pure
Rust core exposed through a bounded JSON/WASM interface. Ordered per-key folds
support lifecycle state without assuming arbitrary partial states can merge.
`Mean`, `Sum`, `Count`, and `Envelope` are convenience constructors for the same
versioned numeric adapters; they preserve state descriptors and load the optional
execution runtime only when computation needs it.

The reducer interface is identity, batched add, merge where valid, and finalize.
Serialized state is versioned. Mathematical state owns no document-ID set, file
cursor, filesystem handle, transport queue, or training concept. Floating-point
merge order is part of reproducibility; arbitrary regrouping need not be bitwise
identical. Native Rust and WASM builds must use the same computation and verify
their supported numerical contracts.

Rebuild and live ingestion use the same map/fold path. Deterministic maps and
folds may use only their document and declared reference inputs. Python callables
are trusted application code. General Python functions are not automatically
incremental, mergeable, portable to a browser, or safe to run untrusted.

## Storage and reads

SQLite transactions commit document identities, derived states, change records,
and source progress together. Exceptions roll back the entire ingestion batch.
Persistent checkpoints allow reopening to process only the remaining documents.
Version changes invalidate incompatible cached states. The source document
sequence remains available for rebuilding a view within an existing database.

A query snapshot identifies the database generation, view revision, and committed
position. Its bounded pages represent one stable prefix while ingestion proceeds.
Changes resume after that prefix. Cursors are continuation markers, not access
credentials. Old-generation and old-revision cursors fail explicitly. Query and
change pages are bounded; applications control polling or transport scheduling.
Slow HTTP consumers must not create an unbounded queue or block producers.
`rows(name, key=..., limit=...)` lazily follows those bounded snapshot pages. Its
first iteration selects the committed prefix; no transaction or lock spans a
yield to application code. Revisions and history expiry retain their normal
cursor behavior.

The initial design allows one coordinated writer and concurrent readers through
SQLite. Group state can grow on disk with key cardinality; bounded reducer state
per key is not a bound on total database storage. Retention and compaction need
an explicit cursor policy before old change history can be removed.

## HyperGAN extraction

Reusable document paging, continuation markers, projection framing/recovery,
stream delivery primitives, Rust sources, and browser reducer assets belong in
Foldlite DB. HyperGAN retains metric catalogs, training attempt and recovery
lineage interpretation, publication policy, custom-map process ownership, and
its application HTTP surface. Existing run files and public wire contracts remain
readable through adapters. Small compatibility imports may preserve the Python
surface while computation and persistence have a single external owner.

## Acceptance

Acceptance requires batch rollback, conflicting replay rejection, restart/catch-up,
snapshot/change continuity, revision invalidation, fresh-rebuild/live equivalence,
structured ordered folds, and shared reducer behavior in Python and a real browser.
Installed-wheel and source-distribution checks establish usable packaging without
Rust at installation. HyperGAN's relevant file-reader, projector, reducer, viewer,
and frontend checks must run against the extracted package.

Performance evidence must report workload, runtime, host, batch size, document
count, distinct keys, database bytes, peak memory, rebuild throughput, reopen cost,
and document-to-query latency. Benchmarks use deterministic fixtures and no
alternate-seed trials. Initial measurements guide optimization; there is no
unmeasured throughput guarantee.

## Implementation checkpoints

The Python API now supports unnamed JSON appends, optional source-scoped names,
atomic payload batches, snapshot row iteration, and numeric reducer shortcuts.
The current production path remains SQLite with Python maps and folds plus the
shared Rust/WASM numeric core. Upcoming work is finite and checked in phases:

- [x] Verify the API behavior tests, examples, and documentation snippets.
- [x] Publish the locally checked repository-native GitHub Pages documentation
  at https://255bits.github.io/foldlitedb/ and verify its public pages and assets.
  Static branch publishing uses the repository's existing authorization; no
  additional workflow permission is needed.
- [x] Finish the explicitly experimental Rust engine and Python bindings using
  both SQLite and a durable custom log behind the same supported view contract.
- [x] Verify parallel map/emission processing and deterministic comparisons covering
  cardinality, skew, payload size, worker counts, numerical behavior, and matched
  durability. Keep arbitrary Python callback support distinct from declarative
  native field maps.
- [x] Run final regressions and report measured results, remaining limitations, and
  which capabilities remain experimental before claiming completion.

Final local verification includes 149 base Python tests (also against the installed
wheel), 39 optional native-binding tests, six browser reducer tests, and separately
built base/native distributions. The primary NVMe/ext4 report contains 156
fresh-process trials with two repeats per configuration; matched fixture and
result hashes agree. Nine additional storage-accounting probes cover both native
backends and the Python reference. A separate tmpfs run is supplementary evidence.

Only native document mapping/emission is parallel. Grouping and per-key reduction
remain serial, expanded emissions are buffered, native indexes/history grow in
memory, and reopening replays the full journal. Native field maps do not replace
arbitrary Python callbacks, ordered folds, multiple views, or compaction. Native
JSON ordering for float-valued keys can differ from the Python API. Small local
benchmarks do not establish general speedups, bounded storage, or production
latency. Source publication and the last static-site refresh follow this local
verification checkpoint.

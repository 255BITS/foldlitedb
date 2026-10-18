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

The canonical public term for an input is **document**. A document has a stable
identity, a source identity, a monotonically increasing source position, and a
finite JSON payload. Replaying the same document is idempotent. A conflicting
reuse of an identity or position is an error. Source order and domain ordering
such as a measurement timestamp are distinct.

## Computation

A versioned map inspects a document and emits zero or more structured key/value
contributions. Views partition contributions by canonical keys. Reducers declare
their accepted input and state contracts. Numeric reducers use a shared pure
Rust core exposed through a bounded JSON/WASM interface. Ordered per-key folds
support lifecycle state without assuming arbitrary partial states can merge.

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

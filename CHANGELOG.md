# Changelog

Release dates below are UTC publication dates. See the
[step-by-step upgrade guide](https://255bits.github.io/foldlitedb/upgrading.html)
before moving existing databases to 0.4.0. Each version links its GitHub release;
the 0.2.0 release also includes a receipt for its tested distributions.

## Unreleased

## [0.4.0](https://github.com/255BITS/foldlitedb/releases/tag/v0.4.0) — 2026-11-10

### Reproducible reducers and portable SDK hardening

- Build the bundled `reducer.wasm` module reproducibly in-repo from `native/reducer` (`foldlitedb-reducer-wasm`, ABI v1: `mean/v1`, `envelope/v1`, `sum/v1`, `count/v1`, and `finite-scalar-position/v1`) with zero-buffering borrowed deserialization, `scripts/build_reducer_wasm.py --check`, generated third-party license notices, and domain-neutral terminology.
- Harden the AssemblyScript view SDK (`views/assembly/sdk/`) to reject lone UTF-16 surrogate escapes, reject trailing non-whitespace JSON tokens, and cap decimal mantissa scanning at 768 significant digits with a sticky nonzero remainder bit to prevent superlinear big-decimal inputs. Rebuild bundled `metrics`, `catalogue`, and `documents` Wasm modules; opening existing `0.2.0`/`0.3.x` logs with bundled views automatically reconstructs their disposable `.cache` without log migration.
- Enforce exact IEEE-754 double / JS shortest-spelling integer validation on Wasm responses in `portable_assets/host.js`, canonicalize `-0` retained keys and align cursor error types in `portable_assets/worker.js`, and validate empty-coverage bootstrap state keys plus pre-write buffer overlap and post-call failure poisoning across Rust and JS reducer hosts.

### Native storage, PyO3, and Python API

- Allow `Database.migrate(source, destination)` on `FOLDLOG1` logs to skip an incomplete torn final frame directly without requiring a separate open/close pass, allow opening read-only logs inside unwriteable parent directories, load `.cache` checkpoints in a single streaming SHA-256 pass, hash log frames without redundant cloning, enforce the 8 MiB frame limit on `:memory:`, and export `MAX_CACHE_BYTES`.
- Replace English substring exception matching across Rust and PyO3 with structured `EngineError` / `ErrorKind` tuples (`DatabaseError`, `DocumentConflict`, `SourceOrderError`, `CursorExpired`).
- Decouple per-batch callback commits from full-state serialization via incremental `callback_updates`, batch `canonical_keys` normalization across groups, raise the PyO3 request ceiling for checkpoint persistence to 1 GiB, and set `opt-level = 1` on `wasmi` in dev/test Cargo profiles.
- Make `Database.query()` and `Database.rows()` use the optional positional parameter strictly for a registered callback `view_name` (requiring `key=...` for exact key selection), support `Database.get(id=None, *, position=None, source="default")` with strict validation, wire up `batch_size` / `max_batch_bytes` per-group flushing in `materialize_jsonl_view`, append incremental suffix deltas (`"delta": True`) in `Database.projection`, and ship `py.typed`.
- Fix `read_jsonl_tail` exact newline-boundary line retention, validate `sse()` event names against empty/`\r`/`\n`/`:`, wake idle `stream_events` generators immediately on `Subscriber.close()`, `fsync` `repair_jsonl_tail` truncations, handle empty `AppendJournal.sync()`, remove vestigial `MANIFEST.in`, and enforce `rustfmt`, `clippy`, `ruff`, and reducer Wasm reproducibility in CI.

## [0.3.3](https://github.com/255BITS/foldlitedb/releases/tag/v0.3.3) — 2026-11-10

### State machine and lifecycle hardening

- Validate callback `View` definitions before opening or resetting the native storage engine so invalid callback views never truncate an existing log when `reset=True` is passed, and make `View.module` read-only.
- Release native ownership without writing a checkpoint (`{"op": "close", "checkpoint": false}`) when `Database(...)` initialization aborts on a generation mismatch or callback registration error, preserving any existing valid `.cache` sidecar on disk; reject opening an existing non-empty untagged database with an explicit `generation`.
- Consolidate Python callback view state into a cohesive `_RegisteredView` structure with atomic rollback on failed `register()`, enforce re-entrancy guards (`_callback_evaluating`) across lazy group finalization and incremental projection reduction, and pass rebuilt callback checkpoints into `{"op": "rebuild"}` so `Database.rebuild()` writes and syncs `.cache` once with accurate `cache.bytes` reporting.
- Support an explicit `view=` keyword argument on `Database.query()` and `Database.rows()`, reject ambiguous simultaneous positional and keyword key selectors, and prevent guest Wasm `"View rejected operation: ..."` messages from being misclassified as `DocumentConflict` or `CursorExpired`.
- Resolve symlink targets (including dangling symlinks) before validating primary log paths so dangling symlinks cannot target reserved `.cache` or `.lock` sidecars, allow read-only `FOLDLOG1` and migration source logs to open without write permissions or in-place tail truncation, skip reading obsolete checkpoints when `reset=True`, and validate ABI input/output buffer disjointness in native and JS reducer hosts.
- Preserve caller watermark dictionaries in `stream_events`, emit `gap` without a false `ready` frame when a subscriber overflows during replay, terminate closed subscriber loops cleanly, reject boolean `max_bytes` in `read_jsonl_tail`, and mark `AppendJournal` failed on `sync()` verification errors.

## [0.3.2](https://github.com/255BITS/foldlitedb/releases/tag/v0.3.2) — 2026-11-03

### Native execution

- Upgrade the native Wasm interpreter from Wasmi 0.47.2 to 2.0.0 and use direct instantiation for import-free modules. Keep fresh instances for portable operations, guest validation, memory bounds, and deterministic fuel interruption.
- Retain the Python API, ABI v1, bundled module bytes, document log and cache formats, and compatible 0.3.1 cursors. No migration is required from 0.3.1.
- Wasmi 2.0 meters fuel by input Wasm operations. Applications supplying custom numeric reducer fuel budgets should recalibrate them; exhausted reducer instances remain unusable.
- Calibrate the bounded portable operation budget to 500 million Wasmi 2.0 units for near-capacity requests. The previous 100 million-unit budget rejected the new 249,882-byte fixture on both interpreter versions; accepting it broadens the measured workload envelope. Bundled numeric reducers keep their configurable 50 million-unit default.
- Add regression coverage for fuel exhaustion, fresh guest state, memory growth limits, and supported maximum-sized workloads, and refresh native dependency notices.

## [0.3.1](https://github.com/255BITS/foldlitedb/releases/tag/v0.3.1) — 2026-11-03

This release includes the range-query changes prepared for 0.3.0. **0.3.0 was
not published to PyPI:** its Windows validation caught test fixture encoding and
open-log locking assumptions. Its GitHub tag is retained without moving it.

### Queries

- Add ascending range queries with `start_key`/`end_key` and inclusive or
  exclusive endpoints, plus string and structural array `prefix` selection.
- Expose the same selectors in Python `query()`/`rows()`, named callback views,
  and the browser worker. Ranges return existing per-key results; query-time
  reduction across selected keys is not implemented.
- Order keys by JSON type, numbers numerically, strings by Unicode codepoint,
  and compound keys recursively. This replaces serialized-JSON row ordering;
  string collation is binary rather than CouchDB's ICU ordering.
- Bind versioned query continuations to the complete selector and snapshot.
  Preserve stable pages during ingestion and allow page-size changes. Reject
  changed selectors and legacy query tokens explicitly.
- Maintain a derived ordered native query index without changing canonical
  group identity, ingestion order, or sequence allocation. Existing 0.2.0 logs
  and compatible caches require no migration; snapshot/change cursors and
  document pagination remain compatible. Start a fresh query traversal after
  upgrading from 0.2.0.

### Documentation

- Add this consolidated history and a dedicated upgrade guide with backup,
  migration, verification, cutover, rollback, and compatibility steps.
- Document range/prefix semantics, executable examples, and the 0.2.0-to-0.3.1
  query compatibility steps. Include the changelog in source distributions.

### Cross-platform verification

- Read the shared Unicode query fixture explicitly as UTF-8 and compare log
  bytes after releasing the database's Windows ownership lock.
- Run the Windows Python 3.11 installed-wheel/source-rebuild checks on every
  pull request and mainline push, alongside the existing Linux checks.

## [0.2.0](https://github.com/255BITS/foldlitedb/releases/tag/v0.2.0) — 2026-11-03

### Compatibility and migration

- New databases use `FOLDLOG2`. Existing `FOLDLOG1` logs remain readable, but
  appends require `Database.migrate(source, destination)` into a new file. Copy
  migration preserves source bytes, document identities, and original batches.
- Primary filenames ending in `.cache` or `.lock`, or containing `.cache.`, are
  reserved for sidecars, case-insensitively.
- Sources beginning `_` are metadata: both native and browser hosts retain them
  and advance source progress without contributing to views.
- A primary Python callback view now supplies `query()` and `rows()` results.
  Callback `preview()` and `changes()` explicitly reject unsupported operations.
  Portable Wasm views keep those operations.
- Callback descriptors govern cache reuse. Changed view/reducer versions or
  references reconstruct ordinary views and invalidate external projection
  progress together with derived state.

### Correctness and durability

- Hold native ownership locks throughout reset and validate modules before
  destructive changes. Prevent checkpoints from overwriting another log through
  a sidecar name collision.
- Persist predecessor hashes in log frames to detect omitted, reordered, and
  cross-log spliced frames. A predecessor chain alone cannot detect removal of
  a complete final suffix without an independently trusted end boundary.
- Route callback appends through native acceptance and deduplication. Stage all
  maps, reducers, and finalizers before durable commit; recover disposable
  callback caches from original batches after reopen or cache loss.
- Bound `ingest` iterator consumption and snapshot each yielded payload. Protect
  callback mutation guards, isolate named snapshot cursors, and preserve ordered
  contributions when external projection keys have equivalent JSON numbers.
- Align native/browser payload depth and relocated Wasm output-buffer checks,
  with a shared conformance corpus and rebuilt portable modules.

### Verification and operations

- Run core checks on pull requests and on `develop`/`master`, with broader weekly
  interpreter and platform coverage.
- Add storage I/O failure injection, a real legacy-log migration fixture, and
  native/callback lifecycle tests against independent results.
- Record replay, latency, and memory measurements at 1,000, 10,000, and 100,000
  documents. These fixtures do not establish a universal capacity limit.
- Verify five wheel platforms and a rebuilt source archive before publication;
  attach a receipt with the tested distribution hashes to the release.

See the [engineering review and resolution](reports/review-2026-11-02.md) and the
[upgrade guide](https://255bits.github.io/foldlitedb/upgrading.html#before-you-start).

## [0.1.4](https://github.com/255BITS/foldlitedb/releases/tag/v0.1.4) — 2026-11-02

- Upgrade `wasmi` to 0.47.2 to address
  [GHSA-g4v2-cjqp-rfmq](https://github.com/wasmi-labs/wasmi/security/advisories/GHSA-g4v2-cjqp-rfmq).
- Reject native JSON integers that cannot be represented exactly as doubles,
  matching Python validation rather than silently rounding them.
- Export Rust limit constants and make limit errors identify the operation,
  observed size, and limit.
- Expose writer health through `writer_poisoned()`, `is_writable()`, and stats;
  release ownership even if the final cache checkpoint fails.
- Require the documented patched Node runtime range for view authoring.

This release includes changes prepared for 0.1.3. **0.1.3 was not published
separately.**

## [0.1.2](https://github.com/255BITS/foldlitedb/releases/tag/v0.1.2) — 2026-10-20

- Batch external projection reductions across source pages.
- Keep projection logs compact by avoiding duplicate raw source frames.
- Cache finalized group results across queries.

## [0.1.1](https://github.com/255BITS/foldlitedb/releases/tag/v0.1.1) — 2026-10-20

- Add native `wasmi` reducers and historical view materialization.

## [0.1.0](https://github.com/255BITS/foldlitedb/releases/tag/v0.1.0) — 2026-10-20

- First PyPI release: immutable JSON document logs, atomic batches,
  source-scoped identity, retry-safe imports, and rebuildable map/reduce views.
- Expose the Rust storage/Wasm engine through Python `Database` and `View`,
  including snapshots, change feeds, and disposable cache checkpoints.
- Bundle metrics/catalogue modules and a browser worker using the same view
  bytes.
- Publish Python 3.11+ native wheels for Linux x86-64/ARM64, macOS Intel/Apple
  Silicon, and Windows x86-64, plus a source distribution.

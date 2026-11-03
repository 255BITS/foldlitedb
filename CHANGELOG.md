# Changelog

Release dates below are UTC publication dates. See the
[step-by-step upgrade guide](https://255bits.github.io/foldlitedb/upgrading.html)
before moving existing databases to 0.2.0. Each version links its GitHub release;
the 0.2.0 release also includes a receipt for its tested distributions.

## Unreleased

- Add this consolidated history and a dedicated upgrade guide with backup,
  migration, verification, cutover, rollback, and compatibility steps.
- Include the changelog in future source distributions.

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

# Foldlite DB initial implementation — 2026-10-18

Foldlite DB now owns the reusable document persistence, replay, view computation,
and Rust/browser reducers extracted from HyperGAN. The implementation is on
`master` in `/nvme/dev/255bits/foldlitedb`. HyperGAN's first-consumer integration
is on `feat/foldlitedb` in `/nvme/dev/hypergan/foldlitedb-integration`, rebased onto
develop `5a2497c4`. Neither HyperGAN develop nor master was merged or released.

## Implemented contracts

- SQLite-backed immutable document copies with source-scoped IDs/positions,
  conflict detection, and atomic ingestion of documents, checkpoints, view state,
  and change records. Replaying an identical document runs no map again.
- Versioned Python maps over structured JSON and explicit declared references;
  ordered lifecycle folds and Rust mean, sum, count, and envelope reducers.
- Batched reduction, bounded inputs/emissions/state/result pages, indexed latest
  state, stable snapshot pagination, resumable changes, cross-instance catch-up,
  explicit compaction, and generation/revision invalidation.
- Shared JSONL paging, incomplete-tail recovery, journals, durable prefix checks,
  atomic metadata, process writer locks, and bounded SSE replay/backpressure.
- Pure typed Rust computation, separate protocol/WASM host, bundled browser
  assets, native Rust verification driver, and reproducible offline WASM build.
- HyperGAN adapters preserve existing source/projection formats, metric catalogs,
  attempt/recovery semantics, process supervision, HTTP contracts, and Python
  compatibility imports. Historical query caches persist in Foldlite DB.

Cancellation drains one worker that owns its database connection. A cache lock
spans the entire historical job across processes, preventing a newer query from
advancing state under an older cursor. Delayed older-watermark queries rebuild
in memory while preserving the useful newer disk cache. Corrupt derived SQLite
files become visible job errors and can be removed and regenerated. Same-generation
source-boundary corruption fails explicitly.

## Verification

Testing used Python 3.14.8, SQLite 3.51.2, Wasmtime 48.0.0, Playwright 1.63.0,
and actual Chromium on Linux. Fresh virtual-environment tests installed wheels
outside both source trees, with no training runtime in the lightweight environment.

| Check | Result |
| --- | --- |
| Foldlite default tests | 101 passed |
| Foldlite browser reducer tests | 6 passed |
| Combined installed Foldlite + both projects' reducer/browser tests | 164 passed |
| HyperGAN web API/lifecycle suite before dashboard rebase | 95 passed |
| Rebased HyperGAN reducer/web/browser suite | 174 passed |
| HyperGAN changed foundation/package boundaries | 83 passed, 1 heavy deselected |
| Explicit heavy custom-map timeout/output-bound case | 1 passed |
| Broad installed HyperGAN foundation | 897 passed, 12 deselected, 1 baseline failure |
| Relevant CPU reference tests, fast and explicit heavy | 22 passed, 1 baseline failure |
| Native Rust unit tests | 2 passed |
| Native Rust/WASM protocol comparisons | 35 cases passed |
| Pinned WASM rebuild | Exact byte match |
| Frontend chart tests / offline bundle check | 3 passed / matched |
| Both wheels and source-distribution rebuilds | Built and installed successfully |

The foundation failure checks fingerprints against external training-run files;
the same ten mismatches reproduce on untouched develop `906e546a`. The heavy
custom-metric failure reads `loss/g_total` at step 2 despite the default cadence
10; the same failure reproduced on the original source. Neither failure was
changed for this extraction.

The host's occupied port 8765 was handled with a separate network namespace,
keeping the current UID and isolating temporary state. A private `XDG_RUNTIME_DIR`
allows the existing systemd unit syntax test to run. Early runs exposed harness
and environment failures; the counts above describe the corrected runs. No
alternate-seed experiments or GPU training were run.

The WASM is 209,562 bytes with SHA256
`864115d9bb62750c69b26313573764ced77bcc7cc7439f62b1b5d516b33da148`.

## Database measurements

[Raw receipt](database-100k-2026-10-18.json). Run:

```sh
python scripts/benchmark_database.py --documents 100000 --keys 128 \
  --live-documents 100 --output /tmp/foldlite.json
```

The fixture uses 128 integer keys, one scalar contribution per document, mean
reduction, batches of 1,024, full SQLite synchronization, and 100 single-document
live commits. Timings include document construction, map/reduce, and transactions.

| Measurement | Result |
| --- | ---: |
| Rebuild 100,000 documents | 4.380 s / 22,831 documents/s |
| Same-process reopen and full 128-key query | 13.35 ms |
| Live commit through single-key query, p99 | 0.904 ms |
| Live commit through full 128-key query, p99 | 13.87 ms |
| Full regeneration after live suffix | 4.272 s; same results |
| Database after 100,100 documents | 12,226,560 bytes |
| Peak executable RSS from Linux `/proc` | 82,702,336 bytes |

These are local fixture measurements, not production latency guarantees. Reopen
shares an already compiled WASM module in the same process. The p99 values are
order statistics from 100 operations. Arbitrary key cardinality, payloads, maps,
and storage devices are not covered by this fixture.

The reducer microbenchmark does not establish a universal improvement: interleaved
1,024-value batches measured mean approximately 0.948 → 1.207 ms (27% slower) and
envelope 1.652 → 1.530 ms (7% faster) versus HyperGAN's original kernel. A native
Python backend has not been implemented; Python and browser use WASM.

## HyperGAN persistence proof

[Raw receipt](hypergan-history-2026-10-18.json). The checked-in HyperGAN
`scripts/foldlite_integration_proof.py` creates 10,000 measurements plus the
initial source document, stops/reopens the real observation service, then appends
one measurement and reopens again.

The reopened historical view performs **zero** `Reducer.add` calls. The new
suffix performs exactly **one** addition containing one contribution. Result
counts match 10,001 measurements. This proves checkpoint reuse and suffix-only
reduction through the application service, not just the standalone database API.

The recorded historical request took 1.599 s initially, 0.230 s after reopening,
and 0.217 s with the single-document suffix. Background source/projection indexing
still took approximately 0.9 s at each service startup. One query cache occupied
21,356,544 bytes for this fixture. These timings have a different workload from
the standalone mean benchmark and are not directly comparable.

## Remaining scope and delivery

Applications must retain authoritative documents and exact reference-input
contents. Python callbacks are trusted, versioned application code. Ordered folds
follow ingestion order; domain-time ordering and cross-record join policies stay
with the application. Numeric reductions are portable; arbitrary Python maps and
folds do not execute in browsers.

HyperGAN currently stores a document copy per historical query. There is no
automatic cache eviction, and its service discovery indexes rebuild on startup.
Deleting a run's `views/<revision>/foldlite/` directory while stopped regenerates
those derived views. A shared projection store and persisted discovery index are
useful later performance work, with a larger concurrency/retention contract.

At initial qualification, pushes were blocked by the shell's SSH setup and an
HTTPS credential without `workflow` scope. Reloading the user's `.zshrc` restored
the configured SSH authentication. The public repository is now
[255BITS/foldlitedb](https://github.com/255BITS/foldlitedb), with `master` pushed.
The HyperGAN integration branch is pushed and
[PR #398](https://github.com/HyperGAN/HyperGAN/pull/398) targets `develop`.

Until the first package release, HyperGAN's `FOLDLITEDB_SOURCE` CI variable points
to the qualified public commit
`git+https://github.com/255BITS/foldlitedb.git@c5b237d1d99d32fadeb0a913a054f89ce50392d4`.
No package publishing or release was performed.

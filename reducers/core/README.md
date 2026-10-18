# Portable reducers

Foldlite DB's reducer kernel is pure Rust, with a typed `rlib` API in
`src/kernel.rs`. The bounded JSON protocol lives in `src/protocol.rs`; WASM
exports in `src/lib.rs` provide only buffers and request dispatch. Database
persistence, source identity, offsets, duplicate delivery and subscriptions stay
outside the mathematical kernel. This code was extracted from HyperGAN under
its MIT license; upstream and third-party notices are preserved.

Python and a dedicated browser worker execute the exact same bundled
`reducer.wasm`, checking its SHA-256 against `reducer.json`. No WASI, filesystem,
clocks or host imports are available. Installation needs no Rust compiler;
rebuilding does. Install `foldlitedb[reducers]` for Python reduction. Document
storage does not require Wasmtime. Missing Wasmtime raises an actionable error.

```python
from foldlitedb.reducers import Reducer
r = Reducer()
state = r.add(r.identity("mean/v1"), [
    {"value": 4.0, "position": [1, "document-1/emission-0"]},
    {"value": 8.0, "position": [2, "document-2/emission-0"]},
])
assert r.finalize(state) == {"count": 2, "value": 6.0}
```

## Computation contract

`identity(reducer_id)` returns versioned initial state. `add(state, values)`
adds a batch. `merge(left, right)` combines compatible mathematical states.
`finalize(state)` produces a result without changing the state. Persist and
merge states, never finalized means.

| Reducer | State | Finalized result |
| --- | --- | --- |
| `mean/v1` | Count and sum | Count and mean; empty value is null |
| `sum/v1` | Count and sum | Count and sum; empty value is zero |
| `count/v1` | Count | Count and count value; empty value is zero |
| `envelope/v1` | Count and first/min/max/last points | Same points and count |

All four accept `finite-scalar-position/v1` contributions. Null or absent
values contribute nothing, including to count. Map a document to value `1` to
count it. A position is a nonnegative JavaScript-safe integer ordinal and
1–128 ASCII source identity bytes. A host may hash arbitrary Unicode document
identities to fit this portable position schema. Multiple emissions must have
distinct positions. First/last order is lexicographic by declared position,
not arrival order. Min/max ties choose the smallest position.

`descriptor()["capabilities"]` declares mergeability, state version, schema,
ordering requirement and whether merge is exact. Count and envelope are exact
within their declared valid-input bounds. Sum and mean use IEEE-754 addition,
which is not associative: identical ordered partitions produce consistent
Python/browser results, but arbitrary merge trees need not be bitwise equal.
A source emission must have one immutable value; duplicate delivery belongs to
the host. The kernel does not retain an unbounded deduplication set.

Unknown fields, reducers and state versions, malformed states, nonfinite values,
unsafe counts/ordinals, overflow and oversized batches fail. Each add accepts
at most 1024 contributions. State size remains bounded by the source identity
lengths, independent of document count. These builtins do not yet accept
structured payloads or execute ordered lifecycle folds; database-host reducers
can declare their own schemas and ordering needs outside this portable ABI.

## Portable state and live coverage

The Python `bootstrap`, `append_frame`, `merge_bootstraps` helpers and JS
coverage wrappers handle source coverage outside the mathematical kernel.
Their opaque ASCII identity must bind source generation, map revision, view
revision, reference-input versions, grouping and bucket alignment as applicable.
Offsets are half-open committed projection positions, not contribution counts.
A complete frame advances coverage even with zero emissions. Covered replay is
ignored; a gap, partial overlap, changed identity or module mismatch requires a
new bootstrap. Only adjacent disjoint states merge. These are portable state
helpers; the database owns durable checkpoints and transactions.

The ABI exports `abi_version`, `input_ptr`, `input_capacity`, `execute(length)`,
`output_ptr` and `memory`. Hosts copy one UTF-8 JSON request into a fixed 256 KiB
input buffer and read at most 64 KiB of response from the output buffer.
Requests have `op` of `identity`, `add`, `merge` or `finalize`; responses contain
`ok` or `error`. The module retains no state between calls. Instances must not
be called concurrently; the Python wrapper serializes calls. The worker client
allows one outstanding request.

Linear memory cannot exceed 16 MiB. Python sets a 50-million-instruction fuel
budget per call; trapped instances are discarded, and new instances can resume
from validated state. Browsers use a dedicated worker with an external deadline
and terminate it on timeout. Custom remote WASM is not supported. Key
cardinality and subscription queues belong to the database engine.

## Rebuild and verification

From repository root, with `cargo` and `rustup` on PATH:

```sh
python scripts/build_reducer.py
python scripts/build_reducer.py --check
cargo test --locked --manifest-path reducers/core/Cargo.toml
python scripts/check_reducer_parity.py
python -m pytest tests/test_reducers.py tests/test_reducer_import.py -q
python -m pytest tests/browser -q
python scripts/reducer_proof.py --output /tmp/proof.json --benchmark
```

The toolchain is pinned to Rust 1.90.0, and dependencies/checksums to
`Cargo.lock`. The build strips symbols and remaps checkout/Cargo paths.
`--check` compares a fresh build to shipped bytes without modifying resources.
The bundled `THIRD_PARTY_LICENSES.txt` preserves dependency and Rust standard
library notices. Sources, lockfile and rebuild scripts ship in the sdist; the
wheel includes the module, manifest and static hosts.

The parity script builds a native Rust JSON driver and compares successful and
invalid requests against the bundled WASM. That driver is for validation and
profiling. There is no native Python backend; adopting one should follow measured
workloads and equivalent numerical and validation behavior.

Browser tests require Playwright plus Chromium:
`python -m playwright install chromium`. They launch a real worker on a loopback
server and verify Python bootstrap plus live browser suffix, duplicate replay,
bad inputs, coverage rejection and deadline isolation. Missing Chromium fails.
To inspect manually, copy `foldlitedb.reducers.assets()` resources to a temporary
folder, add generated `proof.json`, serve with `python -m http.server --bind
127.0.0.1`, and open `proof.html`.

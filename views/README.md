# Portable AssemblyScript views

These modules contain both mapping and reduction. The same compiled Wasm file
runs in the Rust host and a browser worker. The Rust host owns an immutable document log and disposable derived cache; the
browser host owns in-memory documents and groups. Both own batching, identity,
checkpoints, and replay. Modules receive explicit inputs and return JSON outputs.

Build with Node.js 20 or newer:

```sh
npm ci --prefix views
npm run build --prefix views
npm test --prefix views
npm run check --prefix views
```

`build` compiles the examples and writes Wasm and SHA-256 manifests into
`src/foldlitedb/portable_assets/`. `check` recompiles and compares every byte with
the committed artifacts. The npm lockfile pins the compiler, JSON codec, and
transitive dependencies. No compiler or Node.js installation is needed to use the
bundled modules from an installed Python package or website.
The build also copies the linked dependencies' licenses and upstream notices
into `portable_assets/THIRD_PARTY_LICENSES.txt`; `check` verifies this file too.

## Authoring a view

The [complete authoring guide](../docs/portable.html#authoring) includes a working
custom module, its build command, Python usage, and a
static browser page. The sketch below illustrates the map; it is not a complete
compilable view until the reducer methods are implemented.

`npm run build` compiles exactly `metrics` and `catalogue`. Build your own source
with:

```sh
npm run build:view --prefix views -- assembly/examples/metric-totals.ts --out-dir .build
```

Both paths are relative to `views/`, including when the command is run from the
repository root; absolute paths also work. The output directory is created if
needed. The source filename controls the output names: `metric-totals.ts` writes
`metric-totals.wasm` and `metric-totals.json`. Use letters, digits, underscores,
dots, or hyphens in that filename. The manifest contains the descriptor read
from the compiled module and its SHA-256. Its descriptor name may differ from
the filename; browser `connect({name})` selects the artifact filename stem.

The command uses the same pinned compiler settings as the bundled build,
ignores ambient `JSON_*` compiler switches, validates the module ABI and
descriptor, and replaces outputs only after successful compilation and
validation. Compilation failures preserve existing artifacts. SDK, dependency,
build-script, and bundled asset directories are protected output destinations.
Both hosts must receive the same compiled bytes, and the browser also needs the
matching manifest. `node scripts/build-view.mjs --help` from `views/` shows usage.

Extend `View` from `assembly/sdk/index.ts`, implement `map`, `identity`, `add`, and
`finalize`, and optionally implement `merge`. The SDK handles request parsing,
JSON envelopes, UTF-8, byte buffers, and collection of temporary allocations.
`map` returns zero or more `Emission` values per document. States, keys, and
contributions can contain JSON objects and arrays as well as scalars.

```ts
import { JSON, View, Emission, object, numeric, dispatch } from "../sdk";
export { abi_version, input_ptr, input_capacity, output_ptr } from "../sdk";

class MyView extends View {
  constructor() { super("my-view", false, "1", "1"); }

  map(document: JSON.Value): Emission[] {
    const doc = object(document);
    if (doc == null) return [];
    const key = doc.get("metric");
    const value = doc.get("value");
    if (key == null || !numeric(value)) return [];
    return [new Emission(key, value!)];
  }

  // See assembly/examples/metrics.ts for the state and reducer methods.
}
```

Each entry point creates one view and exports the SDK dispatcher:

```ts
const view = new MyView();
export function execute(length: i32): i32 { return dispatch(view, length); }
```

Return `this.fail("description")` for a recoverable reducer error. Throwing or
asserting causes a Wasm trap, which hosts report as module execution failure.
View functions must be deterministic and must not keep mutable data between
requests. Every state update receives its entire previous state explicitly.
The hosts instantiate modules afresh for operations. Splitting one ordered input
sequence across successive `add` calls must produce equivalent state; do not
make results depend on a batch boundary.
The SDK collects unreachable objects before every request and uses AssemblyScript's
incremental garbage collector within requests. The module's maximum linear
memory is 16 MiB. Hosts must also impose execution limits for untrusted modules.

Set `mergeable` to true only when independent states can be merged correctly.
Ordered folds must preserve contribution order and leave it false. Change the
view version for semantic changes and the state version for incompatible state
formats. Identity and merge laws are the view author's responsibility; floating
point sums can differ slightly when merged in different orders.

## Bundled examples

**Metrics** accepts documents with a string `metric` and a finite numeric
`value`. Documents with missing fields, null values, nonnumeric values, or an
incorrect shape emit no contributions. Valid documents emit their metric as the
key and number as the value. Its state is `{count,sum,min,max}`. Finalization adds
`mean`; empty states use `null` for mean, minimum, and maximum. It supports merge.
Invalid contributions, invalid states, and numeric overflow produce errors.
Nonfinite JSON values are rejected at the ABI boundary before mapping.

**Catalogue** accepts `{category:string,id:string,title:string,deleted?:boolean}`.
Each category groups a latest-event fold from item IDs to titles. A document
`{category,id,deleted:true}` removes that item. Finalization returns
`{count,items:{id:title}}`. Replaying events in order reproduces the catalogue;
reordering events can change the result, so this view does not support merge.
Malformed catalogue documents emit no contributions. IDs are limited to 65,535
UTF-16 code units, matching the JSON object's member-name limit.

## ABI v1

Modules have no imports. They export `memory` and these functions:

| Export | Signature | Meaning |
| --- | --- | --- |
| `abi_version` | `() -> i32` | Always 1 |
| `input_ptr` | `() -> i32` | Address of the request buffer |
| `input_capacity` | `() -> i32` | 262,144 bytes |
| `output_ptr` | `() -> i32` | Address of the response buffer |
| `execute` | `(length:i32) -> i32` | Response byte length |

The host writes UTF-8 JSON at `input_ptr()`, calls `execute(length)`, and reads
the returned byte count at `output_ptr()`. Read memory after execution because
the buffer can grow. Responses are exactly one of `{ok:value}` or
`{error:string}`. The response capacity is also 262,144 bytes. Request and
response values must be finite JSON with at most 64 nesting levels; object
member names support up to 65,535 UTF-16 code units. Invalid UTF-8, malformed
JSON, unsupported operations, and invalid argument shapes return error envelopes.

| Request | Successful value |
| --- | --- |
| `{op:"descriptor"}` | `{abi:1,name,version,state_version,mergeable}` |
| `{op:"map",documents:[...]}` | One `[{key,value},...]` array per document |
| `{op:"identity"}` | Initial state |
| `{op:"add",state,values:[...]}` | Updated state |
| `{op:"finalize",state}` | Public result |
| `{op:"merge",left,right}` | Merged state, if supported |

The manifest binds each compiled artifact to its descriptor:

```json
{
  "abi": 1,
  "module": "metrics.wasm",
  "sha256": "<64 lowercase hexadecimal digits>",
  "descriptor": {
    "abi": 1,
    "name": "metrics",
    "version": "1",
    "state_version": "1",
    "mergeable": true
  }
}
```

The SDK uses the pinned json-as scalar codec and syntax validator. Its small
eager parser adapter decodes escaped object keys before constructing state.
This avoids the dependency's dynamic-key escape behavior. The scalar compiler
mode also avoids its optimized string codec's surrogate-pair issue. Tests cover
Unicode, escaped controls and member names, reserved object names, malformed
requests, merge and replay, overflow, 10,000 successive map calls, and exact
round trips for 10,000 finite IEEE-754 numbers. The SDK converts decimal numbers
with integer rational division and nearest-even rounding, avoiding precision
and exponent defects in AssemblyScript 0.28.20's built-in number parser.

## Cache identity and reconstruction

The complete compiled module's SHA-256 identifies native cached state. Human
view and state versions explain semantic changes; matching labels never make
different module bytes interchangeable. Opening an existing native document log
with changed bytes rebuilds its cache from the original accepted batches. The
log's immutable documents remain unchanged. Keep exact module artifacts to
reproduce previous results, and obtain new cursors after a rebuild.

A valid checkpoint avoids Wasm work for its prefix. Reopen still verifies the
complete log; a full rebuild performs linear log I/O plus computation over each
batch and its emissions. The browser uses the same computation in memory and
requires export/replay to retain documents across reloads.

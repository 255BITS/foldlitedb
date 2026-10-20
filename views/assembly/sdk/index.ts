import { JSON } from "json-as";
import { validateJSON } from "json-as/assembly/util/validateJson";
import { Parser } from "./parse";

export { JSON };

// The host only exchanges UTF-8 bytes. Reducer authors never handle pointers.
const CAPACITY: i32 = 262144;
const INPUT: usize = memory.data(CAPACITY);
const OUTPUT: usize = memory.data(CAPACITY);

export function abi_version(): i32 { return 1; }
export function input_ptr(): i32 { return <i32>INPUT; }
export function input_capacity(): i32 { return CAPACITY; }
export function output_ptr(): i32 { return <i32>OUTPUT; }

export function abort(message: usize, fileName: usize, line: u32, column: u32): void {
  unreachable();
}

export class Emission {
  constructor(public key: JSON.Value, public value: JSON.Value) {}
}

/** Return fail(...) for a recoverable validation error; throwing traps Wasm. */
export abstract class View {
  error: string = "";
  constructor(
    public name: string,
    public mergeable: bool = false,
    public version: string = "1",
    public stateVersion: string = "1",
  ) {}
  abstract map(document: JSON.Value): Emission[];
  abstract identity(): JSON.Value;
  abstract add(state: JSON.Value, values: JSON.Arr): JSON.Value;
  abstract finalize(state: JSON.Value): JSON.Value;
  merge(left: JSON.Value, right: JSON.Value): JSON.Value {
    return this.fail("This view does not support merge");
  }
  fail(message: string): JSON.Value {
    this.error = message;
    return JSON.Value.empty();
  }
}

export function object(value: JSON.Value): JSON.Obj | null {
  return value.type == JSON.Types.Object ? value.get<JSON.Obj>() : null;
}

export function numeric(value: JSON.Value | null): bool {
  if (value == null) return false;
  return value.type >= JSON.Types.U8 && value.type <= JSON.Types.F64;
}

export function number(value: JSON.Value): f64 {
  switch (value.type) {
    case JSON.Types.U8: return <f64>value.get<u8>();
    case JSON.Types.U16: return <f64>value.get<u16>();
    case JSON.Types.U32: return <f64>value.get<u32>();
    case JSON.Types.U64: return <f64>value.get<u64>();
    case JSON.Types.I8: return <f64>value.get<i8>();
    case JSON.Types.I16: return <f64>value.get<i16>();
    case JSON.Types.I32: return <f64>value.get<i32>();
    case JSON.Types.I64: return <f64>value.get<i64>();
    case JSON.Types.F32: return <f64>value.get<f32>();
    default: return value.get<f64>();
  }
}

// Reject malformed UTF-8 instead of silently replacing invalid input bytes.
function validUTF8(length: i32): bool {
  for (let i = 0; i < length;) {
    const lead = load<u8>(INPUT + i++);
    if (lead < 0x80) continue;
    let count = 0;
    let code: u32 = 0;
    let minimum: u32 = 0;
    if (lead >= 0xc2 && lead <= 0xdf) {
      count = 1; code = lead & 0x1f; minimum = 0x80;
    } else if (lead >= 0xe0 && lead <= 0xef) {
      count = 2; code = lead & 0x0f; minimum = 0x800;
    } else if (lead >= 0xf0 && lead <= 0xf4) {
      count = 3; code = lead & 7; minimum = 0x10000;
    } else return false;
    if (i + count > length) return false;
    for (let j = 0; j < count; j++) {
      const next = load<u8>(INPUT + i++);
      if ((next & 0xc0) != 0x80) return false;
      code = (code << 6) | (next & 0x3f);
    }
    if (code < minimum || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) return false;
  }
  return true;
}

/** Restrict ABI values to finite, ordinary JSON with bounded nesting. */
function finiteJSON(value: JSON.Value, depth: i32 = 0): bool {
  if (depth > 64) return false;
  if (numeric(value)) return isFinite(number(value));
  if (value.type == JSON.Types.Object) {
    const obj = value.get<JSON.Obj>();
    const keys = obj.keys();
    for (let i = 0; i < keys.length; i++) {
      if (!finiteJSON(obj.get(keys[i])!, depth + 1)) return false;
    }
    return true;
  }
  if (value.type == JSON.Types.Array) {
    const arr = value.get<JSON.Arr>();
    for (let i = 0; i < arr.length; i++) {
      if (!finiteJSON(arr.at(i), depth + 1)) return false;
    }
    return true;
  }
  return value.type == JSON.Types.Null || value.type == JSON.Types.String || value.type == JSON.Types.Bool;
}

function write(text: string): i32 {
  const length = String.UTF8.byteLength(text);
  if (length > CAPACITY) return write('{"error":"Response exceeds ABI byte capacity"}');
  String.UTF8.encodeUnsafe(changetype<usize>(text), text.length, OUTPUT);
  return <i32>length;
}

function failure(message: string): i32 {
  return write('{"error":' + JSON.stringify<string>(message) + '}');
}

export function dispatch(view: View, length: i32): i32 {
  // The previous call's temporary objects are unreachable here. This explicit
  // collection plus the incremental runtime bounds memory across long sessions.
  __collect();
  view.error = "";
  if (length < 0 || length > CAPACITY) return failure("Invalid request byte length");
  if (!validUTF8(length)) return failure("Request is not valid UTF-8");
  const text = String.UTF8.decodeUnsafe(INPUT, <usize>length);
  if (!validateJSON(text)) return failure("Request is not valid JSON");
  const parser = new Parser(text);
  const requestValue = parser.value();
  if (parser.error.length > 0) return failure(parser.error);
  if (!finiteJSON(requestValue)) return failure("Request must contain finite JSON within 64 nesting levels");
  const request = object(requestValue);
  if (request == null) return failure("Request must be an object");
  const operation = request.get("op");
  if (operation == null || operation.type != JSON.Types.String) return failure("Request requires a string op");
  const op = operation.get<string>();
  let result = JSON.Value.empty();
  if (op == "descriptor") {
    const descriptor = new JSON.Obj();
    descriptor.set<i32>("abi", 1);
    descriptor.set<string>("name", view.name);
    descriptor.set<string>("version", view.version);
    descriptor.set<string>("state_version", view.stateVersion);
    descriptor.set<bool>("mergeable", view.mergeable);
    result = JSON.Value.from(descriptor);
  } else if (op == "map") {
    const documents = request.get("documents");
    if (documents == null || documents.type != JSON.Types.Array) return failure("Map requires a documents array");
    const docs = documents.get<JSON.Arr>();
    const mapped = new JSON.Arr();
    for (let i = 0; i < docs.length; i++) {
      const emitted = view.map(docs.at(i));
      const row = new JSON.Arr();
      for (let j = 0; j < emitted.length; j++) {
        const contribution = new JSON.Obj();
        contribution.set<JSON.Value>("key", emitted[j].key);
        contribution.set<JSON.Value>("value", emitted[j].value);
        row.push(contribution);
      }
      mapped.push(row);
      if (view.error.length > 0) return failure(view.error);
    }
    result = JSON.Value.from(mapped);
  } else if (op == "identity") {
    result = view.identity();
  } else if (op == "add") {
    const state = request.get("state");
    const values = request.get("values");
    if (state == null || values == null || values.type != JSON.Types.Array) return failure("Add requires state and a values array");
    result = view.add(state, values.get<JSON.Arr>());
  } else if (op == "finalize") {
    const state = request.get("state");
    if (state == null) return failure("Finalize requires state");
    result = view.finalize(state);
  } else if (op == "merge") {
    const left = request.get("left");
    const right = request.get("right");
    if (left == null || right == null) return failure("Merge requires left and right states");
    result = view.merge(left, right);
  } else return failure("Unsupported operation: " + op);
  if (view.error.length > 0) return failure(view.error);
  if (!finiteJSON(result)) return failure("View returned invalid or nonfinite JSON");
  return write('{"ok":' + JSON.stringify<JSON.Value>(result) + '}');
}

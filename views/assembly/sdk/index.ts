import { JSON } from "json-as";
import { Parser } from "./parse";

export { JSON };

// The host only exchanges UTF-8 bytes. Reducer authors never handle pointers.
const CAPACITY: i32 = 262144;
const INPUT: usize = memory.data(CAPACITY);
const OUTPUT: usize = memory.data(CAPACITY);
let hasRun: bool = false;

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
    public reduceGroups: bool = false,
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

/** Check per-datum nesting depth for parsed inputs that exceeded 64 transport levels. */
function withinDepth(value: JSON.Value, depth: i32 = 0): bool {
  if (depth > 64) return false;
  if (value.type == JSON.Types.Object) {
    const vals = value.get<JSON.Obj>().values();
    for (let i = 0; i < vals.length; i++) {
      if (!withinDepth(vals[i], depth + 1)) return false;
    }
    return true;
  }
  if (value.type == JSON.Types.Array) {
    const arr = value.get<JSON.Arr>();
    for (let i = 0; i < arr.length; i++) {
      if (!withinDepth(arr.at(i), depth + 1)) return false;
    }
    return true;
  }
  return true;
}

/** Restrict ABI output values to finite, ordinary JSON with bounded nesting. */
function finiteJSON(value: JSON.Value, depth: i32 = 0): bool {
  if (depth > 64) return false;
  if (numeric(value)) return isFinite(number(value));
  if (value.type == JSON.Types.Object) {
    const vals = value.get<JSON.Obj>().values();
    for (let i = 0; i < vals.length; i++) {
      if (!finiteJSON(vals[i], depth + 1)) return false;
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

function writeAscii(offset: i32, text: string): i32 {
  const ptr = changetype<usize>(text);
  for (let i = 0; i < text.length; i++) {
    store<u8>(OUTPUT + offset + i, <u8>load<u16>(ptr + ((<usize>i) << 1)));
  }
  return offset + text.length;
}

function writeUTF8(offset: i32, text: string, byteLength: i32): i32 {
  if (text.length > 0) {
    String.UTF8.encodeUnsafe(changetype<usize>(text), text.length, OUTPUT + offset);
  }
  return offset + byteLength;
}

function write(text: string): i32 {
  const length = String.UTF8.byteLength(text);
  if (length > CAPACITY) return write('{"error":"Response exceeds ABI byte capacity"}');
  String.UTF8.encodeUnsafe(changetype<usize>(text), text.length, OUTPUT);
  return <i32>length;
}

function writeOk(body: string): i32 {
  const bodyBytes = <i32>String.UTF8.byteLength(body);
  if (bodyBytes + 7 > CAPACITY) return write('{"error":"Response exceeds ABI byte capacity"}');
  let offset = writeAscii(0, '{"ok":');
  offset = writeUTF8(offset, body, bodyBytes);
  store<u8>(OUTPUT + offset++, 125);
  return offset;
}

function failure(message: string): i32 {
  return write('{"error":' + JSON.stringify<string>(message) + '}');
}

export function dispatch(view: View, length: i32): i32 {
  if (hasRun) {
    // The previous call's temporary objects are unreachable here. This explicit
    // collection plus the incremental runtime bounds memory across long sessions.
    __collect();
  } else {
    hasRun = true;
  }
  view.error = "";
  if (length < 0 || length > CAPACITY) return failure("Invalid request byte length");
  if (!validUTF8(length)) return failure("Request is not valid UTF-8");
  const text = String.UTF8.decodeUnsafe(INPUT, <usize>length);
  const parser = new Parser(text);
  const requestValue = parser.value();
  if (parser.error.length > 0) return failure(parser.error);
  parser.whitespace();
  if (parser.cursor != text.length) return failure("Request is not valid JSON");
  const request = object(requestValue);
  if (request == null) return failure("Request must be an object");
  const operation = request.get("op");
  if (operation == null || operation.type != JSON.Types.String) return failure("Request requires a string op");
  const op = operation.get<string>();
  // Validate each explicit datum at depth zero only when transport nesting exceeded 64.
  if (parser.maxDepth > 64) {
    const fields = request.keys();
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      const child = request.get(field)!;
      if (field == "groups" && op == "reduce_groups" && child.type == JSON.Types.Array) {
        const groups = child.get<JSON.Arr>();
        for (let j = 0; j < groups.length; j++) {
          const entry = groups.at(j);
          const groupObj = object(entry);
          if (groupObj == null) {
            if (!withinDepth(entry)) return failure("Request data exceeds 64 nesting levels");
            continue;
          }
          const gFields = groupObj.keys();
          for (let k = 0; k < gFields.length; k++) {
            const gField = gFields[k];
            const gChild = groupObj.get(gField)!;
            if (gField == "values" && gChild.type == JSON.Types.Array) {
              const vals = gChild.get<JSON.Arr>();
              for (let m = 0; m < vals.length; m++) {
                if (!withinDepth(vals.at(m))) return failure("Request data exceeds 64 nesting levels");
              }
            } else if (!withinDepth(gChild)) {
              return failure("Request data exceeds 64 nesting levels");
            }
          }
        }
      } else if ((field == "documents" || field == "values") && child.type == JSON.Types.Array) {
        const elements = child.get<JSON.Arr>();
        for (let j = 0; j < elements.length; j++) {
          if (!withinDepth(elements.at(j))) return failure("Request data exceeds 64 nesting levels");
        }
      } else if (!withinDepth(child)) {
        return failure("Request data exceeds 64 nesting levels");
      }
    }
  }
  let result = JSON.Value.empty();
  if (op == "descriptor") {
    const descriptor = new JSON.Obj();
    descriptor.set<i32>("abi", 1);
    descriptor.set<string>("name", view.name);
    descriptor.set<string>("version", view.version);
    descriptor.set<string>("state_version", view.stateVersion);
    descriptor.set<bool>("mergeable", view.mergeable);
    if (view.reduceGroups) descriptor.set<bool>("reduce_groups", true);
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
  } else if (op == "reduce_groups") {
    if (!view.reduceGroups) return failure("Unsupported operation: " + op);
    const groupsVal = request.get("groups");
    if (groupsVal == null || groupsVal.type != JSON.Types.Array) {
      return failure("reduce_groups requires a groups array");
    }
    const groups = groupsVal.get<JSON.Arr>();
    let offset = writeAscii(0, '{"ok":{"groups":[');
    const limit = CAPACITY - 3; // Reserve 3 bytes for ']}}'
    for (let i = 0; i < groups.length; i++) {
      const groupObj = object(groups.at(i));
      if (groupObj == null) return failure("Each reduce_groups entry must be an object");
      const valuesVal = groupObj.get("values");
      if (valuesVal == null || valuesVal.type != JSON.Types.Array) {
        return failure("Each reduce_groups entry requires a values array");
      }
      const existingState = groupObj.get("state");
      const isNew = existingState == null;
      let identityStr = "";
      let initialState: JSON.Value;
      if (isNew) {
        const identityVal = view.identity();
        if (view.error.length > 0) return failure(view.error);
        if (!finiteJSON(identityVal)) return failure("View returned invalid or nonfinite JSON");
        // Serialize before add() in case add() mutates state in place.
        identityStr = JSON.stringify<JSON.Value>(identityVal);
        initialState = identityVal;
      } else {
        initialState = existingState!;
      }
      const nextState = view.add(initialState, valuesVal.get<JSON.Arr>());
      if (view.error.length > 0) return failure(view.error);
      if (!finiteJSON(nextState)) return failure("View returned invalid or nonfinite JSON");
      // Serialize before finalize() so mutating finalizers cannot corrupt retained state.
      const stateStr = JSON.stringify<JSON.Value>(nextState);
      const resultVal = view.finalize(nextState);
      if (view.error.length > 0) return failure(view.error);
      if (!finiteJSON(resultVal)) return failure("View returned invalid or nonfinite JSON");
      const resultStr = JSON.stringify<JSON.Value>(resultVal);

      const commaBytes: i32 = i > 0 ? 1 : 0;
      const stateBytes = <i32>String.UTF8.byteLength(stateStr);
      const resultBytes = <i32>String.UTF8.byteLength(resultStr);
      if (isNew) {
        const identityBytes = <i32>String.UTF8.byteLength(identityStr);
        // '{"identity":' (12) + ',"state":' (9) + ',"result":' (10) + '}' (1) = 32
        const needed = commaBytes + 32 + identityBytes + stateBytes + resultBytes;
        if (offset + needed > limit) return write('{"ok":{"split":true}}');
        if (i > 0) store<u8>(OUTPUT + offset++, 44);
        offset = writeAscii(offset, '{"identity":');
        offset = writeUTF8(offset, identityStr, identityBytes);
        offset = writeAscii(offset, ',"state":');
        offset = writeUTF8(offset, stateStr, stateBytes);
        offset = writeAscii(offset, ',"result":');
        offset = writeUTF8(offset, resultStr, resultBytes);
        store<u8>(OUTPUT + offset++, 125);
      } else {
        // '{"state":' (9) + ',"result":' (10) + '}' (1) = 20
        const needed = commaBytes + 20 + stateBytes + resultBytes;
        if (offset + needed > limit) return write('{"ok":{"split":true}}');
        if (i > 0) store<u8>(OUTPUT + offset++, 44);
        offset = writeAscii(offset, '{"state":');
        offset = writeUTF8(offset, stateStr, stateBytes);
        offset = writeAscii(offset, ',"result":');
        offset = writeUTF8(offset, resultStr, resultBytes);
        store<u8>(OUTPUT + offset++, 125);
      }
    }
    return writeAscii(offset, ']}}');
  } else return failure("Unsupported operation: " + op);
  if (view.error.length > 0) return failure(view.error);
  if (!finiteJSON(result)) return failure("View returned invalid or nonfinite JSON");
  return writeOk(JSON.stringify<JSON.Value>(result));
}

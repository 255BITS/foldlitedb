// A generated key must pass the same SDK parser boundary before fused add as
// before a legacy add request, even when add discards the identity entirely.
import {JSON, View, Emission, dispatch} from "../sdk";
export {abi_version, input_ptr, input_capacity, output_ptr} from "../sdk";
class LongIdentity extends View {
  constructor() { super("group-long-identity", false, "1", "1", true); }
  map(document: JSON.Value): Emission[] { return []; }
  identity(): JSON.Value {
    const out = new JSON.Obj();
    out.set<string>("x".repeat(65536), "value");
    return JSON.Value.from(out);
  }
  add(state: JSON.Value, values: JSON.Arr): JSON.Value { return JSON.Value.empty(); }
  finalize(state: JSON.Value): JSON.Value { return JSON.Value.empty(); }
}
const view = new LongIdentity();
export function execute(length: i32): i32 { return dispatch(view,length); }

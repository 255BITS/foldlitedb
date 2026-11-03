// Internal document-only view used when Python supplies the derived projection.
// Arbitrary payloads must not accidentally execute the bundled metrics reducer.
import { JSON, View, Emission, dispatch } from "../sdk";
export { abi_version, input_ptr, input_capacity, output_ptr } from "../sdk";

class Documents extends View {
  constructor() { super("documents", false); }
  map(document: JSON.Value): Emission[] { return []; }
  identity(): JSON.Value { return JSON.Value.empty(); }
  add(state: JSON.Value, values: JSON.Arr): JSON.Value { return state; }
  finalize(state: JSON.Value): JSON.Value { return state; }
}

const view = new Documents();
export function execute(length: i32): i32 { return dispatch(view, length); }

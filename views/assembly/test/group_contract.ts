// Regression fixture: an inexact integer identity is deliberately discarded by
// add, so the host must still validate the intermediate identity response.
import {JSON, View, Emission, object, dispatch} from "../sdk";
export {abi_version, input_ptr, input_capacity, output_ptr} from "../sdk";
class GroupContract extends View {
  constructor() { super("group-contract", false, "1", "1", true); }
  map(document: JSON.Value): Emission[] {
    const doc = object(document);
    if (doc == null) return [];
    const key = doc.get("key"), value = doc.get("value");
    return key == null || value == null ? [] : [new Emission(key, value)];
  }
  identity(): JSON.Value { return JSON.Value.from<u64>(9007199254740993); }
  add(state: JSON.Value, values: JSON.Arr): JSON.Value {
    if (values.length && values.at(0).type == JSON.Types.String) {
      const operation = values.at(0).get<string>();
      if (operation == "discard") return JSON.Value.empty();
      if (operation == "inexact-state") return JSON.Value.from<u64>(9007199254740993);
      if (operation == "long-key") {
        const out = new JSON.Obj();
        out.set<string>("x".repeat(65536), "value");
        return JSON.Value.from(out);
      }
    }
    return state;
  }
  finalize(state: JSON.Value): JSON.Value {
    return state.type >= JSON.Types.U8 && state.type <= JSON.Types.F64 ? JSON.Value.empty() : state;
  }
}
const view = new GroupContract();
export function execute(length: i32): i32 { return dispatch(view, length); }

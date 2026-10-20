import { JSON, View, Emission, object, dispatch } from "../sdk";
export { abi_version, input_ptr, input_capacity, output_ptr } from "../sdk";

/** Ordered latest-event fold: {category,id,title,deleted?} updates a catalogue. */
class Catalogue extends View {
  constructor() { super("catalogue", false); }

  map(document: JSON.Value): Emission[] {
    const doc = object(document);
    if (doc == null) return [];
    const category = doc.get("category");
    const id = doc.get("id");
    if (category == null || category.type != JSON.Types.String || id == null || id.type != JSON.Types.String) return [];
    if (id.get<string>().length > 65535) return [];
    const deleted = doc.get("deleted");
    if (deleted != null && deleted.type != JSON.Types.Bool) return [];
    const isDeleted = deleted != null && deleted.get<bool>();
    const title = doc.get("title");
    if (!isDeleted && (title == null || title.type != JSON.Types.String)) return [];
    const contribution = new JSON.Obj();
    contribution.set<JSON.Value>("id", id);
    contribution.set<JSON.Value>("title", isDeleted ? JSON.Value.empty() : title!);
    contribution.set<bool>("deleted", isDeleted);
    return [new Emission(category, JSON.Value.from(contribution))];
  }

  identity(): JSON.Value { return JSON.Value.from(new JSON.Obj()); }

  add(value: JSON.Value, values: JSON.Arr): JSON.Value {
    const items = object(value);
    if (items == null) return this.fail("Catalogue state must be an object");
    const keys = items.keys();
    for (let i = 0; i < keys.length; i++) {
      if (items.get(keys[i])!.type != JSON.Types.String) return this.fail("Catalogue state entries must be strings");
    }
    for (let i = 0; i < values.length; i++) {
      const event = object(values.at(i));
      if (event == null) return this.fail("Catalogue contributions must be objects");
      const id = event.get("id");
      const title = event.get("title");
      const deleted = event.get("deleted");
      if (id == null || id.type != JSON.Types.String || deleted == null || deleted.type != JSON.Types.Bool) return this.fail("Invalid catalogue contribution");
      const name = id.get<string>();
      if (deleted.get<bool>()) {
        items.delete(name);
        continue;
      }
      if (title == null || title.type != JSON.Types.String) return this.fail("Catalogue titles must be strings");
      items.set<JSON.Value>(name, title);
    }
    return value;
  }

  finalize(value: JSON.Value): JSON.Value {
    const items = object(value);
    if (items == null) return this.fail("Catalogue state must be an object");
    const keys = items.keys();
    for (let i = 0; i < keys.length; i++) {
      if (items.get(keys[i])!.type != JSON.Types.String) return this.fail("Catalogue state entries must be strings");
    }
    const result = new JSON.Obj();
    result.set<i32>("count", items.size);
    result.set<JSON.Value>("items", value);
    return JSON.Value.from(result);
  }
}

const view = new Catalogue();
export function execute(length: i32): i32 { return dispatch(view, length); }

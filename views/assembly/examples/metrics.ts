import { JSON, View, Emission, object, numeric, number, dispatch } from "../sdk";
export { abi_version, input_ptr, input_capacity, output_ptr } from "../sdk";

function state(count: f64, sum: f64, minimum: JSON.Value, maximum: JSON.Value): JSON.Value {
  const out = new JSON.Obj();
  out.set<f64>("count", count);
  out.set<f64>("sum", sum);
  out.set<JSON.Value>("min", minimum);
  out.set<JSON.Value>("max", maximum);
  return JSON.Value.from(out);
}

class Metrics extends View {
  constructor() { super("metrics", true); }

  map(document: JSON.Value): Emission[] {
    const doc = object(document);
    if (doc == null) return [];
    const metric = doc.get("metric");
    const value = doc.get("value");
    if (metric == null || metric.type != JSON.Types.String || !numeric(value)) return [];
    return [new Emission(metric, value!)];
  }

  identity(): JSON.Value { return state(0, 0, JSON.Value.empty(), JSON.Value.empty()); }

  valid(value: JSON.Value): bool {
    const obj = object(value);
    if (obj == null || !numeric(obj.get("count")) || !numeric(obj.get("sum"))) return false;
    const count = number(obj.get("count")!);
    const minimum = obj.get("min");
    const maximum = obj.get("max");
    if (minimum == null || maximum == null || count < 0 || count != Math.floor(count)) return false;
    if (count == 0) return number(obj.get("sum")!) == 0 && minimum.type == JSON.Types.Null && maximum.type == JSON.Types.Null;
    return numeric(minimum) && numeric(maximum) && number(minimum) <= number(maximum);
  }

  add(value: JSON.Value, values: JSON.Arr): JSON.Value {
    if (!this.valid(value)) return this.fail("Invalid metrics state");
    const obj = value.get<JSON.Obj>();
    let count = number(obj.get("count")!);
    let sum = number(obj.get("sum")!);
    let minimum = count > 0 ? number(obj.get("min")!) : Infinity;
    let maximum = count > 0 ? number(obj.get("max")!) : -Infinity;
    for (let i = 0; i < values.length; i++) {
      const contribution = values.at(i);
      if (!numeric(contribution)) return this.fail("Metrics contributions must be numbers");
      const n = number(contribution);
      count += 1;
      sum += n;
      minimum = Math.min(minimum, n);
      maximum = Math.max(maximum, n);
    }
    if (!isFinite(sum)) return this.fail("Metrics sum overflowed");
    return count == 0 ? this.identity() : state(count, sum, JSON.Value.from(minimum), JSON.Value.from(maximum));
  }

  finalize(value: JSON.Value): JSON.Value {
    if (!this.valid(value)) return this.fail("Invalid metrics state");
    const obj = value.get<JSON.Obj>();
    const count = number(obj.get("count")!);
    obj.set<JSON.Value>("mean", count == 0 ? JSON.Value.empty() : JSON.Value.from(number(obj.get("sum")!) / count));
    return value;
  }

  merge(left: JSON.Value, right: JSON.Value): JSON.Value {
    if (!this.valid(left) || !this.valid(right)) return this.fail("Invalid metrics state");
    const l = left.get<JSON.Obj>();
    const r = right.get<JSON.Obj>();
    const lc = number(l.get("count")!);
    const rc = number(r.get("count")!);
    if (lc == 0) return right;
    if (rc == 0) return left;
    const sum = number(l.get("sum")!) + number(r.get("sum")!);
    if (!isFinite(sum)) return this.fail("Metrics sum overflowed");
    return state(lc + rc, sum,
      JSON.Value.from(Math.min(number(l.get("min")!), number(r.get("min")!))),
      JSON.Value.from(Math.max(number(l.get("max")!), number(r.get("max")!))));
  }
}

const view = new Metrics();
export function execute(length: i32): i32 { return dispatch(view, length); }

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
  constructor() { super("metrics", true, "1", "1", true); }

  map(document: JSON.Value): Emission[] {
    const doc = object(document);
    if (doc == null) return [];
    const metric = doc.get("metric");
    const value = doc.get("value");
    if (metric == null || metric.type != JSON.Types.String || !numeric(value)) return [];
    return [new Emission(metric, value!)];
  }

  identity(): JSON.Value { return state(0, 0, JSON.Value.empty(), JSON.Value.empty()); }

  add(value: JSON.Value, values: JSON.Arr): JSON.Value {
    const obj = object(value);
    if (obj == null) return this.fail("Invalid metrics state");
    const countVal = obj.get("count");
    const sumVal = obj.get("sum");
    const minVal = obj.get("min");
    const maxVal = obj.get("max");
    if (!numeric(countVal) || !numeric(sumVal) || minVal == null || maxVal == null) {
      return this.fail("Invalid metrics state");
    }
    let count = number(countVal!);
    let sum = number(sumVal!);
    if (count < 0 || count != Math.floor(count)) return this.fail("Invalid metrics state");
    let minimum: f64;
    let maximum: f64;
    if (count == 0) {
      if (sum != 0 || minVal.type != JSON.Types.Null || maxVal.type != JSON.Types.Null) {
        return this.fail("Invalid metrics state");
      }
      minimum = Infinity;
      maximum = -Infinity;
    } else {
      if (!numeric(minVal) || !numeric(maxVal)) return this.fail("Invalid metrics state");
      minimum = number(minVal);
      maximum = number(maxVal);
      if (minimum > maximum) return this.fail("Invalid metrics state");
    }
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
    const obj = object(value);
    if (obj == null) return this.fail("Invalid metrics state");
    const countVal = obj.get("count");
    const sumVal = obj.get("sum");
    const minVal = obj.get("min");
    const maxVal = obj.get("max");
    if (!numeric(countVal) || !numeric(sumVal) || minVal == null || maxVal == null) {
      return this.fail("Invalid metrics state");
    }
    const count = number(countVal!);
    const sum = number(sumVal!);
    if (count < 0 || count != Math.floor(count)) return this.fail("Invalid metrics state");
    const out = new JSON.Obj();
    out.set<f64>("count", count);
    out.set<f64>("sum", sum);
    if (count == 0) {
      if (sum != 0 || minVal.type != JSON.Types.Null || maxVal.type != JSON.Types.Null) {
        return this.fail("Invalid metrics state");
      }
      out.set<JSON.Value>("min", JSON.Value.empty());
      out.set<JSON.Value>("max", JSON.Value.empty());
      out.set<JSON.Value>("mean", JSON.Value.empty());
    } else {
      if (!numeric(minVal) || !numeric(maxVal)) return this.fail("Invalid metrics state");
      const minimum = number(minVal);
      const maximum = number(maxVal);
      if (minimum > maximum) return this.fail("Invalid metrics state");
      out.set<f64>("min", minimum);
      out.set<f64>("max", maximum);
      out.set<f64>("mean", sum / count);
    }
    return JSON.Value.from(out);
  }

  merge(left: JSON.Value, right: JSON.Value): JSON.Value {
    const l = object(left);
    const r = object(right);
    if (l == null || r == null) return this.fail("Invalid metrics state");
    const lCountVal = l.get("count");
    const lSumVal = l.get("sum");
    const lMinVal = l.get("min");
    const lMaxVal = l.get("max");
    const rCountVal = r.get("count");
    const rSumVal = r.get("sum");
    const rMinVal = r.get("min");
    const rMaxVal = r.get("max");
    if (
      !numeric(lCountVal) || !numeric(lSumVal) || lMinVal == null || lMaxVal == null ||
      !numeric(rCountVal) || !numeric(rSumVal) || rMinVal == null || rMaxVal == null
    ) {
      return this.fail("Invalid metrics state");
    }
    const lc = number(lCountVal!);
    const ls = number(lSumVal!);
    const rc = number(rCountVal!);
    const rs = number(rSumVal!);
    if (lc < 0 || lc != Math.floor(lc) || rc < 0 || rc != Math.floor(rc)) {
      return this.fail("Invalid metrics state");
    }
    let lMin: f64 = 0;
    let lMax: f64 = 0;
    if (lc == 0) {
      if (ls != 0 || lMinVal.type != JSON.Types.Null || lMaxVal.type != JSON.Types.Null) {
        return this.fail("Invalid metrics state");
      }
    } else {
      if (!numeric(lMinVal) || !numeric(lMaxVal)) return this.fail("Invalid metrics state");
      lMin = number(lMinVal);
      lMax = number(lMaxVal);
      if (lMin > lMax) return this.fail("Invalid metrics state");
    }
    let rMin: f64 = 0;
    let rMax: f64 = 0;
    if (rc == 0) {
      if (rs != 0 || rMinVal.type != JSON.Types.Null || rMaxVal.type != JSON.Types.Null) {
        return this.fail("Invalid metrics state");
      }
    } else {
      if (!numeric(rMinVal) || !numeric(rMaxVal)) return this.fail("Invalid metrics state");
      rMin = number(rMinVal);
      rMax = number(rMaxVal);
      if (rMin > rMax) return this.fail("Invalid metrics state");
    }
    if (lc == 0) return right;
    if (rc == 0) return left;
    const sum = ls + rs;
    if (!isFinite(sum)) return this.fail("Metrics sum overflowed");
    return state(
      lc + rc,
      sum,
      JSON.Value.from(Math.min(lMin, rMin)),
      JSON.Value.from(Math.max(lMax, rMax)),
    );
  }
}

const view = new Metrics();
export function execute(length: i32): i32 { return dispatch(view, length); }

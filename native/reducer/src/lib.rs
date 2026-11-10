//! Deterministic, import-free WebAssembly numeric and envelope reducers (ABI v1).
//!
//! Exports the fixed Foldlite reducer ABI (`abi_version`, `input_ptr`,
//! `input_capacity`, `output_ptr`, `execute`) for native `wasmi` and browser
//! Web Worker hosts.

use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub const ABI_VERSION: i32 = 1;
pub const INPUT_CAPACITY: usize = 262_144;
pub const OUTPUT_CAPACITY: usize = 65_536;
pub const MAX_BATCH_VALUES: usize = 1_024;
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
pub const MAX_POSITION_ID_BYTES: usize = 128;
const MAX_JSON_DEPTH: usize = 64;

struct IoBuffer<const N: usize>(UnsafeCell<[u8; N]>);

// Wasm execution is strictly single-threaded per instance.
unsafe impl<const N: usize> Sync for IoBuffer<N> {}

static INPUT_BUFFER: IoBuffer<INPUT_CAPACITY> = IoBuffer(UnsafeCell::new([0; INPUT_CAPACITY]));
static OUTPUT_BUFFER: IoBuffer<OUTPUT_CAPACITY> = IoBuffer(UnsafeCell::new([0; OUTPUT_CAPACITY]));

#[no_mangle]
pub extern "C" fn abi_version() -> i32 {
    ABI_VERSION
}

#[no_mangle]
pub extern "C" fn input_ptr() -> i32 {
    INPUT_BUFFER.0.get() as usize as i32
}

#[no_mangle]
pub extern "C" fn input_capacity() -> i32 {
    INPUT_CAPACITY as i32
}

#[no_mangle]
pub extern "C" fn output_ptr() -> i32 {
    OUTPUT_BUFFER.0.get() as usize as i32
}

#[no_mangle]
pub extern "C" fn execute(input_len: i32) -> i32 {
    let response = if input_len < 0 || (input_len as usize) > INPUT_CAPACITY {
        error_bytes("request length out of bounds")
    } else {
        let input = unsafe { &(&*INPUT_BUFFER.0.get())[..input_len as usize] };
        execute_bytes(input)
    };
    let encoded = if response.len() <= OUTPUT_CAPACITY {
        response
    } else {
        error_bytes("reducer response exceeds output buffer capacity")
    };
    unsafe {
        let out = &mut *OUTPUT_BUFFER.0.get();
        out[..encoded.len()].copy_from_slice(&encoded);
    }
    encoded.len() as i32
}

pub fn execute_bytes(input: &[u8]) -> Vec<u8> {
    match handle_request(input) {
        Ok(value) => serde_json::to_vec(&json!({ "ok": value }))
            .unwrap_or_else(|_| error_bytes("failed to encode reducer response")),
        Err(message) => error_bytes(&message),
    }
}

fn error_bytes(message: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "error": message }))
        .unwrap_or_else(|_| br#"{"error":"internal reducer error"}"#.to_vec())
}

fn check_json_depth(input: &[u8], max_depth: usize) -> Result<(), String> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in input {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                if depth > max_depth {
                    return Err(format!("JSON nesting exceeds maximum depth of {max_depth}"));
                }
            }
            b']' | b'}' => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequest<'a> {
    #[serde(borrow)]
    op: Cow<'a, str>,
    #[serde(borrow, default)]
    reducer: Option<Cow<'a, str>>,
    #[serde(default)]
    state: Option<Value>,
    #[serde(borrow, default)]
    values: Option<Vec<RawContribution<'a>>>,
    #[serde(default)]
    left: Option<Value>,
    #[serde(default)]
    right: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawContribution<'a> {
    #[serde(borrow)]
    position: (u64, Cow<'a, str>),
    #[serde(default)]
    value: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScalarKind {
    Mean,
    Sum,
}

impl ScalarKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mean => "mean/v1",
            Self::Sum => "sum/v1",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Position {
    step: u64,
    id: String,
}

fn validate_position(step: u64, id: &str) -> Result<(), String> {
    if step > MAX_SAFE_INTEGER {
        return Err("position step exceeds safe integer bound".into());
    }
    if id.is_empty() || id.len() > MAX_POSITION_ID_BYTES || !id.is_ascii() {
        return Err("position id must be 1..=128 ASCII bytes".into());
    }
    Ok(())
}

impl Position {
    fn parse(step: u64, id: String) -> Result<Self, String> {
        validate_position(step, &id)?;
        Ok(Self { step, id })
    }

    fn as_key(&self) -> (u64, &str) {
        (self.step, self.id.as_str())
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Point {
    position: Position,
    value: f64,
}

#[derive(Serialize)]
struct SerializedPoint<'a> {
    position: (&'a u64, &'a str),
    value: f64,
}

impl Point {
    fn as_serialized(&self) -> SerializedPoint<'_> {
        SerializedPoint {
            position: (&self.position.step, &self.position.id),
            value: canonical_f64(self.value),
        }
    }
}

#[derive(Clone, Debug)]
enum ReducerState {
    Scalar {
        kind: ScalarKind,
        count: u64,
        sum: f64,
    },
    Count {
        count: u64,
    },
    Envelope {
        count: u64,
        extrema: Option<EnvelopeExtrema>,
    },
}

#[derive(Clone, Debug)]
struct EnvelopeExtrema {
    first: Point,
    last: Point,
    min: Point,
    max: Point,
}

fn canonical_f64(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn prefer_min(candidate: &Point, current: &Point) -> bool {
    match candidate.value.total_cmp(&current.value) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => candidate.position < current.position,
    }
}

fn prefer_max(candidate: &Point, current: &Point) -> bool {
    match candidate.value.total_cmp(&current.value) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => candidate.position < current.position,
    }
}

impl EnvelopeExtrema {
    fn points(&self) -> [&Point; 4] {
        [&self.first, &self.last, &self.min, &self.max]
    }

    fn validate(&self, count: u64) -> Result<(), String> {
        let pts = self.points();
        for i in 0..pts.len() {
            for j in (i + 1)..pts.len() {
                if pts[i].position == pts[j].position && pts[i].value != pts[j].value {
                    return Err("conflicting values at one position".into());
                }
            }
        }
        let distinct: BTreeSet<(&u64, &str)> = pts
            .iter()
            .map(|p| (&p.position.step, p.position.id.as_str()))
            .collect();
        if (distinct.len() as u64) > count {
            return Err("envelope retains more positions than its count".into());
        }
        for point in pts {
            if self.first.position > point.position || point.position > self.last.position {
                return Err("envelope first/last positions are out of order".into());
            }
            if prefer_min(point, &self.min) {
                return Err("envelope min is inconsistent with retained extrema".into());
            }
            if prefer_max(point, &self.max) {
                return Err("envelope max is inconsistent with retained extrema".into());
            }
        }
        Ok(())
    }

    fn check_point_conflict_ref(&self, pos: (u64, &str), value: f64) -> Result<(), String> {
        for existing in self.points() {
            if existing.position.as_key() == pos && existing.value != value {
                return Err("conflicting values at one position".into());
            }
        }
        Ok(())
    }

    fn observe_ref(&mut self, step: u64, id: &str, value: f64) -> Result<(), String> {
        let key = (step, id);
        self.check_point_conflict_ref(key, value)?;
        if key < self.first.position.as_key() {
            self.first = Point {
                position: Position {
                    step,
                    id: id.to_owned(),
                },
                value,
            };
        }
        if key > self.last.position.as_key() {
            self.last = Point {
                position: Position {
                    step,
                    id: id.to_owned(),
                },
                value,
            };
        }
        let min_wins = match value.total_cmp(&self.min.value) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => key < self.min.position.as_key(),
        };
        if min_wins {
            self.min = Point {
                position: Position {
                    step,
                    id: id.to_owned(),
                },
                value,
            };
        }
        let max_wins = match value.total_cmp(&self.max.value) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => key < self.max.position.as_key(),
        };
        if max_wins {
            self.max = Point {
                position: Position {
                    step,
                    id: id.to_owned(),
                },
                value,
            };
        }
        Ok(())
    }

    fn merge(&self, other: &Self, total_count: u64) -> Result<Self, String> {
        for point in other.points() {
            self.check_point_conflict_ref(point.position.as_key(), point.value)?;
        }
        let mut merged = self.clone();
        for point in other.points() {
            merged.observe_ref(point.position.step, &point.position.id, point.value)?;
        }
        merged.validate(total_count)?;
        Ok(merged)
    }
}

impl ReducerState {
    fn identity(name: &str) -> Result<Self, String> {
        match name {
            "mean/v1" => Ok(Self::Scalar {
                kind: ScalarKind::Mean,
                count: 0,
                sum: 0.0,
            }),
            "sum/v1" => Ok(Self::Scalar {
                kind: ScalarKind::Sum,
                count: 0,
                sum: 0.0,
            }),
            "count/v1" => Ok(Self::Count { count: 0 }),
            "envelope/v1" => Ok(Self::Envelope {
                count: 0,
                extrema: None,
            }),
            _ => Err("unknown reducer; select mean/v1, sum/v1, count/v1 or envelope/v1".into()),
        }
    }

    fn parse(value: &Value) -> Result<Self, String> {
        let map = value
            .as_object()
            .ok_or_else(|| "reducer state must be a JSON object".to_string())?;
        let reducer = map
            .get("reducer")
            .and_then(Value::as_str)
            .ok_or_else(|| "state missing string `reducer`".to_string())?;
        let version = parse_safe_u64_field(map, "version")?;
        if version != 1 {
            return Err(format!("unsupported state version {version}"));
        }
        let count = parse_safe_u64_field(map, "count")?;

        match reducer {
            "count/v1" => {
                require_exact_keys(map, &["count", "reducer", "version"])?;
                Ok(Self::Count { count })
            }
            "mean/v1" | "sum/v1" => {
                require_exact_keys(map, &["count", "reducer", "sum", "version"])?;
                let sum = parse_finite_f64_field(map, "sum")?;
                if count == 0 && sum != 0.0 {
                    return Err("empty scalar state must have zero sum".into());
                }
                let kind = if reducer == "mean/v1" {
                    ScalarKind::Mean
                } else {
                    ScalarKind::Sum
                };
                Ok(Self::Scalar {
                    kind,
                    count,
                    sum: canonical_f64(sum),
                })
            }
            "envelope/v1" => {
                require_exact_keys(
                    map,
                    &["count", "first", "last", "max", "min", "reducer", "version"],
                )?;
                let fields = ["first", "last", "min", "max"];
                if count == 0 {
                    for field in fields {
                        if !matches!(map.get(field), Some(Value::Null)) {
                            return Err(format!("empty envelope `{field}` must be null"));
                        }
                    }
                    Ok(Self::Envelope {
                        count: 0,
                        extrema: None,
                    })
                } else {
                    let first = parse_point_field(map, "first")?;
                    let last = parse_point_field(map, "last")?;
                    let min = parse_point_field(map, "min")?;
                    let max = parse_point_field(map, "max")?;
                    let extrema = EnvelopeExtrema {
                        first,
                        last,
                        min,
                        max,
                    };
                    extrema.validate(count)?;
                    Ok(Self::Envelope {
                        count,
                        extrema: Some(extrema),
                    })
                }
            }
            _ => Err("unknown reducer; select mean/v1, sum/v1, count/v1 or envelope/v1".into()),
        }
    }

    fn to_json(&self) -> Value {
        match self {
            Self::Count { count } => json!({
                "count": count,
                "reducer": "count/v1",
                "version": 1,
            }),
            Self::Scalar { kind, count, sum } => json!({
                "count": count,
                "reducer": kind.as_str(),
                "sum": canonical_f64(*sum),
                "version": 1,
            }),
            Self::Envelope { count, extrema } => match extrema {
                None => json!({
                    "count": count,
                    "first": Value::Null,
                    "last": Value::Null,
                    "max": Value::Null,
                    "min": Value::Null,
                    "reducer": "envelope/v1",
                    "version": 1,
                }),
                Some(ext) => json!({
                    "count": count,
                    "first": ext.first.as_serialized(),
                    "last": ext.last.as_serialized(),
                    "max": ext.max.as_serialized(),
                    "min": ext.min.as_serialized(),
                    "reducer": "envelope/v1",
                    "version": 1,
                }),
            },
        }
    }

    fn add<'a>(mut self, contributions: Vec<RawContribution<'a>>) -> Result<Self, String> {
        if contributions.len() > MAX_BATCH_VALUES {
            return Err(format!("batch exceeds {MAX_BATCH_VALUES} contributions"));
        }
        let mut envelope_seen: Vec<(u64, &str, f64)> = match &self {
            Self::Envelope { .. } => Vec::with_capacity(contributions.len()),
            _ => Vec::new(),
        };
        for item in &contributions {
            let step = item.position.0;
            let id = item.position.1.as_ref();
            validate_position(step, id)?;
            let Some(raw_value) = item.value else {
                continue;
            };
            if !raw_value.is_finite() {
                return Err("contribution value must be finite".into());
            }
            let value = canonical_f64(raw_value);
            match &mut self {
                Self::Count { count } => {
                    *count = checked_increment(*count)?;
                }
                Self::Scalar { count, sum, .. } => {
                    *count = checked_increment(*count)?;
                    let next = *sum + value;
                    if !next.is_finite() {
                        return Err("nonfinite scalar sum".into());
                    }
                    *sum = canonical_f64(next);
                }
                Self::Envelope { count, extrema } => {
                    envelope_seen.push((step, id, value));
                    match extrema {
                        None => {
                            let point = Point {
                                position: Position {
                                    step,
                                    id: id.to_owned(),
                                },
                                value,
                            };
                            *extrema = Some(EnvelopeExtrema {
                                first: point.clone(),
                                last: point.clone(),
                                min: point.clone(),
                                max: point,
                            });
                        }
                        Some(ext) => ext.observe_ref(step, id, value)?,
                    }
                    *count = checked_increment(*count)?;
                }
            }
        }
        if !envelope_seen.is_empty() {
            envelope_seen.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
            for pair in envelope_seen.windows(2) {
                if pair[0].0 == pair[1].0 && pair[0].1 == pair[1].1 && pair[0].2 != pair[1].2 {
                    return Err("conflicting values at one position".into());
                }
            }
        }
        Ok(self)
    }

    fn merge(self, right: Self) -> Result<Self, String> {
        match (self, right) {
            (
                Self::Scalar {
                    kind: left_kind,
                    count: left_count,
                    sum: left_sum,
                },
                Self::Scalar {
                    kind: right_kind,
                    count: right_count,
                    sum: right_sum,
                },
            ) if left_kind == right_kind => {
                let count = checked_add_counts(left_count, right_count)?;
                let sum = left_sum + right_sum;
                if !sum.is_finite() {
                    return Err("nonfinite scalar sum".into());
                }
                Ok(Self::Scalar {
                    kind: left_kind,
                    count,
                    sum: canonical_f64(sum),
                })
            }
            (Self::Count { count: left_count }, Self::Count { count: right_count }) => {
                Ok(Self::Count {
                    count: checked_add_counts(left_count, right_count)?,
                })
            }
            (
                Self::Envelope {
                    count: left_count,
                    extrema: left_ext,
                },
                Self::Envelope {
                    count: right_count,
                    extrema: right_ext,
                },
            ) => {
                let count = checked_add_counts(left_count, right_count)?;
                let extrema = match (left_ext, right_ext) {
                    (None, None) => None,
                    (Some(ext), None) | (None, Some(ext)) => {
                        ext.validate(count)?;
                        Some(ext)
                    }
                    (Some(left), Some(right)) => Some(left.merge(&right, count)?),
                };
                Ok(Self::Envelope { count, extrema })
            }
            _ => Err("cannot merge states from different reducers".into()),
        }
    }

    fn finalize(self) -> Result<Value, String> {
        match self {
            Self::Scalar {
                kind: ScalarKind::Mean,
                count,
                sum,
            } => {
                if count == 0 {
                    Ok(json!({ "count": 0, "value": Value::Null }))
                } else {
                    let mean = sum / (count as f64);
                    if !mean.is_finite() {
                        return Err("nonfinite mean result".into());
                    }
                    Ok(json!({ "count": count, "value": canonical_f64(mean) }))
                }
            }
            Self::Scalar {
                kind: ScalarKind::Sum,
                count,
                sum,
            } => {
                if count == 0 {
                    Ok(json!({ "count": 0, "value": 0 }))
                } else {
                    Ok(json!({ "count": count, "value": canonical_f64(sum) }))
                }
            }
            Self::Count { count } => Ok(json!({ "count": count, "value": count })),
            Self::Envelope { count, extrema } => match extrema {
                None => Ok(json!({
                    "count": 0,
                    "first": Value::Null,
                    "last": Value::Null,
                    "max": Value::Null,
                    "min": Value::Null,
                })),
                Some(ext) => Ok(json!({
                    "count": count,
                    "first": ext.first.as_serialized(),
                    "last": ext.last.as_serialized(),
                    "max": ext.max.as_serialized(),
                    "min": ext.min.as_serialized(),
                })),
            },
        }
    }
}

fn checked_increment(count: u64) -> Result<u64, String> {
    checked_add_counts(count, 1)
}

fn checked_add_counts(left: u64, right: u64) -> Result<u64, String> {
    left.checked_add(right)
        .filter(|&sum| sum <= MAX_SAFE_INTEGER)
        .ok_or_else(|| "count exceeds safe integer bound".into())
}

fn require_exact_keys(map: &Map<String, Value>, expected: &[&str]) -> Result<(), String> {
    if map.len() != expected.len() || expected.iter().any(|key| !map.contains_key(*key)) {
        return Err("invalid state fields".into());
    }
    Ok(())
}

fn parse_safe_u64_field(map: &Map<String, Value>, key: &str) -> Result<u64, String> {
    let value = map
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("state field `{key}` must be a non-negative integer"))?;
    if value > MAX_SAFE_INTEGER {
        return Err(format!("{key} exceeds safe integer bound"));
    }
    Ok(value)
}

fn parse_finite_f64_field(map: &Map<String, Value>, key: &str) -> Result<f64, String> {
    let value = map
        .get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("state field `{key}` must be a finite number"))?;
    if !value.is_finite() {
        return Err(format!("nonfinite `{key}` in state"));
    }
    Ok(value)
}

fn parse_point_field(map: &Map<String, Value>, key: &str) -> Result<Point, String> {
    let obj = map
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("envelope `{key}` must be a point object"))?;
    require_exact_keys(obj, &["position", "value"])?;
    let value = parse_finite_f64_field(obj, "value")?;
    let pos_arr = obj
        .get("position")
        .and_then(Value::as_array)
        .filter(|arr| arr.len() == 2)
        .ok_or_else(|| format!("envelope `{key}.position` must be [step, id]"))?;
    let step = pos_arr[0]
        .as_u64()
        .ok_or_else(|| format!("envelope `{key}.position[0]` must be a non-negative integer"))?;
    let id = pos_arr[1]
        .as_str()
        .ok_or_else(|| format!("envelope `{key}.position[1]` must be a string"))?
        .to_string();
    let position = Position::parse(step, id)?;
    Ok(Point {
        position,
        value: canonical_f64(value),
    })
}

fn handle_request(input: &[u8]) -> Result<Value, String> {
    check_json_depth(input, MAX_JSON_DEPTH)?;
    let request: RawRequest<'_> =
        serde_json::from_slice(input).map_err(|err| format!("invalid request: {err}"))?;
    match request.op.as_ref() {
        "identity" => {
            if request.state.is_some()
                || request.values.is_some()
                || request.left.is_some()
                || request.right.is_some()
            {
                return Err("identity request accepts only `reducer`".into());
            }
            let reducer = request
                .reducer
                .ok_or_else(|| "identity requires string `reducer`".to_string())?;
            Ok(ReducerState::identity(&reducer)?.to_json())
        }
        "add" => {
            if request.reducer.is_some() || request.left.is_some() || request.right.is_some() {
                return Err("add request accepts only `state` and `values`".into());
            }
            let state = request
                .state
                .ok_or_else(|| "add requires `state`".to_string())?;
            let values = request
                .values
                .ok_or_else(|| "add requires `values`".to_string())?;
            let parsed = ReducerState::parse(&state)?;
            Ok(parsed.add(values)?.to_json())
        }
        "merge" => {
            if request.reducer.is_some() || request.state.is_some() || request.values.is_some() {
                return Err("merge request accepts only `left` and `right`".into());
            }
            let left = request
                .left
                .ok_or_else(|| "merge requires `left`".to_string())?;
            let right = request
                .right
                .ok_or_else(|| "merge requires `right`".to_string())?;
            let left_state = ReducerState::parse(&left)?;
            let right_state = ReducerState::parse(&right)?;
            Ok(left_state.merge(right_state)?.to_json())
        }
        "finalize" => {
            if request.reducer.is_some()
                || request.values.is_some()
                || request.left.is_some()
                || request.right.is_some()
            {
                return Err("finalize request accepts only `state`".into());
            }
            let state = request
                .state
                .ok_or_else(|| "finalize requires `state`".to_string())?;
            ReducerState::parse(&state)?.finalize()
        }
        _ => Err("unknown reducer operation".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(req: Value) -> Result<Value, String> {
        let raw = serde_json::to_vec(&req).unwrap();
        let out: Value = serde_json::from_slice(&execute_bytes(&raw)).unwrap();
        if let Some(ok) = out.get("ok") {
            Ok(ok.clone())
        } else {
            Err(out["error"].as_str().unwrap().to_string())
        }
    }

    #[test]
    fn scalar_and_count_lifecycles_match_spec() {
        let id = call(json!({"op": "identity", "reducer": "mean/v1"})).unwrap();
        assert_eq!(
            call(json!({"op": "finalize", "state": id})).unwrap(),
            json!({"count": 0, "value": null})
        );
        let added = call(json!({
            "op": "add",
            "state": id,
            "values": [
                {"value": 2, "position": [0, "a"]},
                {"value": null, "position": [1, "b"]},
                {"position": [2, "c"]},
                {"value": 6, "position": [3, "d"]}
            ]
        }))
        .unwrap();
        assert_eq!(
            call(json!({"op": "finalize", "state": added})).unwrap(),
            json!({"count": 2, "value": 4.0})
        );
    }

    #[test]
    fn envelope_tie_breaks_and_conflict_checks() {
        let id = call(json!({"op": "identity", "reducer": "envelope/v1"})).unwrap();
        let state = call(json!({
            "op": "add",
            "state": id,
            "values": [
                {"value": 10, "position": [20, "z"]},
                {"value": -3, "position": [5, "b"]},
                {"value": 10, "position": [20, "a"]},
                {"value": 2, "position": [100, "x"]},
                {"value": -3, "position": [5, "a"]}
            ]
        }))
        .unwrap();
        let fin = call(json!({"op": "finalize", "state": state})).unwrap();
        assert_eq!(fin["first"]["position"], json!([5, "a"]));
        assert_eq!(fin["last"]["position"], json!([100, "x"]));
        assert_eq!(fin["min"]["position"], json!([5, "a"]));
        assert_eq!(fin["max"]["position"], json!([20, "a"]));

        // Mid-batch duplicate position with conflicting values must fail even when neither is an extremum.
        let err = call(json!({
            "op": "add",
            "state": state,
            "values": [
                {"value": 1, "position": [50, "mid"]},
                {"value": 2, "position": [50, "mid"]}
            ]
        }))
        .unwrap_err();
        assert!(err.contains("conflicting"));
    }
}

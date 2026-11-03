//! Portable JSON collation and selector-bound query pagination.
use crate::{canonical_json, validate_exact_integers, validate_json, Result};
use serde_json::{json, Map, Value};
use std::{cmp::Ordering, ops::Bound};

/// Compare finite JSON keys in portable query order: null, false, true,
/// numbers, strings, arrays, then objects. Strings and member names use Unicode
/// scalar order; arrays and canonical object member sequences compare recursively.
/// This is binary ordering, not CouchDB's locale-aware string collation.
pub fn compare_json_keys(left: &Value, right: &Value) -> Ordering {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Bool(false) => 1,
            Value::Bool(true) => 2,
            Value::Number(_) => 3,
            Value::String(_) => 4,
            Value::Array(_) => 5,
            Value::Object(_) => 6,
        }
    }
    let order = rank(left).cmp(&rank(right));
    if order != Ordering::Equal {
        return order;
    }
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .expect("JSON number")
            .partial_cmp(&right.as_f64().expect("JSON number"))
            .expect("finite JSON numbers"),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Array(left), Value::Array(right)) => {
            for (left, right) in left.iter().zip(right) {
                let order = compare_json_keys(left, right);
                if order != Ordering::Equal {
                    return order;
                }
            }
            left.len().cmp(&right.len())
        }
        (Value::Object(left), Value::Object(right)) => {
            let mut left_members: Vec<_> = left.iter().collect();
            let mut right_members: Vec<_> = right.iter().collect();
            left_members.sort_by(|a, b| a.0.cmp(b.0));
            right_members.sort_by(|a, b| a.0.cmp(b.0));
            for ((left_name, left), (right_name, right)) in left_members.iter().zip(&right_members)
            {
                let order = left_name
                    .cmp(right_name)
                    .then_with(|| compare_json_keys(left, right));
                if order != Ordering::Equal {
                    return order;
                }
            }
            left.len().cmp(&right.len())
        }
        _ => Ordering::Equal,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueryKey(pub(crate) Value);

impl PartialEq for QueryKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for QueryKey {}
impl PartialOrd for QueryKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_json_keys(&self.0, &other.0)
    }
}

pub(crate) struct Selector {
    key: Option<QueryKey>,
    start: Option<QueryKey>,
    end: Option<QueryKey>,
    prefix: Option<QueryKey>,
    inclusive_start: bool,
    inclusive_end: bool,
    canonical: String,
}

impl Selector {
    pub(crate) fn parse(request: &Value) -> Result<Self> {
        fn read_key(request: &Value, field: &str) -> Result<Option<QueryKey>> {
            request
                .get(field)
                .map(|value| {
                    validate_json(value, 0)?;
                    validate_exact_integers(value)?;
                    Ok(QueryKey(value.clone()))
                })
                .transpose()
        }
        fn flag(request: &Value, field: &str) -> Result<bool> {
            request
                .get(field)
                .map(|value| {
                    value
                        .as_bool()
                        .ok_or_else(|| format!("{field} must be a boolean"))
                })
                .unwrap_or(Ok(true))
        }
        let key = read_key(request, "key")?;
        let start = read_key(request, "start_key")?;
        let end = read_key(request, "end_key")?;
        let prefix = read_key(request, "prefix")?;
        let range = start.is_some() || end.is_some();
        if usize::from(key.is_some()) + usize::from(range) + usize::from(prefix.is_some()) > 1 {
            return Err(
                "key, range bounds, and prefix are mutually exclusive query selectors".into(),
            );
        }
        if prefix
            .as_ref()
            .is_some_and(|key| !key.0.is_string() && !key.0.is_array())
        {
            return Err("prefix must be a string or an array".into());
        }
        let inclusive_start = flag(request, "inclusive_start")?;
        let inclusive_end = flag(request, "inclusive_end")?;
        let mut normalized = Map::new();
        for field in ["key", "start_key", "end_key", "prefix"] {
            if let Some(value) = request.get(field) {
                normalized.insert(field.into(), value.clone());
            }
        }
        normalized.insert("inclusive_start".into(), json!(inclusive_start));
        normalized.insert("inclusive_end".into(), json!(inclusive_end));
        Ok(Self {
            key,
            start,
            end,
            prefix,
            inclusive_start,
            inclusive_end,
            canonical: canonical_json(&Value::Object(normalized))?,
        })
    }

    pub(crate) fn matches(&self, key: &QueryKey) -> bool {
        if let Some(exact) = &self.key {
            return exact == key;
        }
        if let Some(prefix) = &self.prefix {
            return match (&prefix.0, &key.0) {
                (Value::String(prefix), Value::String(key)) => key.starts_with(prefix),
                (Value::Array(prefix), Value::Array(key)) => {
                    key.len() >= prefix.len()
                        && prefix
                            .iter()
                            .zip(key)
                            .all(|(left, right)| compare_json_keys(left, right) == Ordering::Equal)
                }
                _ => false,
            };
        }
        self.start
            .as_ref()
            .is_none_or(|start| key > start || (self.inclusive_start && key == start))
            && self
                .end
                .as_ref()
                .is_none_or(|end| key < end || (self.inclusive_end && key == end))
    }

    /// None represents an empty interval, avoiding BTreeMap's inverted-bound panic.
    pub(crate) fn bounds(
        &self,
        after: Option<QueryKey>,
    ) -> Option<(Bound<QueryKey>, Bound<QueryKey>)> {
        use Bound::{Excluded, Included, Unbounded};
        let mut lower = if let Some(key) = self.key.as_ref().or(self.prefix.as_ref()) {
            Included(key.clone())
        } else {
            match &self.start {
                Some(key) if self.inclusive_start => Included(key.clone()),
                Some(key) => Excluded(key.clone()),
                None => Unbounded,
            }
        };
        let upper = if let Some(key) = &self.key {
            Included(key.clone())
        } else {
            match &self.end {
                Some(key) if self.inclusive_end => Included(key.clone()),
                Some(key) => Excluded(key.clone()),
                None => Unbounded,
            }
        };
        if let Some(after) = after {
            let replace = match &lower {
                Included(key) | Excluded(key) => after >= *key,
                Unbounded => true,
            };
            if replace {
                lower = Excluded(after);
            }
        }
        if let (Included(start) | Excluded(start), Included(end) | Excluded(end)) = (&lower, &upper)
        {
            if start > end
                || (start == end && (matches!(lower, Excluded(_)) || matches!(upper, Excluded(_))))
            {
                return None;
            }
        }
        Some((lower, upper))
    }

    pub(crate) fn after(&self, request: &Value, cursor: &str) -> Result<Option<QueryKey>> {
        let token = match request.get("after") {
            None | Some(Value::Null) => return Ok(None),
            Some(Value::String(token)) => token,
            _ => return Err("after must be an opaque query pagination string".into()),
        };
        let parsed: Value = serde_json::from_str(token).map_err(|_| "Invalid query after token")?;
        let fields = parsed
            .as_array()
            .filter(|fields| fields.len() == 4)
            .ok_or("Invalid or expired query after token; obtain a fresh query page")?;
        if fields[0].as_str() != Some("foldlite-query-v1") {
            return Err("Invalid or expired query after token; obtain a fresh query page".into());
        }
        if fields[1].as_str() != Some(cursor) {
            return Err("Query after token belongs to a different snapshot".into());
        }
        if fields[2].as_str() != Some(&self.canonical) {
            return Err("Query after token belongs to a different query selector".into());
        }
        let canonical = fields[3].as_str().ok_or("Invalid query after key")?;
        let key: Value = serde_json::from_str(canonical).map_err(|_| "Invalid query after key")?;
        validate_json(&key, 0)?;
        // Host-generated canonical numbers use JavaScript's shortest double
        // spelling. Its integer text may be inexact as an integer (for example
        // 1000000000000000100), while denoting a valid double key. Canonical
        // round-trip validation below rejects alternate or rounded spellings.
        if canonical_json(&key)? != canonical {
            return Err("Invalid noncanonical query after key".into());
        }
        let key = QueryKey(key);
        if !self.matches(&key) {
            return Err("Query after key is outside its selector".into());
        }
        Ok(Some(key))
    }

    pub(crate) fn token(&self, cursor: &str, last_canonical_key: &str) -> String {
        json!([
            "foldlite-query-v1",
            cursor,
            self.canonical,
            last_canonical_key
        ])
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_accepts_shortest_double_integer_spelling() {
        let selector = Selector::parse(&json!({})).unwrap();
        let key = json!(1e18_f64 + 128.0);
        let canonical = canonical_json(&key).unwrap();
        assert_eq!(canonical, "1000000000000000100");
        let token = selector.token("snapshot", &canonical);
        let after = selector
            .after(&json!({"after":token}), "snapshot")
            .unwrap()
            .unwrap();
        assert_eq!(compare_json_keys(&key, &after.0), Ordering::Equal);
        let rounded = selector.token("snapshot", "9007199254740993");
        assert!(selector
            .after(&json!({"after":rounded}), "snapshot")
            .is_err());
    }

    #[test]
    fn selector_depth_is_per_key_rather_than_transport_wrapper() {
        let mut key = json!(0);
        for _ in 0..crate::MAX_JSON_DEPTH {
            key = json!([key]);
        }
        let selector = Selector::parse(&json!({"start_key":key})).unwrap();
        let canonical = canonical_json(&key).unwrap();
        assert!(selector
            .after(
                &json!({"after":selector.token("snapshot", &canonical)}),
                "snapshot"
            )
            .is_ok());
        key = json!([key]);
        assert!(Selector::parse(&json!({"start_key":key})).is_err());
    }
}

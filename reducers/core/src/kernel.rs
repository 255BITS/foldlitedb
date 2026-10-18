//! Typed reducer mathematics. No storage, clocks, offsets, runtime or JSON transport.
use serde::{Deserialize, Serialize};

pub const MAX_VALUES: usize = 1_024;
pub const MAX_COUNT: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position(pub u64, pub String);
impl Position {
    pub fn validate(&self) -> Result<(), String> {
        if self.0 > MAX_COUNT || self.1.is_empty() || self.1.len() > 128 || !self.1.is_ascii() {
            return Err(
                "position requires a safe nonnegative ordinal and 1..128 ASCII source ID".into(),
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Point {
    pub value: f64,
    pub position: Position,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Contribution {
    pub value: Option<f64>,
    pub position: Position,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "reducer", deny_unknown_fields)]
pub enum State {
    #[serde(rename = "mean/v1")]
    Mean { version: u32, count: u64, sum: f64 },
    #[serde(rename = "sum/v1")]
    Sum { version: u32, count: u64, sum: f64 },
    #[serde(rename = "count/v1")]
    Count { version: u32, count: u64 },
    #[serde(rename = "envelope/v1")]
    Envelope {
        version: u32,
        count: u64,
        first: Option<Point>,
        min: Option<Point>,
        max: Option<Point>,
        last: Option<Point>,
    },
}
pub fn identity(reducer: &str) -> Result<State, String> {
    match reducer {
        "mean/v1" => Ok(State::Mean {
            version: 1,
            count: 0,
            sum: 0.0,
        }),
        "sum/v1" => Ok(State::Sum {
            version: 1,
            count: 0,
            sum: 0.0,
        }),
        "count/v1" => Ok(State::Count {
            version: 1,
            count: 0,
        }),
        "envelope/v1" => Ok(State::Envelope {
            version: 1,
            count: 0,
            first: None,
            min: None,
            max: None,
            last: None,
        }),
        _ => Err("unknown reducer; select mean/v1, sum/v1, count/v1 or envelope/v1".into()),
    }
}
pub fn validate(state: &State) -> Result<(), String> {
    let (version, count) = match state {
        State::Mean {
            version,
            count,
            sum,
        }
        | State::Sum {
            version,
            count,
            sum,
        } => {
            if !sum.is_finite() || (*count == 0 && *sum != 0.0) {
                return Err("invalid scalar sum".into());
            }
            (*version, *count)
        }
        State::Count { version, count } => (*version, *count),
        State::Envelope {
            version,
            count,
            first,
            min,
            max,
            last,
        } => {
            let points = [first, min, max, last];
            if points.iter().any(|p| p.is_some() != (*count > 0)) {
                return Err("envelope points disagree with count".into());
            }
            for p in points.into_iter().flatten() {
                p.position.validate()?;
                if !p.value.is_finite() {
                    return Err("nonfinite envelope value".into());
                }
            }
            let mut distinct = 0;
            for (index, candidate) in points.iter().enumerate() {
                if let Some(point) = candidate {
                    if !points[..index].iter().any(|earlier| {
                        earlier
                            .as_ref()
                            .is_some_and(|earlier| earlier.position == point.position)
                    }) {
                        distinct += 1;
                    }
                }
            }
            if distinct > *count {
                return Err("envelope retains more positions than its count".into());
            }
            if let (Some(f), Some(lo), Some(hi), Some(l)) = (first, min, max, last) {
                if points.into_iter().flatten().any(|p| {
                    p.position < f.position
                        || p.position > l.position
                        || p.value < lo.value
                        || p.value > hi.value
                }) {
                    return Err("inconsistent envelope bounds".into());
                }
                for a in points.into_iter().flatten() {
                    for b in points.into_iter().flatten() {
                        if a.position == b.position && a.value != b.value {
                            return Err("conflicting values at one position".into());
                        }
                    }
                }
                if *count == 1
                    && (f.position != l.position || f.value != lo.value || f.value != hi.value)
                {
                    return Err("inconsistent singleton envelope".into());
                }
            }
            (*version, *count)
        }
    };
    if version != 1 || count > MAX_COUNT {
        return Err("unsupported state version or count".into());
    }
    Ok(())
}
fn count_sum(a: u64, b: u64) -> Result<u64, String> {
    a.checked_add(b)
        .filter(|n| *n <= MAX_COUNT)
        .ok_or_else(|| "count exceeds safe integer bound".into())
}
fn choose(a: Option<Point>, b: Option<Point>, mode: &str) -> Option<Point> {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => {
            let pick_b = match mode {
                "first" => b.position < a.position,
                "last" => b.position > a.position,
                "min" => b.value < a.value || (b.value == a.value && b.position < a.position),
                _ => b.value > a.value || (b.value == a.value && b.position < a.position),
            };
            Some(if pick_b { b } else { a })
        }
    }
}
pub fn merge(left: State, right: State) -> Result<State, String> {
    validate(&left)?;
    validate(&right)?;
    let state = match (left, right) {
        (
            State::Mean {
                count: a, sum: x, ..
            },
            State::Mean {
                count: b, sum: y, ..
            },
        ) => State::Mean {
            version: 1,
            count: count_sum(a, b)?,
            sum: x + y,
        },
        (
            State::Sum {
                count: a, sum: x, ..
            },
            State::Sum {
                count: b, sum: y, ..
            },
        ) => State::Sum {
            version: 1,
            count: count_sum(a, b)?,
            sum: x + y,
        },
        (State::Count { count: a, .. }, State::Count { count: b, .. }) => State::Count {
            version: 1,
            count: count_sum(a, b)?,
        },
        (
            State::Envelope {
                count: a,
                first: af,
                min: amin,
                max: amax,
                last: al,
                ..
            },
            State::Envelope {
                count: b,
                first: bf,
                min: bmin,
                max: bmax,
                last: bl,
                ..
            },
        ) => State::Envelope {
            version: 1,
            count: count_sum(a, b)?,
            first: choose(af, bf, "first"),
            min: choose(amin, bmin, "min"),
            max: choose(amax, bmax, "max"),
            last: choose(al, bl, "last"),
        },
        _ => return Err("cannot merge different reducers".into()),
    };
    validate(&state)?;
    Ok(state)
}

/// Add finite scalar contributions in their declared input order.
///
/// Null or absent values contribute nothing. Ownership means a failed call
/// cannot expose partially advanced state. Identity and deduplication are the
/// storage host's responsibility; this function owns mathematical state only.
fn scalar_value(contribution: &Contribution) -> Result<Option<f64>, String> {
    contribution.position.validate()?;
    if contribution.value.is_some_and(|value| !value.is_finite()) {
        return Err("nonfinite contribution".into());
    }
    Ok(contribution.value)
}

fn add_scalar(
    mut count: u64,
    mut sum: f64,
    values: Vec<Contribution>,
) -> Result<(u64, f64), String> {
    for contribution in values {
        if let Some(value) = scalar_value(&contribution)? {
            count = count_sum(count, 1)?;
            sum += value;
            if !sum.is_finite() {
                return Err("nonfinite scalar sum".into());
            }
        }
    }
    Ok((count, sum))
}

pub fn add(state: State, values: Vec<Contribution>) -> Result<State, String> {
    validate(&state)?;
    if values.len() > MAX_VALUES {
        return Err("batch exceeds 1024 contributions".into());
    }
    let state = match state {
        State::Mean { count, sum, .. } => {
            let (count, sum) = add_scalar(count, sum, values)?;
            State::Mean {
                version: 1,
                count,
                sum,
            }
        }
        State::Sum { count, sum, .. } => {
            let (count, sum) = add_scalar(count, sum, values)?;
            State::Sum {
                version: 1,
                count,
                sum,
            }
        }
        State::Count { mut count, .. } => {
            for contribution in values {
                if scalar_value(&contribution)?.is_some() {
                    count = count_sum(count, 1)?;
                }
            }
            State::Count { version: 1, count }
        }
        State::Envelope {
            mut count,
            mut first,
            mut min,
            mut max,
            mut last,
            ..
        } => {
            for contribution in values {
                let Some(value) = scalar_value(&contribution)? else {
                    continue;
                };
                if [first.as_ref(), min.as_ref(), max.as_ref(), last.as_ref()]
                    .into_iter()
                    .flatten()
                    .any(|retained| {
                        retained.position == contribution.position && retained.value != value
                    })
                {
                    return Err("conflicting values at one position".into());
                }
                count = count_sum(count, 1)?;
                let point = Some(Point {
                    value,
                    position: contribution.position,
                });
                first = choose(first, point.clone(), "first");
                min = choose(min, point.clone(), "min");
                max = choose(max, point.clone(), "max");
                last = choose(last, point, "last");
            }
            State::Envelope {
                version: 1,
                count,
                first,
                min,
                max,
                last,
            }
        }
    };
    validate(&state)?;
    Ok(state)
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum Finalized {
    Scalar {
        count: u64,
        value: Option<f64>,
    },
    Envelope {
        count: u64,
        first: Option<Point>,
        min: Option<Point>,
        max: Option<Point>,
        last: Option<Point>,
    },
}

pub fn finalize(state: State) -> Result<Finalized, String> {
    validate(&state)?;
    Ok(match state {
        State::Mean { count, sum, .. } => Finalized::Scalar {
            count,
            value: if count == 0 {
                None
            } else {
                Some(sum / count as f64)
            },
        },
        State::Sum { count, sum, .. } => Finalized::Scalar {
            count,
            value: Some(sum),
        },
        State::Count { count, .. } => Finalized::Scalar {
            count,
            value: Some(count as f64),
        },
        State::Envelope {
            count,
            first,
            min,
            max,
            last,
            ..
        } => Finalized::Envelope {
            count,
            first,
            min,
            max,
            last,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values() -> Vec<Contribution> {
        [Some(2.0), None, Some(-3.0), Some(9.0)]
            .into_iter()
            .enumerate()
            .map(|(i, value)| Contribution {
                value,
                position: Position(i as u64, format!("document-{i}")),
            })
            .collect()
    }

    #[test]
    fn typed_kernel_merges_and_serializes_portable_state() {
        for kind in ["mean/v1", "sum/v1", "count/v1", "envelope/v1"] {
            let all = add(identity(kind).unwrap(), values()).unwrap();
            let left = add(identity(kind).unwrap(), values()[..2].to_vec()).unwrap();
            let right = add(identity(kind).unwrap(), values()[2..].to_vec()).unwrap();
            let restored: State =
                serde_json::from_str(&serde_json::to_string(&left).unwrap()).unwrap();
            assert_eq!(
                serde_json::to_value(merge(restored, right).unwrap()).unwrap(),
                serde_json::to_value(all).unwrap()
            );
        }
    }

    #[test]
    fn typed_kernel_rejects_nonfinite_and_overflow() {
        let invalid = Contribution {
            value: Some(f64::NAN),
            position: Position(0, "a".into()),
        };
        assert!(add(identity("count/v1").unwrap(), vec![invalid]).is_err());
        assert!(merge(
            State::Count {
                version: 1,
                count: MAX_COUNT
            },
            State::Count {
                version: 1,
                count: 1
            }
        )
        .is_err());
        assert!(merge(
            State::Sum {
                version: 1,
                count: 1,
                sum: f64::MAX
            },
            State::Sum {
                version: 1,
                count: 1,
                sum: f64::MAX
            }
        )
        .is_err());
    }
}

//! Version 1 JSON transport. Kept separate from the typed numerical kernel.
use crate::kernel::{self, Contribution, State};
use serde::Deserialize;
use serde_json::{json, Value};

pub const INPUT_BYTES: usize = 262_144;
pub const OUTPUT_BYTES: usize = 65_536;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Identity {
        reducer: String,
    },
    Add {
        state: State,
        values: Vec<Contribution>,
    },
    Merge {
        left: State,
        right: State,
    },
    Finalize {
        state: State,
    },
}

fn handle(request: Request) -> Result<Value, String> {
    match request {
        Request::Identity { reducer } => Ok(json!(kernel::identity(&reducer)?)),
        Request::Add { state, values } => Ok(json!(kernel::add(state, values)?)),
        Request::Merge { left, right } => Ok(json!(kernel::merge(left, right)?)),
        Request::Finalize { state } => Ok(json!(kernel::finalize(state)?)),
    }
}

/// Encode one bounded protocol response without retaining request state.
pub fn execute_json(bytes: &[u8]) -> Vec<u8> {
    let result = if bytes.len() > INPUT_BYTES {
        Err("request exceeds 262144 bytes".to_string())
    } else {
        serde_json::from_slice::<Request>(bytes)
            .map_err(|e| format!("invalid request: {e}"))
            .and_then(handle)
    };
    let response = match result {
        Ok(value) => json!({"ok": value}),
        Err(error) => json!({"error": error}),
    };
    serde_json::to_vec(&response).expect("bounded serializable response")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(value: Value) -> Value {
        let response = execute_json(&serde_json::to_vec(&value).unwrap());
        serde_json::from_slice::<Value>(&response).unwrap()["ok"].clone()
    }

    #[test]
    fn empty_add_preserves_every_bit_of_serialized_scalar_state() {
        let sum = 1.2000000000042133e17_f64;
        for reducer in ["mean/v1", "sum/v1"] {
            let state = json!({"reducer":reducer,"version":1,"count":12,"sum":sum});
            let result = request(json!({"op":"add","state":state,"values":[]}));
            assert_eq!(result["sum"].as_f64().unwrap().to_bits(), sum.to_bits());
            assert_eq!(result["count"], 12);
        }
    }

    #[test]
    fn serialized_batches_preserve_ordered_mixed_magnitude_addition() {
        let initial = 1.2000000000042133e17_f64;
        let values = [
            1.0000000000016772e16_f64,
            1.0000000000030386e16,
            1.0000000000022584e16,
            1.0000000000001892e16,
            -1e17,
            1.0,
            1e-200,
            -1.0,
        ];
        for reducer in ["mean/v1", "sum/v1"] {
            for batch_size in [1, 4, values.len()] {
                let mut state = json!({"reducer":reducer,"version":1,"count":12,"sum":initial});
                let mut expected = initial;
                for (index, batch) in values.chunks(batch_size).enumerate() {
                    let contributions: Vec<_> = batch.iter().enumerate().map(|(ordinal,value)| {
                        json!({"value":value,"position":[index * batch_size + ordinal,"record"]})
                    }).collect();
                    state = request(json!({"op":"add","state":state,"values":contributions}));
                    for value in batch {
                        expected += value;
                    }
                    assert_eq!(state["sum"].as_f64().unwrap().to_bits(), expected.to_bits());
                }
            }
        }
    }
}

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

//! Import-free ABI-v1 Wasm execution with bounded memory, input, output and fuel.
use serde_json::{json, Value};
use wasmi::{Config, Engine, ExternType, Instance, Module, Store, StoreLimits, StoreLimitsBuilder};

pub const BUFFER_SIZE: usize = 262_144;
const MAX_MEMORY: usize = 16 * 1024 * 1024;
// Wasmi 2 meters input Wasm operations, rather than interpreter bytecode. The
// 249,882-byte/1,024-document metrics map fixture consumes 423,077,088 units;
// 500M leaves bounded headroom for requests near the 256 KiB ABI capacity.
// Fuel is an execution limit, so arbitrary views may still exhaust this budget.
const CALL_FUEL: u64 = 500_000_000;

/// Retryable resource limits are distinct from view failures and malformed output.
/// Only the optional group path may split a request after a capacity error.
pub(crate) enum CallError {
    Failure(String),
    Capacity(String),
}
impl From<String> for CallError {
    fn from(message: String) -> Self {
        Self::Failure(message)
    }
}
impl From<&str> for CallError {
    fn from(message: &str) -> Self {
        Self::Failure(message.into())
    }
}
impl CallError {
    fn message(self) -> String {
        match self {
            Self::Failure(message) | Self::Capacity(message) => message,
        }
    }
}

pub struct Runtime {
    engine: Engine,
    module: Module,
    pub descriptor: Value,
}

impl Runtime {
    pub fn new(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > 16 * 1024 * 1024 {
            return Err("Wasm module exceeds 16 MiB".into());
        }
        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let module =
            Module::new(&engine, bytes).map_err(|e| format!("Invalid Wasm module: {e}"))?;
        if module.imports().next().is_some() {
            return Err("Portable view modules must not import host functions or memory".into());
        }
        let memory = module
            .exports()
            .find(|e| e.name() == "memory")
            .ok_or("Wasm module must export memory")?;
        match memory.ty() {
            ExternType::Memory(memory) if memory.maximum().is_some_and(|n| n <= 256) => {}
            _ => return Err("Wasm memory must declare a maximum of at most 16 MiB".into()),
        }
        let mut runtime = Self {
            engine,
            module,
            descriptor: Value::Null,
        };
        let descriptor = runtime.call(json!({"op":"descriptor"}))?;
        if !descriptor.is_object()
            || descriptor.get("abi").and_then(Value::as_u64) != Some(1)
            || !descriptor
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
            || !descriptor
                .get("version")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
            || !descriptor
                .get("state_version")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
            || !descriptor.get("mergeable").is_some_and(Value::is_boolean)
            || descriptor
                .get("reduce_groups")
                .is_some_and(|value| !value.is_boolean())
        {
            return Err("Invalid ABI-v1 view descriptor".into());
        }
        runtime.descriptor = descriptor;
        Ok(runtime)
    }

    pub fn call(&self, request: Value) -> Result<Value, String> {
        self.call_inner(request).map_err(CallError::message)
    }

    fn call_inner(&self, request: Value) -> Result<Value, CallError> {
        let op = request
            .get("op")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let input = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
        if input.len() > BUFFER_SIZE {
            return Err(CallError::Capacity(format!(
                "Wasm {op} request is {} bytes, which exceeds the {BUFFER_SIZE}-byte ABI limit \
                 (ABI_BUFFER_BYTES). Use smaller batches, payloads, or view states.",
                input.len()
            )));
        }
        // A fresh instance makes each operation independent of hidden guest globals.
        let limits = StoreLimitsBuilder::new()
            .memory_size(MAX_MEMORY)
            .instances(1)
            .memories(1)
            .tables(1)
            .table_elements(100_000)
            .build();
        let mut store: Store<StoreLimits> = Store::new(&self.engine, limits);
        store.limiter(|limits| limits);
        store.set_fuel(CALL_FUEL).map_err(|e| e.to_string())?;
        let instance = Instance::new(&mut store, &self.module, &[])
            .map_err(|e| format!("Wasm startup failed: {e}"))?;
        let version = instance
            .get_typed_func::<(), i32>(&store, "abi_version")
            .map_err(|e| format!("Missing abi_version: {e}"))?
            .call(&mut store, ())
            .map_err(|e| e.to_string())?;
        if version != 1 {
            return Err("Unsupported portable view ABI version".into());
        }
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or("Missing memory")?;
        let mut exported = |name: &str| -> Result<usize, String> {
            let address = instance
                .get_typed_func::<(), i32>(&store, name)
                .map_err(|e| format!("Invalid {name} export: {e}"))?
                .call(&mut store, ())
                .map_err(|e| e.to_string())?;
            usize::try_from(address).map_err(|_| format!("Invalid {name} address"))
        };
        let input_ptr = exported("input_ptr")?;
        let capacity = exported("input_capacity")?;
        let output_ptr = exported("output_ptr")?;
        if capacity != BUFFER_SIZE {
            return Err("ABI input capacity must be 262144".into());
        }
        let memory_size = memory.data_size(&store);
        let input_end = input_ptr
            .checked_add(BUFFER_SIZE)
            .ok_or("Input address overflow")?;
        let output_end = output_ptr
            .checked_add(BUFFER_SIZE)
            .ok_or("Output address overflow")?;
        if input_end > memory_size
            || output_end > memory_size
            || (input_ptr < output_end && output_ptr < input_end)
        {
            return Err("Wasm ABI buffers must be in bounds and disjoint".into());
        }
        memory
            .write(&mut store, input_ptr, &input)
            .map_err(|e| e.to_string())?;
        let output_len = instance
            .get_typed_func::<i32, i32>(&store, "execute")
            .map_err(|e| format!("Invalid execute export: {e}"))?
            .call(&mut store, input.len() as i32)
            .map_err(|e| {
                let message = format!("Wasm execution failed: {e}");
                if e.as_trap_code() == Some(wasmi::core::TrapCode::OutOfFuel) {
                    CallError::Capacity(message)
                } else {
                    CallError::Failure(message)
                }
            })?;
        let output_len = usize::try_from(output_len).map_err(|_| "Negative Wasm output length")?;
        if output_len == 0 || output_len > BUFFER_SIZE {
            return Err(format!(
                "Wasm {op} response length {output_len} must be between 1 and {BUFFER_SIZE} bytes \
                 (ABI_BUFFER_BYTES)"
            )
            .into());
        }
        // Guests may relocate their output while executing (for example after
        // growing memory). Validate the post-execution pointer that is read.
        let output_ptr = instance
            .get_typed_func::<(), i32>(&store, "output_ptr")
            .map_err(|e| format!("Invalid output_ptr export: {e}"))?
            .call(&mut store, ())
            .map_err(|e| e.to_string())?;
        let output_ptr = usize::try_from(output_ptr).map_err(|_| "Invalid output_ptr address")?;
        let output_end = output_ptr
            .checked_add(BUFFER_SIZE)
            .ok_or("Output address overflow")?;
        if output_end > memory.data_size(&store)
            || (input_ptr < output_end && output_ptr < input_end)
        {
            return Err("Wasm output buffer must be in bounds and disjoint from input".into());
        }
        let mut output = vec![0; output_len];
        memory
            .read(&store, output_ptr, &mut output)
            .map_err(|e| e.to_string())?;
        let envelope: Value = serde_json::from_slice(&output)
            .map_err(|e| format!("Invalid Wasm response JSON: {e}"))?;
        if op == "reduce_groups" {
            validate_group_response(&envelope)?;
        } else {
            validate_response_json(&envelope, 0)?;
        }
        let object = envelope
            .as_object()
            .filter(|object| object.len() == 1)
            .ok_or("Wasm response must contain exactly one ok or error field")?;
        if let Some(error) = object.get("error") {
            let message = error
                .as_str()
                .ok_or("Wasm error envelope must contain a string")?;
            return Err(format!("View rejected operation: {message}").into());
        }
        envelope.get("ok").cloned().ok_or_else(|| {
            CallError::Failure("Wasm response requires an ok or error envelope".into())
        })
    }

    /// Inputs are in canonical group order. A missing state requests identity;
    /// JSON null is an ordinary existing state. Every attempt uses a fresh guest.
    pub fn reduce_groups(&self, groups: &[Value]) -> Result<Vec<(Value, Value)>, String> {
        let mut output = Vec::with_capacity(groups.len());
        if self.descriptor.get("reduce_groups") != Some(&Value::Bool(true)) {
            for group in groups {
                output.push(self.reduce_one(group)?);
            }
        } else {
            for chunk in groups.chunks(32) {
                self.reduce_chunk(chunk, &mut output)?;
            }
        }
        Ok(output)
    }

    fn reduce_one(&self, group: &Value) -> Result<(Value, Value), String> {
        let state = match group.get("state") {
            Some(state) => state.clone(),
            None => self.call(json!({"op":"identity"}))?,
        };
        let state = self.call(json!({"op":"add","state":state,"values":group["values"]}))?;
        let result = self.call(json!({"op":"finalize","state":state}))?;
        Ok((state, result))
    }

    fn split_groups(
        &self,
        groups: &[Value],
        output: &mut Vec<(Value, Value)>,
    ) -> Result<(), String> {
        if groups.len() == 1 {
            output.push(self.reduce_one(&groups[0])?);
        } else {
            let middle = groups.len() / 2;
            self.reduce_chunk(&groups[..middle], output)?;
            self.reduce_chunk(&groups[middle..], output)?;
        }
        Ok(())
    }

    fn reduce_chunk(
        &self,
        groups: &[Value],
        output: &mut Vec<(Value, Value)>,
    ) -> Result<(), String> {
        let request = json!({"op":"reduce_groups","groups":groups});
        let response = match self.call_inner(request) {
            Ok(response) => response,
            Err(CallError::Capacity(_)) => return self.split_groups(groups, output),
            Err(error) => return Err(error.message()),
        };
        if response == json!({"split":true}) {
            return self.split_groups(groups, output);
        }
        let rows = response
            .as_object()
            .filter(|object| object.len() == 1)
            .and_then(|object| object.get("groups"))
            .and_then(Value::as_array)
            .filter(|rows| rows.len() == groups.len())
            .ok_or(
                "reduce_groups must return one state/result per group or an exact split signal",
            )?;
        for (row, input) in rows.iter().zip(groups) {
            let new_group = input.get("state").is_none();
            let object = row
                .as_object()
                .filter(|object| object.len() == if new_group { 3 } else { 2 })
                .filter(|object| object.contains_key("identity") == new_group)
                .ok_or(
                    "A reduced group must contain state/result and identity only for a new group",
                )?;
            let state = object
                .get("state")
                .ok_or("A reduced group requires state")?;
            let result = object
                .get("result")
                .ok_or("A reduced group requires result")?;
            output.push((state.clone(), result.clone()));
        }
        Ok(())
    }
}

// Transport levels must not shrink the legacy per-response data depth budget.
// Shape validation in reduce_chunk rejects any other success payload.
fn validate_group_response(envelope: &Value) -> Result<(), String> {
    let groups = envelope
        .get("ok")
        .and_then(|value| value.get("groups"))
        .and_then(Value::as_array);
    if let Some(groups) = groups {
        for group in groups {
            if let Some(identity) = group.get("identity") {
                validate_response_json(identity, 1)?;
            }
            if let Some(state) = group.get("state") {
                validate_response_json(state, 1)?;
            }
            if let Some(result) = group.get("result") {
                validate_response_json(result, 1)?;
            }
        }
        // Validate unknown fields too, so malformed responses cannot hide values.
        let mut shell = envelope.clone();
        shell["ok"]["groups"] = json!([]);
        validate_response_json(&shell, 0)
    } else {
        validate_response_json(envelope, 0)
    }
}

// Guests have JavaScript number semantics, and JavaScript-style serializers
// (JSON.stringify, the AssemblyScript SDK) spell some integral doubles above
// 2^53 as their shortest round-trip decimal, e.g. 1e18 + 128 as
// 1000000000000000100. That spelling denotes a double and is accepted. Any
// other inexact integer, such as 9007199254740993 from 64-bit integer
// arithmetic, cannot come from a double and would otherwise be silently rounded.
fn guest_integer_is_double(number: &serde_json::Number) -> bool {
    crate::integer_is_exact_double(number)
        || number
            .as_f64()
            .is_some_and(|value| ryu_js::Buffer::new().format_finite(value) == number.to_string())
}

// Match the browser finiteJSON depth convention: the envelope starts at zero,
// and every array element/object property adds one, including primitive leaves.
fn validate_response_json(value: &Value, depth: usize) -> Result<(), String> {
    if depth > 64 {
        return Err("Wasm response JSON nesting exceeds 64".into());
    }
    match value {
        Value::Number(number) if !number.as_f64().is_some_and(f64::is_finite) => {
            return Err("Wasm response JSON numbers must be finite".into());
        }
        Value::Number(number) if !guest_integer_is_double(number) => {
            return Err(format!(
                "Wasm response JSON integer {number} is not exactly representable as an \
                 IEEE-754 double; views must emit double-valued numbers or strings"
            ));
        }
        Value::Array(values) => {
            for value in values {
                validate_response_json(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_response_json(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;

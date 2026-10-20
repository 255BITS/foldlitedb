//! Import-free ABI-v1 Wasm execution with bounded memory, input, output and fuel.
use serde_json::{json, Value};
use wasmi::{Config, Engine, ExternType, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};

pub const BUFFER_SIZE: usize = 262_144;
const MAX_MEMORY: usize = 16 * 1024 * 1024;
const CALL_FUEL: u64 = 100_000_000;

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
        {
            return Err("Invalid ABI-v1 view descriptor".into());
        }
        runtime.descriptor = descriptor;
        Ok(runtime)
    }

    pub fn call(&self, request: Value) -> Result<Value, String> {
        let input = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
        if input.len() > BUFFER_SIZE {
            return Err(
                "Wasm request exceeds the 262144-byte ABI buffer. Use smaller batches or iterate \
                 db.ingest_many(..., batch_size=...) with a smaller batch_size."
                    .into(),
            );
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
        let instance = Linker::<StoreLimits>::new(&self.engine)
            .instantiate(&mut store, &self.module)
            .and_then(|pre| pre.start(&mut store))
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
            .map_err(|e| format!("Wasm execution failed: {e}"))?;
        let output_len = usize::try_from(output_len).map_err(|_| "Negative Wasm output length")?;
        if output_len == 0 || output_len > BUFFER_SIZE {
            return Err("Wasm output length must be between 1 and 262144 bytes".into());
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
        validate_response_json(&envelope, 0)?;
        let object = envelope
            .as_object()
            .filter(|object| object.len() == 1)
            .ok_or("Wasm response must contain exactly one ok or error field")?;
        if let Some(error) = object.get("error") {
            let message = error
                .as_str()
                .ok_or("Wasm error envelope must contain a string")?;
            return Err(format!("View rejected operation: {message}"));
        }
        envelope
            .get("ok")
            .cloned()
            .ok_or_else(|| "Wasm response requires an ok or error envelope".into())
    }
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

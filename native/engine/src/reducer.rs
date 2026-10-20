//! Embedded wasmi-backed execution of ABI-v1 reducer kernels.
use sha2::{Digest, Sha256};
use wasmi::{
    Config, Engine, ExternType, Linker, Module, Store, StoreLimits, StoreLimitsBuilder,
};

pub struct ReducerInstance {
    #[allow(dead_code)]
    engine: Engine,
    store: Store<StoreLimits>,
    instance: wasmi::Instance,
    fuel: u64,
    failed: bool,
    module_sha256: String,
}

impl ReducerInstance {
    pub fn new(module_bytes: &[u8], fuel: u64) -> Result<Self, String> {
        if module_bytes.len() > 16 * 1024 * 1024 {
            return Err("Wasm module exceeds 16 MiB".into());
        }
        let module_sha256 = format!("{:x}", Sha256::digest(module_bytes));
        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let module =
            Module::new(&engine, module_bytes).map_err(|e| format!("Invalid Wasm module: {e}"))?;
        if module.imports().next().is_some() {
            return Err("Wasm module must not import host functions or memory".into());
        }
        let memory = module
            .exports()
            .find(|e| e.name() == "memory")
            .ok_or("Wasm module must export memory")?;
        match memory.ty() {
            ExternType::Memory(memory) if memory.maximum().is_some_and(|n| n <= 256) => {}
            _ => return Err("Wasm memory must declare a maximum of at most 16 MiB".into()),
        }

        let limits = StoreLimitsBuilder::new()
            .memory_size(16 * 1024 * 1024)
            .instances(1)
            .memories(1)
            .tables(1)
            .table_elements(100_000)
            .build();
        let mut store: Store<StoreLimits> = Store::new(&engine, limits);
        store.limiter(|limits| limits);
        store.set_fuel(50_000_000).map_err(|e| e.to_string())?;

        let instance = Linker::<StoreLimits>::new(&engine)
            .instantiate(&mut store, &module)
            .and_then(|pre| pre.start(&mut store))
            .map_err(|e| format!("Wasm startup failed: {e}"))?;

        let version = instance
            .get_typed_func::<(), i32>(&store, "abi_version")
            .map_err(|e| format!("Missing abi_version: {e}"))?
            .call(&mut store, ())
            .map_err(|e| e.to_string())?;
        if version != 1 {
            return Err("Unsupported reducer ABI version".into());
        }

        let input_capacity = instance
            .get_typed_func::<(), i32>(&store, "input_capacity")
            .map_err(|e| format!("Missing input_capacity: {e}"))?
            .call(&mut store, ())
            .map_err(|e| e.to_string())?;
        if input_capacity != 262_144 {
            return Err("ABI input capacity must be 262144".into());
        }

        Ok(Self {
            engine,
            store,
            instance,
            fuel,
            failed: false,
            module_sha256,
        })
    }

    pub fn execute_json(&mut self, input: &[u8]) -> Result<Vec<u8>, String> {
        if self.failed {
            return Err("Reducer instance trapped; create a new instance".into());
        }
        if input.len() > 262_144 {
            return Err("Request exceeds 262144 bytes".into());
        }
        if let Err(e) = self.store.set_fuel(self.fuel) {
            self.failed = true;
            return Err(format!("Reducer execution budget exhausted or trapped: {e}"));
        }
        let memory = self
            .instance
            .get_memory(&self.store, "memory")
            .ok_or("Missing memory")?;
        let input_ptr = match self.instance.get_typed_func::<(), i32>(&self.store, "input_ptr") {
            Ok(func) => match func.call(&mut self.store, ()) {
                Ok(ptr) => ptr,
                Err(e) => {
                    self.failed = true;
                    return Err(format!("Reducer execution budget exhausted or trapped: {e}"));
                }
            },
            Err(e) => return Err(format!("Missing input_ptr export: {e}")),
        };
        let input_ptr = usize::try_from(input_ptr).map_err(|_| "Invalid input_ptr address")?;
        if input_ptr.checked_add(input.len()).is_none()
            || input_ptr + input.len() > memory.data_size(&self.store)
        {
            return Err("Input buffer out of bounds".into());
        }
        if let Err(e) = memory.write(&mut self.store, input_ptr, input) {
            self.failed = true;
            return Err(format!("Reducer execution budget exhausted or trapped: {e}"));
        }

        let execute = self
            .instance
            .get_typed_func::<i32, i32>(&self.store, "execute")
            .map_err(|e| format!("Missing execute export: {e}"))?;
        let output_len = match execute.call(&mut self.store, input.len() as i32) {
            Ok(len) => len,
            Err(e) => {
                self.failed = true;
                return Err(format!("Reducer execution budget exhausted or trapped: {e}"));
            }
        };
        if output_len <= 0 || output_len > 65_536 {
            return Err("Invalid reducer response length".into());
        }
        let output_len = output_len as usize;

        let output_ptr = match self
            .instance
            .get_typed_func::<(), i32>(&self.store, "output_ptr")
        {
            Ok(func) => match func.call(&mut self.store, ()) {
                Ok(ptr) => ptr,
                Err(e) => {
                    self.failed = true;
                    return Err(format!("Reducer execution budget exhausted or trapped: {e}"));
                }
            },
            Err(e) => return Err(format!("Missing output_ptr export: {e}")),
        };
        let output_ptr = usize::try_from(output_ptr).map_err(|_| "Invalid output_ptr address")?;
        if output_ptr.checked_add(output_len).is_none()
            || output_ptr + output_len > memory.data_size(&self.store)
        {
            return Err("Output buffer out of bounds".into());
        }
        let mut output = vec![0u8; output_len];
        memory
            .read(&self.store, output_ptr, &mut output)
            .map_err(|e| e.to_string())?;
        Ok(output)
    }

    pub fn memory_bytes(&self) -> usize {
        self.instance
            .get_memory(&self.store, "memory")
            .map(|m| m.data_size(&self.store))
            .unwrap_or(0)
    }

    pub fn module_sha256(&self) -> &str {
        &self.module_sha256
    }
}

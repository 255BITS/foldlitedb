//! Embedded wasmi-backed execution of ABI-v1 reducer kernels.
use crate::wasm::{compile_module, new_store};
use sha2::{Digest, Sha256};
use wasmi::{Instance, Store, StoreLimits};

const STARTUP_FUEL: u64 = 50_000_000;
const INPUT_CAPACITY: usize = 262_144;
const OUTPUT_CAPACITY: usize = 65_536;

pub struct ReducerInstance {
    store: Store<StoreLimits>,
    instance: Instance,
    fuel: u64,
    failed: bool,
    module_sha256: String,
}

impl ReducerInstance {
    pub fn new(module_bytes: &[u8], fuel: u64) -> Result<Self, String> {
        let (engine, module) = compile_module(
            module_bytes,
            "Wasm module must not import host functions or memory",
        )?;
        let module_sha256 = format!("{:x}", Sha256::digest(module_bytes));

        let mut store = new_store(&engine, STARTUP_FUEL)?;

        let instance = Instance::new(&mut store, &module, &[])
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
        if input_capacity != INPUT_CAPACITY as i32 {
            return Err(format!("ABI input capacity must be {INPUT_CAPACITY}"));
        }

        Ok(Self {
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
        if input.len() > INPUT_CAPACITY {
            return Err(format!("Request exceeds {INPUT_CAPACITY} bytes"));
        }
        if let Err(e) = self.store.set_fuel(self.fuel) {
            self.failed = true;
            return Err(format!(
                "Reducer execution budget exhausted or trapped: {e}"
            ));
        }
        let memory = self
            .instance
            .get_memory(&self.store, "memory")
            .ok_or("Missing memory")?;
        let input_ptr = match self
            .instance
            .get_typed_func::<(), i32>(&self.store, "input_ptr")
        {
            Ok(func) => match func.call(&mut self.store, ()) {
                Ok(ptr) => ptr,
                Err(e) => {
                    self.failed = true;
                    return Err(format!(
                        "Reducer execution budget exhausted or trapped: {e}"
                    ));
                }
            },
            Err(e) => return Err(format!("Missing input_ptr export: {e}")),
        };
        let input_ptr = usize::try_from(input_ptr).map_err(|_| "Invalid input_ptr address")?;
        let input_end = input_ptr
            .checked_add(INPUT_CAPACITY)
            .ok_or("Input buffer out of bounds")?;
        if input_end > memory.data_size(&self.store) {
            return Err("Input buffer out of bounds".into());
        }
        let pre_output_ptr = match self
            .instance
            .get_typed_func::<(), i32>(&self.store, "output_ptr")
        {
            Ok(func) => match func.call(&mut self.store, ()) {
                Ok(ptr) => ptr,
                Err(e) => {
                    self.failed = true;
                    return Err(format!(
                        "Reducer execution budget exhausted or trapped: {e}"
                    ));
                }
            },
            Err(e) => return Err(format!("Missing output_ptr export: {e}")),
        };
        let pre_output_ptr =
            usize::try_from(pre_output_ptr).map_err(|_| "Invalid output_ptr address")?;
        let pre_output_end = pre_output_ptr
            .checked_add(OUTPUT_CAPACITY)
            .ok_or("Output buffer out of bounds")?;
        if pre_output_end > memory.data_size(&self.store) {
            return Err("Output buffer out of bounds".into());
        }
        if input_ptr < pre_output_end && pre_output_ptr < input_end {
            return Err("ABI buffers must not overlap".into());
        }

        if let Err(e) = memory.write(&mut self.store, input_ptr, input) {
            self.failed = true;
            return Err(format!(
                "Reducer execution budget exhausted or trapped: {e}"
            ));
        }

        let execute = match self
            .instance
            .get_typed_func::<i32, i32>(&self.store, "execute")
        {
            Ok(func) => func,
            Err(e) => {
                self.failed = true;
                return Err(format!("Missing execute export: {e}"));
            }
        };
        let output_len = match execute.call(&mut self.store, input.len() as i32) {
            Ok(len) => len,
            Err(e) => {
                self.failed = true;
                return Err(format!(
                    "Reducer execution budget exhausted or trapped: {e}"
                ));
            }
        };
        if output_len <= 0 || output_len > OUTPUT_CAPACITY as i32 {
            self.failed = true;
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
                    return Err(format!(
                        "Reducer execution budget exhausted or trapped: {e}"
                    ));
                }
            },
            Err(e) => {
                self.failed = true;
                return Err(format!("Missing output_ptr export: {e}"));
            }
        };
        let Ok(output_ptr) = usize::try_from(output_ptr) else {
            self.failed = true;
            return Err("Invalid output_ptr address".into());
        };
        let Some(output_end) = output_ptr.checked_add(output_len) else {
            self.failed = true;
            return Err("Output buffer out of bounds".into());
        };
        let mem_size = memory.data_size(&self.store);
        if input_end > mem_size || output_end > mem_size {
            self.failed = true;
            return Err("Output buffer out of bounds".into());
        }
        if input_ptr < output_end && output_ptr < input_end {
            self.failed = true;
            return Err("ABI buffers must not overlap".into());
        }
        Ok(memory.data(&self.store)[output_ptr..output_end].to_vec())
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

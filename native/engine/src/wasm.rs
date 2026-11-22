//! Shared Wasmi module validation and bounded store construction.
use wasmi::{Config, Engine, ExternType, Module, Store, StoreLimits, StoreLimitsBuilder};

pub(crate) const MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_MEMORY_PAGES: u64 = 256;
pub(crate) const MAX_TABLE_ELEMENTS: usize = 100_000;

/// Compile and validate an import-free Wasm module with bounded exported memory.
pub(crate) fn compile_module(bytes: &[u8], import_error: &str) -> Result<(Engine, Module), String> {
    if bytes.len() > MAX_MODULE_BYTES {
        return Err("Wasm module exceeds 16 MiB".into());
    }
    let mut config = Config::default();
    config.consume_fuel(true);
    let engine = Engine::new(&config);
    let module = Module::new(&engine, bytes).map_err(|e| format!("Invalid Wasm module: {e}"))?;
    if module.imports().next().is_some() {
        return Err(import_error.into());
    }
    let memory = module
        .exports()
        .find(|e| e.name() == "memory")
        .ok_or("Wasm module must export memory")?;
    match memory.ty() {
        ExternType::Memory(memory) if memory.maximum().is_some_and(|n| n <= MAX_MEMORY_PAGES) => {}
        _ => return Err("Wasm memory must declare a maximum of at most 16 MiB".into()),
    }
    Ok((engine, module))
}

/// Construct a bounded, fuel-metered store for a single Wasm instance.
pub(crate) fn new_store(engine: &Engine, fuel: u64) -> Result<Store<StoreLimits>, String> {
    let limits = StoreLimitsBuilder::new()
        .memory_size(MAX_MEMORY_BYTES)
        .instances(1)
        .memories(1)
        .tables(1)
        .table_elements(MAX_TABLE_ELEMENTS)
        .build();
    let mut store: Store<StoreLimits> = Store::new(engine, limits);
    store.limiter(|limits| limits);
    store.set_fuel(fuel).map_err(|e| e.to_string())?;
    Ok(store)
}

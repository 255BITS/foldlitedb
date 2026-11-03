use foldlitedb_portable_engine::{Engine, ReducerInstance, ABI_BUFFER_BYTES, MAX_BATCH_DOCUMENTS};
use serde_json::{json, Value};

const METRICS: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/metrics.wasm");
const REDUCERS: &[u8] = include_bytes!("../../../src/foldlitedb/reducers/assets/reducer.wasm");

fn reduce(instance: &mut ReducerInstance, request: Value) -> Value {
    let input = serde_json::to_vec(&request).unwrap();
    assert!(input.len() <= ABI_BUFFER_BYTES);
    let response = instance.execute_json(&input).unwrap();
    let response: Value = serde_json::from_slice(&response).unwrap();
    assert!(response.get("error").is_none(), "{response}");
    response["ok"].clone()
}

#[test]
fn default_fuel_accepts_maximum_numeric_batches_with_long_positions() {
    // Exercise the default 50M budget with close to 200 KiB of input, including
    // maximum-length position ties. Fuel units changed in Wasmi 2.0; the public
    // count limit must still work for all four bundled kernels.
    let values: Vec<_> = (0..1024)
        .map(|index| {
            json!({"value":index as f64 / 7.0,
                "position":[index, format!("{index:0128}")]})
        })
        .collect();
    for kind in ["mean/v1", "sum/v1", "count/v1", "envelope/v1"] {
        let mut instance = ReducerInstance::new(REDUCERS, 50_000_000).unwrap();
        let state = reduce(&mut instance, json!({"op":"identity","reducer":kind}));
        let state = reduce(
            &mut instance,
            json!({"op":"add","state":state,"values":values}),
        );
        let result = reduce(&mut instance, json!({"op":"finalize","state":state}));
        assert_eq!(result["count"], 1024, "{kind}: {result}");
    }
}

#[test]
fn exhausted_numeric_reducer_stays_poisoned_and_a_new_instance_recovers() {
    let request = serde_json::to_vec(&json!({"op":"identity","reducer":"mean/v1"})).unwrap();
    let mut instance = ReducerInstance::new(REDUCERS, 100).unwrap();
    let error = instance.execute_json(&request).unwrap_err();
    assert!(error.contains("budget exhausted"), "{error}");
    let error = instance.execute_json(&request).unwrap_err();
    assert!(error.contains("create a new instance"), "{error}");
    let mut replacement = ReducerInstance::new(REDUCERS, 50_000_000).unwrap();
    assert_eq!(
        reduce(
            &mut replacement,
            json!({"op":"identity","reducer":"mean/v1"})
        )["count"],
        0
    );
}

#[test]
fn default_view_fuel_accepts_a_near_capacity_maximum_document_batch() {
    let payload = json!({"metric":"large","value":1.2345678901234567e-100,
        "padding":"x".repeat(180)});
    let documents = vec![json!({"payload":payload}); MAX_BATCH_DOCUMENTS];
    let mapped = json!({"op":"map","documents":vec![payload; MAX_BATCH_DOCUMENTS]});
    let bytes = serde_json::to_vec(&mapped).unwrap().len();
    assert!(
        bytes > ABI_BUFFER_BYTES * 9 / 10 && bytes <= ABI_BUFFER_BYTES,
        "{bytes}"
    );
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    let ingested = engine
        .execute(json!({"op":"ingest","documents":documents}))
        .unwrap();
    assert_eq!(ingested["inserted"], MAX_BATCH_DOCUMENTS);
    let rows = engine.execute(json!({"op":"query"})).unwrap();
    assert_eq!(rows["rows"][0]["result"]["count"], MAX_BATCH_DOCUMENTS);
}

fn portable_module(extra: &str, execute: &str) -> Vec<u8> {
    let responses = [
        r#"{"ok":{"abi":1,"name":"isolation","version":"1","state_version":"1","mergeable":false}}"#,
        r#"{"ok":[[{"key":"x","value":1}]]}"#,
        r#"{"ok":0}"#,
        r#"{"ok":1}"#,
    ];
    let data: String = responses
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let escaped: String = value.bytes().map(|byte| format!("\\{byte:02x}")).collect();
            format!("(data (i32.const {}) \"{escaped}\")", 300000 + index * 1000)
        })
        .collect();
    wat::parse_str(format!(
        r#"(module
        (memory (export "memory") 10 256)
        (global $output (mut i32) (i32.const 300000))
        {extra} {data}
        (func (export "abi_version") (result i32) i32.const 1)
        (func (export "input_ptr") (result i32) i32.const 0)
        (func (export "input_capacity") (result i32) i32.const 262144)
        (func (export "output_ptr") (result i32) global.get $output)
        (func (export "execute") (param i32) (result i32) {execute}))"#
    ))
    .unwrap()
}

#[test]
fn direct_instantiation_keeps_guest_globals_fresh_for_every_view_operation() {
    // Reusing an instance, even within a transaction, would trap on the second
    // request. All descriptor/map/identity/add/finalize requests must succeed.
    let module = portable_module(
        "(global $called (mut i32) (i32.const 0))",
        r#"global.get $called if unreachable end
           i32.const 1 global.set $called
           i32.const 7 i32.load8_u
           i32.const 100 i32.eq
           if (result i32) i32.const 87
           else
             i32.const 2 i32.load8_u i32.const 100 i32.eq
             if (result i32)
               i32.const 301000 global.set $output i32.const 32
             else
               i32.const 7 i32.load8_u i32.const 105 i32.eq
               if (result i32)
                 i32.const 302000 global.set $output i32.const 8
               else
                 i32.const 303000 global.set $output i32.const 8
               end
             end
           end"#,
    );
    let mut engine = Engine::open(":memory:", &module).unwrap();
    for _ in 0..2 {
        let result = engine
            .execute(json!({"op":"ingest","documents":[{"payload":{}}]}))
            .unwrap();
        assert_eq!(result["inserted"], 1);
    }
    assert_eq!(
        engine.execute(json!({"op":"query"})).unwrap()["rows"][0]["result"],
        1
    );
}

#[test]
fn direct_instantiation_enforces_startup_fuel_and_table_limits() {
    let looping = portable_module("(func $start (loop br 0)) (start $start)", "i32.const 0");
    let table = portable_module("(table 100001 funcref)", "i32.const 0");
    for module in [looping, table] {
        let error = Engine::open(":memory:", &module).err().unwrap();
        assert!(error.contains("Wasm startup failed"), "{error}");
        let error = ReducerInstance::new(&module, 50_000_000).err().unwrap();
        assert!(error.contains("Wasm startup failed"), "{error}");
    }
}

#[test]
fn direct_instantiation_preserves_the_guest_memory_growth_ceiling() {
    // From 10 initial pages, growth to the declared 256-page/16MiB ceiling is
    // permitted; the next page must fail without trapping or growing memory.
    let module = portable_module(
        r#"(func $start
             i32.const 246 memory.grow i32.const 10 i32.ne if unreachable end
             i32.const 1 memory.grow i32.const -1 i32.ne if unreachable end
             memory.size i32.const 256 i32.ne if unreachable end)
           (start $start)"#,
        "i32.const 87",
    );
    Engine::open(":memory:", &module).unwrap();
    ReducerInstance::new(&module, 50_000_000).unwrap();
}

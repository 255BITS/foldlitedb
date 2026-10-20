use foldlitedb_portable_engine::{canonical_json, Engine};
use serde_json::{json, Value};

const METRICS: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/metrics.wasm");
const CATALOGUE: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/catalogue.wasm");

fn execute(engine: &mut Engine, request: Value) -> Value {
    engine.execute(request).unwrap()
}
fn insert(engine: &mut Engine, metric: &str, value: f64) -> Value {
    execute(
        engine,
        json!({"op":"ingest","documents":[{"payload":{"metric":metric,"value":value}}]}),
    )
}

#[test]
fn actual_assemblyscript_metrics_survive_reopen_and_replay() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metrics.db");
    let mut engine = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    let response = execute(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"id":"one","source":"sensor","position":0,"payload":{"metric":"temperature","value":12}},
            {"id":"two","source":"sensor","position":1,"payload":{"metric":"temperature","value":18}},
            {"payload":{"metric":"humidity","value":25}},
            {"payload":{"ignored":true}}
        ]}),
    );
    assert_eq!(response["inserted"], 4);
    let stats = execute(&mut engine, json!({"op":"stats"}));
    assert_eq!(stats["checkpoints"], json!({"default":1,"sensor":1}));
    let snapshot = execute(&mut engine, json!({"op":"query"}));
    assert_eq!(
        snapshot["rows"][1]["result"],
        json!({"count":2,"sum":30,"mean":15,"min":12,"max":18})
    );
    assert_eq!(snapshot["rows"][0]["key"], "humidity");
    drop(engine);
    let mut engine = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    assert_eq!(execute(&mut engine, json!({"op":"query"})), snapshot);
    let replay = execute(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"id":"one","source":"sensor","payload":{"value":12.0,"metric":"temperature"}},
            {"source":"sensor","position":1,"payload":{"value":18,"metric":"temperature"}}
        ]}),
    );
    assert_eq!(replay["replayed"], 2);
    assert_eq!(replay["inserted"], 0);
    assert_eq!(replay["documents"][1]["id"], "two");
    let reopened = execute(&mut engine, json!({"op":"stats"}));
    for field in ["documents", "sequence", "checkpoints", "cursor"] {
        assert_eq!(reopened[field], stats[field]);
    }
    assert_eq!(reopened["cache"]["status"], "loaded");
    assert_eq!(reopened["replay_batches"], 0);
}

#[test]
fn snapshots_changes_and_document_exports_are_continuous() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    for key in ["alpha", "beta", "gamma"] {
        insert(&mut engine, key, 1.0);
    }
    let first = execute(&mut engine, json!({"op":"query","limit":1}));
    assert_eq!(first["rows"][0]["key"], "alpha");
    assert_eq!(first["complete"], false);
    insert(&mut engine, "beta", 5.0);
    insert(&mut engine, "delta", 8.0);
    let second = execute(
        &mut engine,
        json!({"op":"query","cursor":first["cursor"],"after":first["after"],"limit":1}),
    );
    assert_eq!(second["rows"][0]["key"], "beta");
    assert_eq!(second["rows"][0]["result"]["sum"], 1);
    let third = execute(
        &mut engine,
        json!({"op":"query","cursor":second["cursor"],"after":second["after"],"limit":1}),
    );
    assert_eq!(third["rows"][0]["key"], "gamma");
    assert_eq!(third["complete"], true);
    let changes = execute(
        &mut engine,
        json!({"op":"changes","since":first["cursor"],"limit":1}),
    );
    assert_eq!(changes["rows"][0]["key"], "beta");
    assert_eq!(changes["rows"][0]["result"]["sum"], 6);
    assert_eq!(changes["complete"], false);
    let rest = execute(
        &mut engine,
        json!({"op":"changes","since":changes["cursor"],"limit":1}),
    );
    assert_eq!(rest["rows"][0]["key"], "delta");
    assert_eq!(rest["complete"], true);
    assert_eq!(
        execute(&mut engine, json!({"op":"changes","since":rest["cursor"]}))["rows"],
        json!([])
    );
    let export = execute(
        &mut engine,
        json!({"op":"documents","limit":2,"cursor":first["cursor"]}),
    );
    assert_eq!(export["documents"].as_array().unwrap().len(), 2);
    let next = execute(
        &mut engine,
        json!({"op":"documents","limit":2,"cursor":export["cursor"],"after":export["after"]}),
    );
    assert_eq!(next["documents"].as_array().unwrap().len(), 1);
    assert_eq!(next["complete"], true);
    assert!(engine
        .execute(json!({"op":"query","after":first["after"]}))
        .is_err());
    let mut another = Engine::open(":memory:", METRICS).unwrap();
    assert!(another
        .execute(json!({"op":"query","cursor":first["cursor"]}))
        .is_err());
}

#[test]
fn conflicts_roll_back_documents_checkpoints_and_view_changes() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    execute(
        &mut engine,
        json!({"op":"ingest","documents":[{"id":"existing","source":"s","position":0,"payload":{"metric":"a","value":1}}]}),
    );
    let before = execute(&mut engine, json!({"op":"stats"}));
    assert!(engine
        .execute(json!({"op":"ingest","documents":[
            {"id":"tentative","source":"s","position":1,"payload":{"metric":"a","value":2}},
            {"id":"existing","source":"s","payload":{"metric":"a","value":999}}
        ]}))
        .is_err());
    assert_eq!(execute(&mut engine, json!({"op":"stats"})), before);
    assert_eq!(
        execute(&mut engine, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        1
    );
    assert_eq!(
        execute(&mut engine, json!({"op":"documents"}))["documents"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(engine
        .execute(json!({"op":"ingest","documents":[{"payload":{},"position":9007199254740992u64}]}))
        .is_err());
}

#[test]
fn exclusive_owner_prevents_lost_updates_and_can_be_reopened() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metrics.db");
    let mut first = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    insert(&mut first, "same", 2.0);
    assert!(Engine::open(path.to_str().unwrap(), METRICS)
        .err()
        .unwrap()
        .contains("already open"));
    drop(first);
    let mut second = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    insert(&mut second, "same", 3.0);
    drop(second);
    let mut first = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    insert(&mut first, "same", 4.0);
    assert_eq!(
        execute(&mut first, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        9
    );
    assert_eq!(execute(&mut first, json!({"op":"stats"}))["documents"], 3);
}

#[test]
fn ordered_structured_catalogue_uses_the_same_native_runtime() {
    let mut engine = Engine::open(":memory:", CATALOGUE).unwrap();
    execute(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"payload":{"category":"books","id":"a","title":"Original"}},
            {"payload":{"category":"books","id":"b","title":"Second"}},
            {"payload":{"category":"books","id":"a","title":"Revised"}}
        ]}),
    );
    execute(
        &mut engine,
        json!({"op":"ingest","documents":[{"payload":{"category":"books","id":"b","title":"Second","deleted":true}}]}),
    );
    let query = execute(&mut engine, json!({"op":"query","key":"books"}));
    assert_eq!(
        query["rows"][0]["result"],
        json!({"count":1,"items":{"a":"Revised"}})
    );
}

#[test]
fn canonical_keys_match_javascript_numbers_and_utf16_object_order() {
    assert_eq!(
        canonical_json(&json!({"z":1.0,"a":-0.0,"b":[1e-7,1e21,1e20]})).unwrap(),
        "{\"a\":0,\"b\":[1e-7,1e+21,100000000000000000000],\"z\":1}"
    );
    assert_eq!(
        canonical_json(&json!({"\u{e000}":1,"\u{10000}":2})).unwrap(),
        "{\"𐀀\":2,\"\":1}"
    );
}

fn failing_module(loop_forever: bool) -> Vec<u8> {
    let descriptor =
        r#"{"ok":{"abi":1,"name":"test","version":"1","state_version":"1","mergeable":false}}"#;
    let escaped: String = descriptor
        .bytes()
        .map(|byte| format!("\\{byte:02x}"))
        .collect();
    let failure = if loop_forever {
        "(loop $forever (br $forever)) (i32.const 0)"
    } else {
        "unreachable"
    };
    wat::parse_str(format!(
        r#"(module
        (memory (export "memory") 10 256)
        (data (i32.const 300000) "{escaped}")
        (func (export "abi_version") (result i32) i32.const 1)
        (func (export "input_ptr") (result i32) i32.const 0)
        (func (export "input_capacity") (result i32) i32.const 262144)
        (func (export "output_ptr") (result i32) i32.const 300000)
        (func (export "execute") (param i32) (result i32)
          (if (result i32) (i32.eq (i32.load8_u (i32.const 7)) (i32.const 100))
              (then (i32.const {length})) (else {failure}))))"#,
        length = descriptor.len()
    ))
    .unwrap()
}

#[test]
fn wasm_traps_and_fuel_exhaustion_are_atomic_and_do_not_hang() {
    for looping in [false, true] {
        let mut engine = Engine::open(":memory:", &failing_module(looping)).unwrap();
        let error = engine
            .execute(json!({"op":"ingest","documents":[{"payload":{"anything":true}}]}))
            .unwrap_err();
        assert!(error.contains("Wasm execution failed"), "{error}");
        assert_eq!(execute(&mut engine, json!({"op":"stats"}))["documents"], 0);
        assert_eq!(execute(&mut engine, json!({"op":"stats"}))["sequence"], 0);
    }
    let imports = wat::parse_str(
        "(module (import \"env\" \"f\" (func)) (memory (export \"memory\") 10 256))",
    )
    .unwrap();
    assert!(Engine::open(":memory:", &imports)
        .err()
        .unwrap()
        .contains("must not import"));
    let unbounded = wat::parse_str("(module (memory (export \"memory\") 10))").unwrap();
    assert!(Engine::open(":memory:", &unbounded)
        .err()
        .unwrap()
        .contains("16 MiB"));
}

#[test]
fn document_ids_are_source_scoped_and_retries_preserve_identity() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    execute(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"id":"same","source":"a","position":3,"payload":{"metric":"x","value":1}},
            {"id":"same","source":"b","position":7,"payload":{"metric":"x","value":2}}
        ]}),
    );
    let replay = execute(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"id":"same","source":"a","payload":{"metric":"x","value":1}},
            {"id":"same","source":"b","payload":{"metric":"x","value":2}}
        ]}),
    );
    assert_eq!(replay["replayed"], 2);
    assert_eq!(replay["documents"][0]["position"], 3);
    assert_eq!(replay["documents"][1]["position"], 7);
    assert!(engine
        .execute(json!({"op":"ingest","documents":[
            {"id":"same","source":"a","position":4,"payload":{"metric":"x","value":1}}
        ]}))
        .is_err());
    assert_eq!(execute(&mut engine, json!({"op":"stats"}))["documents"], 2);
}

fn static_response_module(response: Value) -> Vec<u8> {
    let text = response.to_string();
    let escaped: String = text.bytes().map(|byte| format!("\\{byte:02x}")).collect();
    wat::parse_str(format!(
        r#"(module
        (memory (export "memory") 10 256)
        (data (i32.const 300000) "{escaped}")
        (func (export "abi_version") (result i32) i32.const 1)
        (func (export "input_ptr") (result i32) i32.const 0)
        (func (export "input_capacity") (result i32) i32.const 262144)
        (func (export "output_ptr") (result i32) i32.const 300000)
        (func (export "execute") (param i32) (result i32) i32.const {length}))"#,
        length = text.len()
    ))
    .unwrap()
}

#[test]
fn malformed_descriptors_and_ambiguous_envelopes_are_rejected() {
    let valid = json!({"abi":1,"name":"valid","version":"1","state_version":"1","mergeable":false});
    for field in ["name", "version", "state_version"] {
        let mut descriptor = valid.clone();
        descriptor[field] = json!("");
        assert!(Engine::open(
            ":memory:",
            &static_response_module(json!({"ok":descriptor}))
        )
        .err()
        .unwrap()
        .contains("descriptor"));
    }
    let mut invalid = valid.clone();
    invalid["mergeable"] = json!("true");
    assert!(
        Engine::open(":memory:", &static_response_module(json!({"ok":invalid})))
            .err()
            .unwrap()
            .contains("descriptor")
    );
    assert!(Engine::open(
        ":memory:",
        &static_response_module(json!({"ok":valid,"error":"ambiguous"}))
    )
    .err()
    .unwrap()
    .contains("exactly one"));
    assert!(
        Engine::open(":memory:", &static_response_module(json!({"error":42})))
            .err()
            .unwrap()
            .contains("string")
    );
    assert!(Engine::open(":memory:", b"(module)").is_err());
}

#[test]
fn browser_number_spellings_are_normalized_for_mapping_export_and_replay() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("large-numbers.db");
    let mut engine = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    // JSON.stringify(1e18 + 128) emits this decimal spelling even though its
    // integer mathematical value differs from the represented binary double.
    let wire = r#"{"op":"ingest","documents":[{"id":"large","payload":{"metric":"x","value":1000000000000000100,"nested":[9007199254740993]}}]}"#;
    let inserted = execute(&mut engine, serde_json::from_str(wire).unwrap());
    let payload = inserted["documents"][0]["payload"].clone();
    let expected = 1e18_f64 + 128.0;
    assert_eq!(payload["value"].as_f64(), Some(expected));
    assert!(payload["value"].is_f64());
    assert_eq!(payload["nested"][0].as_f64(), Some(9_007_199_254_740_992.0));
    let rows = execute(&mut engine, json!({"op":"query"}));
    assert_eq!(rows["rows"][0]["result"]["sum"].as_f64(), Some(expected));
    let exported = execute(&mut engine, json!({"op":"documents"}));
    assert_eq!(payload, exported["documents"][0]["payload"]);
    let replay = execute(&mut engine, serde_json::from_str(wire).unwrap());
    assert_eq!(replay["replayed"], 1);
    let replay = execute(
        &mut engine,
        json!({"op":"ingest","documents":[{"id":"large","payload":payload}]}),
    );
    assert_eq!(replay["replayed"], 1);
    // Replies use a float/exponent representation, retaining double semantics
    // for clients such as Python whose JSON integer parser is arbitrary precision.
    let reply = serde_json::to_string(&exported).unwrap();
    assert!(reply.contains("1.0000000000000001e18"), "{reply}");
    drop(engine);
    let mut reopened = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    assert_eq!(execute(&mut reopened, json!({"op":"documents"})), exported);
}

#[test]
fn finite_float_bits_survive_json_parsing_mapping_storage_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("precise-floats.db");
    let mut engine = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    let decimals = ["-9.281557522556183e-188", "-1.7486753975874687e180"];
    for (index, decimal) in decimals.iter().enumerate() {
        let wire = format!(
            r#"{{"op":"ingest","documents":[{{"id":"{index}","payload":{{"metric":"m{index}","value":{decimal}}}}}]}}"#
        );
        let inserted = execute(&mut engine, serde_json::from_str(&wire).unwrap());
        let expected = decimal.parse::<f64>().unwrap().to_bits();
        let payload = &inserted["documents"][0]["payload"];
        assert_eq!(payload["value"].as_f64().unwrap().to_bits(), expected);
        // Both parsing the request and encoding the normalized payload for Wasm
        // must preserve the original binary double.
        let row = execute(&mut engine, json!({"op":"query","key":format!("m{index}")}));
        assert_eq!(
            row["rows"][0]["result"]["sum"].as_f64().unwrap().to_bits(),
            expected
        );
    }
    drop(engine);
    let mut reopened = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    let exported = execute(&mut reopened, json!({"op":"documents"}));
    for (index, decimal) in decimals.iter().enumerate() {
        let expected = decimal.parse::<f64>().unwrap().to_bits();
        assert_eq!(
            exported["documents"][index]["payload"]["value"]
                .as_f64()
                .unwrap()
                .to_bits(),
            expected
        );
        let response = serde_json::to_string(&exported["documents"][index]["payload"]).unwrap();
        let reparsed: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(reparsed["value"].as_f64().unwrap().to_bits(), expected);
    }
}

#[test]
fn advertised_batch_limit_processes_1024_normal_documents() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    let documents: Vec<_> = (0..1024)
        .map(|value| json!({"payload":{"metric":"x","value":value}}))
        .collect();
    let ingested = execute(&mut engine, json!({"op":"ingest","documents":documents}));
    assert_eq!(ingested["inserted"], 1024);
    let rows = execute(&mut engine, json!({"op":"query"}));
    assert_eq!(rows["rows"][0]["result"]["count"], 1024);
    assert_eq!(rows["rows"][0]["result"]["sum"], 523776);
}

#[test]
fn bounded_fuel_processes_128_documents_with_varied_float_magnitudes() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    // Keep values positive to avoid cancellation obscuring an overflow; magnitudes
    // span denormals, normal tiny values and large finite exponents.
    let values: Vec<_> = (0..128)
        .map(|index| {
            let exponent = -320 + index * 628 / 127;
            1.2345678901234567 * 10.0_f64.powi(exponent)
        })
        .collect();
    let expected: f64 = values.iter().sum();
    let documents: Vec<_> = values
        .iter()
        .map(|value| json!({"payload":{"metric":"x","value":value}}))
        .collect();
    let ingested = execute(&mut engine, json!({"op":"ingest","documents":documents}));
    assert_eq!(ingested["inserted"], 128);
    let rows = execute(&mut engine, json!({"op":"query"}));
    assert_eq!(rows["rows"][0]["result"]["count"], 128);
    assert_eq!(
        rows["rows"][0]["result"]["sum"].as_f64().unwrap().to_bits(),
        expected.to_bits()
    );
}

#[test]
fn response_depth_matches_browser_envelope_counting() {
    let mut nested = Value::Null;
    for _ in 0..65 {
        nested = json!([nested]);
    }
    let descriptor = json!({"abi":1,"name":"deep","version":"1","state_version":"1","mergeable":false,"extra":nested});
    let error = Engine::open(
        ":memory:",
        &static_response_module(json!({"ok":descriptor})),
    )
    .err()
    .unwrap();
    assert!(error.contains("nesting exceeds 64"), "{error}");
    // Envelope=0, descriptor=1, extra=2, then 62 nested arrays put the final
    // primitive leaf at exactly64 and remain valid.
    let mut nested = Value::Null;
    for _ in 0..62 {
        nested = json!([nested]);
    }
    let descriptor = json!({"abi":1,"name":"boundary","version":"1","state_version":"1","mergeable":false,"extra":nested});
    assert!(Engine::open(
        ":memory:",
        &static_response_module(json!({"ok":descriptor}))
    )
    .is_ok());
}

fn moving_output_module(output_pointer: usize) -> Vec<u8> {
    let response =
        json!({"ok":{"abi":1,"name":"moved","version":"1","state_version":"1","mergeable":false}})
            .to_string();
    let escaped: String = response
        .bytes()
        .map(|byte| format!("\\{byte:02x}"))
        .collect();
    wat::parse_str(format!(
        r#"(module
        (memory (export "memory") 16 256)
        (global $output (mut i32) (i32.const 300000))
        (data (i32.const 600000) "{escaped}")
        (func (export "abi_version") (result i32) i32.const 1)
        (func (export "input_ptr") (result i32) i32.const 0)
        (func (export "input_capacity") (result i32) i32.const 262144)
        (func (export "output_ptr") (result i32) global.get $output)
        (func (export "execute") (param i32) (result i32)
            i32.const {output_pointer} global.set $output i32.const {length}))"#,
        length = response.len()
    ))
    .unwrap()
}

#[test]
fn output_pointer_is_read_and_validated_after_execution() {
    let mut engine = Engine::open(":memory:", &moving_output_module(600000)).unwrap();
    assert_eq!(
        execute(&mut engine, json!({"op":"descriptor"}))["name"],
        "moved"
    );
    for pointer in [100, 1048500] {
        let error = Engine::open(":memory:", &moving_output_module(pointer))
            .err()
            .unwrap();
        assert!(
            error.contains("output buffer must be in bounds and disjoint"),
            "{error}"
        );
    }
}

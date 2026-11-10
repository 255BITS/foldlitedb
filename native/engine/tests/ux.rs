use foldlitedb_portable_engine::{
    Engine, ErrorKind, ReducerInstance, ABI_BUFFER_BYTES, MAX_BATCH_DOCUMENTS, MAX_BATCH_EMISSIONS,
    MAX_JSON_DEPTH, MAX_LOG_FRAME_BYTES, MAX_SAFE_INTEGER,
};
use serde_json::{json, Value};
use std::fs;

const METRICS: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/metrics.wasm");
const CATALOGUE: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/catalogue.wasm");
const RECOVERY: &str = "Obtain a fresh snapshot with db.query() and replace the previous results before resuming changes.";

fn call(engine: &mut Engine, request: Value) -> Value {
    engine.execute(request).unwrap()
}

#[test]
fn named_batches_allocate_per_source_and_keep_retry_positions() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    let initial = json!({"op":"ingest","documents":[
        {"id":"same","source":"a","position":10,"payload":{"metric":"x","value":1}},
        {"id":"same","source":"b","payload":{"metric":"x","value":2}},
        {"id":"next","source":"a","payload":{"metric":"x","value":3}},
        {"id":"same","source":"b","payload":{"metric":"x","value":2}}
    ]});
    let inserted = call(&mut engine, initial.clone());
    assert_eq!(inserted["inserted"], 3);
    assert_eq!(inserted["replayed"], 1);
    let positions: Vec<_> = inserted["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|doc| doc["position"].clone())
        .collect();
    assert_eq!(positions, vec![json!(10), json!(0), json!(11), json!(0)]);
    let before = call(&mut engine, json!({"op":"stats"}));
    let replay = call(&mut engine, initial);
    assert_eq!(replay["inserted"], 0);
    assert_eq!(replay["replayed"], 4);
    assert_eq!(replay["documents"], inserted["documents"]);
    assert_eq!(call(&mut engine, json!({"op":"stats"})), before);
    assert_eq!(before["checkpoints"], json!({"a":11,"b":0}));
}

#[test]
fn auto_position_conflict_rolls_back_the_whole_batch_and_log() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("atomic.fold");
    let mut engine = Engine::open(path.to_str().unwrap(), METRICS).unwrap();
    call(
        &mut engine,
        json!({"op":"ingest","documents":[{"id":"old","payload":1}]}),
    );
    let before = call(&mut engine, json!({"op":"stats"}));
    let log = fs::read(&path).unwrap();
    let error = engine
        .execute(json!({"op":"ingest","documents":[
            {"id":"new","payload":2}, {"id":"old","payload":999}
        ]}))
        .unwrap_err();
    assert!(error.contains("conflicts"));
    assert_eq!(call(&mut engine, json!({"op":"stats"})), before);
    assert_eq!(fs::read(&path).unwrap(), log);
    let next = call(
        &mut engine,
        json!({"op":"ingest","documents":[{"id":"new","payload":2}]}),
    );
    assert_eq!(next["documents"][0]["position"], 1);
}

#[test]
fn preview_maps_valid_and_malformed_catalogue_payloads_without_writes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("preview.fold");
    let mut engine = Engine::open(path.to_str().unwrap(), CATALOGUE).unwrap();
    call(&mut engine, json!({"op":"checkpoint"}));
    let before = call(&mut engine, json!({"op":"stats"}));
    let log = fs::read(&path).unwrap();
    let cache_path = path.with_extension("fold.cache");
    let cache = fs::read(&cache_path).unwrap();
    assert_eq!(
        call(
            &mut engine,
            json!({"op":"preview","payload":{
                "category":"books","id":"a","title":"Original"
            }})
        ),
        json!([{"key":"books","value":{"id":"a","title":"Original","deleted":false}}])
    );
    assert_eq!(
        call(
            &mut engine,
            json!({"op":"preview","payload":{
                "category":"books","id":"a","name":"Wrong field"
            }})
        ),
        json!([])
    );
    assert_eq!(call(&mut engine, json!({"op":"stats"})), before);
    assert_eq!(fs::read(&path).unwrap(), log);
    assert_eq!(fs::read(&cache_path).unwrap(), cache);
    assert_eq!(
        call(&mut engine, json!({"op":"documents"}))["documents"],
        json!([])
    );
    let added = call(
        &mut engine,
        json!({"op":"ingest","documents":[{"payload":null}]}),
    );
    assert_eq!(added["documents"][0]["position"], 0);
}

// This module permits descriptor and map only. Any reducer operation traps.
fn map_only_module(response: Value) -> Vec<u8> {
    raw_map_only_module(&response.to_string())
}

// The raw form keeps integer spellings that serde_json::Value cannot hold.
fn raw_map_only_module(response: &str) -> Vec<u8> {
    let descriptor = json!({"ok":{"abi":1,"name":"map-only","version":"1","state_version":"1","mergeable":false}}).to_string();
    let escape =
        |text: &str| -> String { text.bytes().map(|byte| format!("\\{byte:02x}")).collect() };
    wat::parse_str(format!(
        r#"(module
      (memory (export "memory") 16 256)
      (global $output (mut i32) (i32.const 300000))
      (data (i32.const 300000) "{descriptor_data}")
      (data (i32.const 400000) "{response_data}")
      (func (export "abi_version") (result i32) i32.const 1)
      (func (export "input_ptr") (result i32) i32.const 0)
      (func (export "input_capacity") (result i32) i32.const 262144)
      (func (export "output_ptr") (result i32) global.get $output)
      (func (export "execute") (param i32) (result i32)
        (if (result i32) (i32.eq (i32.load8_u (i32.const 7)) (i32.const 100))
          (then i32.const {descriptor_len})
          (else
            (if (result i32) (i32.eq (i32.load8_u (i32.const 2)) (i32.const 100))
              (then i32.const 400000 global.set $output i32.const {response_len})
              (else unreachable))))))"#,
        descriptor_data = escape(&descriptor),
        response_data = escape(response),
        descriptor_len = descriptor.len(),
        response_len = response.len()
    ))
    .unwrap()
}

#[test]
fn preview_only_maps_and_normalizes_emissions_without_extra_fields() {
    // 9007199254740994 is an exact double; 1000000000000000100 is the
    // JavaScript shortest spelling of the double 1e18 + 128.
    let module = map_only_module(json!({"ok":[[{
        "key":9007199254740994_u64,"value":{"large":1000000000000000100_u64,"zero":-0.0},"ignored":true
    }]]}));
    let mut engine = Engine::open(":memory:", &module).unwrap();
    let before = call(&mut engine, json!({"op":"stats"}));
    let expected =
        json!([{"key":9007199254740994.0_f64,"value":{"large":1e18_f64+128.0,"zero":-0.0}}]);
    for _ in 0..2 {
        let preview = call(&mut engine, json!({"op":"preview","payload":null}));
        assert_eq!(preview, expected);
        assert_eq!(
            preview[0]["value"]["zero"].as_f64().unwrap().to_bits(),
            (-0.0_f64).to_bits()
        );
    }
    let error = engine
        .execute(json!({"op":"ingest","documents":[{"payload":null}]}))
        .unwrap_err();
    assert!(error.contains("Wasm execution failed"), "{error}");
    assert_eq!(call(&mut engine, json!({"op":"stats"})), before);
}

#[test]
fn public_limits_match_the_documented_contract() {
    assert_eq!(ABI_BUFFER_BYTES, 262_144);
    assert_eq!(MAX_BATCH_DOCUMENTS, 1024);
    assert_eq!(MAX_BATCH_EMISSIONS, 4096);
    assert_eq!(MAX_LOG_FRAME_BYTES, 8 * 1024 * 1024);
    assert_eq!(MAX_JSON_DEPTH, 64);
    assert_eq!(MAX_SAFE_INTEGER, (1 << 53) - 1);
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    let documents: Vec<Value> = (0..MAX_BATCH_DOCUMENTS)
        .map(|_| json!({"payload":{"metric":"x","value":1}}))
        .collect();
    assert_eq!(
        call(&mut engine, json!({"op":"ingest","documents":documents}))["inserted"],
        MAX_BATCH_DOCUMENTS
    );
    // A single payload just above the ABI limit names the map operation.
    let padding = "x".repeat(ABI_BUFFER_BYTES);
    let error = engine
        .execute(json!({"op":"preview","payload":{"metric":"x","value":1,"padding":padding}}))
        .unwrap_err();
    assert!(error.starts_with("Wasm map request is"), "{error}");
    assert!(error.contains(&ABI_BUFFER_BYTES.to_string()), "{error}");
}

#[test]
fn inexact_integer_view_outputs_are_rejected_before_any_write() {
    // A guest using 64-bit integer arithmetic can emit integers that no
    // double represents; they are rejected rather than silently rounded.
    for (response, literal) in [
        (
            r#"{"ok":[[{"key":9007199254740993,"value":1}]]}"#,
            "9007199254740993",
        ),
        (
            r#"{"ok":[[{"key":"x","value":{"n":18446744073709551615}}]]}"#,
            "18446744073709551615",
        ),
        (
            r#"{"ok":[[{"key":"x","value":-9223372036854775807}]]}"#,
            "-9223372036854775807",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("guest.fold");
        let mut engine =
            Engine::open(path.to_str().unwrap(), &raw_map_only_module(response)).unwrap();
        let log = fs::read(&path).unwrap();
        for request in [
            json!({"op":"preview","payload":null}),
            json!({"op":"ingest","documents":[{"payload":null}]}),
        ] {
            let error = engine.execute(request).unwrap_err();
            assert!(
                error.contains(literal) && error.contains("not exactly representable"),
                "{error}"
            );
        }
        assert_eq!(fs::read(&path).unwrap(), log);
        assert_eq!(call(&mut engine, json!({"op":"stats"}))["documents"], 0);
    }
}

#[test]
fn shared_mapping_preserves_signed_zero_for_existing_custom_reducers() {
    // The map emits -0.0. The reducer traps unless its serialized input still
    // contains the minus sign, protecting the pre-preview ingestion contract.
    let descriptor = json!({"ok":{"abi":1,"name":"signed-zero","version":"1","state_version":"1","mergeable":false}}).to_string();
    let map = r#"{"ok":[[{"key":"zero","value":-0.0}]]}"#;
    let null = r#"{"ok":null}"#;
    let accepted = r#"{"ok":true}"#;
    let escape =
        |text: &str| -> String { text.bytes().map(|byte| format!("\\{byte:02x}")).collect() };
    let module = wat::parse_str(format!(
        r#"(module
      (memory (export "memory") 16 256)
      (global $output (mut i32) (i32.const 300000))
      (data (i32.const 300000) "{descriptor_data}")
      (data (i32.const 400000) "{map_data}")
      (data (i32.const 500000) "{null_data}")
      (data (i32.const 500100) "{accepted_data}")
      (func (export "abi_version") (result i32) i32.const 1)
      (func (export "input_ptr") (result i32) i32.const 0)
      (func (export "input_capacity") (result i32) i32.const 262144)
      (func (export "output_ptr") (result i32) global.get $output)
      (func $reply (param $ptr i32) (param $len i32) (result i32)
        local.get $ptr global.set $output local.get $len)
      (func (export "execute") (param $length i32) (result i32) (local $index i32)
        (if (i32.eq (i32.load8_u (i32.const 7)) (i32.const 100))
          (then (return (call $reply (i32.const 300000) (i32.const {descriptor_len})))))
        (if (i32.eq (i32.load8_u (i32.const 2)) (i32.const 100))
          (then (return (call $reply (i32.const 400000) (i32.const {map_len})))))
        (if (i32.eq (i32.load8_u (i32.const 7)) (i32.const 97))
          (then
            (block $done (loop $scan
              (br_if $done (i32.ge_u (local.get $index) (local.get $length)))
              (if (i32.eq (i32.load8_u (local.get $index)) (i32.const 45))
                (then (return (call $reply (i32.const 500100) (i32.const {accepted_len})))))
              local.get $index i32.const 1 i32.add local.set $index
              br $scan))
            unreachable))
        (call $reply (i32.const 500000) (i32.const {null_len}))))"#,
        descriptor_data = escape(&descriptor),
        map_data = escape(map),
        null_data = escape(null),
        accepted_data = escape(accepted),
        descriptor_len = descriptor.len(),
        map_len = map.len(),
        null_len = null.len(),
        accepted_len = accepted.len()
    ))
    .unwrap();
    let mut engine = Engine::open(":memory:", &module).unwrap();
    let preview = call(&mut engine, json!({"op":"preview","payload":null}));
    assert_eq!(
        preview[0]["value"].as_f64().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    call(
        &mut engine,
        json!({"op":"ingest","documents":[{"payload":null}]}),
    );
    assert_eq!(
        call(&mut engine, json!({"op":"query"}))["rows"][0]["state"],
        true
    );
}

#[test]
fn failed_maps_and_invalid_emissions_leave_state_log_and_cache_unchanged() {
    let cases = vec![
        (json!({"error":"bad map"}), "bad map"),
        (json!({"ok":{}}), "one contribution array"),
        (json!({"ok":[]}), "wrong number"),
        (json!({"ok":[1]}), "must be an array"),
        (json!({"ok":[[{}]]}), "requires key"),
        (json!({"ok":[[{"key":"x"}]]}), "requires value"),
        (
            json!({"ok":[vec![json!({"key":"x","value":0});4097]]}),
            "4096 contributions",
        ),
    ];
    for (response, expected) in cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("invalid.fold");
        let mut engine = Engine::open(path.to_str().unwrap(), &map_only_module(response)).unwrap();
        call(&mut engine, json!({"op":"checkpoint"}));
        let before = call(&mut engine, json!({"op":"stats"}));
        let log = fs::read(&path).unwrap();
        let cache_path = path.with_extension("fold.cache");
        let cache = fs::read(&cache_path).unwrap();
        let preview_error = engine
            .execute(json!({"op":"preview","payload":null}))
            .unwrap_err();
        let ingest_error = engine
            .execute(json!({"op":"ingest","documents":[{"payload":null}]}))
            .unwrap_err();
        assert!(preview_error.contains(expected), "{preview_error}");
        assert_eq!(preview_error, ingest_error);
        assert_eq!(call(&mut engine, json!({"op":"stats"})), before);
        assert_eq!(fs::read(&path).unwrap(), log);
        assert_eq!(fs::read(&cache_path).unwrap(), cache);
    }
}

#[test]
fn preview_and_ingest_preserve_ieee_mapping_and_do_not_promise_reducer_success() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    for decimal in ["-9.281557522556183e-188", "1.0000000000000001e18", "5e-324"] {
        let payload: Value =
            serde_json::from_str(&format!(r#"{{"metric":"x","value":{decimal}}}"#)).unwrap();
        let emissions = call(&mut engine, json!({"op":"preview","payload":payload}));
        assert_eq!(
            emissions[0]["value"].as_f64().unwrap().to_bits(),
            decimal.parse::<f64>().unwrap().to_bits()
        );
        assert_eq!(
            emissions,
            call(&mut engine, json!({"op":"preview","payload":payload}))
        );
    }
    call(
        &mut engine,
        json!({"op":"ingest","documents":[{"payload":{"metric":"huge","value":1e308}}]}),
    );
    let before = call(&mut engine, json!({"op":"stats"}));
    assert_eq!(
        call(
            &mut engine,
            json!({"op":"preview","payload":{"metric":"huge","value":1e308}})
        ),
        json!([{"key":"huge","value":1e308}])
    );
    assert!(engine
        .execute(json!({"op":"ingest","documents":[{"payload":{"metric":"huge","value":1e308}}]}))
        .is_err());
    assert_eq!(call(&mut engine, json!({"op":"stats"})), before);
}

#[test]
fn batch_and_cursor_errors_give_recovery_and_closed_preview_fails() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    let old = call(&mut engine, json!({"op":"query"}));
    call(&mut engine, json!({"op":"rebuild"}));
    let error = engine
        .execute(json!({"op":"changes","since":old["cursor"]}))
        .unwrap_err();
    assert!(error.contains("different database, view module, or cache generation"));
    assert!(error.ends_with(RECOVERY));
    let before = call(&mut engine, json!({"op":"stats"}));
    let error = engine
        .execute(json!({"op":"ingest","documents":vec![json!({"payload":null});1025]}))
        .unwrap_err();
    assert!(
        error.contains("1024 documents")
            && error.contains("MAX_BATCH_DOCUMENTS")
            && error.contains("1025")
            && error.contains("smaller batches")
            && !error.contains("ingest_many"),
        "{error}"
    );
    let payload = json!({"metric":"x","value":1,"padding":"x".repeat(150000)});
    assert_eq!(
        call(&mut engine, json!({"op":"preview","payload":payload})),
        json!([{"key":"x","value":1}])
    );
    let error = engine
        .execute(json!({"op":"ingest","documents":[{"payload":payload},{"payload":payload}]}))
        .unwrap_err();
    // The message is host-neutral: it names the operation, size and limit.
    assert!(error.starts_with("Wasm map request is 3"), "{error}");
    assert!(error.contains("exceeds the 262144-byte ABI limit (ABI_BUFFER_BYTES)"));
    assert!(error.contains("smaller batches") && !error.contains("ingest_many"));
    assert_eq!(call(&mut engine, json!({"op":"stats"})), before);
    let error = engine.execute(json!({"op":"query","limit":0})).unwrap_err();
    assert!(!error.contains(RECOVERY));
    call(&mut engine, json!({"op":"close"}));
    assert_eq!(
        engine
            .execute(json!({"op":"preview","payload":null}))
            .unwrap_err(),
        "Database is closed"
    );
}

#[test]
fn get_strictly_validates_id_position_and_source_and_execute_typed_classifies_errors() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    call(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"id":"doc-a","source":"events","position":5,"payload":{"metric":"x","value":9}}
        ]}),
    );
    assert_eq!(
        call(
            &mut engine,
            json!({"op":"get","source":"events","id":"doc-a"})
        )["position"],
        5
    );
    assert_eq!(
        call(
            &mut engine,
            json!({"op":"get","source":"events","position":5})
        )["id"],
        "doc-a"
    );
    assert_eq!(
        call(
            &mut engine,
            json!({"op":"get","source":"events","position":6})
        ),
        Value::Null
    );

    // Reject missing selector, both selectors, non-string id, non-string source, negative position.
    for invalid in [
        json!({"op":"get","source":"events"}),
        json!({"op":"get","source":"events","id":"doc-a","position":5}),
        json!({"op":"get","source":"events","id":5}),
        json!({"op":"get","source":123,"id":"doc-a"}),
        json!({"op":"get","source":"events","position":-1}),
        json!({"op":"get","source":"events","position":"5"}),
    ] {
        let err = engine.execute_typed(invalid).unwrap_err();
        assert_eq!(err.kind, ErrorKind::DatabaseError);
    }

    // Verify structured ErrorKind classification without substring fragility.
    let conflict = engine
        .execute_typed(json!({"op":"ingest","documents":[
            {"id":"doc-a","source":"events","position":5,"payload":{"metric":"x","value":99}}
        ]}))
        .unwrap_err();
    assert_eq!(conflict.kind, ErrorKind::DocumentConflict);

    let order = engine
        .execute_typed(json!({"op":"ingest","documents":[
            {"id":"doc-b","source":"events","position":2,"payload":{"metric":"x","value":1}}
        ]}))
        .unwrap_err();
    assert_eq!(order.kind, ErrorKind::SourceOrderError);

    let expired = engine
        .execute_typed(json!({"op":"changes","since":"foreign:deadbeef:1"}))
        .unwrap_err();
    assert_eq!(expired.kind, ErrorKind::CursorExpired);
}

#[test]
fn reducer_instance_detects_prewrite_overlap_and_poisons_on_postcall_abi_violation() {
    // 1. Overlapping input_ptr (0) and output_ptr (1024) caught before writing input.
    let overlapping = wat::parse_str(
        r#"(module
          (memory (export "memory") 8 256)
          (func (export "abi_version") (result i32) i32.const 1)
          (func (export "input_ptr") (result i32) i32.const 0)
          (func (export "input_capacity") (result i32) i32.const 262144)
          (func (export "output_ptr") (result i32) i32.const 1024)
          (func (export "execute") (param i32) (result i32) i32.const 2))"#,
    )
    .unwrap();
    let mut r_overlap = ReducerInstance::new(&overlapping, 1_000_000).unwrap();
    let err = r_overlap.execute_json(b"{}").unwrap_err();
    assert!(err.contains("overlap"), "{err}");

    // 2. Post-call invalid response length (0) poisons the stateful reducer instance.
    let zero_len = wat::parse_str(
        r#"(module
          (memory (export "memory") 8 256)
          (func (export "abi_version") (result i32) i32.const 1)
          (func (export "input_ptr") (result i32) i32.const 0)
          (func (export "input_capacity") (result i32) i32.const 262144)
          (func (export "output_ptr") (result i32) i32.const 300000)
          (func (export "execute") (param i32) (result i32) i32.const 0))"#,
    )
    .unwrap();
    let mut r_poison = ReducerInstance::new(&zero_len, 1_000_000).unwrap();
    let first_err = r_poison.execute_json(b"{}").unwrap_err();
    assert!(first_err.contains("length"), "{first_err}");
    let second_err = r_poison.execute_json(b"{}").unwrap_err();
    assert!(second_err.contains("create a new instance"), "{second_err}");
}

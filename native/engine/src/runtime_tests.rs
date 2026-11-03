use super::*;
use crate::Engine as Database;

const METRICS: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/metrics.wasm");
const CATALOGUE: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/catalogue.wasm");

#[test]
fn grouped_metrics_match_legacy_and_keep_state_before_mutating_finalize() {
    let runtime = Runtime::new(METRICS).unwrap();
    assert_eq!(runtime.descriptor["reduce_groups"], true);
    let groups: Vec<_> = (0..70).map(|i| json!({"values":[i, 2, -3.5]})).collect();
    let grouped = runtime.reduce_groups(&groups).unwrap();
    for (state, result) in &grouped {
        assert!(state.get("mean").is_none());
        assert_eq!(result["mean"], result["sum"].as_f64().unwrap() / 3.0);
    }
    let mut legacy = Runtime::new(METRICS).unwrap();
    legacy.descriptor["reduce_groups"] = json!(false);
    assert_eq!(grouped, legacy.reduce_groups(&groups).unwrap());
    let next: Vec<_> = grouped
        .iter()
        .map(|(state, _)| json!({"state":state,"values":[1e-7,2]}))
        .collect();
    assert_eq!(
        runtime.reduce_groups(&next).unwrap(),
        legacy.reduce_groups(&next).unwrap()
    );
}

#[test]
fn ordered_catalogue_chunks_and_large_responses_match_legacy() {
    let runtime = Runtime::new(CATALOGUE).unwrap();
    let mut legacy = Runtime::new(CATALOGUE).unwrap();
    legacy.descriptor["reduce_groups"] = json!(false);
    let groups: Vec<_> = (0..65)
        .map(|i| {
            json!({"values":[
        {"id":"x","title":format!("first{i}"),"deleted":false},
        {"id":"x","title":"last","deleted":false},
        {"id":"y","title":"drop","deleted":false},
        {"id":"y","title":null,"deleted":true}]})
        })
        .collect();
    let results = runtime.reduce_groups(&groups).unwrap();
    assert_eq!(results, legacy.reduce_groups(&groups).unwrap());
    assert!(results
        .iter()
        .all(|(state, result)| *state == json!({"x":"last"}) && result["count"] == 1));
    // Together these requests exceed 256 KiB; a singleton combined response
    // also exceeds 256 KiB even though its add/finalize responses each fit.
    let groups: Vec<_> = (0..3)
        .map(|_| {
            json!({"values":[
        {"id":"x","title":"x".repeat(140_000),"deleted":false}]})
        })
        .collect();
    assert_eq!(
        runtime.reduce_groups(&groups).unwrap(),
        legacy.reduce_groups(&groups).unwrap()
    );
}

// A tiny import-free ABI-v1 fixture with independently selectable operation
// responses. It intentionally has no SDK, so host protocol validation is tested.
fn fixture(
    group_response: Value,
    capability: bool,
    group_code: &str,
    identity_code: &str,
    add_code: &str,
) -> Vec<u8> {
    let descriptor = json!({"ok":{"abi":1,"name":"fixture","version":"1","state_version":"1","mergeable":false,"reduce_groups":capability}});
    let responses = [
        descriptor,
        group_response,
        json!({"ok":"identity"}),
        json!({"ok":null}),
        json!({"ok":1}),
        json!({"ok":[[{"key":"a","value":1},{"key":"b","value":1}]]}),
    ];
    let names = [
        "descriptor",
        "reduce_groups",
        "identity",
        "add",
        "finalize",
        "map",
    ];
    let mut data = String::new();
    let mut arms = String::new();
    for (i, (name, response)) in names.iter().zip(&responses).enumerate() {
        let response = response.to_string();
        let escaped: String = response.bytes().map(|b| format!("\\{b:02x}")).collect();
        let needle = format!("\"op\":\"{name}\"");
        let escaped_needle: String = needle.bytes().map(|b| format!("\\{b:02x}")).collect();
        let output = 300_000 + i * 1024;
        let needle_at = 800_000 + i * 64;
        data += &format!("(data (i32.const {output}) \"{escaped}\") (data (i32.const {needle_at}) \"{escaped_needle}\")\n");
        let extra = match i {
            1 => group_code,
            2 => identity_code,
            3 => add_code,
            _ => "",
        };
        arms += &format!("local.get $len i32.const {needle_at} i32.const {} call $contains if {extra} i32.const {output} global.set $output i32.const {} return end\n", needle.len(), response.len());
    }
    wat::parse_str(format!(
        r#"(module
      (memory (export "memory") 16 256)
      (global $output (mut i32) (i32.const 300000))
      (global $calls (mut i32) (i32.const 0))
      {data}
      (func (export "abi_version") (result i32) i32.const 1)
      (func (export "input_ptr") (result i32) i32.const 0)
      (func (export "input_capacity") (result i32) i32.const 262144)
      (func (export "output_ptr") (result i32) global.get $output)
      (func $contains (param $len i32) (param $needle i32) (param $width i32) (result i32)
        (local $i i32) (local $j i32)
        (block $absent (loop $search
          local.get $i local.get $width i32.add local.get $len i32.gt_u br_if $absent
          i32.const 0 local.set $j
          (block $different (loop $compare
            local.get $j local.get $width i32.eq if i32.const 1 return end
            local.get $i local.get $j i32.add i32.load8_u
            local.get $needle local.get $j i32.add i32.load8_u i32.ne br_if $different
            local.get $j i32.const 1 i32.add local.set $j br $compare))
          local.get $i i32.const 1 i32.add local.set $i br $search)) i32.const 0)
      (func (export "execute") (param $len i32) (result i32)
        global.get $calls i32.const 1 i32.add global.set $calls
        {arms} unreachable))"#
    ))
    .unwrap()
}

#[test]
fn missing_and_null_state_are_distinct_and_legacy_calls_have_fresh_globals() {
    let only_null = r#"local.get $len i32.const 262140 i32.gt_u if unreachable end
        i32.const 1 global.get $calls i32.ne if unreachable end"#;
    let bytes = fixture(
        json!({"ok":{"split":true}}),
        true,
        "",
        "unreachable",
        only_null,
    );
    let runtime = Runtime::new(&bytes).unwrap();
    // Identity traps, so this can succeed only if null is treated as supplied.
    assert_eq!(
        runtime
            .reduce_groups(&[json!({"state":null,"values":[1]})])
            .unwrap(),
        vec![(Value::Null, json!(1))]
    );
    assert!(runtime.reduce_groups(&[json!({"values":[1]})]).is_err());
    let mut legacy = Runtime::new(&bytes).unwrap();
    legacy.descriptor["reduce_groups"] = json!(false);
    assert_eq!(
        legacy
            .reduce_groups(&[json!({"state":null,"values":[1]})])
            .unwrap(),
        vec![(Value::Null, json!(1))]
    );
}

#[test]
fn typed_fuel_exhaustion_can_fall_back_but_other_traps_and_view_errors_cannot() {
    let groups = [json!({"state":null,"values":[1]})];
    let bytes = fixture(Value::Null, true, "(loop $forever br $forever)", "", "");
    assert_eq!(
        Runtime::new(&bytes)
            .unwrap()
            .reduce_groups(&groups)
            .unwrap(),
        vec![(Value::Null, json!(1))]
    );
    for (response, code) in [
        (Value::Null, "unreachable"),
        (json!({"error":"Response exceeds ABI byte capacity"}), ""),
        (json!({"ok":{"split":true,"extra":1}}), ""),
    ] {
        let bytes = fixture(response, true, code, "", "");
        assert!(Runtime::new(&bytes)
            .unwrap()
            .reduce_groups(&groups)
            .is_err());
    }
}

#[test]
fn group_response_depth_is_measured_like_individual_legacy_responses() {
    let groups = [json!({"state":null,"values":[1]})];
    for depth in [63, 64] {
        let mut state = Value::Null;
        for _ in 0..depth {
            state = json!([state]);
        }
        let bytes = fixture(
            json!({"ok":{"groups":[{"state":state,"result":null}]}}),
            true,
            "",
            "",
            "",
        );
        assert_eq!(
            Runtime::new(&bytes).unwrap().reduce_groups(&groups).is_ok(),
            depth == 63
        );
    }
}

#[test]
fn malformed_or_rejected_group_output_cannot_commit_a_batch() {
    for response in [
        json!({"error":"view rejected group"}),
        json!({"ok":{"groups":[]}}),
        json!({"ok":{"groups":[{"state":null,"result":1},{"state":null,"result":1,"extra":false}]}}),
    ] {
        let bytes = fixture(response, true, "", "", "");
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("atomic.fold");
        let mut database = Database::open(path.to_str().unwrap(), &bytes).unwrap();
        let before = database.execute(json!({"op":"stats"})).unwrap();
        let log = std::fs::read(&path).unwrap();
        assert!(database
            .execute(json!({"op":"ingest","documents":[{"payload":{}}]}))
            .is_err());
        assert_eq!(database.execute(json!({"op":"stats"})).unwrap(), before);
        assert_eq!(std::fs::read(&path).unwrap(), log);
    }
}

#[test]
fn sdk_preserves_intermediate_identity_and_add_integer_validation() {
    let bytes = include_bytes!("../../../tests/fixtures/group_contract.wasm");
    let runtime = Runtime::new(bytes).unwrap();
    let mut legacy = Runtime::new(bytes).unwrap();
    legacy.descriptor["reduce_groups"] = json!(false);
    for groups in [
        vec![json!({"values":["discard"]})],
        vec![json!({"state":null,"values":["inexact-state"]})],
    ] {
        for engine in [&runtime, &legacy] {
            let error = engine.reduce_groups(&groups).unwrap_err();
            assert!(error.contains("not exactly representable"), "{error}");
        }
    }
    assert_eq!(
        runtime
            .reduce_groups(&[json!({"state":null,"values":[]})])
            .unwrap(),
        vec![(Value::Null, Value::Null)]
    );
    for depth in [63, 64] {
        let mut nested = Value::Null;
        for _ in 0..depth {
            nested = json!([nested]);
        }
        let request = [json!({"state":null,"values":[nested]})];
        assert!(
            runtime.reduce_groups(&request).is_ok(),
            "Request datum depth {depth}"
        );
        let response = [json!({"state":request[0]["values"][0],"values":[]})];
        assert_eq!(runtime.reduce_groups(&response).is_ok(), depth == 63);
        assert_eq!(legacy.reduce_groups(&response).is_ok(), depth == 63);
    }
}

#[test]
fn sdk_copy_parsers_propagate_generated_member_name_errors() {
    let cases: &[(&[u8], Value)] = &[
        (
            include_bytes!("../../../tests/fixtures/group_long_identity.wasm"),
            json!({"values":["discard"]}),
        ),
        (
            include_bytes!("../../../tests/fixtures/group_contract.wasm"),
            json!({"state":null,"values":["long-key"]}),
        ),
    ];
    for (bytes, group) in cases {
        let grouped = Runtime::new(bytes).unwrap();
        let mut legacy = Runtime::new(bytes).unwrap();
        legacy.descriptor["reduce_groups"] = json!(false);
        for runtime in [&grouped, &legacy] {
            let error = runtime.reduce_groups(&[group.clone()]).unwrap_err();
            assert!(error.contains("65535"), "{error}");
        }
    }
}

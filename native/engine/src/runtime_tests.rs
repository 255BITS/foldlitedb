use super::*;
use crate::Engine as Database;

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
        let output = 300_000 + i * 8192;
        let needle_at = 800_000 + i * 64;
        data += &format!(
            "(data (i32.const {output}) \"{escaped}\") (data (i32.const {needle_at}) \"{escaped_needle}\")\n"
        );
        let extra = match i {
            1 => group_code,
            2 => identity_code,
            3 => add_code,
            _ => "",
        };
        arms += &format!(
            "local.get $len i32.const {needle_at} i32.const {} call $contains if {extra} i32.const {output} global.set $output i32.const {} return end\n",
            needle.len(),
            response.len()
        );
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
fn multi_chunk_grouped_reduction_and_capacity_split_match_legacy() {
    let chunk_resp = json!({
        "ok": {
            "groups": (0..32)
                .map(|_| json!({"identity": "identity", "state": null, "result": 1}))
                .collect::<Vec<_>>()
        }
    });
    let bytes_32 = fixture(chunk_resp, true, "", "", "");
    let runtime = Runtime::new(&bytes_32).unwrap();
    let mut legacy = Runtime::new(&bytes_32).unwrap();
    legacy.descriptor["reduce_groups"] = json!(false);

    let groups_64: Vec<_> = (0..64).map(|i| json!({"values": [i]})).collect();
    assert_eq!(
        runtime.reduce_groups(&groups_64).unwrap(),
        legacy.reduce_groups(&groups_64).unwrap()
    );

    // Oversized request (> 256 KiB combined) splits down to singletons cleanly.
    let split_bytes = fixture(json!({"ok": {"split": true}}), true, "", "", "");
    let split_runtime = Runtime::new(&split_bytes).unwrap();
    let oversized_groups: Vec<_> = (0..3)
        .map(|_| json!({"state": null, "values": ["x".repeat(120_000)]}))
        .collect();
    assert_eq!(
        split_runtime.reduce_groups(&oversized_groups).unwrap(),
        vec![
            (Value::Null, json!(1)),
            (Value::Null, json!(1)),
            (Value::Null, json!(1)),
        ]
    );
}

#[test]
fn descriptor_rejects_non_boolean_reduce_groups() {
    let bad_descriptor = json!({"ok":{"abi":1,"name":"bad","version":"1","state_version":"1","mergeable":false,"reduce_groups":"yes"}}).to_string();
    let escaped: String = bad_descriptor
        .bytes()
        .map(|b| format!("\\{b:02x}"))
        .collect();
    let bytes = wat::parse_str(format!(
        r#"(module
      (memory (export "memory") 16 256)
      (data (i32.const 300000) "{escaped}")
      (func (export "abi_version") (result i32) i32.const 1)
      (func (export "input_ptr") (result i32) i32.const 0)
      (func (export "input_capacity") (result i32) i32.const 262144)
      (func (export "output_ptr") (result i32) i32.const 300000)
      (func (export "execute") (param $len i32) (result i32) i32.const {}))"#,
        bad_descriptor.len()
    ))
    .unwrap();
    assert!(Runtime::new(&bytes).is_err());
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
    let bytes = fixture(
        Value::Null,
        true,
        "(loop $forever i32.const 0 i32.const 0 i32.const 1048576 memory.fill br $forever)",
        "",
        "",
    );
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
fn group_response_depth_and_identity_presence_are_enforced() {
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

    // Existing group must NOT return `identity`; new group MUST return `identity`.
    let with_identity = fixture(
        json!({"ok":{"groups":[{"identity":0,"state":0,"result":0}]}}),
        true,
        "",
        "",
        "",
    );
    let rt_with_id = Runtime::new(&with_identity).unwrap();
    assert!(rt_with_id.reduce_groups(&[json!({"values":[1]})]).is_ok());
    assert!(rt_with_id
        .reduce_groups(&[json!({"state":0,"values":[1]})])
        .is_err());
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

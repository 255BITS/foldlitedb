use foldlitedb_portable_engine::{canonical_json, compare_json_keys, Engine};
use serde_json::{json, Value};
use std::cmp::Ordering;

const METRICS: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/metrics.wasm");

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../tests/fixtures/query_keys.json")).unwrap()
}

fn module() -> Vec<u8> {
    let fixture = fixture();
    let bytes = wat::parse_str(fixture["wat"].as_str().unwrap()).unwrap();
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(
        fixture["hex"], hex,
        "shared browser fixture must match its WAT"
    );
    bytes
}

fn execute(engine: &mut Engine, request: Value) -> Value {
    engine.execute(request).unwrap()
}

fn query_keys(engine: &mut Engine, mut request: Value) -> Value {
    request["op"] = json!("query");
    request["limit"] = json!(1024);
    Value::Array(
        execute(engine, request)["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["key"].clone())
            .collect(),
    )
}

fn ingest_keys(engine: &mut Engine) {
    execute(engine, json!({"op":"ingest","documents":[{"payload":{}}]}));
}

fn engine() -> Engine {
    let mut engine = Engine::open(":memory:", &module()).unwrap();
    ingest_keys(&mut engine);
    engine
}

#[test]
fn portable_json_collation_uses_types_numbers_compound_keys_and_unicode_scalars() {
    let fixture = fixture();
    let keys = fixture["keys"].as_array().unwrap();
    for (index, left) in keys.iter().enumerate() {
        for (other_index, right) in keys.iter().enumerate() {
            assert_eq!(
                compare_json_keys(left, right),
                index.cmp(&other_index),
                "{left} versus {right}"
            );
        }
    }
    assert_eq!(compare_json_keys(&json!(-0.0), &json!(0)), Ordering::Equal);
    assert_eq!(compare_json_keys(&json!(2.0), &json!(2)), Ordering::Equal);
    assert_eq!(
        compare_json_keys(&json!({"b":2,"a":1}), &json!({"a":1.0,"b":2.0})),
        Ordering::Equal
    );
    assert_eq!(
        compare_json_keys(&json!("\n"), &json!("\"")),
        Ordering::Less
    );
    assert_eq!(
        compare_json_keys(&json!("\""), &json!("\\")),
        Ordering::Less
    );
    let mut engine = engine();
    assert_eq!(query_keys(&mut engine, json!({})), fixture["keys"]);

    // Query presentation order must not alter the established batch sequence
    // allocation or change-feed order, which use canonical group identity.
    let changes = execute(&mut engine, json!({"op":"changes","limit":1024}));
    let actual: Vec<_> = changes["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| canonical_json(&row["key"]).unwrap())
        .collect();
    let mut expected: Vec<_> = keys
        .iter()
        .map(|key| canonical_json(key).unwrap())
        .collect();
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn ranges_distinguish_omitted_and_null_bounds_and_honor_inclusivity() {
    let mut engine = engine();
    for (request, expected) in [
        (json!({"start_key":2,"end_key":10}), json!([2, 10])),
        (
            json!({"start_key":2,"end_key":10,"inclusive_start":false}),
            json!([10]),
        ),
        (
            json!({"start_key":2,"end_key":10,"inclusive_end":false}),
            json!([2]),
        ),
        (json!({"start_key":2,"end_key":2}), json!([2])),
        (
            json!({"start_key":2,"end_key":2,"inclusive_start":false}),
            json!([]),
        ),
        (
            json!({"start_key":2,"end_key":2,"inclusive_end":false}),
            json!([]),
        ),
        (json!({"start_key":10,"end_key":2}), json!([])),
        (json!({"end_key":null}), json!([null])),
        (json!({"end_key":null,"inclusive_end":false}), json!([])),
        (
            json!({"start_key":null,"end_key":true,"inclusive_start":false}),
            json!([false, true]),
        ),
        (json!({"key":null}), json!([null])),
        (json!({"key":2.0}), json!([2])),
        (json!({"key":{"a":2.0}}), json!([{"a":2}])),
        (
            json!({"start_key":["sales","alice"],"end_key":["sales","bob"]}),
            json!([
                ["sales", "alice"],
                ["sales", "alice", 2],
                ["sales", "alice", 10]
            ]),
        ),
    ] {
        assert_eq!(
            query_keys(&mut engine, request.clone()),
            expected,
            "{request}"
        );
    }
}

#[test]
fn prefixes_are_explicit_type_specific_and_structural() {
    let mut engine = engine();
    for (request, expected) in [
        (json!({"prefix":"ali"}), json!(["alice", "alice/one"])),
        (json!({"prefix":"\u{10ffff}"}), json!(["\u{10ffff}"])),
        (json!({"prefix":"missing"}), json!([])),
        (
            json!({"prefix":["sales","alice"]}),
            json!([
                ["sales", "alice"],
                ["sales", "alice", 2],
                ["sales", "alice", 10]
            ]),
        ),
        (json!({"prefix":["sal"]}), json!([])),
        (json!({"prefix":[0.0]}), json!([[0]])),
        (json!({"prefix":[[0.0]]}), json!([[[0]], [[0], 1]])),
    ] {
        assert_eq!(
            query_keys(&mut engine, request.clone()),
            expected,
            "{request}"
        );
    }
    let fixture = fixture();
    for (prefix, matching_type) in [(json!(""), "string"), (json!([]), "array")] {
        let expected: Vec<_> = fixture["keys"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|key| {
                if matching_type == "string" {
                    key.is_string()
                } else {
                    key.is_array()
                }
            })
            .cloned()
            .collect();
        assert_eq!(
            query_keys(&mut engine, json!({"prefix":prefix})),
            json!(expected)
        );
    }
}

#[test]
fn invalid_selectors_and_continuations_fail_explicitly() {
    let mut engine = engine();
    for selector in [
        json!({"key":null,"start_key":null}),
        json!({"key":2,"prefix":[]}),
        json!({"start_key":2,"prefix":[]}),
        json!({"end_key":null,"prefix":""}),
        json!({"prefix":null}),
        json!({"prefix":2}),
        json!({"prefix":false}),
        json!({"prefix":{}}),
        json!({"inclusive_start":1}),
        json!({"inclusive_end":null}),
        json!({"inclusive_start":"true"}),
    ] {
        let mut request = selector.clone();
        request["op"] = json!("query");
        assert!(engine.execute(request).is_err(), "accepted {selector}");
    }
    let first = execute(
        &mut engine,
        json!({"op":"query","start_key":2,"end_key":10,"limit":1}),
    );
    let mut request = json!({"op":"query","start_key":2,"end_key":10,
                             "cursor":first["cursor"],"after":first["after"]});
    request["end_key"] = json!(11);
    assert!(engine
        .execute(request)
        .unwrap_err()
        .contains("different query selector"));
    let mut token: Value = serde_json::from_str(first["after"].as_str().unwrap()).unwrap();
    for forged in ["2.0", "not json", "11"] {
        token[3] = json!(forged);
        assert!(engine
            .execute(json!({"op":"query","start_key":2,"end_key":10,
            "cursor":first["cursor"],"after":token.to_string()}))
            .is_err());
    }
    for invalid in [
        json!(17),
        json!("not json"),
        json!(json!([first["cursor"], "2"]).to_string()),
    ] {
        assert!(engine
            .execute(json!({"op":"query","after":invalid}))
            .is_err());
    }
    // Page-size changes preserve the same selection and snapshot.
    let next = execute(
        &mut engine,
        json!({"op":"query","start_key":2,"end_key":10,
        "cursor":first["cursor"],"after":first["after"],"limit":25}),
    );
    assert_eq!(next["rows"][0]["key"], 10);
    assert_eq!(next["complete"], true);
}

#[test]
fn range_pages_pin_a_snapshot_while_ingestion_continues() {
    let mut engine = Engine::open(":memory:", METRICS).unwrap();
    execute(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"payload":{"metric":"alice/01","value":1}},
            {"payload":{"metric":"alice/03","value":3}},
            {"payload":{"metric":"bob/01","value":100}}
        ]}),
    );
    let first = execute(
        &mut engine,
        json!({"op":"query","prefix":"alice/","limit":1}),
    );
    execute(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"payload":{"metric":"alice/02","value":2}},
            {"payload":{"metric":"alice/03","value":30}}
        ]}),
    );
    let second = execute(
        &mut engine,
        json!({"op":"query","prefix":"alice/",
        "cursor":first["cursor"],"after":first["after"],"limit":1}),
    );
    assert_eq!(first["rows"][0]["key"], "alice/01");
    assert_eq!(second["rows"][0]["key"], "alice/03");
    assert_eq!(second["rows"][0]["result"]["sum"], 3);
    assert_eq!(second["complete"], true);
    assert!(engine
        .execute(json!({"op":"query","prefix":"alice/","after":first["after"]}))
        .is_err());
    let current = execute(&mut engine, json!({"op":"query","prefix":"alice/"}));
    assert_eq!(current["rows"].as_array().unwrap().len(), 3);
    assert_eq!(current["rows"][2]["result"]["sum"], 33);
}

#[test]
fn derived_query_index_and_continuations_survive_valid_checkpoint_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("keys.fold");
    let module = module();
    let mut engine = Engine::open(path.to_str().unwrap(), &module).unwrap();
    ingest_keys(&mut engine);
    let first = execute(
        &mut engine,
        json!({"op":"query","start_key":2,"end_key":10,"limit":1}),
    );
    let log = std::fs::read(&path).unwrap();
    drop(engine);
    let mut reopened = Engine::open(path.to_str().unwrap(), &module).unwrap();
    assert_eq!(
        execute(&mut reopened, json!({"op":"stats"}))["cache"]["status"],
        "loaded"
    );
    assert_eq!(query_keys(&mut reopened, json!({})), fixture()["keys"]);
    let second = execute(
        &mut reopened,
        json!({"op":"query","start_key":2,"end_key":10,
        "cursor":first["cursor"],"after":first["after"],"limit":1}),
    );
    assert_eq!(second["rows"][0]["key"], 10);
    assert_eq!(second["complete"], true);
    assert_eq!(std::fs::read(&path).unwrap(), log);
    drop(reopened);
    std::fs::remove_file(path.with_file_name("keys.fold.cache")).unwrap();
    let mut rebuilt = Engine::open(path.to_str().unwrap(), &module).unwrap();
    assert_eq!(query_keys(&mut rebuilt, json!({})), fixture()["keys"]);
    assert!(rebuilt
        .execute(json!({"op":"query","start_key":2,"end_key":10,
        "cursor":first["cursor"],"after":first["after"]}))
        .is_err());
}

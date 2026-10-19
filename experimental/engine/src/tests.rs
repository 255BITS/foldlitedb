use super::*;
use std::io::Write;
use std::path::PathBuf;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("foldlite-native-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self, suffix: &str) -> String {
        self.0.join(suffix).to_str().unwrap().into()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn spec() -> Value {
    json!({"name":"metrics","version":"1","key_field":"series","value_field":"value","reducer":"mean/v1"})
}
fn doc(id: &str, position: u64, key: Value, value: Value) -> Value {
    json!({"id":id,"position":position,"payload":{"series":key,"value":value}})
}
fn ingest(engine: &mut Engine, documents: Vec<Value>) -> Result<Value, String> {
    engine.execute(json!({"op":"ingest","documents":documents}))
}
fn query(engine: &mut Engine) -> Value {
    engine.execute(json!({"op":"query","limit":4096})).unwrap()
}

#[test]
fn both_backends_replay_resolved_metadata_and_reopen_without_mapping() {
    for backend in ["sqlite", "log"] {
        let directory = Directory::new();
        let path = directory.path("cache");
        let mut engine = Engine::open(&path, backend, spec(), 4).unwrap();
        let first = ingest(
            &mut engine,
            vec![json!({"id":"named","payload":{"series":"x","value":1}})],
        )
        .unwrap();
        assert_eq!(first["documents"][0]["position"], 0);
        ingest(&mut engine, vec![doc("later", 40, json!("x"), json!(2))]).unwrap();
        let retry = ingest(
            &mut engine,
            vec![json!({"id":"named","payload":{"series":"x","value":1}})],
        )
        .unwrap();
        assert_eq!(retry["replayed"], 1);
        assert_eq!(retry["documents"][0]["position"], 0);
        let offset_retry = ingest(
            &mut engine,
            vec![json!({"position":0,"payload":{"series":"x","value":1}})],
        )
        .unwrap();
        assert_eq!(offset_retry["documents"][0]["id"], "named");
        assert!(
            ingest(&mut engine, vec![doc("named", 0, json!("x"), json!(9))])
                .unwrap_err()
                .contains("DocumentConflict")
        );
        let expected = query(&mut engine)["rows"].clone();
        drop(engine);
        let mut reopened = Engine::open(&path, backend, spec(), 2).unwrap();
        assert_eq!(query(&mut reopened)["rows"], expected);
        assert_eq!(
            reopened.execute(json!({"op":"checkpoint"})).unwrap()["position"],
            40
        );
        assert_eq!(
            reopened.execute(json!({"op":"get","id":"named"})).unwrap()["document"],
            first["documents"][0]
        );
    }
}

#[test]
fn failures_leave_documents_sources_states_and_history_unchanged() {
    for backend in ["sqlite", "log"] {
        let directory = Directory::new();
        let path = directory.path("cache");
        let mut engine = Engine::open(&path, backend, spec(), 4).unwrap();
        ingest(&mut engine, vec![doc("initial", 0, json!("x"), json!(1))]).unwrap();
        let before = query(&mut engine);
        assert!(ingest(
            &mut engine,
            vec![
                doc("good", 1, json!("x"), json!(2)),
                json!({"id":"bad","position":2,"payload":{"series":"x"}})
            ]
        )
        .is_err());
        assert_eq!(query(&mut engine), before);
        assert_eq!(
            engine.execute(json!({"op":"stats"})).unwrap()["documents"],
            1
        );
        assert_eq!(
            engine.execute(json!({"op":"checkpoint"})).unwrap()["position"],
            0
        );
        assert!(ingest(
            &mut engine,
            vec![
                doc("huge", 1, json!("x"), json!(f64::MAX)),
                doc("overflow", 2, json!("x"), json!(f64::MAX))
            ]
        )
        .is_err());
        assert_eq!(query(&mut engine), before);
        assert!(ingest(
            &mut engine,
            vec![doc("source-order", 0, json!("x"), json!(1))]
        )
        .is_err());
    }
}

#[test]
fn worker_counts_and_backends_preserve_nonassociative_input_order() {
    let directory = Directory::new();
    let values = [1e16, 1.0, -1e16, 3.0, 2.0, -1.0];
    let documents: Vec<_> = (0..1024)
        .map(|i| {
            doc(
                &i.to_string(),
                i,
                json!(i % 3),
                json!(values[(i / 3) as usize % values.len()]),
            )
        })
        .collect();
    let mut expected = None;
    for backend in ["sqlite", "log"] {
        for workers in [1, 2, 4] {
            let mut engine = Engine::open(
                &directory.path(&format!("{backend}-{workers}")),
                backend,
                spec(),
                workers,
            )
            .unwrap();
            ingest(&mut engine, documents.clone()).unwrap();
            let rows = query(&mut engine)["rows"].clone();
            if let Some(expected) = &expected {
                assert_eq!(&rows, expected);
            } else {
                expected = Some(rows);
            }
        }
    }
}

#[test]
fn snapshots_and_partial_changes_pin_committed_prefixes() {
    for backend in ["sqlite", "log"] {
        let directory = Directory::new();
        let mut engine = Engine::open(&directory.path("cache"), backend, spec(), 2).unwrap();
        ingest(
            &mut engine,
            vec![
                doc("a", 0, json!("a"), json!(1)),
                doc("b", 1, json!("b"), json!(2)),
            ],
        )
        .unwrap();
        let first = engine.execute(json!({"op":"query","limit":1})).unwrap();
        assert_eq!(first["complete"], false);
        ingest(
            &mut engine,
            vec![
                doc("new-a", 2, json!("a"), json!(100)),
                doc("new-b", 3, json!("b"), json!(200)),
                doc("c", 4, json!("c"), json!(3)),
            ],
        )
        .unwrap();
        let second = engine
            .execute(
                json!({"op":"query","limit":1,"cursor":first["cursor"],"after":first["after"]}),
            )
            .unwrap();
        assert_eq!(second["rows"][0]["result"]["value"], 2.0);
        assert_eq!(second["complete"], true);
        let mut since = first["cursor"].clone();
        let mut keys = Vec::new();
        loop {
            let page = engine
                .execute(json!({"op":"changes","since":since,"limit":1}))
                .unwrap();
            keys.extend(
                page["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["key"].clone()),
            );
            since = page["cursor"].clone();
            if page["complete"] == true {
                break;
            }
        }
        assert_eq!(keys, vec![json!("a"), json!("b"), json!("c")]);
    }
}

#[test]
fn empty_emissions_still_commit_progress_and_single_writer_is_enforced() {
    for backend in ["sqlite", "log"] {
        let directory = Directory::new();
        let path = directory.path("cache");
        let mut empty_spec = spec();
        empty_spec["emissions"] = json!(0);
        let mut engine = Engine::open(&path, backend, empty_spec.clone(), 2).unwrap();
        assert!(Engine::open(&path, backend, empty_spec.clone(), 1).is_err());
        let before = query(&mut engine);
        ingest(&mut engine, vec![json!({"payload":[1,2,3]})]).unwrap();
        assert!(query(&mut engine)["rows"].as_array().unwrap().is_empty());
        assert_ne!(query(&mut engine)["cursor"], before["cursor"]);
        assert_eq!(
            engine.execute(json!({"op":"checkpoint"})).unwrap()["position"],
            0
        );
        drop(engine);
        assert!(Engine::open(&path, backend, empty_spec, 1).is_ok());
        assert!(Engine::open(&path, backend, spec(), 1).is_err());
    }
}

#[test]
fn synthetic_work_changes_results_and_preserves_worker_equivalence() {
    let directory = Directory::new();
    let mut work_spec = spec();
    work_spec["work"] = json!(17);
    let documents = vec![
        doc("a", 0, json!("x"), json!(1)),
        doc("b", 1, json!("x"), Value::Null),
    ];
    let mut one = Engine::open(&directory.path("one"), "log", work_spec.clone(), 1).unwrap();
    let mut many = Engine::open(&directory.path("many"), "log", work_spec, 4).unwrap();
    ingest(&mut one, documents.clone()).unwrap();
    ingest(&mut many, documents).unwrap();
    assert_eq!(query(&mut one)["rows"], query(&mut many)["rows"]);
    assert_ne!(query(&mut one)["rows"][0]["result"]["value"], 1.0);
}

#[test]
fn log_repairs_only_torn_tail_and_rejects_complete_corruption() {
    let directory = Directory::new();
    let path = directory.path("cache");
    let mut engine = Engine::open(&path, "log", spec(), 1).unwrap();
    ingest(&mut engine, vec![doc("a", 0, json!("x"), json!(1))]).unwrap();
    drop(engine);
    let boundary = std::fs::metadata(&path).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"FLDLOG").unwrap();
    drop(file);
    let reopened = Engine::open(&path, "log", spec(), 1).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), boundary);
    drop(reopened);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[82] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(Engine::open(&path, "log", spec(), 1)
        .err()
        .unwrap()
        .contains("checksum"));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), boundary);
}

#[test]
fn log_rejects_corrupt_acknowledged_length_without_truncating() {
    let directory = Directory::new();
    let path = directory.path("cache");
    let mut engine = Engine::open(&path, "log", spec(), 1).unwrap();
    ingest(&mut engine, vec![doc("a", 0, json!("x"), json!(1))]).unwrap();
    drop(engine);
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..8], b"FLDLOG\x02\0");
    let first_length = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let last = 80 + first_length;
    let length = u64::from_le_bytes(bytes[last + 8..last + 16].try_into().unwrap());
    bytes[last + 8..last + 16].copy_from_slice(&(length + 64).to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    assert!(Engine::open(&path, "log", spec(), 1)
        .err()
        .unwrap()
        .contains("header checksum"));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[test]
fn log_recovers_true_eof_in_header_or_body_to_previous_prefix() {
    let directory = Directory::new();
    for suffix in ["header", "body"] {
        let path = directory.path(suffix);
        let mut engine = Engine::open(&path, "log", spec(), 1).unwrap();
        ingest(&mut engine, vec![doc("a", 0, json!("x"), json!(1))]).unwrap();
        let expected = query(&mut engine);
        drop(engine);
        let committed = std::fs::read(&path).unwrap();
        let first_length = u64::from_le_bytes(committed[8..16].try_into().unwrap()) as usize;
        let last = 80 + first_length;
        let clipped = if suffix == "header" { 40 } else { 87 };
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&committed[last..last + clipped]).unwrap();
        drop(file);
        let mut reopened = Engine::open(&path, "log", spec(), 1).unwrap();
        assert_eq!(query(&mut reopened), expected);
        assert_eq!(std::fs::read(&path).unwrap(), committed);
    }
}

#[test]
fn bounds_null_filters_and_epoch_validation() {
    for backend in ["sqlite", "log"] {
        let directory = Directory::new();
        let mut engine = Engine::open(&directory.path("cache"), backend, spec(), 1).unwrap();
        assert!(ingest(&mut engine, vec![doc("a", 0, json!("x"), json!(1)); 1025]).is_err());
        ingest(
            &mut engine,
            vec![doc("a", MAX_POSITION, Value::Null, json!(1))],
        )
        .unwrap();
        assert!(ingest(
            &mut engine,
            vec![json!({"payload":{"series":"x","value":1}})]
        )
        .is_err());
        let retry = ingest(
            &mut engine,
            vec![json!({"id":"a","payload":{"series":null,"value":1}})],
        )
        .unwrap();
        assert_eq!(retry["replayed"], 1);
        let filtered = engine.execute(json!({"op":"query","key":null})).unwrap();
        assert_eq!(filtered["rows"].as_array().unwrap().len(), 1);
        let mut other = Engine::open(&directory.path("other"), backend, spec(), 1).unwrap();
        assert!(other
            .execute(json!({"op":"query","cursor":filtered["cursor"]}))
            .is_err());
    }
}

#[test]
fn point_queries_use_exact_structured_key_and_snapshot_boundary() {
    for backend in ["sqlite", "log"] {
        let directory = Directory::new();
        let mut engine = Engine::open(&directory.path("cache"), backend, spec(), 2).unwrap();
        let key = json!({"tags":["x",1],"enabled":true});
        ingest(
            &mut engine,
            vec![
                doc("a", 0, json!("before"), json!(1)),
                doc("b", 1, key.clone(), json!(2)),
                doc("c", 2, json!("after"), json!(3)),
            ],
        )
        .unwrap();
        let snapshot = query(&mut engine);
        ingest(&mut engine, vec![doc("new-b", 3, key.clone(), json!(100))]).unwrap();
        let point = engine
            .execute(json!({"op":"query","key":key,"limit":1,"cursor":snapshot["cursor"]}))
            .unwrap();
        assert_eq!(point["rows"].as_array().unwrap().len(), 1);
        assert_eq!(point["rows"][0]["result"]["value"], 2.0);
        assert_eq!(point["complete"], true);
        assert!(point["after"].is_null());
        let missing = engine
            .execute(json!({"op":"query","key":["absent"],"limit":1}))
            .unwrap();
        assert!(missing["rows"].as_array().unwrap().is_empty());
        let encoded_key = text(&key).unwrap();
        let after = serde_json::to_string(&(
            point["cursor"].as_str().unwrap(),
            encoded_key.clone(),
            Some(encoded_key),
        ))
        .unwrap();
        let completed = engine
            .execute(
                json!({"op":"query","key":key,"limit":1,"cursor":point["cursor"],"after":after}),
            )
            .unwrap();
        assert!(completed["rows"].as_array().unwrap().is_empty());
        assert_eq!(completed["complete"], true);
    }
}

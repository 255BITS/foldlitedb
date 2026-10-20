use foldlitedb_portable_engine::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

const METRICS: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/metrics.wasm");
const CATALOGUE: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/catalogue.wasm");

fn call(engine: &mut Engine, request: Value) -> Value {
    engine.execute(request).unwrap()
}
fn open(path: &Path) -> Engine {
    Engine::open(path.to_str().unwrap(), METRICS).unwrap()
}
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}
fn add(engine: &mut Engine, id: &str, value: i64) {
    call(
        engine,
        json!({"op":"ingest","documents":[{"id":id,"payload":{"metric":"x","value":value}}]}),
    );
}

#[test]
fn cache_is_disposable_and_warm_open_preserves_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 2);
    add(&mut engine, "two", 3);
    let expected = call(&mut engine, json!({"op":"query"}));
    let checkpoint = call(&mut engine, json!({"op":"checkpoint"}));
    assert_eq!(checkpoint["cache"]["status"], "checkpointed");
    assert!(checkpoint["cache"]["bytes"].as_u64().unwrap() > 0);
    let log = fs::read(&path).unwrap();
    drop(engine);
    let mut warm = open(&path);
    assert_eq!(call(&mut warm, json!({"op":"query"})), expected);
    assert_eq!(call(&mut warm, json!({"op":"stats"}))["replay_batches"], 0);
    drop(warm);
    fs::remove_file(sibling(&path, ".cache")).unwrap();
    let mut cold = open(&path);
    let actual = call(&mut cold, json!({"op":"query"}));
    assert_eq!(actual["rows"], expected["rows"]);
    assert_ne!(actual["cursor"], expected["cursor"]);
    let stats = call(&mut cold, json!({"op":"stats"}));
    assert_eq!(stats["cache"]["status"], "missing");
    assert_eq!(stats["replay"], json!({"batches":2,"documents":2}));
    assert!(cold
        .execute(json!({"op":"query","cursor":expected["cursor"]}))
        .is_err());
    drop(cold);
    let mut cache = fs::read(sibling(&path, ".cache")).unwrap();
    *cache.last_mut().unwrap() ^= 1;
    fs::write(sibling(&path, ".cache"), cache).unwrap();
    let mut recovered = open(&path);
    assert_eq!(
        call(&mut recovered, json!({"op":"stats"}))["cache"]["status"],
        "invalid"
    );
    assert_eq!(
        call(&mut recovered, json!({"op":"query"}))["rows"],
        expected["rows"]
    );
    assert_eq!(fs::read(&path).unwrap(), log);
}

#[test]
fn crash_writer_child() {
    let Some(path) = std::env::var_os("FOLD_NATIVE_CRASH_PATH") else {
        return;
    };
    let mut engine = open(Path::new(&path));
    add(&mut engine, "crashed", 7);
    // No close or Drop: acknowledged log data must survive without a checkpoint.
    std::process::exit(0);
}

#[test]
fn acknowledged_crash_suffix_replays_only_original_uncached_batch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 3);
    let original = call(&mut engine, json!({"op":"stats"}));
    drop(engine);
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_writer_child", "--nocapture"])
        .env("FOLD_NATIVE_CRASH_PATH", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let mut recovered = open(&path);
    let stats = call(&mut recovered, json!({"op":"stats"}));
    assert_eq!(stats["cache"]["status"], "loaded");
    assert_eq!(stats["generation"], original["generation"]);
    assert_eq!(stats["replay"], json!({"batches":1,"documents":1}));
    assert_eq!(stats["log"]["batches"], 2);
    assert_eq!(
        call(&mut recovered, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        10
    );
}

#[test]
fn explicit_rebuild_replays_transaction_boundaries_and_resets_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    call(
        &mut engine,
        json!({"op":"ingest","documents":[
            {"payload":{"metric":"b","value":2}}, {"payload":{"metric":"a","value":3}}
        ]}),
    );
    add(&mut engine, "one", 4);
    let rows = call(&mut engine, json!({"op":"query"}));
    let changes = call(&mut engine, json!({"op":"changes"}));
    let documents = call(&mut engine, json!({"op":"documents"}));
    let bytes = fs::read(&path).unwrap();
    let rebuilt = call(&mut engine, json!({"op":"rebuild"}));
    assert_eq!(rebuilt["rebuilt"], true);
    assert_eq!(rebuilt["replay"], json!({"batches":2,"documents":3}));
    assert_ne!(rebuilt["cursor"], rows["cursor"]);
    assert_eq!(
        call(&mut engine, json!({"op":"query"}))["rows"],
        rows["rows"]
    );
    assert_eq!(
        call(&mut engine, json!({"op":"changes"}))["rows"],
        changes["rows"]
    );
    assert_eq!(
        call(&mut engine, json!({"op":"documents"}))["documents"],
        documents["documents"]
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let cursor = rebuilt["cursor"].clone();
    call(&mut engine, json!({"op":"close"}));
    assert!(engine
        .execute(json!({"op":"stats"}))
        .unwrap_err()
        .contains("closed"));
    let mut reopened = open(&path);
    assert_eq!(call(&mut reopened, json!({"op":"stats"}))["cursor"], cursor);
    assert_eq!(
        call(&mut reopened, json!({"op":"stats"}))["replay_batches"],
        0
    );
}

#[test]
fn changed_module_rebuilds_log_and_reassigns_view_dependent_sequences() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut metrics = open(&path);
    call(
        &mut metrics,
        json!({"op":"ingest","documents":[
            {"id":"one","payload":{"metric":"same","value":1,"category":"a","id":"1","title":"One"}},
            {"id":"two","payload":{"metric":"same","value":2,"category":"b","id":"2","title":"Two"}}
        ]}),
    );
    call(
        &mut metrics,
        json!({"op":"ingest","documents":[
            {"id":"three","payload":{"metric":"same","value":3,"category":"a","id":"3","title":"Three"}}
        ]}),
    );
    let old = call(&mut metrics, json!({"op":"query"}));
    assert_eq!(
        call(&mut metrics, json!({"op":"documents"}))["documents"][2]["sequence"],
        4
    );
    drop(metrics);
    let before = fs::read(&path).unwrap();
    let mut catalogue = Engine::open(path.to_str().unwrap(), CATALOGUE).unwrap();
    let stats = call(&mut catalogue, json!({"op":"stats"}));
    assert_eq!(stats["cache"]["status"], "module-mismatch");
    assert_eq!(stats["replay_batches"], 2);
    assert_eq!(
        call(&mut catalogue, json!({"op":"documents"}))["documents"][2]["sequence"],
        5
    );
    assert!(catalogue
        .execute(json!({"op":"query","cursor":old["cursor"]}))
        .is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    call(
        &mut catalogue,
        json!({"op":"ingest","documents":[
            {"id":"four","payload":{"metric":"same","value":4,"category":"c","id":"4","title":"Four"}}
        ]}),
    );
    let expected = call(&mut catalogue, json!({"op":"query"}));
    assert_eq!(expected["rows"].as_array().unwrap().len(), 3);
    drop(catalogue);
    let mut reopened = Engine::open(path.to_str().unwrap(), CATALOGUE).unwrap();
    assert_eq!(call(&mut reopened, json!({"op":"query"})), expected);
    assert_eq!(
        call(&mut reopened, json!({"op":"stats"}))["replay_batches"],
        0
    );
    drop(reopened);
    let mut metrics = open(&path);
    assert_eq!(
        call(&mut metrics, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        10
    );
}

#[test]
fn incomplete_final_frame_header_and_payload_are_recovered_but_corruption_is_not() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 1);
    drop(engine);
    let original = fs::read(&path).unwrap();
    for fragment in [b"BA".to_vec(), original[56..original.len() - 1].to_vec()] {
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&fragment).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let mut recovered = open(&path);
        assert_eq!(
            call(&mut recovered, json!({"op":"stats"}))["log"]["recovered_tail_bytes"],
            fragment.len()
        );
        assert_eq!(fs::read(&path).unwrap(), original);
        drop(recovered);
    }
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert!(Engine::open(path.to_str().unwrap(), METRICS)
        .err()
        .unwrap()
        .contains("checksum"));
    assert_eq!(fs::read(&path).unwrap(), corrupt);
    fs::write(&path, [original.as_slice(), b"NO"].concat()).unwrap();
    assert!(Engine::open(path.to_str().unwrap(), METRICS)
        .err()
        .unwrap()
        .contains("header corrupt"));
}

#[test]
fn corrupt_lengths_headers_and_prefix_mismatches_fail_or_rebuild_safely() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 1);
    drop(engine);
    let original = fs::read(&path).unwrap();
    let mut corrupt = original.clone();
    let oversized = 8 * 1024 * 1024u64 + 1;
    corrupt[60..68].copy_from_slice(&oversized.to_le_bytes());
    corrupt[68..76].copy_from_slice(&(!oversized).to_le_bytes());
    fs::write(&path, &corrupt).unwrap();
    assert!(Engine::open(path.to_str().unwrap(), METRICS)
        .err()
        .unwrap()
        .contains("header corrupt"));
    fs::write(&path, &original).unwrap();
    let old_cache = fs::read(sibling(&path, ".cache")).unwrap();
    fs::remove_file(&path).unwrap();
    let mut replacement = open(&path);
    assert_eq!(
        call(&mut replacement, json!({"op":"stats"}))["cache"]["status"],
        "invalid"
    );
    assert_eq!(
        call(&mut replacement, json!({"op":"stats"}))["documents"],
        0
    );
    drop(replacement);
    fs::write(sibling(&path, ".cache"), old_cache).unwrap();
    assert_eq!(
        call(&mut open(&path), json!({"op":"stats"}))["documents"],
        0
    );
}

#[test]
fn semantically_invalid_cache_is_rejected_even_with_correct_outer_checksum() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 5);
    drop(engine);
    let cache_path = sibling(&path, ".cache");
    let original = fs::read(&cache_path).unwrap();
    for alter in ["document", "history", "prefix", "generation"] {
        let mut decoded: Value = serde_json::from_slice(&original[48..]).unwrap();
        match alter {
            "document" => decoded["state"]["documents"][0]["payload"]["value"] = json!(999),
            "history" => decoded["state"]["history"][0]["sequence"] = json!(1),
            "prefix" => decoded["offset"] = json!(56),
            "generation" => decoded["state"]["generation"] = json!("invalid"),
            _ => unreachable!(),
        }
        let payload = serde_json::to_vec(&decoded).unwrap();
        let mut bytes = b"FOLDCAC1".to_vec();
        bytes.extend((payload.len() as u64).to_le_bytes());
        bytes.extend(Sha256::digest(&payload));
        bytes.extend(payload);
        fs::write(&cache_path, bytes).unwrap();
        let mut recovered = open(&path);
        assert_eq!(
            call(&mut recovered, json!({"op":"stats"}))["cache"]["status"],
            "invalid",
            "{alter}"
        );
        assert_eq!(
            call(&mut recovered, json!({"op":"query"}))["rows"][0]["result"]["sum"],
            5
        );
        drop(recovered);
    }
}

#[test]
fn failed_batch_does_not_append_or_publish_and_large_emission_is_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 5);
    let original = fs::read(&path).unwrap();
    let rows = call(&mut engine, json!({"op":"query"}));
    assert!(engine
        .execute(json!({"op":"ingest","documents":[
            {"id":"tentative","payload":{"metric":"x","value":3}},
            {"id":"one","payload":{"metric":"x","value":999}}
        ]}))
        .is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    assert_eq!(call(&mut engine, json!({"op":"query"})), rows);
    assert!(engine
        .execute(json!({"op":"ingest","documents":[
            {"id":"overflow1","payload":{"metric":"overflow","value":1e308}},
            {"id":"overflow2","payload":{"metric":"overflow","value":1e308}}
        ]}))
        .is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    assert_eq!(call(&mut engine, json!({"op":"stats"}))["documents"], 1);
}

#[test]
fn external_log_modification_poisoned_writer_never_publishes_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 5);
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"BA")
        .unwrap();
    let error = engine
        .execute(json!({"op":"ingest","documents":[{"payload":{"metric":"x","value":3}}]}))
        .unwrap_err();
    assert!(error.contains("uncertain"));
    assert_eq!(call(&mut engine, json!({"op":"stats"}))["documents"], 1);
    assert!(engine
        .execute(json!({"op":"checkpoint"}))
        .unwrap_err()
        .contains("poisoned"));
    assert!(engine
        .execute(json!({"op":"ingest","documents":[{"payload":{"metric":"x","value":4}}]}))
        .unwrap_err()
        .contains("poisoned"));
    drop(engine);
    let mut recovered = open(&path);
    assert_eq!(call(&mut recovered, json!({"op":"stats"}))["documents"], 1);
}

#[cfg(unix)]
#[test]
fn log_inode_lock_rejects_symlink_and_hardlink_alias_owners() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let owner = open(&path);
    let hard = directory.path().join("hard.fold");
    let symbolic = directory.path().join("symbolic.fold");
    fs::hard_link(&path, &hard).unwrap();
    std::os::unix::fs::symlink(&path, &symbolic).unwrap();
    for alias in [&hard, &symbolic] {
        assert!(Engine::open(alias.to_str().unwrap(), METRICS)
            .err()
            .unwrap()
            .contains("already open"));
    }
    drop(owner);
    drop(open(&symbolic));
}

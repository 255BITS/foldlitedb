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
    let mut digest = Sha256::new();
    digest.update(Sha256::digest(&original[..56]));
    digest.update(&original[56..]);
    let prefix: [u8; 32] = digest.finalize().into();
    let mut interrupted = original[56..original.len() - 1].to_vec();
    interrupted[52..84].copy_from_slice(&prefix);
    for fragment in [b"BA".to_vec(), interrupted] {
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
    assert!(!engine.writer_poisoned() && engine.is_writable());
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
    assert!(engine.writer_poisoned() && !engine.is_writable());
    let stats = call(&mut engine, json!({"op":"stats"}));
    assert_eq!(stats["documents"], 1);
    assert_eq!(stats["log"]["writer_poisoned"], true);
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

#[test]
fn poisoned_writer_recovers_by_close_reopen_and_retry_with_explicit_ids() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut engine = open(&path);
    add(&mut engine, "one", 5);
    let memory = Engine::open(":memory:", METRICS).unwrap();
    assert!(!memory.writer_poisoned() && memory.is_writable());
    // An interrupted foreign append makes the next durable append fail.
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"B")
        .unwrap();
    let retry =
        json!({"op":"ingest","documents":[{"id":"two","payload":{"metric":"x","value":3}}]});
    assert!(engine.execute(retry.clone()).is_err());
    assert!(engine.writer_poisoned());
    // Reads keep serving the last acknowledged state.
    assert_eq!(
        call(&mut engine, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        5
    );
    // Close reports the checkpoint failure but still releases ownership.
    assert!(engine
        .execute(json!({"op":"close"}))
        .unwrap_err()
        .contains("poisoned"));
    assert!(!engine.writer_poisoned() && !engine.is_writable());
    let mut reopened = open(&path);
    assert!(reopened.is_writable());
    assert_eq!(
        call(&mut reopened, json!({"op":"stats"}))["log"]["recovered_tail_bytes"],
        1
    );
    let retried = call(&mut reopened, retry.clone());
    assert_eq!(retried["inserted"], 1);
    // A second identical retry is an idempotent replay.
    assert_eq!(call(&mut reopened, retry)["replayed"], 1);
    assert_eq!(
        call(&mut reopened, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        8
    );
    drop(engine);
}

#[test]
fn reset_keeps_ownership_and_validates_the_module_before_mutating_documents() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reset.fold");
    let mut owner = open(&path);
    add(&mut owner, "one", 1);
    assert!(Engine::open_with_options(path.to_str().unwrap(), METRICS, true).is_err());
    add(&mut owner, "two", 2);
    drop(owner);
    let original = fs::read(&path).unwrap();
    assert!(Engine::open_with_options(path.to_str().unwrap(), b"invalid", true).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    let mut reset = Engine::open_with_options(path.to_str().unwrap(), METRICS, true).unwrap();
    assert_eq!(call(&mut reset, json!({"op":"stats"}))["documents"], 0);
    add(&mut reset, "replacement", 3);
    assert!(Engine::open_with_options(path.to_str().unwrap(), METRICS, true).is_err());
    drop(reset);
    let mut reopened = open(&path);
    assert_eq!(call(&mut reopened, json!({"op":"stats"}))["documents"], 1);
    assert_eq!(
        call(&mut reopened, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        3
    );
}

#[cfg(unix)]
#[test]
fn reset_rejects_live_owners_through_hardlinks_and_symbolic_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("original.fold");
    let mut owner = open(&path);
    add(&mut owner, "one", 7);
    let hard = directory.path().join("hard.fold");
    let symbolic = directory.path().join("symbolic.fold");
    fs::hard_link(&path, &hard).unwrap();
    std::os::unix::fs::symlink(&path, &symbolic).unwrap();
    for alias in [&hard, &symbolic] {
        assert!(Engine::open_with_options(alias.to_str().unwrap(), METRICS, true).is_err());
    }
    assert_eq!(call(&mut owner, json!({"op":"stats"}))["documents"], 1);
}

#[test]
fn sidecar_namespace_cannot_be_opened_as_a_primary_in_either_order() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data");
    for suffix in [".cache", ".lock", ".cache.anything.tmp", ".CACHE"] {
        let reserved = sibling(&path, suffix);
        assert!(Engine::open(reserved.to_str().unwrap(), METRICS)
            .err()
            .unwrap()
            .contains("reserved"));
        assert!(!reserved.exists());
    }
    let mut owner = open(&path);
    add(&mut owner, "one", 3);
    for suffix in [".cache", ".lock"] {
        assert!(Engine::open(sibling(&path, suffix).to_str().unwrap(), METRICS).is_err());
    }
    call(&mut owner, json!({"op":"checkpoint"}));
    add(&mut owner, "two", 4);
    drop(owner);
    let mut reopened = open(&path);
    assert_eq!(
        call(&mut reopened, json!({"op":"query"}))["rows"][0]["result"]["sum"],
        7
    );
}

#[test]
fn an_existing_log_or_unrelated_file_in_a_sidecar_is_not_overwritten() {
    let directory = tempfile::tempdir().unwrap();
    let other = directory.path().join("other.fold");
    let mut other_owner = open(&other);
    add(&mut other_owner, "one", 11);
    drop(other_owner);
    let source = fs::read(&other).unwrap();
    let path = directory.path().join("data");
    let cache = sibling(&path, ".cache");
    fs::write(&cache, &source).unwrap();
    assert!(Engine::open(path.to_str().unwrap(), METRICS)
        .err()
        .unwrap()
        .contains("authoritative"));
    assert_eq!(fs::read(&cache).unwrap(), source);
    assert!(!path.exists());
    fs::remove_file(&cache).unwrap();

    let mut owner = open(&path);
    add(&mut owner, "two", 13);
    let unrelated = b"a separate file whose bytes must survive";
    fs::write(&cache, unrelated).unwrap();
    call(&mut owner, json!({"op":"checkpoint"}));
    let preserved = fs::read_dir(directory.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("data.cache.unrecognized.")
        })
        .unwrap();
    assert_eq!(fs::read(preserved).unwrap(), unrelated);
    assert_eq!(&fs::read(&cache).unwrap()[..8], b"FOLDCAC1");

    let lock_path = directory.path().join("unrelated.lock");
    fs::write(&lock_path, unrelated).unwrap();
    assert!(Engine::open(
        directory.path().join("unrelated").to_str().unwrap(),
        METRICS
    )
    .is_err());
    assert_eq!(fs::read(&lock_path).unwrap(), unrelated);
}

#[cfg(unix)]
#[test]
fn sidecar_symbolic_links_cannot_redirect_checkpoint_or_ownership_writes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data");
    let victim = directory.path().join("victim");
    fs::write(&victim, b"keep").unwrap();
    std::os::unix::fs::symlink(&victim, sibling(&path, ".cache")).unwrap();
    assert!(Engine::open(path.to_str().unwrap(), METRICS).is_err());
    fs::remove_file(sibling(&path, ".cache")).unwrap();
    // The first failed open established a stable lock. Replacing it is an
    // external operation and must be rejected by the next open as well.
    fs::remove_file(sibling(&path, ".lock")).unwrap();
    std::os::unix::fs::symlink(&victim, sibling(&path, ".lock")).unwrap();
    assert!(Engine::open(path.to_str().unwrap(), METRICS).is_err());
    assert_eq!(fs::read(&victim).unwrap(), b"keep");
}

fn frames(bytes: &[u8]) -> Vec<&[u8]> {
    let mut offset = 56;
    let mut result = Vec::new();
    while offset < bytes.len() {
        let size = u64::from_le_bytes(bytes[offset + 4..offset + 12].try_into().unwrap()) as usize;
        result.push(&bytes[offset..offset + 84 + size]);
        offset += 84 + size;
    }
    result
}

#[test]
fn persisted_predecessors_detect_whole_frame_omission_reorder_and_cross_log_splices() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.fold");
    let mut owner = open(&path);
    for (id, value) in [("one", 1), ("two", 10), ("three", 100)] {
        add(&mut owner, id, value);
    }
    drop(owner);
    let original = fs::read(&path).unwrap();
    assert_eq!(&original[..8], b"FOLDLOG2");
    let original_frames = frames(&original);
    let other_path = directory.path().join("other.fold");
    let mut other = open(&other_path);
    add(&mut other, "foreign", 999);
    drop(other);
    let foreign_bytes = fs::read(other_path).unwrap();
    let foreign_frames = frames(&foreign_bytes);
    for replacement in [
        [&original[..56], original_frames[0], original_frames[2]].concat(),
        // Even an unfinished tail must not hide a provably wrong predecessor.
        [
            &original[..56],
            original_frames[0],
            &original_frames[2][..84],
        ]
        .concat(),
        [
            &original[..56],
            original_frames[0],
            &original_frames[2][..68],
        ]
        .concat(),
        [
            &original[..56],
            original_frames[1],
            original_frames[0],
            original_frames[2],
        ]
        .concat(),
        [
            &original[..56],
            original_frames[0],
            foreign_frames[0],
            original_frames[2],
        ]
        .concat(),
    ] {
        fs::write(&path, &replacement).unwrap();
        assert!(Engine::open(path.to_str().unwrap(), METRICS)
            .err()
            .unwrap()
            .contains("predecessor"));
        assert_eq!(fs::read(&path).unwrap(), replacement);
    }
}

fn legacy_log(bytes: &[u8]) -> Vec<u8> {
    let mut header = b"FOLDLOG1".to_vec();
    header.extend(&bytes[8..24]);
    header.extend(Sha256::digest(&header));
    for frame in frames(bytes) {
        header.extend(b"BAT1");
        header.extend(&frame[4..52]);
        header.extend(&frame[84..]);
    }
    header
}

#[test]
fn legacy_logs_are_read_only_and_copy_migration_preserves_batches_and_source_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy.fold");
    let mut owner = open(&path);
    call(
        &mut owner,
        json!({"op":"ingest","documents":[
            {"id":"one","payload":{"metric":"x","value":1}},
            {"id":"two","payload":{"metric":"x","value":2}}
        ]}),
    );
    add(&mut owner, "three", 3);
    let expected = call(&mut owner, json!({"op":"query"}))["rows"].clone();
    drop(owner);
    let bytes = legacy_log(&fs::read(&path).unwrap());
    fs::write(&path, &bytes).unwrap();
    let mut legacy = open(&path);
    let stats = call(&mut legacy, json!({"op":"stats"}));
    assert_eq!(stats["log"]["read_only"], true);
    assert_eq!(stats["log"]["format"], 1);
    assert!(!legacy.is_writable());
    assert_eq!(call(&mut legacy, json!({"op":"query"}))["rows"], expected);
    assert!(legacy
        .execute(json!({"op":"ingest","documents":[{"payload":null}]}))
        .unwrap_err()
        .contains("read-only"));
    assert!(!legacy.writer_poisoned());
    assert!(Engine::migrate_log(
        path.to_str().unwrap(),
        directory.path().join("blocked.fold").to_str().unwrap()
    )
    .is_err());
    drop(legacy);
    let destination = directory.path().join("migrated.fold");
    Engine::migrate_log(path.to_str().unwrap(), destination.to_str().unwrap()).unwrap();
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let mut migrated = open(&destination);
    assert!(migrated.is_writable());
    let stats = call(&mut migrated, json!({"op":"stats"}));
    assert_eq!(stats["log"]["batches"], 2);
    assert_eq!(stats["log"]["format"], 2);
    assert_eq!(call(&mut migrated, json!({"op":"query"}))["rows"], expected);
    add(&mut migrated, "four", 4);
    drop(migrated);
    let original_destination = fs::read(&destination).unwrap();
    assert!(Engine::migrate_log(path.to_str().unwrap(), destination.to_str().unwrap()).is_err());
    assert_eq!(fs::read(destination).unwrap(), original_destination);
    // Names now reserved for sidecars remain recoverable by explicit migration.
    let reserved_source = directory.path().join("old-database.cache");
    fs::write(&reserved_source, &bytes).unwrap();
    let rescued = directory.path().join("rescued.fold");
    Engine::migrate_log(reserved_source.to_str().unwrap(), rescued.to_str().unwrap()).unwrap();
    assert_eq!(fs::read(reserved_source).unwrap(), bytes);
    assert_eq!(
        call(&mut open(&rescued), json!({"op":"stats"}))["documents"],
        3
    );
}

#[test]
fn migration_never_creates_missing_sources_or_modifies_incomplete_source_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.fold");
    let destination = directory.path().join("destination.fold");
    assert!(Engine::migrate_log(missing.to_str().unwrap(), destination.to_str().unwrap()).is_err());
    assert!(!missing.exists() && !destination.exists());
    let mut owner = open(&missing);
    add(&mut owner, "one", 1);
    drop(owner);
    let mut bytes = fs::read(&missing).unwrap();
    bytes.extend(b"BA");
    fs::write(&missing, &bytes).unwrap();
    assert!(
        Engine::migrate_log(missing.to_str().unwrap(), destination.to_str().unwrap())
            .unwrap_err()
            .contains("incomplete tail")
    );
    assert_eq!(fs::read(&missing).unwrap(), bytes);
    assert!(!destination.exists());
}

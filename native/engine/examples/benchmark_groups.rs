//! Same-runtime comparison of opted-in group reduction and legacy operations.
use foldlitedb_portable_engine::Engine;
use serde_json::{json, Value};
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len(),
        5,
        "benchmark_groups GROUPED.wasm LEGACY.wasm DOCUMENTS REPEATS"
    );
    let modules = [
        std::fs::read(&args[1]).unwrap(),
        std::fs::read(&args[2]).unwrap(),
    ];
    let documents: usize = args[3].parse().unwrap();
    let repeats: usize = args[4].parse().unwrap();
    let mut samples = Vec::new();
    for keys in [1, 16, 1024] {
        for batch_size in [100, 1024] {
            let mut expected = None;
            for repeat in 0..repeats {
                // Alternate order to reduce systematic first-run effects.
                for variant in if repeat % 2 == 0 { [0, 1] } else { [1, 0] } {
                    let mut engine = Engine::open(":memory:", &modules[variant]).unwrap();
                    let started = Instant::now();
                    for begin in (0..documents).step_by(batch_size) {
                        let records: Vec<_> = (begin..(begin + batch_size).min(documents)).map(|position| {
                            json!({"id":position.to_string(),"source":"benchmark","position":position,
                                "payload":{"metric":(position % keys).to_string(),"value":position % 1000}})
                        }).collect();
                        engine
                            .execute(json!({"op":"ingest","documents":records}))
                            .unwrap();
                    }
                    let seconds = started.elapsed().as_secs_f64();
                    let rows =
                        engine.execute(json!({"op":"query","limit":1024})).unwrap()["rows"].clone();
                    let total: u64 = rows
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|row| row["state"]["count"].as_u64().unwrap())
                        .sum();
                    assert_eq!(total, documents as u64);
                    if let Some(expected) = &expected {
                        assert_eq!(&rows, expected);
                    } else {
                        expected = Some(rows);
                    }
                    samples.push(json!({"variant":if variant==0 {"grouped"} else {"legacy"},
                        "keys":keys,"batch_size":batch_size,"repeat":repeat,"seconds":seconds,
                        "documents_per_second":documents as f64 / seconds}));
                }
            }
        }
    }
    println!("{}", serde_json::to_string(&Value::Array(samples)).unwrap());
}

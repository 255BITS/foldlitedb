"""Compare grouped/legacy reduction on the same runtime, SDK, data and batches.

Requires Node and npm ci --prefix views, plus Cargo. Runs only in-memory engine
work; the existing benchmark_database.py measures durable Python ingestion.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--documents", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("groups-benchmark.json"))
    options = parser.parse_args()
    if options.documents < 1 or options.repeats < 1:
        parser.error("documents and repeats must be positive")
    staging_root = ROOT / "views/.build"
    staging_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="groups-benchmark-", dir=staging_root) as directory:
        temporary = Path(directory)
        sdk = os.path.relpath(ROOT / "views/assembly/sdk", temporary).replace(os.sep, "/")
        original = (ROOT / "views/assembly/examples/metrics.ts").read_text()
        old = 'super("metrics", true, "1", "1", true)'
        if old not in original:
            raise RuntimeError("Expected explicit metrics grouped-reduction opt-in")
        legacy_source = original.replace(old, 'super("metrics", true, "1", "1", false)')
        legacy_source = legacy_source.replace('"../sdk"', json.dumps(sdk))
        source = temporary / "metrics_legacy.ts"
        source.write_text(legacy_source)
        subprocess.run(["node", str(ROOT / "views/scripts/build-view.mjs"), str(source),
                        "--out-dir", str(temporary)], cwd=ROOT, check=True)
        grouped = ROOT / "src/foldlitedb/portable_assets/metrics.wasm"
        legacy = temporary / "metrics_legacy.wasm"
        output = subprocess.check_output([
            "cargo", "run", "--locked", "--release", "--manifest-path", str(ROOT / "native/Cargo.toml"),
            "-p", "foldlitedb-portable-engine", "--example", "benchmark_groups", "--",
            str(grouped), str(legacy), str(options.documents), str(options.repeats),
        ], cwd=ROOT, text=True)
        samples = json.loads(output)
        summary = []
        for keys in (1, 16, 1024):
            for batch_size in (100, 1024):
                medians = {variant: statistics.median(
                    row["documents_per_second"] for row in samples
                    if row["keys"] == keys and row["batch_size"] == batch_size and row["variant"] == variant
                ) for variant in ("grouped", "legacy")}
                summary.append({"keys": keys, "batch_size": batch_size, "median_documents_per_second": medians,
                                "throughput_ratio": medians["grouped"] / medians["legacy"]})
        report = {
            "schema": "foldlitedb-grouped-reduction-benchmark/v1",
            "settings": vars(options) | {"output": str(options.output)},
            "runtime": {"platform": platform.platform(), "machine": platform.machine(),
                        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True).strip()},
            "method": "In-memory release Rust Engine with identical document and batch boundaries. Modules use the same SDK/source; only reduce_groups opt-in differs. Engine open and result verification are outside the timer. Variant order alternates by repetition. Every result state, finalized result and sequence is compared against the other variant. No Python transport or durable sync is timed.",
            "module_sha256": {"grouped": hashlib.sha256(grouped.read_bytes()).hexdigest(),
                              "legacy": hashlib.sha256(legacy.read_bytes()).hexdigest()},
            "source_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in (
                "native/Cargo.lock", "native/engine/src/runtime.rs", "native/engine/src/lib.rs",
                "views/assembly/sdk/index.ts", "views/assembly/examples/metrics.ts",
                "native/engine/examples/benchmark_groups.rs", "scripts/benchmark_groups.py")},
            "summary": summary, "samples": samples,
        }
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"output": str(options.output), "summary": summary}, indent=2))


if __name__ == "__main__":
    main()

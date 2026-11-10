"""Build or verify the bundled WebAssembly reducer artifact from native/reducer."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "src/foldlitedb/reducers/assets"
WASM_PATH = ASSETS / "reducer.wasm"
SPEC_PATH = ASSETS / "reducer.json"

MAX_MEMORY_BYTES = 16_777_216
MAX_REQUEST_BYTES = 262_144
MAX_BATCH_VALUES = 1_024
REDUCERS = ["mean/v1", "envelope/v1", "sum/v1", "count/v1"]
CAPABILITIES = {
    "mean/v1": {
        "mergeable": True,
        "ordered": False,
        "state_version": 1,
        "contribution_schema": "finite-scalar-position/v1",
        "exact_merge": False,
    },
    "envelope/v1": {
        "mergeable": True,
        "ordered": False,
        "state_version": 1,
        "contribution_schema": "finite-scalar-position/v1",
        "exact_merge": True,
    },
    "sum/v1": {
        "mergeable": True,
        "ordered": False,
        "state_version": 1,
        "contribution_schema": "finite-scalar-position/v1",
        "exact_merge": False,
    },
    "count/v1": {
        "mergeable": True,
        "ordered": False,
        "state_version": 1,
        "contribution_schema": "finite-scalar-position/v1",
        "exact_merge": True,
    },
}


def rustc_version() -> str:
    output = subprocess.check_output(["rustc", "--version"], text=True).strip()
    parts = output.split()
    if len(parts) < 2 or parts[0] != "rustc":
        raise RuntimeError(f"Unexpected rustc --version output: {output!r}")
    return parts[1]


def build_wasm(root: Path = ROOT) -> tuple[bytes, dict]:
    root = root.resolve()
    sysroot = Path(
        subprocess.check_output(["rustc", "--print", "sysroot"], text=True).strip()
    ).resolve()
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")).resolve()
    flags = [
        "-C",
        "codegen-units=1",
        "-C",
        "strip=debuginfo",
        "-C",
        f"link-arg=--max-memory={MAX_MEMORY_BYTES}",
        f"--remap-path-prefix={root}=/workspace",
        f"--remap-path-prefix={cargo_home}=/cargo",
        f"--remap-path-prefix={sysroot}=/sysroot",
    ]
    env = os.environ.copy()
    env["CARGO_INCREMENTAL"] = "0"
    env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(flags)

    with tempfile.TemporaryDirectory(prefix="foldlitedb-reducer-wasm-") as target_dir:
        subprocess.run(
            [
                "cargo",
                "build",
                "--locked",
                "--manifest-path",
                str(root / "native/Cargo.toml"),
                "-p",
                "foldlitedb-reducer-wasm",
                "--target",
                "wasm32-unknown-unknown",
                "--release",
                "--target-dir",
                target_dir,
            ],
            check=True,
            env=env,
        )
        artifact = (
            Path(target_dir)
            / "wasm32-unknown-unknown"
            / "release"
            / "foldlitedb_reducer_wasm.wasm"
        )
        wasm_bytes = artifact.read_bytes()

    digest = hashlib.sha256(wasm_bytes).hexdigest()
    spec = {
        "abi": 1,
        "sha256": digest,
        "bytes": len(wasm_bytes),
        "rust": rustc_version(),
        "max_memory_bytes": MAX_MEMORY_BYTES,
        "max_request_bytes": MAX_REQUEST_BYTES,
        "max_batch_values": MAX_BATCH_VALUES,
        "reducers": REDUCERS,
        "capabilities": CAPABILITIES,
    }
    return wasm_bytes, spec


def format_spec(spec: dict) -> str:
    return json.dumps(spec, indent=2) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify bundled reducer.wasm and reducer.json match a fresh reproducible build",
    )
    options = parser.parse_args()
    wasm_bytes, spec = build_wasm()
    spec_text = format_spec(spec)
    if options.check:
        if not WASM_PATH.is_file() or WASM_PATH.read_bytes() != wasm_bytes:
            raise SystemExit(
                "Stale or modified reducer.wasm; run python scripts/build_reducer_wasm.py"
            )
        if not SPEC_PATH.is_file() or SPEC_PATH.read_text(encoding="utf-8") != spec_text:
            raise SystemExit(
                "Stale or modified reducer.json; run python scripts/build_reducer_wasm.py"
            )
        print(f"Verified reproducible reducer.wasm ({spec['bytes']} bytes, sha256={spec['sha256']})")
    else:
        WASM_PATH.write_bytes(wasm_bytes)
        SPEC_PATH.write_text(spec_text, encoding="utf-8")
        print(f"Wrote reducer.wasm ({spec['bytes']} bytes, sha256={spec['sha256']}) and reducer.json")


if __name__ == "__main__":
    main()

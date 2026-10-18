"""Compile the native Rust protocol driver and compare it to the bundled WASM.

This verifies a future native host can use the same typed computation. The
driver is a development tool; Python and browser execution use bundled WASM.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess

from foldlitedb.reducers import Reducer, ReducerError, descriptor


def check() -> dict:
    core = Path(__file__).resolve().parents[1] / "reducers/core"
    subprocess.run(
        ["cargo", "build", "--locked", "--release", "--bin", "reducer-json"],
        cwd=core,
        check=True,
    )
    reducer = Reducer()
    values = [{"value": value, "position": [index, f"document-{index}"]}
              for index, value in enumerate([None, 2, -3, 9, 0, 1e-200])]
    cases = []
    for kind in descriptor()["reducers"]:
        identity = reducer.identity(kind)
        left = reducer.add(identity, values[:3])
        right = reducer.add(identity, values[3:])
        cases.extend([
            {"op": "identity", "reducer": kind},
            {"op": "add", "state": identity, "values": values},
            {"op": "merge", "left": left, "right": right},
            {"op": "finalize", "state": reducer.add(identity, values)},
            {"op": "finalize", "state": identity},
            {"op": "finalize", "state": {**identity, "version": 2}},
            {"op": "finalize", "state": {**identity, "unknown": True}},
            {"op": "add", "state": identity,
             "values": [{"value": 1, "position": [2**53, "invalid"]}]},
        ])
    cases.extend([
        {"op": "identity", "reducer": "unknown"},
        {"op": "add", "state": reducer.identity("sum/v1"),
         "values": [{"value": 1e308, "position": [i, str(i)]} for i in range(2)]},
        {"op": "merge", "left": reducer.identity("mean/v1"),
         "right": reducer.identity("count/v1")},
    ])
    payload = "".join(json.dumps(case, allow_nan=False, separators=(",", ":")) + "\n"
                      for case in cases)
    native = subprocess.run(
        [str(core / "target/release/reducer-json")],
        input=payload,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    responses = [json.loads(line) for line in native.stdout.splitlines()]
    assert len(responses) == len(cases), native.stderr
    for case, response in zip(cases, responses, strict=True):
        try:
            expected = {"ok": reducer.request(case)}
        except ReducerError as error:
            expected = {"error": str(error)}
        assert response == expected, (case, response, expected)
    return {"cases": len(cases), "native_wasm_parity": True,
            "module_sha256": reducer.module_sha256}


if __name__ == "__main__":
    print(json.dumps(check(), indent=2))

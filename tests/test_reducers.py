import json
from pathlib import Path
import random
import subprocess
import sys

import pytest

import foldlitedb
from foldlitedb.reducers import (
    MAX_BATCH_VALUES,
    MAX_REQUEST_BYTES,
    MAX_SAFE_INTEGER,
    Reducer,
    ReducerError,
    append_frame,
    assets,
    bootstrap,
    descriptor,
    merge_bootstraps,
    validate_bootstrap,
)


@pytest.fixture(scope="module")
def reducer():
    return Reducer()


def points(values, start=0):
    return [
        {"value": value, "position": [start + index, f"document-{start + index:04d}"]}
        for index, value in enumerate(values)
    ]


def test_module_exports():
    import foldlitedb.reducers as r

    assert sorted(r.__all__) == sorted([
        "MAX_BATCH_VALUES",
        "MAX_REQUEST_BYTES",
        "MAX_SAFE_INTEGER",
        "Reducer",
        "ReducerError",
        "append_frame",
        "assets",
        "bootstrap",
        "descriptor",
        "merge_bootstraps",
        "validate_bootstrap",
    ])
    assert r.MAX_BATCH_VALUES == 1024
    assert r.MAX_REQUEST_BYTES == 262144
    assert r.MAX_SAFE_INTEGER == 9007199254740991


def test_packaged_assets_and_descriptor():
    spec = descriptor()
    assert spec["abi"] == 1
    assert spec["max_memory_bytes"] == 16777216
    assert spec["max_request_bytes"] == 262144
    assert spec["max_batch_values"] == 1024
    assert set(spec["reducers"]) == {"mean/v1", "envelope/v1", "sum/v1", "count/v1"}
    import hashlib

    binary = assets().joinpath("reducer.wasm").read_bytes()
    assert hashlib.sha256(binary).hexdigest() == spec["sha256"]


@pytest.mark.parametrize("kind", ["mean/v1", "envelope/v1", "sum/v1", "count/v1"])
def test_bootstrap_suffix_and_disjoint_merge(reducer, kind):
    values = points([None, 3, -2, 8, 1, 4, 8, 0])
    whole = reducer.add(reducer.identity(kind), values)
    left = append_frame(
        reducer,
        bootstrap(reducer, kind, "source/generation/map/view/reference/group"),
        identity="source/generation/map/view/reference/group",
        start=0,
        end=5,
        values=values[:5],
    )
    left = json.loads(json.dumps(left))
    right = append_frame(
        reducer,
        bootstrap(reducer, kind, left["identity"], start=5),
        identity=left["identity"],
        start=5,
        end=8,
        values=values[5:],
    )
    continued = append_frame(
        reducer,
        left,
        identity=left["identity"],
        start=5,
        end=8,
        values=values[5:],
    )
    assert reducer.finalize(continued["state"]) == reducer.finalize(whole)
    assert merge_bootstraps(reducer, left, right)["state"] == whole
    assert (
        append_frame(
            reducer,
            continued,
            identity=left["identity"],
            start=5,
            end=8,
            values=values[5:],
        )
        == continued
    )
    empty = append_frame(
        reducer, continued, identity=left["identity"], start=8, end=9, values=[]
    )
    assert empty["state"] == whole and empty["end"] == 9
    for start, end in [(7, 9), (9, 10), (0, 0)]:
        with pytest.raises(ReducerError):
            append_frame(
                reducer,
                continued,
                identity=left["identity"],
                start=start,
                end=end,
                values=[],
            )
    with pytest.raises(ReducerError, match="adjacent"):
        merge_bootstraps(reducer, left, left)
    with pytest.raises(ReducerError, match="identity"):
        append_frame(
            reducer, left, identity="new-generation", start=5, end=6, values=[]
        )


def test_mean_unequal_counts_missing_and_empty(reducer):
    identity = reducer.identity("mean/v1")
    assert reducer.finalize(identity) == {"count": 0, "value": None}
    a = reducer.add(identity, points([2]))
    b = reducer.add(identity, points([10, 10, 10, None]))
    assert reducer.finalize(reducer.merge(a, b)) == {"count": 4, "value": 8}
    assert reducer.add(identity, [{"position": [0, "missing"]}]) == identity


@pytest.mark.parametrize("kind, expected", [("sum/v1", 8), ("count/v1", 3)])
def test_sum_count_and_empty(reducer, kind, expected):
    identity = reducer.identity(kind)
    assert reducer.finalize(identity) == {"count": 0, "value": 0}
    state = reducer.add(identity, points([2, None, -3, 9]))
    assert reducer.finalize(state) == {"count": 3, "value": expected}
    assert reducer.add(identity, [{"position": [0, "missing"]}]) == identity


def test_declared_reducer_capabilities():
    spec = descriptor()
    assert set(spec["reducers"]) == {"mean/v1", "envelope/v1", "sum/v1", "count/v1"}
    for kind, capability in spec["capabilities"].items():
        assert capability["mergeable"] is True
        assert capability["state_version"] == 1
        assert capability["exact_merge"] is (kind in {"count/v1", "envelope/v1"})


def test_envelope_declared_order_and_tie_breaks(reducer):
    values = [
        {"value": v, "position": p}
        for v, p in [
            (10, [20, "z"]),
            (-3, [5, "b"]),
            (10, [20, "a"]),
            (2, [100, "x"]),
            (-3, [5, "a"]),
        ]
    ]
    state = reducer.add(reducer.identity("envelope/v1"), values)
    result = reducer.finalize(state)
    assert result["first"]["position"] == [5, "a"]
    assert result["last"]["position"] == [100, "x"]
    assert result["min"]["position"] == [5, "a"]
    assert result["max"]["position"] == [20, "a"]
    rng = random.Random(17)
    for _ in range(20):
        rng.shuffle(values)
        cut = rng.randrange(len(values) + 1)
        a = reducer.add(reducer.identity("envelope/v1"), values[:cut])
        b = reducer.add(reducer.identity("envelope/v1"), values[cut:])
        assert reducer.merge(a, b) == state


@pytest.mark.parametrize(
    "mutation",
    [
        {"count": -1},
        {"count": True},
        {"count": 2**53},
        {"version": 2},
        {"count": 0, "sum": 1},
        {"sum": float("inf")},
        {"unknown": 1},
    ],
)
@pytest.mark.parametrize("kind", ["mean/v1", "sum/v1"])
def test_malformed_scalar_state(reducer, mutation, kind):
    state = {**reducer.identity(kind), **mutation}
    with pytest.raises(ReducerError):
        reducer.finalize(state)


@pytest.mark.parametrize(
    "mutation",
    [
        {"count": -1},
        {"count": True},
        {"count": 2**53},
        {"version": 2},
        {"sum": 1},
    ],
)
def test_malformed_count_state(reducer, mutation):
    with pytest.raises(ReducerError):
        reducer.finalize({**reducer.identity("count/v1"), **mutation})


def test_count_overflow_and_conflicting_envelope_positions(reducer):
    with pytest.raises(ReducerError, match="count"):
        reducer.add({**reducer.identity("count/v1"), "count": 2**53 - 1}, points([1]))
    state = reducer.add(reducer.identity("envelope/v1"), points([1]))
    with pytest.raises(ReducerError, match="conflicting"):
        reducer.add(state, points([2, 0, 3]))


def test_malformed_envelope(reducer):
    four = reducer.add(reducer.identity("envelope/v1"), points([2, 0, 4, 3]))
    with pytest.raises(ReducerError, match="more positions"):
        reducer.finalize({**four, "count": 2})
    state = reducer.add(reducer.identity("envelope/v1"), points([1, 2, 3]))
    for field, value in [("first", None), ("count", 0), ("count", 1)]:
        with pytest.raises(ReducerError):
            reducer.finalize({**state, field: value})
    state["min"]["value"] = 5
    with pytest.raises(ReducerError):
        reducer.finalize(state)


@pytest.mark.parametrize(
    "position",
    [
        [-1, "a"],
        [2**53, "a"],
        [True, "a"],
        [1.5, "a"],
        [0, ""],
        [0, "x" * 129],
        [0, "λ"],
    ],
)
def test_invalid_positions(reducer, position):
    with pytest.raises(ReducerError):
        reducer.add(reducer.identity("mean/v1"), [{"value": 1, "position": position}])


def test_limits_and_overflow(reducer):
    state = reducer.identity("mean/v1")
    with pytest.raises(ReducerError, match="batch"):
        reducer.add(state, points([1] * 1025))
    with pytest.raises(ReducerError, match="262144"):
        reducer.request({"op": "identity", "reducer": "x" * 262144})
    with pytest.raises(ReducerError, match="sum"):
        reducer.add(state, points([1e308, 1e308]))
    with pytest.raises(ReducerError, match="count"):
        reducer.add({**state, "count": 2**53 - 1}, points([1]))
    with pytest.raises(ReducerError):
        reducer.merge(state, reducer.identity("envelope/v1"))
    with pytest.raises(ReducerError):
        reducer.request({"op": "identity", "reducer": "unknown"})
    with pytest.raises(ReducerError):
        reducer.add(state, points([float("nan")]))


def test_fuel_trap_poisoned_instance_and_independent_recovery():
    exhausted = Reducer(fuel=100)
    with pytest.raises(ReducerError, match="budget"):
        exhausted.identity("mean/v1")
    with pytest.raises(ReducerError, match="create a new"):
        exhausted.identity("mean/v1")
    assert Reducer().finalize(Reducer().identity("mean/v1"))["count"] == 0


def test_bounded_memory_after_repeated_batches(reducer):
    values = points(range(1024))
    state = reducer.identity("envelope/v1")
    state = reducer.add(state, values)
    warm = reducer.memory_bytes
    for _ in range(200):
        state = reducer.add(state, values)
    assert reducer.memory_bytes == warm
    assert len(json.dumps(state)) < 1024
    assert reducer.memory_bytes <= descriptor()["max_memory_bytes"]


def test_bootstrap_corruption(reducer):
    view = bootstrap(reducer, "mean/v1", "scope")
    for changes in [
        {"module_sha256": "wrong"},
        {"identity": ""},
        {"end": True},
        {"extra": 1},
        {"state": {**view["state"], "count": 1, "sum": 2}},
    ]:
        with pytest.raises(ReducerError):
            validate_bootstrap(reducer, {**view, **changes})


def test_native_wasmi_independence_from_wasmtime(tmp_path):
    code = """
import sys
class BlockWasmtime:
    def find_spec(self, fullname, path, target=None):
        if 'wasmtime' in fullname:
            raise ImportError(f"No module named {fullname}")
        return None
sys.meta_path.insert(0, BlockWasmtime())

from foldlitedb.reducers import Reducer
assert 'wasmtime' not in sys.modules
r = Reducer()
assert r.identity('mean/v1')['count'] == 0
"""
    package_parent = str(Path(foldlitedb.__file__).resolve().parent.parent)
    code = f"import sys; sys.path.insert(0, {package_parent!r})\n" + code
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""

"""Public range queries agree across portable views and Python callbacks.

Expected boundaries below are written explicitly rather than using the query
implementation's comparator or selector helpers as their own oracle.
"""
from pathlib import Path
import base64
import json
import zlib

import pytest

from foldlitedb import Database, DatabaseError, OrderedFold, View


FIXTURE = Path(__file__).parent / "fixtures" / "query_keys.json"


def callback_view(name="range-fixture"):
    return View(
        name,
        version="1",
        map=lambda document: [(key, 1) for key in document.payload["keys"]],
        reducer=OrderedFold(0, lambda state, value: state + value, version="1"),
    )


@pytest.fixture(params=["portable", "callback"])
def mixed_db(request):
    fixture = json.loads(FIXTURE.read_text())
    view = View(bytes.fromhex(fixture["hex"])) if request.param == "portable" else callback_view()
    with Database(":memory:", view) as database:
        database.add({"keys": list(reversed(fixture["keys"]))})
        yield database


@pytest.fixture(params=["portable", "callback"])
def string_view(request):
    if request.param == "portable":
        return View.bundled("metrics")
    return View(
        "string-ranges",
        version="1",
        map=lambda document: [(document.payload["metric"], document.payload["value"])],
        reducer=OrderedFold(0, lambda state, value: state + value, version="1"),
    )


def key_labels(keys):
    """Keep false distinct from zero when asserting ordered JSON keys."""
    return [json.dumps(key, ensure_ascii=False, sort_keys=True) for key in keys]


def assert_keys(rows, expected):
    assert key_labels([row.key for row in rows]) == key_labels(expected)


def collect(database, **selector):
    page = database.query(limit=1, **selector)
    cursor = page.cursor
    rows = list(page.rows)
    while not page.complete:
        # A caller can change page size without changing its selection.
        page = database.query(limit=2, cursor=cursor, after=page.after, **selector)
        assert page.cursor == cursor
        rows.extend(page.rows)
    assert page.after is None
    return rows


def test_complete_mixed_key_order_and_paged_rows(mixed_db):
    expected = json.loads(FIXTURE.read_text())["keys"]
    assert_keys(collect(mixed_db), expected)
    assert_keys(list(mixed_db.rows(limit=3)), expected)


@pytest.mark.parametrize("selector, expected", [
    ({"end_key": None}, [None]),
    ({"start_key": None, "end_key": False}, [None, False]),
    ({"start_key": False, "end_key": True}, [False, True]),
    ({"start_key": False, "end_key": True, "inclusive_start": False}, [True]),
    ({"start_key": False, "end_key": True, "inclusive_end": False}, [False]),
    ({"start_key": -10, "end_key": 10, "inclusive_start": False, "inclusive_end": False},
     [-2, -0.5, 0, 1e-7, 2]),
    ({"start_key": 2, "end_key": 10}, [2, 10]),
    ({"start_key": 10, "end_key": 2}, []),
    ({"start_key": 2, "end_key": 2}, [2]),
    ({"start_key": 2, "end_key": 2, "inclusive_end": False}, []),
    ({"start_key": "a", "end_key": "alpine"}, ["a", "alice", "alice/one", "alpine"]),
    ({"start_key": "\ue000", "end_key": "\U0010ffff"},
     ["\ue000", "\U00010000", "\U0001f680", "\U0010ffff"]),
    ({"start_key": ["sales", "alice", 2], "end_key": ["sales", "alice", 10]},
     [["sales", "alice", 2], ["sales", "alice", 10]]),
    ({"start_key": {"a": 2}, "end_key": {"a": 10}}, [{"a": 2}, {"a": 10}]),
])
def test_explicit_range_boundaries(mixed_db, selector, expected):
    assert_keys(collect(mixed_db, **selector), expected)
    assert_keys(list(mixed_db.rows(limit=1, **selector)), expected)


@pytest.mark.parametrize("key", [None, False, True, 0, 2, 10, [], [None], {"a": 10}])
def test_exact_key_still_distinguishes_json_types(mixed_db, key):
    assert_keys(collect(mixed_db, key=key), [key])


@pytest.mark.parametrize("prefix, expected", [
    ("ali", ["alice", "alice/one"]),
    ("alice/", ["alice/one"]),
    ("missing", []),
    (["sales"], [["sales"], ["sales", "alice"], ["sales", "alice", 2],
                 ["sales", "alice", 10], ["sales", "bob", 2]]),
    (["sales", "alice"], [["sales", "alice"], ["sales", "alice", 2], ["sales", "alice", 10]]),
    ([[0]], [[[0]], [[0], 1]]),
])
def test_prefixes_are_structural_and_include_exact_prefix(mixed_db, prefix, expected):
    assert_keys(collect(mixed_db, prefix=prefix), expected)
    assert_keys(list(mixed_db.rows(prefix=prefix, limit=1)), expected)


def test_empty_prefixes_are_restricted_to_their_container_type(mixed_db):
    expected = json.loads(FIXTURE.read_text())["keys"]
    assert_keys(collect(mixed_db, prefix=""), [key for key in expected if isinstance(key, str)])
    assert_keys(collect(mixed_db, prefix=[]), [key for key in expected if isinstance(key, list)])


@pytest.mark.parametrize("selector", [
    {"key": None, "start_key": None},
    {"key": False, "end_key": False},
    {"key": "alice", "prefix": "ali"},
    {"prefix": [], "start_key": []},
    {"prefix": "ali", "end_key": "z"},
    {"prefix": None}, {"prefix": False}, {"prefix": 0}, {"prefix": {}},
    {"start_key": float("nan")}, {"end_key": float("inf")},
    {"prefix": [2**53 + 1]}, {"start_key": "\ud800"},
])
def test_invalid_selectors_are_read_only_errors(mixed_db, selector):
    before = mixed_db.stats()
    with pytest.raises(DatabaseError):
        mixed_db.query(**selector)
    assert mixed_db.stats() == before


@pytest.mark.parametrize("flag", ["inclusive_start", "inclusive_end"])
@pytest.mark.parametrize("value", [None, 0, 1, "true", []])
def test_inclusivity_flags_require_actual_booleans(mixed_db, flag, value):
    with pytest.raises(DatabaseError, match="bool"):
        mixed_db.query(start_key=0, end_key=10, **{flag: value})


@pytest.mark.parametrize("changed", [
    {"prefix": ["sales", "alice"]},
    {"prefix": []},
    {"start_key": ["sales"]},
    {"prefix": ["sales"], "inclusive_end": False},
    {},
])
def test_continuation_cannot_change_selector(mixed_db, changed):
    first = mixed_db.query(prefix=["sales"], limit=1)
    assert not first.complete
    with pytest.raises(DatabaseError, match="[Ss]elector|[Aa]fter|snapshot"):
        mixed_db.query(cursor=first.cursor, after=first.after, **changed)


@pytest.mark.parametrize("after", [
    0, [], {}, "", "null", "[]", "{}", '["bad"]',
    pytest.param("[" * 1500 + "0" + "]" * 1500, id="deep-json"),
])
def test_malformed_continuations_fail_explicitly(mixed_db, after):
    first = mixed_db.query(limit=1)
    with pytest.raises(DatabaseError):
        mixed_db.query(cursor=first.cursor, after=after)


def test_pre_range_query_continuations_are_rejected(mixed_db):
    first = mixed_db.query(limit=1)
    canonical_key = json.dumps(first.rows[-1].key, separators=(",", ":"))
    for old_token in ([first.cursor, canonical_key], [first.cursor, canonical_key, None]):
        with pytest.raises(DatabaseError, match="[Aa]fter|format|snapshot"):
            mixed_db.query(cursor=first.cursor, after=json.dumps(old_token))


def test_valid_continuation_bytes_are_rejected_as_non_string(mixed_db):
    first = mixed_db.query(limit=1)
    with pytest.raises(DatabaseError):
        mixed_db.query(cursor=first.cursor, after=first.after.encode("utf-8"))


def test_numeric_equivalence_does_not_break_continuation(mixed_db):
    first = mixed_db.query(start_key=2, end_key=10, limit=1)
    second = mixed_db.query(start_key=2.0, end_key=10.0, limit=1,
                            cursor=first.cursor, after=first.after)
    assert_keys(first.rows + second.rows, [2, 10])
    assert second.complete


def test_continuation_with_an_outside_key_is_rejected(mixed_db):
    first = mixed_db.query(prefix=["sales"], limit=1)
    token = json.loads(first.after)
    token[-1] = json.dumps(["inventory"], separators=(",", ":"))
    with pytest.raises(DatabaseError):
        mixed_db.query(prefix=["sales"], cursor=first.cursor, after=json.dumps(token))


def test_stable_range_snapshot_survives_appends_and_reopen(tmp_path, string_view):
    path = tmp_path / "range.fold"
    selector = {"start_key": "b", "end_key": "e", "inclusive_end": False}
    with Database(path, string_view) as database:
        database.add_many({"metric": key, "value": 1} for key in ("a", "b", "c", "d", "e"))
        original = database.query(**selector)
        first = database.query(limit=1, **selector)
        database.add_many([{"metric": "bb", "value": 3}, {"metric": "c", "value": 10}])
        current_cursor = database.query().cursor
        with pytest.raises(DatabaseError):
            database.query(cursor=current_cursor, after=first.after, limit=1, **selector)
    with Database(path, string_view) as database:
        assert database.stats()["cache"]["status"] == "loaded"
        rows = list(first.rows)
        page = first
        while not page.complete:
            page = database.query(cursor=first.cursor, after=page.after, limit=1, **selector)
            rows.extend(page.rows)
        assert tuple(rows) == original.rows
        assert_keys(rows, ["b", "c", "d"])
        assert_keys(collect(database, **selector), ["b", "bb", "c", "d"])


def test_iterator_snapshots_mutable_prefix_and_data(string_view):
    with Database(":memory:", string_view) as database:
        database.add_many({"metric": key, "value": 1} for key in ("alice/a", "alice/b", "bob/a"))
        iterator = database.rows(prefix="alice/", limit=1)
        first = next(iterator)
        database.add_many([{"metric": "alice/aa", "value": 2}, {"metric": "alice/b", "value": 3}])
        assert_keys([first, *iterator], ["alice/a", "alice/b"])


def test_control_characters_sort_as_characters_not_json_escapes(string_view):
    expected = ["", "\0", "\n", '"', "\\", "a", "\ue000", "\U00010000", "\U0010ffff"]
    with Database(":memory:", string_view) as database:
        database.add_many({"metric": key, "value": 1} for key in reversed(expected))
        assert_keys(collect(database), expected)
        assert_keys(collect(database, start_key="\0", end_key='"'), ["\0", "\n", '"'])
        assert_keys(collect(database, prefix="\0"), ["\0"])


def test_callback_structured_prefix_uses_canonical_equality():
    keys = [[{"b": 2, "a": 1}], [{"a": 1, "b": 2}, 10], [{"a": 1, "b": 3}], [False], [0]]
    with Database(":memory:", callback_view()) as database:
        database.add({"keys": keys})
        assert_keys(collect(database, prefix=[{"a": 1.0, "b": 2.0}]), keys[:2])
        assert_keys(collect(database, prefix=[False]), [[False]])
        assert_keys(collect(database, prefix=[0.0]), [[0]])


def test_callback_mutable_array_prefix_is_copied_before_iteration_continues():
    with Database(":memory:", callback_view()) as database:
        database.add({"keys": [["sales", "alice", 1], ["sales", "alice", 2], ["sales", "bob", 1]]})
        prefix = ["sales", "alice"]
        iterator = database.rows(prefix=prefix, limit=1)
        first = next(iterator)
        prefix[1] = "bob"
        database.add({"keys": [["sales", "alice", 3]]})
        assert_keys([first, *iterator], [["sales", "alice", 1], ["sales", "alice", 2]])


def test_callback_full_depth_keys_support_all_selectors_and_continuation():
    def nested(value):
        for _ in range(64):
            value = [value]
        return value

    first_key, second_key = nested(0), nested(1)
    view = View(
        "deep-ranges", version="1",
        map=lambda document: [(first_key, 1), (second_key, 1)],
        reducer=OrderedFold(0, lambda state, value: state + value, version="1"),
    )
    with Database(":memory:", view) as database:
        database.add({})
        assert_keys(collect(database, key=first_key), [first_key])
        assert_keys(collect(database, prefix=first_key), [first_key])
        assert_keys(collect(database, start_key=first_key, end_key=second_key), [first_key, second_key])


def test_callback_continuation_accepts_shortest_large_double_integer_spelling():
    first, second = 1e18 + 128, 1e18 + 256
    with Database(":memory:", callback_view()) as database:
        database.add({"keys": [second, first]})
        assert_keys(collect(database, start_key=first, end_key=second), [first, second])


# Emitted by the unmodified PyPI 0.2.0 wheel using three metrics A=2, "=3,
# B=5 in one batch, followed by query(limit=1) and clean close. Keeping actual
# log/cache bytes proves the upgrade path rather than synthesizing old state
# using the current implementation. The strings are zlib-compressed base64.
V020_CURSOR = "2f52e39b-b38c-4fe5-b6d2-790b760231b7:e23a6ede5b3c21bfd263682fbd0bc5402c48b18871f481915adea5666919fa93:6"
V020_LOG = (
    "eJyFzU8oBFEcwHFu3Byc0e+gVfPqvdm3M29GDqSl/NnEzZ96896bmto17M4qtpHYhOSkSC5S3EhKKcpBDopCuUpS4kRxkH+R2ovyPXyvn3iitbE10aSf7RzMTOWbO1cOvdK6eLV5/XRq7210vG/ePQ6N7SZG7t/ajjrNsRe/6Lik/fXtuaG+SxfFRT8tf3z+dLs+dxLpq71K32hlLZF8kBzOr21MbmHcHjkQ5dsXcTm3uNC0ChOjNYvT5xWXxeNV3Q+Vq7P7S/N376nZwRxIX2RTqj/IgN2dA0+CDTFKCVXKQJgZHFHiWoi5lkDMcg0pdSEUs0CDjJ9NCwU2SOXybDIADQb8jBd4fj/YWIMBPpz0uQQ7BykVpD0BNtSDBkM8mVVg62Go/YLSdLEwdBM5JOYgyqlEnDgEUU6i3CHCYjj6H0j+BnugIEYLoimkwQ1MkIN1huj3HEcyhCmLKSZp1OXkP1H/W2wogLEw7A2/ANHOsTY="
)
V020_CACHE = (
    "eJyFk7tuE1EQhoOUJ0B0dNMyRzpz7rtdLqKKREGZhOjcFluxvY53FyWy3PEClLxAGho6XoAGiXdAvAMNHVrHiR1hOVtsNfN/n/6jef3m5Pjo4Ih+7O8tv6/Ht3+e62+/9l+ddN3L75/e/a4v9v5+/PxsevXi55fbt3Oo6tnYt1ASQvKtD77JF8MEJSQKJnLnmXNOMeW0ZJ6iYqbghUzKCCMtIIzr1I3yRTPwQhsoIQvpTU5ZBxkFhSoJI40TVUg8RK24iMoFcs5SpRwVpH3KXhtjCioqX0hAqKuqyS2UqtAI01muhtdQngpeoLEouEKyAjVHsqjREiqB1qFxSNag4AKtQjIKSSskxVFZNBKJDJITKLhF0oR9nu1DJEqLhUCtUEp01O+dIwTfxkFuls00rW8zlHN4nyd55tthPYESRKVFlkVgQbrIVJU1CyYJZgserOFCUugbavJVlycxQ2kQUh27cZ60DZSnc1gWrZUilbNh3BnPFFUFc1URmSsqk5KIMbuij6m7WR8CKVe+G7WAMK2b4Z0LR5j6m1HtU285zu1sGKGEA0D44EddhlIsNk1ogSt6shWPRlgWSAemvErMUyCmPEkfKBaOy6fotJ1+Bmu8fIQXD3gbk/GGEwtcOKb6XwjJMa6czi4pWXl6Ci+24w/XdP2ILhfnCINh09azm+UzXOabpe7B0vjhsWPdTe4uY+yvl5jx8A7XdOO7Rme56W22D2c/2b61VlF9Eff4w514vQrSqyC9C6/v8du21ni9iT87O4OdBnKVJVdZcpeBvDfYtrVxEovNUzudbx6I/G8w+tEo+Hh5EQc5Xk7r4XJuvlgs/gEgWnem"
)


def test_real_020_cache_and_snapshot_cursor_upgrade_without_wasm_replay(tmp_path):
    path = tmp_path / "from-020.fold"
    original_log = zlib.decompress(base64.b64decode(V020_LOG))
    path.write_bytes(original_log)
    path.with_suffix(".fold.cache").write_bytes(zlib.decompress(base64.b64decode(V020_CACHE)))
    with Database(path) as database:
        assert database.stats()["cache"]["status"] == "loaded"
        assert database.stats()["replay"]["documents"] == 0
        assert path.read_bytes() == original_log
        assert_keys(database.query(cursor=V020_CURSOR).rows, ['"', "A", "B"])
        assert database.changes(since=V020_CURSOR).rows == ()
        with pytest.raises(DatabaseError, match="[Aa]fter|expired"):
            database.query(cursor=V020_CURSOR, after=json.dumps([V020_CURSOR, '"A"']))
        database.add({"metric": "A", "value": 7})
        assert database.changes(since=V020_CURSOR).rows[0].result["sum"] == 9
        snapshot = database.query(cursor=V020_CURSOR)
        assert snapshot.rows[1].result["sum"] == 2

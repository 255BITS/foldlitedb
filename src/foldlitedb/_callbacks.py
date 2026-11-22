"""Callback and external projection helpers shared by ``Database``."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from operator import itemgetter
from typing import Any

from ._query import QUERY_FORMAT, compile_selector, key_order
from .types import (
    CursorExpired,
    DatabaseError,
    Document,
    Row,
    Snapshot,
    _clone_trusted,
    _copy,
    _json,
)

_MISSING = object()


def _view_signature(name: str, descriptor: dict[str, Any]) -> str:
    return hashlib.sha256(_json([name, descriptor]).encode()).hexdigest()


@dataclass
class _RegisteredView:
    view: Any
    descriptor: dict[str, Any]
    states: dict[str, tuple[Any, Any, Any, int]]
    mode: str = "documents"
    external_descriptor_matches: bool = False
    dirty: bool = False
    dirty_keys: set[str] | None = None
    delta_count: int = 0
    signature: str = ""

    def __post_init__(self) -> None:
        if not self.signature:
            self.signature = _view_signature(self.view.name, self.descriptor)

    def copy(self) -> _RegisteredView:
        return _RegisteredView(
            view=self.view,
            descriptor=_clone_trusted(self.descriptor),
            states=dict(self.states),
            mode=self.mode,
            external_descriptor_matches=self.external_descriptor_matches,
            dirty=self.dirty,
            dirty_keys=set(self.dirty_keys) if self.dirty_keys is not None else None,
            delta_count=self.delta_count,
            signature=self.signature,
        )


def validate_callback_view(view: Any, identity_fn: Any, view_cls: Any) -> tuple[Any, dict[str, Any]]:
    identity_fn(view.name, "view name")
    identity_fn(view.version, "view version")
    if not callable(view.map):
        raise DatabaseError("View map must be callable")
    if any(not callable(getattr(view.reducer, method, None))
           for method in ("identity", "add", "finalize")):
        raise DatabaseError("View reducer must implement identity, add, and finalize")
    descriptor = _copy({"version": view.version, "references": view.references,
                        "reducer": getattr(view.reducer, "descriptor", str(view.reducer))})
    registered = view_cls(view.name, version=view.version, map=view.map,
                          reducer=view.reducer, references=descriptor["references"])
    return registered, descriptor


def callback_checkpoint(db: Any, name: str, state_map: dict[str, tuple[Any, Any, Any, int]], sequence: int):
    entry = db._views[name]
    groups = []
    for canon_key in sorted(state_map):
        key, state, result, row_sequence = state_map[canon_key]
        if result is _MISSING:
            evaluating = db._callback_evaluating
            db._callback_evaluating = True
            try:
                result = _copy(entry.view.reducer.finalize(_clone_trusted(state)))
            finally:
                db._callback_evaluating = evaluating
            state_map[canon_key] = (key, state, result, row_sequence)
        groups.append({"key": key, "state": state, "result": result, "sequence": row_sequence})
    return {"format": 1, "descriptor": entry.descriptor,
            "sequence": sequence, "groups": groups}


def collect_view_emissions(db: Any, view: Any, documents: list[dict[str, Any]], *, copy_payload: bool = True):
    raw_items = []
    has_prepare = getattr(view.reducer, "positioned", True) is False and hasattr(view.reducer, "prepare")
    for value in documents:
        source = value["source"]
        if source.startswith("_"):
            continue
        payload = _clone_trusted(value["payload"]) if copy_payload else value["payload"]
        doc = Document(value["id"], payload, value["position"], source)
        for ordinal, (key, contribution) in enumerate(view.map(doc)):
            if len(raw_items) >= 4096:
                raise DatabaseError("A callback batch may emit at most 4096 contributions")
            contribution = _copy(contribution)
            if has_prepare:
                contribution = _copy(view.reducer.prepare(contribution, doc, ordinal))
            raw_items.append((key, contribution))
    if not raw_items:
        return {}
    canonicalized = db._canonical_keys([item[0] for item in raw_items])
    emissions: dict[str, tuple[Any, list[Any]]] = {}
    for (canon_key, key), (_, contribution) in zip(canonicalized, raw_items, strict=True):
        group = emissions.get(canon_key)
        if group is None:
            emissions[canon_key] = (key, [contribution])
        else:
            group[1].append(contribution)
    return emissions


def stage_callbacks(
    db: Any,
    documents: list[dict[str, Any]],
    sequence: int,
    names: list[str],
    *,
    initial: dict[str, dict[str, tuple[Any, Any, Any, int]]] | None = None,
    deltas_only: bool = False,
):
    """Map, reduce, and finalize a batch across callback views.

    When ``deltas_only`` is True, returns only the changed keys per view without
    mutating ``db._views[name].states``. Otherwise returns updated state maps
    starting from ``initial`` (or shallow copies of current view states).
    """
    if deltas_only:
        results: dict[str, dict[str, tuple[Any, Any, Any, int]]] = {}
    else:
        results = {name: dict(db._views[name].states) for name in names} if initial is None else initial

    last_index = len(names) - 1
    evaluating = db._callback_evaluating
    db._callback_evaluating = True
    try:
        for index, name in enumerate(names):
            entry = db._views[name]
            view = entry.view
            reducer = view.reducer
            source_map = entry.states if deltas_only else results[name]
            target_map = {} if deltas_only else source_map
            # ``documents`` comes from a freshly decoded native reply that is not
            # reused after this call; only earlier views need an isolated copy.
            emissions = collect_view_emissions(db, view, documents, copy_payload=(index < last_index))
            for canon_key, (key, contributions) in sorted(emissions.items()):
                existing = source_map.get(canon_key)
                current = _clone_trusted(existing[1]) if existing is not None else _copy(reducer.identity())
                state = _copy(reducer.add(current, contributions))
                # Finalizers may mutate their argument; state and result are independent.
                result = _copy(reducer.finalize(_clone_trusted(state)))
                target_map[canon_key] = (key, state, result, sequence)
            if deltas_only:
                results[name] = target_map
    finally:
        db._callback_evaluating = evaluating
    return results


def reduce_view_group(
    db: Any,
    view_name: str,
    canon_key: str | None,
    orig_key: Any,
    vals: list[Any],
    sequence: int,
) -> None:
    if not vals:
        return
    db._assert_mutable()
    entry = db._views[view_name]
    view = entry.view
    if canon_key is None:
        canon_key, orig_key = db._canonical_key(orig_key)
    else:
        orig_key = _copy(orig_key)
    state_map = entry.states
    evaluating = db._callback_evaluating
    db._callback_evaluating = True
    try:
        copied_vals = [_copy(value) for value in vals]
        existing = state_map.get(canon_key)
        if existing is not None:
            new_state = view.reducer.add(_clone_trusted(existing[1]), copied_vals)
        else:
            new_state = view.reducer.add(_copy(view.reducer.identity()), copied_vals)
    finally:
        db._callback_evaluating = evaluating
    state_map[canon_key] = (orig_key, _copy(new_state), _MISSING, sequence)
    entry.dirty = True
    if entry.dirty_keys is None:
        entry.dirty_keys = set()
    entry.dirty_keys.add(canon_key)


def finalize_view_group(db: Any, view_name: str, canon_key: str):
    entry = db._views[view_name]
    orig_key, state, fin, seq = entry.states[canon_key]
    if fin is _MISSING:
        evaluating = db._callback_evaluating
        db._callback_evaluating = True
        try:
            fin = _copy(entry.view.reducer.finalize(_clone_trusted(state)))
        finally:
            db._callback_evaluating = evaluating
        entry.states[canon_key] = (orig_key, state, fin, seq)
    return orig_key, state, fin, seq


def commit_projection_suffix(
    db: Any,
    view_name: str,
    pending_emissions: dict[str, tuple[Any, list[Any]] | tuple[Any, list[Any], int]],
    checkpoint: int,
    replay: Any,
) -> None:
    for canon_key, pending in pending_emissions.items():
        orig_key, vals = pending[0], pending[1]
        db._reduce_view_group(view_name, canon_key, orig_key, vals, checkpoint)
    records = [
        {
            "id": str(checkpoint),
            "source": "projection",
            "position": checkpoint,
            "payload": {"cursor": replay},
        }
    ]
    entry = db._views[view_name]
    dirty_keys = entry.dirty_keys or set()
    write_delta = bool(
        entry.external_descriptor_matches
        and entry.delta_count < 64
        and len(dirty_keys) < len(entry.states)
    )
    keys_to_encode = sorted(dirty_keys) if write_delta else sorted(entry.states.keys())
    groups = []
    for canon_key in keys_to_encode:
        k, s, fin, seq = db._finalize_view_group(view_name, canon_key)
        groups.append({"key": k, "state": s, "result": fin, "sequence": seq})
    payload: dict[str, Any] = {
        "descriptor": entry.descriptor,
        "checkpoint": checkpoint,
        "groups": groups,
    }
    if write_delta:
        payload["delta"] = True
    records.append(
        {
            "id": str(checkpoint),
            "source": f"_view:{view_name}",
            "position": checkpoint,
            "payload": payload,
        }
    )
    db._ingest_records(records, callbacks=False)
    entry.external_descriptor_matches = True
    entry.dirty_keys = set()
    entry.delta_count = (entry.delta_count + 1) if write_delta else 0


def restore_callback_groups(db: Any, checkpoint: dict[str, Any]):
    raw_groups = checkpoint["groups"]
    if not raw_groups:
        return {}
    canonicalized = db._canonical_keys([item["key"] for item in raw_groups])
    states = {}
    for (canon_key, key), item in zip(canonicalized, raw_groups, strict=True):
        if canon_key in states:
            raise DatabaseError("Callback checkpoint repeats a group key")
        states[canon_key] = (key, _copy(item["state"]), _copy(item["result"]), item["sequence"])
    return states


def restore_external_projection(db: Any, view_name: str, descriptor: dict[str, Any], watermark: int):
    source_name = f"_view:{view_name}"
    latest = db.get(str(watermark), source=source_name)
    if latest is None or not isinstance(latest.payload, dict):
        return False, {}, 0
    if latest.payload.get("descriptor") != descriptor:
        return False, {}, 0
    if not latest.payload.get("delta", False):
        return True, db._restore_callback_groups(latest.payload), 0
    chain = []
    for doc in db.documents():
        if doc.source != source_name or doc.position > watermark:
            continue
        if not isinstance(doc.payload, dict) or doc.payload.get("descriptor") != descriptor:
            chain = []
            continue
        if not doc.payload.get("delta", False):
            chain = [doc.payload]
        else:
            chain.append(doc.payload)
    if not chain or chain[0].get("delta", False):
        return False, {}, 0
    states = db._restore_callback_groups(chain[0])
    for delta_payload in chain[1:]:
        states.update(db._restore_callback_groups(delta_payload))
    return True, states, len(chain) - 1


def replay_callbacks(db: Any, names: list[str], coverage: int = 0, *, cursor: str | None = None, initial=None):
    states = {name: dict(db._views[name].states) for name in names} if initial is None else initial
    while True:
        request: dict[str, Any] = {"op": "document_batches", "since": coverage}
        if cursor is not None:
            request["cursor"] = cursor
        batch = db._call(request)
        if batch["documents"]:
            states = db._stage_callbacks(batch["documents"], batch["sequence"], names, initial=states)
            coverage = batch["sequence"]
        if batch["complete"]:
            return states


def query_callback(
    db: Any,
    name: str,
    *,
    key: Any,
    start_key: Any,
    end_key: Any,
    prefix: Any,
    inclusive_start: Any,
    inclusive_end: Any,
    limit: Any,
    cursor: str | None,
    after: str | None,
) -> Snapshot:
    if type(limit) is not int or not 1 <= limit <= 1024:
        raise DatabaseError("limit must be between 1 and 1024")
    if type(inclusive_start) is not bool or type(inclusive_end) is not bool:
        raise DatabaseError("inclusive_start and inclusive_end must be booleans")
    has_range = start_key is not _MISSING or end_key is not _MISSING
    if sum((key is not _MISSING, has_range, prefix is not _MISSING)) > 1:
        raise DatabaseError("key, range bounds, and prefix selectors are mutually exclusive")
    selector: dict[str, Any] = {"inclusive_start": inclusive_start, "inclusive_end": inclusive_end}
    canonical: dict[str, str] = {
        "inclusive_start": "true" if inclusive_start else "false",
        "inclusive_end": "true" if inclusive_end else "false",
    }
    for option, value in (("key", key), ("start_key", start_key), ("end_key", end_key), ("prefix", prefix)):
        if value is not _MISSING:
            canonical[option], selector[option] = db._canonical_key(value)
    if "prefix" in selector and not isinstance(selector["prefix"], (str, list)):
        raise DatabaseError("prefix must be a string or array")
    # Compose canonical children directly: the selector envelope does not
    # consume any of the supported key nesting depth.
    selected = "{" + ",".join(_json(option) + ":" + canonical[option]
                               for option in sorted(canonical)) + "}"
    entry = db._views[name]
    signature = entry.signature
    current = db._cursor()
    if cursor is None:
        native_cursor = current
        cursor = current + "~" + signature
    else:
        if not isinstance(cursor, str) or not cursor.endswith("~" + signature):
            raise CursorExpired("Callback cursor belongs to a different view descriptor; obtain a fresh query snapshot")
        native_cursor = cursor.rsplit("~", 1)[0]
        db._call({"op": "query", "cursor": native_cursor, "limit": 1})
    state_map = entry.states
    if native_cursor != current:
        if entry.mode != "documents":
            raise CursorExpired("Historical external projection snapshots are unavailable; obtain a fresh query snapshot")
        state_map = db._replay_callbacks([name], cursor=native_cursor, initial={name: {}})[name]

    matcher = compile_selector(selector)
    after_order = None
    if after is not None:
        try:
            if not isinstance(after, str):
                raise ValueError("format")
            token = json.loads(after)
            if not isinstance(token, list) or len(token) != 4:
                raise ValueError("format")
            format_version, saved_cursor, saved_selector, after_key = token
            if (format_version != QUERY_FORMAT or saved_cursor != cursor
                    or saved_selector != selected or not isinstance(after_key, str)):
                raise ValueError("snapshot")
            # Tokens contain JavaScript's shortest double spelling, which
            # can look like an inexact integer above the safe integer range.
            canonical_after, value = db._canonical_key(json.loads(after_key, parse_int=float))
            after_order = key_order(value)
            if canonical_after != after_key or not matcher.matches(value, after_order):
                raise ValueError("key")
        except (TypeError, ValueError, RecursionError):
            raise DatabaseError(
                "After token belongs to a different callback snapshot, selector, or query format; "
                "obtain a fresh query snapshot"
            ) from None

    # Exact-key lookup is O(1) directly against canonical keys in state_map.
    kind = matcher.kind
    if kind == "key":
        target_canon = canonical["key"]
        found = state_map.get(target_canon)
        if found is not None and (after_order is None or key_order(found[0]) > after_order):
            selected_entries = [(target_canon, found)]
        else:
            selected_entries = []
    elif kind == "str_prefix":
        str_prefix = matcher.str_prefix
        matched = []
        for encoded, value in state_map.items():
            k = value[0]
            if type(k) is str and k.startswith(str_prefix):
                order = (4, k)
                if after_order is None or order > after_order:
                    matched.append((order, encoded, value))
        matched.sort(key=itemgetter(0))
        selected_entries = [(encoded, value) for _, encoded, value in matched]
    elif kind == "list_prefix":
        prefix_len = matcher.list_prefix_len
        prefix_orders = matcher.list_prefix_orders
        matched = []
        for encoded, value in state_map.items():
            k = value[0]
            if type(k) is list and len(k) >= prefix_len:
                order = key_order(k)
                if order[1][:prefix_len] == prefix_orders and (after_order is None or order > after_order):
                    matched.append((order, encoded, value))
        matched.sort(key=itemgetter(0))
        selected_entries = [(encoded, value) for _, encoded, value in matched]
    else:
        start_order = matcher.start_order
        inc_start = matcher.inclusive_start
        end_order = matcher.end_order
        inc_end = matcher.inclusive_end
        matched = []
        if start_order is None and end_order is None and after_order is None:
            for encoded, value in state_map.items():
                matched.append((key_order(value[0]), encoded, value))
        else:
            for encoded, value in state_map.items():
                order = key_order(value[0])
                if after_order is not None and order <= after_order:
                    continue
                if start_order is not None and (order < start_order or (order == start_order and not inc_start)):
                    continue
                if end_order is not None and (order > end_order or (order == end_order and not inc_end)):
                    continue
                matched.append((order, encoded, value))
        matched.sort(key=itemgetter(0))
        selected_entries = [(encoded, value) for _, encoded, value in matched]

    complete = len(selected_entries) <= limit
    rows = []
    for encoded, (orig_key, state, fin, sequence) in selected_entries[:limit]:
        if fin is _MISSING:
            orig_key, state, fin, sequence = db._finalize_view_group(name, encoded)
        rows.append(Row(_clone_trusted(orig_key), _clone_trusted(fin), _clone_trusted(state), sequence))
    continuation = None if complete else _json([QUERY_FORMAT, cursor, selected, selected_entries[limit - 1][0]])
    return Snapshot(tuple(rows), cursor=cursor, after=continuation, complete=complete)


def flush_view_checkpoints(db: Any) -> None:
    if db._engine is None:
        return
    dirty_views = [
        name for name, entry in db._views.items()
        if entry.mode == "documents" and entry.dirty
    ]
    if not dirty_views:
        return
    sequence = db.stats()["sequence"]
    checkpoints = {
        name: db._callback_checkpoint(name, db._views[name].states, sequence)
        for name in dirty_views
    }
    db._call({"op": "set_callback_checkpoints", "checkpoints": checkpoints})
    for name in dirty_views:
        db._views[name].dirty = False

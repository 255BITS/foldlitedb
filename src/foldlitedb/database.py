"""Native append-only document storage and disposable Wasm view caches.

Python supplies a small API over the Rust engine. Immutable document batches are
its durable input; every view state, index, and checkpoint can be recreated.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
import hashlib
from importlib.resources import files
import json
import os
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Iterable, Iterator, Mapping

from ._query import QUERY_FORMAT, key_order, matches_selector
from .types import (
    ChangePage, CursorExpired, DatabaseError, Document, DocumentConflict,
    GenerationMismatch, IngestResult, Row, Snapshot, SourceOrderError, _json,
)


_MISSING = object()


def _identity(value: str, label: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise DatabaseError(f"{label} must contain 1..4096 UTF-8 bytes")
    return value


def _encode(value, *, max_depth=64):
    def validate(item, depth=0):
        if depth > max_depth:
            raise DatabaseError("Portable JSON nesting exceeds 64")
        if isinstance(item, int) and not isinstance(item, bool):
            try:
                if int(float(item)) != item:
                    raise ValueError("integer precision")
            except (ValueError, OverflowError) as error:
                raise DatabaseError(
                    "Portable JSON integers must be exactly representable as IEEE-754 doubles; "
                    "encode larger identifiers as strings"
                ) from error
        elif isinstance(item, str):
            try:
                item.encode("utf-8")
            except UnicodeError as error:
                raise DatabaseError("Portable JSON strings must contain valid Unicode scalar values") from error
        elif isinstance(item, dict):
            for key, child in item.items():
                if isinstance(key, str):
                    validate(key, depth + 1)
                validate(child, depth + 1)
        elif isinstance(item, (list, tuple)):
            for child in item:
                validate(child, depth + 1)
    try:
        validate(value)
    except RecursionError as error:
        raise DatabaseError("Portable JSON nesting is too deep") from error
    return _json(value)


def _copy(value):
    return json.loads(_encode(value))


def _synchronized(method):
    @wraps(method)
    def synchronized(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return synchronized


def portable_assets():
    """Return packaged Wasm examples and the buildless browser worker/client."""
    return files("foldlitedb").joinpath("portable_assets")


class RustReducer:
    """Adapter for the bundled, bounded Rust/WASM numeric reducers.

    Maps emit finite numbers or null. Source and document identity plus emission
    order provide deterministic tie breaking for envelope positions.
    """

    def __init__(self, name: str, *, runtime=None, positioned: bool = False):
        self.name = name
        self._runtime = runtime
        self._owns_runtime = runtime is None
        self.positioned = positioned

    @property
    def descriptor(self):
        from .reducers import descriptor
        return {"kind": "rust", "name": self.name, "positioned": self.positioned,
                "module_sha256": descriptor()["sha256"]}

    @property
    def runtime(self):
        if self._runtime is None:
            from .reducers import Reducer
            self._runtime = Reducer()
        return self._runtime

    def _invoke(self, fn):
        from .reducers import Reducer, ReducerError
        try:
            return fn(self.runtime)
        except ReducerError as error:
            if self._owns_runtime and "trapped" in str(error):
                self._runtime = Reducer()
            raise DatabaseError(str(error)) from error

    def identity(self):
        return self._invoke(lambda rt: rt.identity(self.name))

    def prepare(self, value, document: Document, ordinal: int):
        if self.positioned:
            return value
        if value is not None and (type(value) not in (int, float)):
            raise DatabaseError("Rust numeric reducers accept numbers or null")
        tie = hashlib.sha256(_json([document.source, document.id, ordinal]).encode()).hexdigest()
        return {"value": value, "position": [document.position, tie]}

    def add(self, state, values):
        from .reducers import MAX_BATCH_VALUES, MAX_REQUEST_BYTES
        batch = []

        def overhead(current):
            return len(json.dumps({"op": "add", "state": current, "values": []},
                                  allow_nan=False, separators=(",", ":"), ensure_ascii=True).encode())

        used = overhead(state)
        for value in values:
            size = len(json.dumps(value, allow_nan=False, separators=(",", ":"), ensure_ascii=True).encode())
            if batch and (len(batch) >= MAX_BATCH_VALUES or used + size + 1 > MAX_REQUEST_BYTES):
                state = self._invoke(lambda rt: rt.add(state, batch))
                batch = []
                used = overhead(state)
            if used + size + bool(batch) > MAX_REQUEST_BYTES:
                raise DatabaseError("One numeric contribution exceeds the reducer request byte budget")
            used += size + bool(batch)
            batch.append(value)
        return self._invoke(lambda rt: rt.add(state, batch)) if batch else state

    def merge(self, left, right):
        return self._invoke(lambda rt: rt.merge(left, right))

    def finalize(self, state):
        return self._invoke(lambda rt: rt.finalize(state))


class OrderedFold:
    """A versioned Python fold in persisted ingestion order, without merge.

    ``step(state, contribution)`` returns the next finite JSON state. Changing
    code or captured reference inputs requires a new version or view references.
    """

    def __init__(self, initial: Any, step: Callable[[Any, Any], Any], *, version: str,
                 finalize: Callable[[Any], Any] | None = None):
        self._initial = _json(initial)
        self.step = step
        self.version = _identity(version, "fold version")
        self._finalize = finalize

    @property
    def descriptor(self):
        return {"kind": "ordered-fold", "version": self.version,
                "initial": json.loads(self._initial), "order": "ingestion"}

    def identity(self):
        return json.loads(self._initial)

    def add(self, state, values):
        for value in values:
            state = self.step(state, value)
        return state

    def finalize(self, state):
        return self._finalize(state) if self._finalize else state


class View:
    """Immutable view definition, supporting compiled Wasm modules or callback specifications."""

    def __init__(
        self,
        name_or_module: bytes | str | os.PathLike,
        *,
        version: str | None = None,
        map: Callable[[Document], Iterable[tuple[Any, Any]]] | None = None,
        reducer: Any = None,
        references: Mapping[str, Any] | None = None,
        sha256: str | None = None,
    ):
        if (
            map is not None
            or reducer is not None
            or version is not None
            or references is not None
        ):
            self.name = _identity(str(name_or_module), "view name")
            self.version = _identity("1" if version is None else version, "view version")
            self.map = map
            self.reducer = reducer
            self.references = _copy(dict(references or {}))
            self.is_callback = True
            self._bundled_module = None
            self._module = None
            self.sha256 = ""
        else:
            binary = name_or_module if isinstance(name_or_module, bytes) else Path(name_or_module).read_bytes()
            if not binary or len(binary) > 16 * 1024 * 1024:
                raise DatabaseError("View module must contain 1..16777216 bytes")
            digest = hashlib.sha256(binary).hexdigest()
            if sha256 is not None and digest != sha256:
                raise DatabaseError("View module digest mismatch")
            self._module = binary
            self.sha256 = digest
            self.is_callback = False
            self.name = "default"
            self.version = "1"
            self.map = None
            self.reducer = None
            self.references = {}
            self._bundled_module = None

    @property
    def module(self) -> bytes:
        if self.is_callback:
            if self._bundled_module is None:
                resources = portable_assets()
                manifest = json.loads(resources.joinpath("documents.json").read_text())
                self._bundled_module = View(resources.joinpath("documents.wasm").read_bytes(),
                                            sha256=manifest["sha256"]).module
            return self._bundled_module
        return self._module

    @classmethod
    def bundled(cls, name: str = "metrics") -> View:
        """Load a checked, precompiled example without an AssemblyScript compiler."""
        if name not in ("metrics", "catalogue"):
            raise DatabaseError("Bundled view must be metrics or catalogue")
        resources = portable_assets()
        manifest = json.loads(resources.joinpath(name + ".json").read_text())
        return cls(resources.joinpath(name + ".wasm").read_bytes(), sha256=manifest["sha256"])


def _validate_callback_view(view: View) -> tuple[View, dict[str, Any]]:
    _identity(view.name, "view name")
    _identity(view.version, "view version")
    if not callable(view.map):
        raise DatabaseError("View map must be callable")
    if any(not callable(getattr(view.reducer, method, None))
           for method in ("identity", "add", "finalize")):
        raise DatabaseError("View reducer must implement identity, add, and finalize")
    descriptor = _copy({"version": view.version, "references": view.references,
                        "reducer": getattr(view.reducer, "descriptor", str(view.reducer))})
    registered = View(view.name, version=view.version, map=view.map,
                      reducer=view.reducer, references=descriptor["references"])
    return registered, descriptor


@dataclass
class _RegisteredView:
    view: View
    descriptor: dict[str, Any]
    states: dict[str, tuple[Any, Any, Any, int]]
    mode: str = "documents"
    external_descriptor_matches: bool = False

    def copy(self) -> _RegisteredView:
        return _RegisteredView(
            view=self.view,
            descriptor=_copy(self.descriptor),
            states=dict(self.states),
            mode=self.mode,
            external_descriptor_matches=self.external_descriptor_matches,
        )


def _document(value):
    return Document(value["id"], value["payload"], value["position"], value["source"])


def _rows(values):
    return tuple(Row(value["key"], value["result"], value["state"], value["sequence"])
                 for value in values)


class Database:
    """A Rust document log with a disposable, reproducible Wasm view cache.

    ``path`` names the immutable append-only log. Its sibling ``path.cache`` is
    an optional checkpoint, and ``path.lock`` coordinates exclusive ownership.
    Use ``":memory:"`` for an entirely transient database. One active owner may
    open a disk log; calls through that owner are safe across Python threads.

    Ingest acknowledges a batch after syncing the log. Missing, corrupt, or
    incompatible checkpoints are rebuilt from the original document batches.
    Opening the log with a different view automatically rebuilds its cache.
    """

    def __init__(
        self,
        path,
        view: View | bytes | str | os.PathLike | None = None,
        *,
        generation: str | None = None,
        reset: bool = False,
    ):
        self._lock = RLock()
        self._callback_evaluating = False
        path_str = os.fspath(path)

        if view is None:
            self.view = View.bundled()
        elif isinstance(view, View):
            self.view = view
        else:
            self.view = View(view)
        if getattr(self.view, "is_callback", False):
            _validate_callback_view(self.view)

        self._views: dict[str, _RegisteredView] = {}

        try:
            from ._native import Engine
        except ImportError as error:
            raise RuntimeError(
                "Database requires the native Foldlite DB extension. "
                "Install a built foldlitedb wheel, or build this checkout with "
                "python -m pip install . (requires Rust when building from source)."
            ) from error
        try:
            self._engine = (Engine(path_str, self.view.module, reset=True) if reset
                            else Engine(path_str, self.view.module))
        except (ValueError, TypeError, OverflowError) as error:
            raise DatabaseError(str(error)) from error

        try:
            if generation is not None:
                _identity(generation, "generation")
                existing_gen = self.get("_generation", source="_meta")
                if existing_gen is not None:
                    found = existing_gen.payload.get("generation") if isinstance(existing_gen.payload, dict) else None
                    if found != generation:
                        raise GenerationMismatch(f"Database generation mismatch: expected {generation}, found {found}")
                elif self.stats()["documents"] > 0:
                    raise GenerationMismatch(f"Database generation mismatch: expected {generation}, found None")
                else:
                    self._ingest_records([{"id": "_generation", "source": "_meta", "position": 0, "payload": {"generation": generation}}])

            if getattr(self.view, "is_callback", False):
                self.register(self.view)
        except BaseException:
            self._close_without_checkpoint()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @_synchronized
    def _close_without_checkpoint(self):
        engine, self._engine = self._engine, None
        if engine is not None:
            try:
                self._execute_json(engine, '{"op":"close","checkpoint":false}')
            except BaseException:
                pass

    @_synchronized
    def close(self):
        """Write a disposable checkpoint and release ownership of the log.

        Closing an already closed database is harmless. Checkpoint errors are
        reported, and the native handle is released even when writing fails.
        """
        self._assert_mutable()
        error = None
        if self._engine is not None:
            try:
                self._flush_view_checkpoints()
            except BaseException as failure:
                error = failure
        engine, self._engine = self._engine, None
        if engine is not None:
            try:
                self._execute_json(engine, '{"op":"close"}')
            except BaseException:
                if error is None:
                    raise
        if error is not None:
            raise error

    @staticmethod
    def migrate(source, destination):
        """Copy an older log into the current format without changing its source."""
        from ._native import Engine
        try:
            Engine.migrate_log(os.fspath(source), os.fspath(destination))
        except (ValueError, TypeError, OverflowError) as error:
            raise DatabaseError(str(error)) from error

    def _assert_mutable(self):
        if self._callback_evaluating:
            raise DatabaseError("A view callback must not mutate its database")

    def _has_matching_external_descriptor(self, name: str) -> bool:
        entry = self._views.get(name)
        return bool(entry is not None and entry.external_descriptor_matches)

    def _call(self, request):
        return self._call_json(_encode(request, max_depth=65))

    @_synchronized
    def _call_json(self, request):
        engine = self._engine
        if engine is None:
            raise DatabaseError("Database is closed")
        return self._execute_json(engine, request)

    def _execute_json(self, engine, request):
        try:
            return json.loads(engine.execute_json(request))
        except (ValueError, TypeError, OverflowError) as error:
            message = str(error)
            kind = DatabaseError
            if not message.startswith("View rejected operation:"):
                lower = message.lower()
                if "conflict" in lower:
                    kind = DocumentConflict
                elif (
                    "to recover, run query() for a fresh snapshot" in lower
                    or "cursor belongs to" in lower
                    or "cursor is ahead" in lower
                ):
                    kind = CursorExpired
                elif "source order" in lower or "source positions must advance" in lower:
                    kind = SourceOrderError
            raise kind(message) from error

    @_synchronized
    def _ingest_records(self, records, *, callbacks=True):
        self._assert_mutable()
        encoded = []
        for index, record in enumerate(records):
            if index >= 1024:
                raise DatabaseError(
                    "Batch exceeds max_batch (1024). Use smaller batches or iterate "
                    "db.ingest_many(..., batch_size=...) with a smaller batch_size."
                )
            # Snapshot at each yield; the record envelope is not payload depth.
            encoded.append(_encode(record, max_depth=65))
        try:
            regular_views = [name for name, entry in self._views.items() if entry.mode == "documents"]
            if not callbacks or not regular_views:
                return self._call_json('{"op":"ingest","documents":[' + ",".join(encoded) + "]}")
            prepared = self._call_json('{"op":"prepare_ingest","documents":[' + ",".join(encoded) + "]}")
            try:
                states = self._stage_callbacks(
                    prepared["new_documents"], prepared["sequence"], regular_views
                )
                checkpoints = {name: self._callback_checkpoint(name, state, prepared["sequence"])
                               for name, state in states.items()}
                result = self._call_json(_json({"op": "commit_prepared", "token": prepared["token"],
                                                "checkpoints": checkpoints}))
            except BaseException:
                self._call({"op": "cancel_prepared"})
                raise
            for name, state in states.items():
                self._views[name].states = state
            return result
        except DatabaseError as error:
            # The engine's limit errors are host-neutral; add the Python recovery.
            if "(ABI_BUFFER_BYTES)" in str(error) or "(MAX_BATCH_" in str(error):
                raise type(error)(
                    f"{error} In Python, iterate db.ingest_many(..., batch_size=...) "
                    "with a smaller batch_size."
                ) from error
            raise

    def add(self, payload: Any, *, id=None, source="default", position=None) -> Document:
        record = {"payload": payload, "source": source}
        if id is not None:
            record["id"] = id
        if position is not None:
            record["position"] = position
        return _document(self._ingest_records([record])["documents"][0])

    def add_many(self, payloads: Iterable[Any], *, source="default") -> tuple[Document, ...]:
        _encode(source)
        if not isinstance(source, str) or not source or len(source.encode("utf-8")) > 4096:
            raise DatabaseError("source must contain 1..4096 UTF-8 bytes")
        result = self._ingest_records({"payload": payload, "source": source} for payload in payloads)
        return tuple(_document(value) for value in result["documents"])

    def ingest(self, documents: Iterable[Document]) -> IngestResult:
        """Atomically append named observations or replay retained records.

        A ``Document`` with no position receives the next source position; an
        identical named retry retains its original position. Explicit positions
        retain their source ordering and replay semantics.
        """
        def records():
            for document in documents:
                if not isinstance(document, Document):
                    raise DatabaseError("ingest expects Document instances")
                record = {"id": document.id, "payload": document.payload,
                          "source": document.source}
                if document.position is not None:
                    record["position"] = document.position
                yield record

        result = self._ingest_records(records())
        return IngestResult(result["inserted"], result["replayed"], result["sequence"])

    def _canonical_key(self, key):
        key = _copy(key)
        # Validate from the key itself, independently of the transport envelope.
        return self._call_json(_json({"op": "canonical_key", "key": key})), key

    def _callback_checkpoint(self, name, state_map, sequence):
        entry = self._views[name]
        groups = []
        for canon_key in sorted(state_map):
            key, state, result, row_sequence = state_map[canon_key]
            if result is _MISSING:
                evaluating = self._callback_evaluating
                self._callback_evaluating = True
                try:
                    result = _copy(entry.view.reducer.finalize(_copy(state)))
                finally:
                    self._callback_evaluating = evaluating
                state_map[canon_key] = (key, state, result, row_sequence)
            groups.append({"key": _copy(key), "state": _copy(state),
                           "result": _copy(result), "sequence": row_sequence})
        return {"format": 1, "descriptor": _copy(entry.descriptor),
                "sequence": sequence, "groups": groups}

    def _stage_callbacks(self, documents, sequence, names, *, initial=None):
        states = {}
        evaluating = self._callback_evaluating
        self._callback_evaluating = True
        try:
            for name in names:
                entry = self._views[name]
                view = entry.view
                state_map = dict(initial[name] if initial is not None else entry.states)
                emissions = {}
                count = 0
                for value in documents:
                    if value["source"].startswith("_"):
                        continue
                    doc = Document(value["id"], _copy(value["payload"]), value["position"], value["source"])
                    for ordinal, (key, contribution) in enumerate(view.map(doc)):
                        count += 1
                        if count > 4096:
                            raise DatabaseError("A callback batch may emit at most 4096 contributions")
                        canon_key, key = self._canonical_key(key)
                        contribution = _copy(contribution)
                        if getattr(view.reducer, "positioned", True) is False and hasattr(view.reducer, "prepare"):
                            contribution = _copy(view.reducer.prepare(contribution, doc, ordinal))
                        if canon_key not in emissions:
                            emissions[canon_key] = (key, [])
                        emissions[canon_key][1].append(contribution)
                for canon_key, (key, contributions) in sorted(emissions.items()):
                    current = _copy(state_map[canon_key][1]) if canon_key in state_map else _copy(view.reducer.identity())
                    state = _copy(view.reducer.add(current, contributions))
                    # Finalizers may mutate their argument; state and result are independent.
                    result = _copy(view.reducer.finalize(_copy(state)))
                    state_map[canon_key] = (key, state, result, sequence)
                states[name] = state_map
        finally:
            self._callback_evaluating = evaluating
        return states

    @_synchronized
    def _reduce_view_group(
        self,
        view_name: str,
        canon_key: str | None,
        orig_key: Any,
        vals: list[Any],
        sequence: int,
    ) -> None:
        if not vals:
            return
        self._assert_mutable()
        entry = self._views[view_name]
        view = entry.view
        if canon_key is None:
            canon_key, orig_key = self._canonical_key(orig_key)
        state_map = entry.states
        evaluating = self._callback_evaluating
        self._callback_evaluating = True
        try:
            copied_vals = [_copy(value) for value in vals]
            if canon_key in state_map:
                _, cur_state, _, _ = state_map[canon_key]
                new_state = view.reducer.add(_copy(cur_state), copied_vals)
            else:
                new_state = view.reducer.add(_copy(view.reducer.identity()), copied_vals)
        finally:
            self._callback_evaluating = evaluating
        state_map[canon_key] = (_copy(orig_key), _copy(new_state), _MISSING, sequence)

    def _finalize_view_group(self, view_name: str, canon_key: str):
        entry = self._views[view_name]
        orig_key, state, fin, seq = entry.states[canon_key]
        if fin is _MISSING:
            evaluating = self._callback_evaluating
            self._callback_evaluating = True
            try:
                fin = _copy(entry.view.reducer.finalize(_copy(state)))
            finally:
                self._callback_evaluating = evaluating
            entry.states[canon_key] = (orig_key, state, fin, seq)
        return orig_key, state, fin, seq

    @_synchronized
    def _commit_projection_suffix(
        self,
        view_name: str,
        pending_emissions: dict[str, tuple[Any, list[Any]]],
        checkpoint: int,
        replay: Any,
    ) -> None:
        for canon_key, (orig_key, vals) in pending_emissions.items():
            self._reduce_view_group(
                view_name, canon_key, orig_key, vals, checkpoint
            )
        records = [
            {
                "id": str(checkpoint),
                "source": "projection",
                "position": checkpoint,
                "payload": {"cursor": replay},
            }
        ]
        entry = self._views[view_name]
        groups = []
        for canon_key in list(entry.states.keys()):
            k, s, fin, seq = self._finalize_view_group(view_name, canon_key)
            groups.append({"key": _copy(k), "state": _copy(s), "result": _copy(fin), "sequence": seq})
        records.append(
                {
                    "id": str(checkpoint),
                    "source": f"_view:{view_name}",
                    "position": checkpoint,
                    "payload": {
                        "descriptor": entry.descriptor,
                        "checkpoint": checkpoint,
                        "groups": groups,
                    },
                }
            )
        self._ingest_records(records, callbacks=False)

    def ingest_many(self, documents: Iterable[Document], *, batch_size=100) -> Iterator[IngestResult]:
        """Replay a larger iterable as bounded, separately committed batches."""
        if type(batch_size) is not int or not 1 <= batch_size <= 1024:
            raise DatabaseError("batch_size must be between 1 and 1024")
        batch = []
        for document in documents:
            if not isinstance(document, Document):
                raise DatabaseError("ingest expects Document instances")
            batch.append(Document(document.id, json.loads(_encode(document.payload)),
                                  document.position, document.source))
            if len(batch) == batch_size:
                yield self.ingest(batch)
                batch = []
        if batch:
            yield self.ingest(batch)

    def preview(self, payload: Any) -> list[dict[str, Any]]:
        """Map one payload to ``{key, value}`` emissions without writing."""
        if self.view.is_callback:
            raise DatabaseError("Callback preview requires a resolved Document; call view.map(document) explicitly")
        return self._call({"op": "preview", "payload": payload})

    @_synchronized
    def register(self, view: View, *, _external=False) -> str:
        """Attach a descriptor-bound callback projection and recover its coverage.

        Ordinary callback projections replay retained source batches. The external
        materializer separately validates its authoritative stream and stores one
        atomic progress/state record; it is not reconstructed from pointer records.
        """
        self._assert_mutable()
        registered, descriptor = _validate_callback_view(view)
        previous = self._views.get(view.name)
        if previous is not None:
            previous = previous.copy()
        entry = _RegisteredView(
            view=registered,
            descriptor=descriptor,
            states={},
            mode="external" if _external else "documents",
        )
        self._views[view.name] = entry
        try:
            if _external:
                checkpoint = None
                watermark = self.stats()["checkpoints"].get(f"_view:{view.name}")
                if watermark is not None:
                    saved = self.get(str(watermark), source=f"_view:{view.name}")
                    checkpoint = saved.payload if saved is not None else None
                matches = checkpoint is not None and checkpoint.get("descriptor") == descriptor
                entry.external_descriptor_matches = matches
                if matches:
                    entry.states = self._restore_callback_groups(checkpoint)
            else:
                saved = self._call({"op": "callback_checkpoints"}).get(view.name)
                coverage = 0
                if saved is not None and saved.get("descriptor") == descriptor:
                    try:
                        entry.states = self._restore_callback_groups(saved)
                        coverage = saved["sequence"]
                        # Coverage must be a real boundary in this native generation.
                        self._call({"op": "document_batches", "since": coverage})
                    except (DatabaseError, KeyError, TypeError):
                        entry.states = {}
                        coverage = 0
                states = self._replay_callbacks([view.name], coverage)
                sequence = self.stats()["sequence"]
                checkpoints = {view.name: self._callback_checkpoint(view.name, states[view.name], sequence)}
                self._call_json(_json({"op": "set_callback_checkpoints", "checkpoints": checkpoints}))
                entry.states = states[view.name]
        except BaseException:
            if previous is None:
                self._views.pop(view.name, None)
            else:
                self._views[view.name] = previous
            raise
        return view.version

    def _restore_callback_groups(self, checkpoint):
        states = {}
        for item in checkpoint["groups"]:
            canon_key, key = self._canonical_key(item["key"])
            if canon_key in states:
                raise DatabaseError("Callback checkpoint repeats a group key")
            states[canon_key] = (key, _copy(item["state"]), _copy(item["result"]), item["sequence"])
        return states

    def _replay_callbacks(self, names, coverage=0, *, cursor=None, initial=None):
        states = {name: dict(self._views[name].states) for name in names} if initial is None else initial
        while True:
            request = {"op": "document_batches", "since": coverage}
            if cursor is not None:
                request["cursor"] = cursor
            batch = self._call(request)
            if batch["documents"]:
                states = self._stage_callbacks(batch["documents"], batch["sequence"], names, initial=states)
                coverage = batch["sequence"]
            if batch["complete"]:
                return states

    def get(self, id: str, *, source: str = "default") -> Document | None:
        """Fetch an immutable document by its id and source in O(1)."""
        result = self._call({"op": "get", "id": str(id), "source": source})
        return _document(result) if result else None

    @_synchronized
    def query(self, name_or_key=_MISSING, *, view: str | None = None, key=_MISSING,
              start_key=_MISSING, end_key=_MISSING, prefix=_MISSING,
              inclusive_start=True, inclusive_end=True, limit=100, cursor=None, after=None) -> Snapshot:
        """Read exact keys, an ascending key range, or a string/array prefix.

        Bounds are inclusive by default. Supply the same selector and both
        continuation tokens on subsequent pages; page size may change.
        """
        if view is not None:
            if not isinstance(view, str) or view not in self._views:
                raise DatabaseError(f"Unknown registered callback view: {view!r}")
            if name_or_key is not _MISSING:
                if key is not _MISSING:
                    raise DatabaseError("Cannot specify both positional key and keyword key=")
                key = name_or_key
            return self._query_callback(
                view, key=key, start_key=start_key, end_key=end_key, prefix=prefix,
                inclusive_start=inclusive_start, inclusive_end=inclusive_end,
                limit=limit, cursor=cursor, after=after,
            )

        name = name_or_key if isinstance(name_or_key, str) and name_or_key in self._views else None
        if name is None:
            has_selector = any(v is not _MISSING for v in (key, start_key, end_key, prefix))
            if name_or_key is not _MISSING and has_selector:
                if isinstance(name_or_key, str):
                    raise DatabaseError(f"Unknown registered callback view: {name_or_key!r}")
                raise DatabaseError("Cannot combine a positional key with key, range, or prefix selectors")
            if self.view.is_callback:
                name = self.view.name
                if name_or_key is not _MISSING:
                    key = name_or_key
        if name is not None:
            return self._query_callback(
                name, key=key, start_key=start_key, end_key=end_key, prefix=prefix,
                inclusive_start=inclusive_start, inclusive_end=inclusive_end,
                limit=limit, cursor=cursor, after=after,
            )

        request = {"op": "query", "limit": limit,
                   "inclusive_start": inclusive_start, "inclusive_end": inclusive_end}
        actual_key = key if key is not _MISSING else (name_or_key if name_or_key is not _MISSING else _MISSING)
        if actual_key is not _MISSING:
            request["key"] = _copy(actual_key)
        for option, value in (("start_key", start_key), ("end_key", end_key), ("prefix", prefix)):
            if value is not _MISSING:
                request[option] = _copy(value)
        if cursor is not None:
            request["cursor"] = cursor
        if after is not None:
            request["after"] = after
        # Keys have their own JSON depth budget, without counting this envelope.
        result = self._call_json(_json(request))
        return Snapshot(_rows(result["rows"]), result["cursor"], result.get("after"), result["complete"])

    def _query_callback(self, name, *, key, start_key, end_key, prefix,
                        inclusive_start, inclusive_end, limit, cursor, after):
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise DatabaseError("limit must be between 1 and 1024")
        if type(inclusive_start) is not bool or type(inclusive_end) is not bool:
            raise DatabaseError("inclusive_start and inclusive_end must be booleans")
        has_range = start_key is not _MISSING or end_key is not _MISSING
        if sum((key is not _MISSING, has_range, prefix is not _MISSING)) > 1:
            raise DatabaseError("key, range bounds, and prefix selectors are mutually exclusive")
        selector = {"inclusive_start": inclusive_start, "inclusive_end": inclusive_end}
        canonical = {"inclusive_start": _json(inclusive_start), "inclusive_end": _json(inclusive_end)}
        for option, value in (("key", key), ("start_key", start_key), ("end_key", end_key), ("prefix", prefix)):
            if value is not _MISSING:
                canonical[option], selector[option] = self._canonical_key(value)
        if "prefix" in selector and not isinstance(selector["prefix"], (str, list)):
            raise DatabaseError("prefix must be a string or array")
        # Compose canonical children directly: the selector envelope does not
        # consume any of the supported key nesting depth.
        selected = "{" + ",".join(_json(option) + ":" + canonical[option]
                                   for option in sorted(canonical)) + "}"
        entry = self._views[name]
        signature = hashlib.sha256(_json([name, entry.descriptor]).encode()).hexdigest()
        current = self.stats()["cursor"]
        if cursor is None:
            native_cursor = current
            cursor = current + "~" + signature
        else:
            if not isinstance(cursor, str) or not cursor.endswith("~" + signature):
                raise CursorExpired("Callback cursor belongs to a different view descriptor; obtain a fresh query snapshot")
            native_cursor = cursor.rsplit("~", 1)[0]
            self._call({"op": "query", "cursor": native_cursor, "limit": 1})
        state_map = entry.states
        if native_cursor != current:
            if entry.mode != "documents":
                raise CursorExpired("Historical external projection snapshots are unavailable; obtain a fresh query snapshot")
            state_map = self._replay_callbacks([name], cursor=native_cursor, initial={name: {}})[name]
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
                canonical_after, value = self._canonical_key(json.loads(after_key, parse_int=float))
                if canonical_after != after_key or not matches_selector(value, selector):
                    raise ValueError("key")
                after_order = key_order(value)
            except (TypeError, ValueError, RecursionError):
                raise DatabaseError("After token belongs to a different callback snapshot, selector, or query format; obtain a fresh query snapshot") from None
        entries = sorted(
            ((encoded, value) for encoded, value in state_map.items()
             if matches_selector(value[0], selector)
             and (after_order is None or key_order(value[0]) > after_order)),
            key=lambda entry: key_order(entry[1][0]),
        )
        complete = len(entries) <= limit
        rows = []
        for encoded, (orig_key, state, fin, sequence) in entries[:limit]:
            if fin is _MISSING:
                orig_key, state, fin, sequence = self._finalize_view_group(name, encoded)
            rows.append(Row(_copy(orig_key), _copy(fin), _copy(state), sequence))
        continuation = None if complete else _json([QUERY_FORMAT, cursor, selected, entries[limit - 1][0]])
        return Snapshot(tuple(rows), cursor=cursor, after=continuation, complete=complete)

    def rows(self, name_or_key=_MISSING, *, view: str | None = None, key=_MISSING,
             start_key=_MISSING, end_key=_MISSING, prefix=_MISSING,
             inclusive_start=True, inclusive_end=True, limit=100) -> Iterator[Row]:
        options = {"inclusive_start": inclusive_start, "inclusive_end": inclusive_end}
        if view is not None:
            options["view"] = view
        for option, value in (("key", key), ("start_key", start_key), ("end_key", end_key), ("prefix", prefix)):
            if value is not _MISSING:
                options[option] = _copy(value)
        args = (_copy(name_or_key),) if name_or_key is not _MISSING else ()
        page = self.query(*args, limit=limit, **options)
        while True:
            yield from page.rows
            if page.complete:
                return
            page = self.query(*args, limit=limit, cursor=page.cursor, after=page.after, **options)

    def changes(self, *, since, limit=100) -> ChangePage:
        if self.view.is_callback:
            raise DatabaseError("Callback views do not expose a change feed; obtain a fresh query snapshot")
        result = self._call({"op": "changes", "since": since, "limit": limit})
        return ChangePage(_rows(result["rows"]), result["cursor"], result["complete"])

    def documents(self, *, limit=100) -> Iterator[Document]:
        """Export resolved source records in ingestion order at a fixed snapshot."""
        request = {"op": "documents", "limit": limit}
        while True:
            page = self._call(request)
            yield from (_document(value) for value in page["documents"])
            if page["complete"]:
                return
            request.update(cursor=page["cursor"], after=page["after"])

    def stats(self):
        """Return document, view, log, cache, and replay metadata."""
        return self._call({"op": "stats"})

    @_synchronized
    def _flush_view_checkpoints(self):
        """Update disposable native callback caches without changing source bytes."""
        if self._engine is None:
            return
        sequence = self.stats()["sequence"]
        checkpoints = {name: self._callback_checkpoint(name, entry.states, sequence)
                       for name, entry in self._views.items()
                       if entry.mode == "documents"}
        if checkpoints:
            self._call_json(_json({"op": "set_callback_checkpoints", "checkpoints": checkpoints}))

    @_synchronized
    def checkpoint(self, source: str | None = None):
        """Persist a disposable cache snapshot without modifying the log.

        If source is specified, return the committed source watermark position in O(1).
        """
        self._assert_mutable()
        if source is not None:
            return self.stats().get("checkpoints", {}).get(source)
        self._flush_view_checkpoints()
        return self._call({"op": "checkpoint"})

    @_synchronized
    def rebuild(self):
        """Reconstruct native and callback projections from original source batches."""
        self._assert_mutable()
        names = [name for name, entry in self._views.items() if entry.mode == "documents"]
        states = self._replay_callbacks(names, initial={name: {} for name in names}) if names else {}
        request = {"op": "rebuild"}
        if names:
            sequence = self.stats()["sequence"]
            request["checkpoints"] = {name: self._callback_checkpoint(name, states[name], sequence)
                                      for name in names}
        result = self._call_json(_json(request))
        for name, state in states.items():
            self._views[name].states = state
        return result

"""Native append-only document storage and disposable Wasm view caches.

Python supplies a small API over the Rust engine. Immutable document batches are
its durable input; every view state, index, and checkpoint can be recreated.
"""
from __future__ import annotations

from functools import wraps
import hashlib
from importlib.resources import files
import json
import os
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Iterable, Iterator, Mapping

from ._callbacks import (
    _MISSING,
    _RegisteredView,
    callback_checkpoint,
    collect_view_emissions,
    commit_projection_suffix,
    finalize_view_group,
    flush_view_checkpoints,
    query_callback,
    reduce_view_group,
    replay_callbacks,
    restore_callback_groups,
    restore_external_projection,
    stage_callbacks,
    validate_callback_view,
)
from .types import (
    ChangePage,
    CursorExpired,
    DatabaseError,
    Document,
    DocumentConflict,
    GenerationMismatch,
    IngestResult,
    Row,
    Snapshot,
    SourceOrderError,
    _copy,
    _encode,
    _json,
    _transport_json,
)


def _identity(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DatabaseError(f"{label} must contain 1..4096 UTF-8 bytes")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise DatabaseError("Portable JSON strings must contain valid Unicode scalar values") from error
    if len(encoded) > 4096:
        raise DatabaseError(f"{label} must contain 1..4096 UTF-8 bytes")
    return value


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
                state = self._invoke(lambda rt, s=state, b=batch: rt.add(s, b))
                batch = []
                used = overhead(state)
            if used + size + bool(batch) > MAX_REQUEST_BYTES:
                raise DatabaseError("One numeric contribution exceeds the reducer request byte budget")
            used += size + bool(batch)
            batch.append(value)
        return self._invoke(lambda rt, s=state, b=batch: rt.add(s, b)) if batch else state

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
                manifest = json.loads(resources.joinpath("documents.json").read_text(encoding="utf-8"))
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
        manifest = json.loads(resources.joinpath(name + ".json").read_text(encoding="utf-8"))
        return cls(resources.joinpath(name + ".wasm").read_bytes(), sha256=manifest["sha256"])


def _validate_callback_view(view: View) -> tuple[View, dict[str, Any]]:
    return validate_callback_view(view, _identity, View)


_ERROR_KINDS = {
    "DatabaseError": DatabaseError,
    "DocumentConflict": DocumentConflict,
    "SourceOrderError": SourceOrderError,
    "CursorExpired": CursorExpired,
}


def _native_error(error: BaseException) -> DatabaseError:
    args = getattr(error, "args", ())
    if (
        len(args) == 2
        and isinstance(args[0], str)
        and isinstance(args[1], str)
        and args[0] in _ERROR_KINDS
    ):
        return _ERROR_KINDS[args[0]](args[1])
    return DatabaseError(str(error))


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
            raise _native_error(error) from error

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
            raise _native_error(error) from error

    def _assert_mutable(self):
        if self._callback_evaluating:
            raise DatabaseError("A view callback must not mutate its database")

    def _has_matching_external_descriptor(self, name: str) -> bool:
        entry = self._views.get(name)
        return bool(entry is not None and entry.external_descriptor_matches)

    def _call(self, request):
        return self._call_json(_encode(request, max_depth=65, sort_keys=False))

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
            raise _native_error(error) from error

    @_synchronized
    def _ingest_encoded(self, encoded: list[str], *, callbacks: bool = True):
        self._assert_mutable()
        try:
            regular_views = [name for name, entry in self._views.items() if entry.mode == "documents"]
            if not callbacks or not regular_views:
                return self._call_json('{"op":"ingest","documents":[' + ",".join(encoded) + "]}")
            prepared = self._call_json('{"op":"prepare_ingest","documents":[' + ",".join(encoded) + "]}")
            try:
                if not prepared["new_documents"]:
                    return self._call_json(_transport_json({"op": "commit_prepared", "token": prepared["token"]}))
                deltas = self._stage_callback_deltas(
                    prepared["new_documents"], prepared["sequence"], regular_views
                )
                updates = {name: self._callback_checkpoint(name, delta, prepared["sequence"])
                           for name, delta in deltas.items()}
                result = self._call_json(_transport_json({"op": "commit_prepared", "token": prepared["token"],
                                                          "callback_updates": updates}))
            except BaseException:
                self._call({"op": "cancel_prepared"})
                raise
            for name, delta in deltas.items():
                self._views[name].states.update(delta)
            return result
        except DatabaseError as error:
            # The engine's limit errors are host-neutral; add the Python recovery.
            if "(ABI_BUFFER_BYTES)" in str(error) or "(MAX_BATCH_" in str(error):
                raise type(error)(
                    f"{error} In Python, iterate db.ingest_many(..., batch_size=...) "
                    "with a smaller batch_size."
                ) from error
            raise

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
            encoded.append(_encode(record, max_depth=65, sort_keys=False))
        return self._ingest_encoded(encoded, callbacks=callbacks)

    def add(self, payload: Any, *, id=None, source="default", position=None) -> Document:
        record = {"payload": payload, "source": source}
        if id is not None:
            record["id"] = id
        if position is not None:
            record["position"] = position
        return _document(self._ingest_records([record])["documents"][0])

    def add_many(self, payloads: Iterable[Any], *, source="default") -> tuple[Document, ...]:
        _identity(source, "source")
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

    def ingest_many(self, documents: Iterable[Document], *, batch_size=100) -> Iterator[IngestResult]:
        """Replay a larger iterable as bounded, separately committed batches."""
        if type(batch_size) is not int or not 1 <= batch_size <= 1024:
            raise DatabaseError("batch_size must be between 1 and 1024")
        encoded = []
        for document in documents:
            if not isinstance(document, Document):
                raise DatabaseError("ingest expects Document instances")
            record = {"id": document.id, "payload": document.payload, "source": document.source}
            if document.position is not None:
                record["position"] = document.position
            # Snapshot directly to JSON once per yield, avoiding encode -> decode -> encode.
            encoded.append(_encode(record, max_depth=65, sort_keys=False))
            if len(encoded) == batch_size:
                result = self._ingest_encoded(encoded)
                yield IngestResult(result["inserted"], result["replayed"], result["sequence"])
                encoded = []
        if encoded:
            result = self._ingest_encoded(encoded)
            yield IngestResult(result["inserted"], result["replayed"], result["sequence"])

    def _canonical_key(self, key):
        key = _copy(key)
        # Validate from the key itself, independently of the transport envelope.
        return self._call_json(_transport_json({"op": "canonical_key", "key": key})), key

    def _canonical_keys(self, keys: list[Any]) -> list[tuple[str, Any]]:
        if not keys:
            return []
        copied = [_copy(key) for key in keys]
        results: list[tuple[str, Any]] = []
        for offset in range(0, len(copied), 4096):
            chunk = copied[offset:offset + 4096]
            encoded = self._call_json(_transport_json({"op": "canonical_keys", "keys": chunk}))
            results.extend(zip(encoded, chunk, strict=True))
        return results

    def _callback_checkpoint(self, name, state_map, sequence):
        return callback_checkpoint(self, name, state_map, sequence)

    def _collect_view_emissions(self, view: View, documents, *, copy_payload: bool = True):
        return collect_view_emissions(self, view, documents, copy_payload=copy_payload)

    def _stage_callback_deltas(self, documents, sequence, names):
        return stage_callbacks(self, documents, sequence, names, deltas_only=True)

    def _stage_callbacks(self, documents, sequence, names, *, initial=None):
        return stage_callbacks(self, documents, sequence, names, initial=initial, deltas_only=False)

    @_synchronized
    def _reduce_view_group(
        self,
        view_name: str,
        canon_key: str | None,
        orig_key: Any,
        vals: list[Any],
        sequence: int,
    ) -> None:
        reduce_view_group(self, view_name, canon_key, orig_key, vals, sequence)

    def _finalize_view_group(self, view_name: str, canon_key: str):
        return finalize_view_group(self, view_name, canon_key)

    @_synchronized
    def _commit_projection_suffix(
        self,
        view_name: str,
        pending_emissions: dict[str, tuple[Any, list[Any]] | tuple[Any, list[Any], int]],
        checkpoint: int,
        replay: Any,
    ) -> None:
        commit_projection_suffix(self, view_name, pending_emissions, checkpoint, replay)

    def preview(self, payload: Any) -> list[dict[str, Any]]:
        """Map one payload to ``{key, value}`` emissions without writing."""
        if self.view.is_callback:
            raise DatabaseError("Callback preview requires a resolved Document; call view.map(document) explicitly")
        return self._call({"op": "preview", "payload": payload})

    def _restore_external_projection(self, view_name: str, descriptor: dict[str, Any], watermark: int):
        return restore_external_projection(self, view_name, descriptor, watermark)

    @_synchronized
    def register(self, view: View, *, _external=False) -> str:
        """Attach a descriptor-bound callback projection and recover its coverage.

        Ordinary callback projections replay retained source batches. The external
        materializer separately validates its authoritative stream and stores
        atomic progress/state records.
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
            dirty_keys=set(),
        )
        self._views[view.name] = entry
        try:
            if _external:
                watermark = self.stats()["checkpoints"].get(f"_view:{view.name}")
                if watermark is not None:
                    matches, restored, delta_count = self._restore_external_projection(
                        view.name, descriptor, watermark
                    )
                    entry.external_descriptor_matches = matches
                    if matches:
                        entry.states = restored
                        entry.delta_count = delta_count
            else:
                saved = self._call({"op": "callback_checkpoints"}).get(view.name)
                sequence = self.stats()["sequence"]
                coverage = 0
                loaded_warm = False
                if saved is not None and saved.get("descriptor") == descriptor:
                    try:
                        entry.states = self._restore_callback_groups(saved)
                        coverage = saved["sequence"]
                        # Coverage must be a real boundary in this native generation.
                        self._call({"op": "document_batches", "since": coverage})
                        loaded_warm = True
                    except (DatabaseError, KeyError, TypeError):
                        entry.states = {}
                        coverage = 0
                        loaded_warm = False
                if not loaded_warm or coverage != sequence:
                    states = self._replay_callbacks([view.name], coverage)
                    checkpoints = {view.name: self._callback_checkpoint(view.name, states[view.name], sequence)}
                    self._call_json(_transport_json({"op": "set_callback_checkpoints", "checkpoints": checkpoints}))
                    entry.states = states[view.name]
                entry.dirty = False
        except BaseException:
            if previous is None:
                self._views.pop(view.name, None)
            else:
                self._views[view.name] = previous
            raise
        return view.version

    def _restore_callback_groups(self, checkpoint):
        return restore_callback_groups(self, checkpoint)

    def _replay_callbacks(self, names, coverage=0, *, cursor=None, initial=None):
        return replay_callbacks(self, names, coverage, cursor=cursor, initial=initial)

    def get(
        self,
        id: str | None = None,
        *,
        position: int | None = None,
        source: str = "default",
    ) -> Document | None:
        """Fetch an immutable document by its id or source position in O(1)."""
        _identity(source, "source")
        if (id is None) == (position is None):
            raise DatabaseError("get requires either id or position, not both")
        request: dict[str, Any] = {"op": "get", "source": source}
        if id is not None:
            request["id"] = _identity(id, "id")
        else:
            if type(position) is not int or not 0 <= position <= 9_007_199_254_740_991:
                raise DatabaseError("position must be a nonnegative JavaScript-safe integer")
            request["position"] = position
        result = self._call(request)
        return _document(result) if result else None

    @_synchronized
    def query(self, view_name=_MISSING, *, view: str | None = None, key=_MISSING,
              start_key=_MISSING, end_key=_MISSING, prefix=_MISSING,
              inclusive_start=True, inclusive_end=True, limit=100, cursor=None, after=None) -> Snapshot:
        """Read exact keys, an ascending key range, or a string/array prefix.

        Bounds are inclusive by default. Supply the same selector and both
        continuation tokens on subsequent pages; page size may change.
        """
        if view_name is not _MISSING and view is not None:
            raise DatabaseError("Cannot specify both positional view name and keyword view=")
        selected_view = view if view is not None else (view_name if view_name is not _MISSING else None)
        if selected_view is not None:
            if not isinstance(selected_view, str) or selected_view not in self._views:
                raise DatabaseError(f"Unknown registered callback view: {selected_view!r}")
            return self._query_callback(
                selected_view, key=key, start_key=start_key, end_key=end_key, prefix=prefix,
                inclusive_start=inclusive_start, inclusive_end=inclusive_end,
                limit=limit, cursor=cursor, after=after,
            )
        if self.view.is_callback:
            return self._query_callback(
                self.view.name, key=key, start_key=start_key, end_key=end_key, prefix=prefix,
                inclusive_start=inclusive_start, inclusive_end=inclusive_end,
                limit=limit, cursor=cursor, after=after,
            )

        request = {"op": "query", "limit": limit,
                   "inclusive_start": inclusive_start, "inclusive_end": inclusive_end}
        if key is not _MISSING:
            request["key"] = _copy(key)
        for option, value in (("start_key", start_key), ("end_key", end_key), ("prefix", prefix)):
            if value is not _MISSING:
                request[option] = _copy(value)
        if cursor is not None:
            request["cursor"] = cursor
        if after is not None:
            request["after"] = after
        # Keys have their own JSON depth budget, without counting this envelope.
        result = self._call_json(_transport_json(request))
        return Snapshot(_rows(result["rows"]), result["cursor"], result.get("after"), result["complete"])

    def _query_callback(self, name, *, key, start_key, end_key, prefix,
                        inclusive_start, inclusive_end, limit, cursor, after):
        return query_callback(
            self,
            name,
            key=key,
            start_key=start_key,
            end_key=end_key,
            prefix=prefix,
            inclusive_start=inclusive_start,
            inclusive_end=inclusive_end,
            limit=limit,
            cursor=cursor,
            after=after,
        )

    def rows(self, view_name=_MISSING, *, view: str | None = None, key=_MISSING,
             start_key=_MISSING, end_key=_MISSING, prefix=_MISSING,
             inclusive_start=True, inclusive_end=True, limit=100) -> Iterator[Row]:
        options = {"inclusive_start": inclusive_start, "inclusive_end": inclusive_end}
        if view is not None:
            options["view"] = view
        for option, value in (("key", key), ("start_key", start_key), ("end_key", end_key), ("prefix", prefix)):
            if value is not _MISSING:
                options[option] = _copy(value)
        args = (view_name,) if view_name is not _MISSING else ()
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
        flush_view_checkpoints(self)

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
        result = self._call_json(_transport_json(request))
        for name, state in states.items():
            self._views[name].states = state
            self._views[name].dirty = False
        return result

"""Native append-only document storage and disposable Wasm view caches.

Python supplies a small API over the Rust engine. Immutable document batches are
its durable input; every view state, index, and checkpoint can be recreated.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from importlib.resources import files
import json
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping

from .types import (
    ChangePage, CursorExpired, DatabaseError, Document, DocumentConflict,
    GenerationMismatch, IngestResult, Row, Snapshot, SourceOrderError, _json,
)


_MISSING = object()


def _identity(value: str, label: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise DatabaseError(f"{label} must contain 1..4096 UTF-8 bytes")
    return value


def _encode(value):
    def validate(item, depth=0):
        if depth > 64:
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

    def identity(self):
        return self.runtime.identity(self.name)

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
                state = self.runtime.add(state, batch)
                batch = []
                used = overhead(state)
            if used + size + bool(batch) > MAX_REQUEST_BYTES:
                raise DatabaseError("One numeric contribution exceeds the reducer request byte budget")
            used += size + bool(batch)
            batch.append(value)
        return self.runtime.add(state, batch) if batch else state

    def merge(self, left, right):
        return self.runtime.merge(left, right)

    def finalize(self, state):
        return self.runtime.finalize(state)


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
            or (version is not None and isinstance(name_or_module, str) and not os.path.exists(str(name_or_module)))
        ):
            self.name = _identity(str(name_or_module), "view name")
            self.version = _identity(version or "1", "view version")
            self.map = map
            self.reducer = reducer
            self.references = dict(references or {})
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
                self._bundled_module = View.bundled("metrics").module
            return self._bundled_module
        return self._module

    @module.setter
    def module(self, value: bytes):
        self._module = value

    @classmethod
    def bundled(cls, name: str = "metrics") -> View:
        """Load a checked, precompiled example without an AssemblyScript compiler."""
        if name not in ("metrics", "catalogue"):
            raise DatabaseError("Bundled view must be metrics or catalogue")
        resources = portable_assets()
        manifest = json.loads(resources.joinpath(name + ".json").read_text())
        return cls(resources.joinpath(name + ".wasm").read_bytes(), sha256=manifest["sha256"])


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
        path_str = os.fspath(path)
        if reset and path_str != ":memory:":
            for ext in ("", ".cache", ".lock"):
                p = Path(path_str + ext)
                if p.is_file() or p.is_symlink():
                    try:
                        p.unlink()
                    except OSError:
                        pass

        if view is None:
            self.view = View.bundled()
        elif isinstance(view, View):
            self.view = view
        else:
            self.view = View(view)

        self._views: dict[str, View] = {}
        self._view_descriptors: dict[str, Any] = {}
        self._view_states: dict[str, dict[str, tuple[Any, Any, int]]] = {}

        try:
            from ._native import Engine
        except ImportError as error:
            raise RuntimeError(
                "Database requires the native Foldlite DB extension. "
                "Install a built foldlitedb wheel, or build this checkout with "
                "python -m pip install . (requires Rust when building from source)."
            ) from error
        try:
            self._engine = Engine(path_str, self.view.module)
        except (ValueError, TypeError, OverflowError) as error:
            raise DatabaseError(str(error)) from error

        if getattr(self.view, "is_callback", False):
            self.register(self.view)

        if generation is not None:
            existing_gen = self.get("_generation", source="_meta")
            if existing_gen is not None:
                found = existing_gen.payload.get("generation") if isinstance(existing_gen.payload, dict) else None
                if found != generation:
                    self.close()
                    raise GenerationMismatch(f"Database generation mismatch: expected {generation}, found {found}")
            else:
                self._ingest_records([{"id": "_generation", "source": "_meta", "position": 0, "payload": {"generation": generation}}])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        """Write a disposable checkpoint and release ownership of the log.

        Closing an already closed database is harmless. Checkpoint errors are
        reported, and the native handle is released even when writing fails.
        """
        engine, self._engine = self._engine, None
        if engine is not None:
            self._execute_json(engine, '{"op":"close"}')

    def _call(self, request):
        return self._call_json(_encode(request))

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
            if "conflict" in message.lower():
                kind = DocumentConflict
            elif "cursor" in message.lower():
                kind = CursorExpired
            elif "source order" in message.lower() or "source positions must advance" in message.lower():
                kind = SourceOrderError
            raise kind(message) from error

    def _ingest_records(self, records):
        encoded = []
        for index, record in enumerate(records):
            if index >= 1024:
                raise DatabaseError(
                    "Batch exceeds max_batch (1024). Use smaller batches or iterate "
                    "db.ingest_many(..., batch_size=...) with a smaller batch_size."
                )
            encoded.append(_encode(record))
        return self._call_json('{"op":"ingest","documents":[' + ",".join(encoded) + "]}")

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
        docs = list(documents)

        def records():
            for document in docs:
                if not isinstance(document, Document):
                    raise DatabaseError("ingest expects Document instances")
                record = {"id": document.id, "payload": document.payload,
                          "source": document.source}
                if document.position is not None:
                    record["position"] = document.position
                yield record

        result = self._ingest_records(records())
        res = IngestResult(result["inserted"], result["replayed"], result["sequence"])

        if self._views and docs:
            for view_name, view in self._views.items():
                emissions: dict[str, tuple[Any, list[Any]]] = {}
                for doc in docs:
                    for key, val in view.map(doc):
                        canon_key = json.dumps(key, sort_keys=True)
                        if canon_key not in emissions:
                            emissions[canon_key] = (key, [])
                        emissions[canon_key][1].append(val)
                if emissions:
                    for canon_key, (orig_key, vals) in emissions.items():
                        if canon_key in self._view_states[view_name]:
                            _, cur_state, _ = self._view_states[view_name][canon_key]
                            new_state = view.reducer.add(cur_state, vals)
                        else:
                            new_state = view.reducer.add(view.reducer.identity(), vals)
                        self._view_states[view_name][canon_key] = (orig_key, new_state, res.sequence)

                    groups = []
                    for k, s, seq in self._view_states[view_name].values():
                        fin = view.reducer.finalize(s)
                        groups.append({"key": k, "state": s, "result": fin, "sequence": seq})

                    view_checkpoint = self.stats().get("checkpoints", {}).get("projection", res.sequence)
                    last_watermark = self.stats().get("checkpoints", {}).get(f"_view:{view_name}")
                    if last_watermark is None or view_checkpoint > last_watermark:
                        state_payload = {
                            "descriptor": self._view_descriptors[view_name],
                            "checkpoint": view_checkpoint,
                            "groups": groups,
                        }
                        state_doc = {
                            "id": str(view_checkpoint),
                            "source": f"_view:{view_name}",
                            "position": view_checkpoint,
                            "payload": state_payload,
                        }
                        self._ingest_records([state_doc])

        return res

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
        return self._call({"op": "preview", "payload": payload})

    def register(self, view: View) -> str:
        """Attach a callback view; restore or initialize view states."""
        _identity(view.name, "view name")
        _identity(view.version, "view version")
        if not callable(view.map):
            raise DatabaseError("View map must be callable")
        descriptor = {
            "version": view.version,
            "references": dict(view.references),
            "reducer": getattr(view.reducer, "descriptor", str(view.reducer)),
        }
        self._views[view.name] = view
        self._view_descriptors[view.name] = descriptor
        if view.name not in self._view_states:
            self._view_states[view.name] = {}

        watermark = self.stats().get("checkpoints", {}).get(f"_view:{view.name}")
        if watermark is not None:
            last = self.get(str(watermark), source=f"_view:{view.name}")
            if last is not None and isinstance(last.payload, dict):
                saved_desc = last.payload.get("descriptor")
                if saved_desc == descriptor:
                    for item in last.payload.get("groups", []):
                        k = item["key"]
                        s = item["state"]
                        seq = item.get("sequence", watermark)
                        self._view_states[view.name][json.dumps(k, sort_keys=True)] = (k, s, seq)
                else:
                    self._view_states[view.name].clear()
        return view.version

    def get(self, id: str, *, source: str = "default") -> Document | None:
        """Fetch an immutable document by its id and source in O(1)."""
        result = self._call({"op": "get", "id": str(id), "source": source})
        return _document(result) if result else None

    def query(self, name_or_key=_MISSING, *, key=_MISSING, limit=100, cursor=None, after=None) -> Snapshot:
        if isinstance(name_or_key, str) and name_or_key in self._views:
            view = self._views[name_or_key]
            state_map = self._view_states.get(name_or_key, {})
            rows = []
            for orig_key, state, seq in state_map.values():
                fin = view.reducer.finalize(state)
                rows.append(Row(orig_key, fin, state, seq))
            rows.sort(key=lambda r: json.dumps(r.key, sort_keys=True))
            complete = len(rows) <= limit
            limited_rows = tuple(rows[:limit])
            return Snapshot(limited_rows, cursor="0", after=None, complete=complete)

        request = {"op": "query", "limit": limit}
        actual_key = key if key is not _MISSING else (name_or_key if name_or_key is not _MISSING else _MISSING)
        if actual_key is not _MISSING:
            request["key"] = actual_key
        if cursor is not None:
            request["cursor"] = cursor
        if after is not None:
            request["after"] = after
        result = self._call(request)
        return Snapshot(_rows(result["rows"]), result["cursor"], result.get("after"), result["complete"])

    def rows(self, *, key=_MISSING, limit=100) -> Iterator[Row]:
        if key is not _MISSING:
            key = json.loads(_encode(key))
        page = self.query(key=key, limit=limit)
        while True:
            yield from page.rows
            if page.complete:
                return
            page = self.query(key=key, limit=limit, cursor=page.cursor, after=page.after)

    def changes(self, *, since, limit=100) -> ChangePage:
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

    def checkpoint(self, source: str | None = None):
        """Persist a disposable cache snapshot without modifying the log.

        If source is specified, return the committed source watermark position in O(1).
        """
        if source is not None:
            return self.stats().get("checkpoints", {}).get(source)
        return self._call({"op": "checkpoint"})

    def rebuild(self):
        """Replay every original document batch and replace the view cache."""
        return self._call({"op": "rebuild"})

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
from typing import Any, Iterable, Iterator

from .types import (
    ChangePage, CursorExpired, DatabaseError, Document, DocumentConflict,
    IngestResult, Row, Snapshot, SourceOrderError, _json,
)


_MISSING = object()


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


@dataclass(frozen=True, init=False)
class View:
    """Immutable compiled map/reduce module, optionally checked against a digest.

    Accept a filesystem path or bytes. Module exports and its descriptor are
    validated by the Rust host when opening a database. Content hashing binds
    each cache to the exact map, reducer, and state schema it was built with.
    Document logs remain independent of the selected module.
    """
    module: bytes = field(repr=False)
    sha256: str

    def __init__(self, module: bytes | str | os.PathLike, *, sha256: str | None = None):
        binary = module if isinstance(module, bytes) else Path(module).read_bytes()
        if not binary or len(binary) > 16 * 1024 * 1024:
            raise DatabaseError("View module must contain 1..16777216 bytes")
        digest = hashlib.sha256(binary).hexdigest()
        if sha256 is not None and digest != sha256:
            raise DatabaseError("View module digest mismatch")
        object.__setattr__(self, "module", binary)
        object.__setattr__(self, "sha256", digest)

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

    def __init__(self, path, view: View | bytes | str | os.PathLike | None = None):
        self.view = View.bundled() if view is None else (
            view if isinstance(view, View) else View(view))
        try:
            from ._native import Engine
        except ImportError as error:
            raise RuntimeError(
                "Database requires the native Foldlite DB extension. "
                "Install a built foldlitedb wheel, or build this checkout with "
                "python -m pip install . (requires Rust when building from source)."
            ) from error
        try:
            self._engine = Engine(os.fspath(path), self.view.module)
        except (ValueError, TypeError, OverflowError) as error:
            raise DatabaseError(str(error)) from error

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        """Write a disposable checkpoint and release ownership of the log.

        Closing an already closed database is harmless. Checkpoint errors are
        reported, and the native handle is released even when writing fails.
        """
        # Detach before releasing the GIL in native code. Concurrent close calls
        # become harmless and new calls cannot acquire a closing handle.
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
            # Snapshot yielded values before their producer resumes and mutates them.
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
        """Replay a larger iterable as bounded, separately committed batches.

        Batch size also needs to respect the 256 KiB Wasm map input limit. Use
        batch_size=1 for large individual documents accepted by the view ABI.
        """
        if type(batch_size) is not int or not 1 <= batch_size <= 1024:
            raise DatabaseError("batch_size must be between 1 and 1024")
        batch = []
        for document in documents:
            if not isinstance(document, Document):
                raise DatabaseError("ingest expects Document instances")
            # A producer may reuse a mutable payload while yielding frozen
            # Document records. Capture each JSON value before resuming it.
            batch.append(Document(document.id, json.loads(_encode(document.payload)),
                                  document.position, document.source))
            if len(batch) == batch_size:
                yield self.ingest(batch)
                batch = []
        if batch:
            yield self.ingest(batch)

    def preview(self, payload: Any) -> list[dict[str, Any]]:
        """Map one payload to ``{key, value}`` emissions without writing.

        An empty list means the view emitted nothing. This checks mapping only:
        it cannot validate reducers or guarantee that a larger batch fits the
        Wasm request limit. No documents, IDs, positions, or cursors are changed.
        """
        return self._call({"op": "preview", "payload": payload})

    def query(self, *, key=_MISSING, limit=100, cursor=None, after=None) -> Snapshot:
        request = {"op": "query", "limit": limit}
        if key is not _MISSING:
            request["key"] = key
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

    def checkpoint(self):
        """Persist a disposable cache snapshot without modifying the log.

        Returns native statistics with ``checkpointed=True``. New document
        batches are durable before checkpointing; checkpoints speed reopening.
        """
        return self._call({"op": "checkpoint"})

    def rebuild(self):
        """Replay every original document batch and replace the view cache.

        The log and document identities remain unchanged. Existing cursors
        expire because the rebuilt cache receives a new generation. Returns
        native statistics with ``rebuilt=True``.
        """
        return self._call({"op": "rebuild"})

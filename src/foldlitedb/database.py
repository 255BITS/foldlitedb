"""Embedded, regenerable views over immutable JSON documents.

SQLite is the durable cache, not the document authority. Retain input documents
outside this database if deleting and rebuilding it must remain possible.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import itertools
import json
import math
import sqlite3
import threading
from typing import Any, Callable, Iterable, Mapping
import uuid


MAX_POSITION = 2**53 - 1
_MISSING = object()


class DatabaseError(ValueError):
    """Invalid input, incompatible persisted state, or an invalid cursor."""


class DocumentConflict(DatabaseError):
    """A source document ID or position was reused with different contents."""


class SourceOrderError(DatabaseError):
    """An unseen document arrived behind a source's committed checkpoint."""


class CursorExpired(DatabaseError):
    """The database generation, view revision, or retained history changed."""


class GenerationMismatch(DatabaseError):
    """An existing cache belongs to a different set of source documents."""


def _json(value: Any) -> str:
    # JSON's coercion of integer object keys otherwise aliases {1: ...} and
    # {"1": ...}. Check object keys, while allowing tuples as JSON arrays.
    def check(item):
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise DatabaseError("JSON object keys must be strings")
            for child in item.values():
                check(child)
        elif isinstance(item, (list, tuple)):
            for child in item:
                check(child)
    try:
        check(value)
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise DatabaseError(f"Expected finite JSON values: {error}") from error


def _identity(value: str, label: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise DatabaseError(f"{label} must contain 1..4096 UTF-8 bytes")
    return value


def _token(value: Any) -> str:
    return base64.urlsafe_b64encode(_json(value).encode()).decode().rstrip("=")


def _untoken(value: str) -> Any:
    try:
        if not isinstance(value, str) or len(value) > 32768:
            raise ValueError("invalid token length")
        return json.loads(base64.b64decode(value + "=" * (-len(value) % 4),
                                           altchars=b"-_", validate=True))
    except (ValueError, TypeError, UnicodeError) as error:
        raise DatabaseError("Invalid pagination token") from error


@dataclass(frozen=True)
class Document:
    """An immutable source record; IDs and increasing positions are source scoped.

    Positions may have gaps, for example byte offsets or a filtered upstream log.
    The database preserves ingestion order across sources for ordered folds.
    """
    id: str
    payload: Any
    position: int
    source: str = "default"


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
        # WASM caps bytes as well as contribution count. Caller-positioned
        # values can contain heavily escaped identifiers; a batch of 1024 such
        # values can exceed the ABI's 256 KiB request budget.
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


@dataclass(frozen=True)
class View:
    name: str
    version: str
    map: Callable[[Document], Iterable[tuple[Any, Any]]]
    reducer: Any
    references: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Row:
    key: Any
    result: Any
    state: Any
    sequence: int


@dataclass(frozen=True)
class Snapshot:
    rows: tuple[Row, ...]
    cursor: str
    after: str | None
    complete: bool


@dataclass(frozen=True)
class ChangePage:
    rows: tuple[Row, ...]
    cursor: str
    complete: bool


@dataclass(frozen=True)
class IngestResult:
    inserted: int
    replayed: int
    sequence: int


_SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (source TEXT PRIMARY KEY, position INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS documents (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL, id TEXT NOT NULL, position INTEGER NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE(source, id), UNIQUE(source, position)
);
CREATE TABLE IF NOT EXISTS views (
    name TEXT PRIMARY KEY, descriptor TEXT NOT NULL, revision TEXT NOT NULL,
    checkpoint INTEGER NOT NULL DEFAULT 0, history_floor INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS states (
    view TEXT NOT NULL, key TEXT NOT NULL, state TEXT NOT NULL, sequence INTEGER NOT NULL,
    PRIMARY KEY(view, key)
);
CREATE TABLE IF NOT EXISTS history (
    view TEXT NOT NULL, sequence INTEGER NOT NULL, key TEXT NOT NULL, state TEXT NOT NULL,
    PRIMARY KEY(view, sequence, key)
);
CREATE INDEX IF NOT EXISTS history_key ON history(view, key, sequence DESC);
"""


class _Writer:
    """Bounded reducer batches, with dirty-key tracking kept in SQLite."""

    def __init__(self, database, view, head):
        self.database, self.view, self.head = database, view, head
        self.pending = {}
        self.count = 0
        self.bytes = 0

    def document(self, document):
        emissions = self.view.map(document)
        if emissions is None:
            return
        for ordinal, emission in enumerate(emissions):
            if ordinal >= self.database.max_emissions_per_document:
                raise DatabaseError("Map exceeded max_emissions_per_document")
            try:
                key, value = emission
            except (TypeError, ValueError) as error:
                raise DatabaseError("Maps must yield (key, contribution) pairs") from error
            encoded_key = self.database._encode_key(key)
            # Copy before a generator resumes and can mutate its yielded values.
            encoded_value = self.database._encode(value, "contribution")
            value = json.loads(encoded_value)
            prepare = getattr(self.view.reducer, "prepare", None)
            value = prepare(value, document, ordinal) if prepare else value
            self.pending.setdefault(encoded_key, []).append(value)
            self.count += 1
            self.bytes += len(encoded_key.encode()) + len(encoded_value.encode())
            if (len(self.pending[encoded_key]) >= 1024 or self.count >= 4096
                    or self.bytes >= self.database.max_batch_bytes):
                self.flush()

    def flush(self):
        connection = self.database._connection
        reducer = self.view.reducer
        for key, values in self.pending.items():
            row = connection.execute("SELECT state FROM states WHERE view=? AND key=?",
                                     (self.view.name, key)).fetchone()
            state = json.loads(row[0]) if row else reducer.identity()
            state = self.database._encode(reducer.add(state, values), "reducer state")
            connection.execute("INSERT INTO states(view,key,state,sequence) VALUES(?,?,?,?) "
                               "ON CONFLICT(view,key) DO UPDATE SET state=excluded.state,sequence=excluded.sequence",
                               (self.view.name, key, state, self.head))
            connection.execute("INSERT OR IGNORE INTO pending_keys(view,key) VALUES(?,?)",
                               (self.view.name, key))
        self.pending.clear()
        self.count = 0
        self.bytes = 0


class Database:
    """A local SQLite view cache, safe to share between Python threads.

    Every ingest call is one atomic transaction, including document identity,
    source checkpoints, view state, and change history. Separate instances and
    processes serialize through SQLite. Register callbacks on each instance
    before querying; a newly registered view catches up from cached documents.
    """

    def __init__(self, path, *, generation: str = "default", reset: bool = False,
                 max_batch: int = 1024, max_batch_bytes: int = 8 * 1024 * 1024,
                 max_value_bytes: int = 1024 * 1024,
                 max_key_bytes: int = 4096,
                 max_page_bytes: int = 8 * 1024 * 1024,
                 max_emissions_per_document: int = 1024, timeout: float = 30):
        _identity(generation, "generation")
        if (type(timeout) not in (int, float) or not math.isfinite(timeout)
                or not 0 <= timeout <= (2**31 - 1) / 1000):
            raise DatabaseError("timeout must be finite seconds in 0..2147483.647")
        for name, value in (("max_batch", max_batch), ("max_batch_bytes", max_batch_bytes),
                            ("max_value_bytes", max_value_bytes),
                            ("max_key_bytes", max_key_bytes),
                            ("max_page_bytes", max_page_bytes),
                            ("max_emissions_per_document", max_emissions_per_document)):
            if type(value) is not int or value < 1:
                raise DatabaseError(f"{name} must be a positive integer")
        self.max_batch, self.max_batch_bytes = max_batch, max_batch_bytes
        self.max_value_bytes = max_value_bytes
        if max_key_bytes > 4096:
            raise DatabaseError("max_key_bytes must not exceed 4096")
        self.max_key_bytes = max_key_bytes
        self.max_page_bytes = max_page_bytes
        self.max_emissions_per_document = max_emissions_per_document
        self._lock = threading.RLock()
        self._views = {}
        self._closed = False
        self._connection = sqlite3.connect(str(path), timeout=timeout,
                                           isolation_level=None, check_same_thread=False)
        try:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA temp_store=FILE")
            self._connection.executescript(_SCHEMA)
            self._connection.execute("CREATE TEMP TABLE pending_keys (view TEXT,key TEXT,PRIMARY KEY(view,key))")
            self._connection.execute("BEGIN IMMEDIATE")
            previous = self._connection.execute("SELECT value FROM metadata WHERE key='generation'").fetchone()
            schema = self._connection.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()
            if schema and schema[0] != "1":
                raise DatabaseError("Unsupported database schema; rebuild from source documents")
            if previous and previous[0] != generation and not reset:
                raise GenerationMismatch("Database generation changed; use reset=True to regenerate the cache")
            if reset:
                for table in ("documents", "sources", "views", "states", "history", "metadata"):
                    self._connection.execute(f"DELETE FROM {table}")
            self._connection.execute("INSERT OR IGNORE INTO metadata VALUES('generation',?)", (generation,))
            self._connection.execute("INSERT OR IGNORE INTO metadata VALUES('epoch',?)", (uuid.uuid4().hex,))
            self._connection.execute("INSERT OR IGNORE INTO metadata VALUES('schema','1')")
            self.epoch = self._connection.execute("SELECT value FROM metadata WHERE key='epoch'").fetchone()[0]
            self.generation = generation
            self._connection.execute("COMMIT")
        except BaseException:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            self._connection.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def _encode(self, value, label):
        encoded = _json(value)
        if len(encoded.encode("utf-8")) > self.max_value_bytes:
            raise DatabaseError(f"{label} exceeds max_value_bytes")
        return encoded

    def _head(self):
        return self._connection.execute("SELECT coalesce(max(sequence),0) FROM documents").fetchone()[0]

    def _encode_key(self, key):
        encoded = _json(key)
        if len(encoded.encode()) > self.max_key_bytes:
            raise DatabaseError("Key exceeds max_key_bytes")
        return encoded

    def _view(self, name):
        if name not in self._views:
            raise DatabaseError(f"Register view {name!r} on this database instance first")
        view, revision, descriptor = self._views[name]
        if _json({"version": view.version, "references": dict(view.references),
                  "reducer": view.reducer.descriptor}) != descriptor:
            raise DatabaseError("View configuration changed; register the updated definition before using it")
        row = self._connection.execute("SELECT revision,checkpoint,history_floor,descriptor FROM views WHERE name=?",
                                       (name,)).fetchone()
        epoch = self._connection.execute("SELECT value FROM metadata WHERE key='epoch'").fetchone()
        if not epoch or epoch[0] != self.epoch or not row or row[0] != revision or row[3] != descriptor:
            raise CursorExpired("The database generation or view revision changed; reopen and register the view")
        return view, row

    def _check_epoch(self):
        epoch = self._connection.execute("SELECT value FROM metadata WHERE key='epoch'").fetchone()
        if not epoch or epoch[0] != self.epoch:
            raise CursorExpired("The database was reset; reopen it")

    def _cursor(self, name, revision, sequence, key=None):
        return _token([1, self.epoch, name, revision, sequence, key])

    def _parse_cursor(self, name, row, cursor, *, snapshot=False):
        value = _untoken(cursor)
        if (not isinstance(value, list) or len(value) != 6 or value[:4] != [1, self.epoch, name, row[0]]
                or type(value[4]) is not int or not row[2] <= value[4] <= row[1]
                or (value[5] is not None and not isinstance(value[5], str))):
            raise CursorExpired("Cursor does not belong to this view revision or retained history")
        if value[5] is not None and (snapshot or value[4] <= row[2]):
            raise CursorExpired("Partial change cursors cannot identify a snapshot or compacted history")
        return value[4], value[5]

    def _catch_up(self, view, checkpoint, head):
        if checkpoint >= head:
            return
        writer = _Writer(self, view, head)
        documents = self._connection.execute(
            "SELECT id,payload,position,source FROM documents WHERE sequence>? AND sequence<=? ORDER BY sequence",
            (checkpoint, head))
        for identifier, payload, position, source in documents:
            writer.document(Document(identifier, json.loads(payload), position, source))
        writer.flush()
        self._connection.execute(
            "INSERT INTO history(view,sequence,key,state) SELECT s.view,?,s.key,s.state "
            "FROM states s JOIN pending_keys p ON s.view=p.view AND s.key=p.key WHERE s.view=?",
            (head, view.name))
        self._connection.execute("DELETE FROM pending_keys WHERE view=?", (view.name,))
        self._connection.execute("UPDATE views SET checkpoint=? WHERE name=?", (head, view.name))

    def register(self, view: View) -> str:
        """Attach callbacks; rebuild atomically when version/reference/reducer changes.

        Map code is identified by the explicit version, not Python bytecode.
        Reusing a version after changing behavior is a caller error.
        """
        _identity(view.name, "view name")
        _identity(view.version, "view version")
        if not callable(view.map):
            raise DatabaseError("View map must be callable")
        descriptor = self._encode({"version": view.version, "references": dict(view.references),
                                   "reducer": view.reducer.descriptor}, "view descriptor")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                epoch = self._connection.execute("SELECT value FROM metadata WHERE key='epoch'").fetchone()[0]
                if epoch != self.epoch:
                    raise CursorExpired("The database was reset; reopen it")
                row = self._connection.execute("SELECT descriptor,revision,checkpoint FROM views WHERE name=?",
                                               (view.name,)).fetchone()
                if row and row[0] == descriptor:
                    revision, checkpoint = row[1:]
                else:
                    revision, checkpoint = uuid.uuid4().hex, 0
                    self._connection.execute("DELETE FROM states WHERE view=?", (view.name,))
                    self._connection.execute("DELETE FROM history WHERE view=?", (view.name,))
                    self._connection.execute("INSERT INTO views(name,descriptor,revision) VALUES(?,?,?) "
                                             "ON CONFLICT(name) DO UPDATE SET descriptor=excluded.descriptor,"
                                             "revision=excluded.revision,checkpoint=0,history_floor=0",
                                             (view.name, descriptor, revision))
                self._catch_up(view, checkpoint, self._head())
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            self._views[view.name] = (view, revision, descriptor)
            return revision

    def checkpoint(self, source: str = "default") -> int | None:
        with self._lock:
            self._check_epoch()
            row = self._connection.execute("SELECT position FROM sources WHERE source=?", (source,)).fetchone()
            return row[0] if row else None

    def get(self, identifier: str, *, source: str = "default") -> Document | None:
        """Read a cached document, for example an application's resume token.

        The decoded payload is a fresh value; modifying it does not change the
        retained immutable document.
        """
        with self._lock:
            self._check_epoch()
            row = self._connection.execute(
                "SELECT id,payload,position,source FROM documents WHERE source=? AND id=?",
                (source, identifier)).fetchone()
            return Document(row[0], json.loads(row[1]), row[2], row[3]) if row else None

    def refresh(self, name: str):
        """Catch up a registered view after another instance retained documents.

        Query and change polling call this automatically. A read still describes
        a committed prefix if another writer advances immediately afterward.
        """
        with self._lock:
            _, row = self._view(name)
            if row[1] >= self._head():
                return
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                view, row = self._view(name)
                self._catch_up(view, row[1], self._head())
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def ingest(self, documents: Iterable[Document]) -> IngestResult:
        """Atomically ingest one bounded batch; replay must match ID, position and payload.

        To ingest a large iterable with bounded memory and separate commits, use
        :meth:`ingest_many`. Replayed documents do not run maps again.
        """
        encoded = []
        byte_count = 0
        for index, document in enumerate(documents):
            if index >= self.max_batch:
                raise DatabaseError("Batch exceeds max_batch; use ingest_many")
            if not isinstance(document, Document):
                raise DatabaseError("ingest expects Document instances")
            _identity(document.id, "document ID")
            _identity(document.source, "document source")
            if type(document.position) is not int or not 0 <= document.position <= MAX_POSITION:
                raise DatabaseError("Document position must be a nonnegative safe integer")
            payload = self._encode(document.payload, "document payload")
            byte_count += len(payload.encode("utf-8")) + len(document.id.encode()) + len(document.source.encode())
            if byte_count > self.max_batch_bytes:
                raise DatabaseError("Batch exceeds max_batch_bytes")
            encoded.append((document.source, document.id, document.position, payload))
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                inserted = replayed = 0
                epoch = self._connection.execute("SELECT value FROM metadata WHERE key='epoch'").fetchone()[0]
                if epoch != self.epoch:
                    raise CursorExpired("The database was reset; reopen it")
                for source, identifier, position, payload in encoded:
                    previous = self._connection.execute("SELECT position,payload FROM documents WHERE source=? AND id=?",
                                                        (source, identifier)).fetchone()
                    if previous:
                        if previous != (position, payload):
                            raise DocumentConflict(f"Document {source!r}/{identifier!r} changed")
                        replayed += 1
                        continue
                    if self._connection.execute("SELECT 1 FROM documents WHERE source=? AND position=?",
                                                (source, position)).fetchone():
                        raise DocumentConflict(f"Source {source!r} position {position} already has a different document")
                    checkpoint = self._connection.execute("SELECT position FROM sources WHERE source=?", (source,)).fetchone()
                    if checkpoint and position <= checkpoint[0]:
                        raise SourceOrderError(f"New document at {source!r}:{position} is behind checkpoint {checkpoint[0]}")
                    self._connection.execute("INSERT INTO documents(source,id,position,payload) VALUES(?,?,?,?)",
                                             (source, identifier, position, payload))
                    self._connection.execute("INSERT INTO sources VALUES(?,?) ON CONFLICT(source) DO UPDATE SET position=excluded.position",
                                             (source, position))
                    inserted += 1
                head = self._head()
                for name in self._views:
                    view, row = self._view(name)
                    self._catch_up(view, row[1], head)
                self._connection.execute("COMMIT")
                return IngestResult(inserted, replayed, head)
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def ingest_many(self, documents: Iterable[Document], *, batch_size: int | None = None):
        """Yield one result per committed batch; errors roll back only that batch."""
        size = self.max_batch if batch_size is None else batch_size
        if type(size) is not int or not 1 <= size <= self.max_batch:
            raise DatabaseError("batch_size must be in 1..max_batch")
        iterator = iter(documents)
        while batch := tuple(itertools.islice(iterator, size)):
            yield self.ingest(batch)

    @staticmethod
    def _limit(limit):
        if type(limit) is not int or not 1 <= limit <= 4096:
            raise DatabaseError("Page limit must be in 1..4096")

    def _rows(self, view, rows):
        result = []
        for key, state, sequence in rows:
            decoded = json.loads(state)
            # A custom finalize callback may mutate its state argument.
            finalized = view.reducer.finalize(json.loads(state))
            finalized = json.loads(self._encode(finalized, "finalized result"))
            result.append(Row(json.loads(key), finalized, decoded, sequence))
        return tuple(result)

    def _page(self, view, sql_rows, limit):
        rows, byte_count, last = [], 0, None
        for raw in sql_rows:
            if len(rows) >= limit:
                return tuple(rows), last, False
            decoded = self._rows(view, [raw])[0]
            size = len(raw[0].encode()) + len(raw[1].encode()) + len(_json(decoded.result).encode())
            if byte_count + size > self.max_page_bytes:
                if not rows:
                    raise DatabaseError("Result row exceeds max_page_bytes")
                return tuple(rows), last, False
            rows.append(decoded)
            byte_count += size
            last = raw
        return tuple(rows), last, True

    def query(self, name: str, *, key=_MISSING, limit: int = 100,
              after: str | None = None, cursor: str | None = None) -> Snapshot:
        """Page a stable view snapshot and return its exact change-feed cursor.

        Reuse both ``cursor`` and ``after`` for subsequent pages. Keys use
        canonical JSON lexical order, not numeric or locale collation.
        """
        self._limit(limit)
        if after is not None and cursor is None:
            raise DatabaseError("Snapshot pagination requires both cursor and after")
        with self._lock:
            self.refresh(name)
            self._connection.execute("BEGIN")
            try:
                view, row = self._view(name)
                sequence = row[1] if cursor is None else self._parse_cursor(name, row, cursor, snapshot=True)[0]
                cursor = self._cursor(name, row[0], sequence)
                query_key = None if key is _MISSING else self._encode_key(key)
                after_key = ""
                if after is not None:
                    decoded_after = _untoken(after)
                    if (not isinstance(decoded_after, list) or len(decoded_after) != 3
                            or decoded_after[0] != cursor or decoded_after[2] != query_key
                            or not isinstance(decoded_after[1], str)):
                        raise DatabaseError("Key pagination token belongs to a different snapshot or filter")
                    after_key = decoded_after[1]
                params = [name, after_key]
                exact = ""
                if key is not _MISSING:
                    exact = " AND key=?"
                    params.append(query_key)
                if sequence == row[1]:
                    sql_rows = self._connection.execute(
                        "SELECT key,state,sequence FROM states WHERE view=? AND key>?" + exact +
                        " ORDER BY key LIMIT ?", [*params, limit + 1])
                else:
                    # Scan distinct keys via the maintained state index, then
                    # seek one historical version per key. Avoid scanning all
                    # historical updates of a frequently changing key.
                    historical_params = [sequence, name, after_key]
                    if key is not _MISSING:
                        historical_params.append(query_key)
                    historical_params.append(limit + 1)
                    sql_rows = self._connection.execute(
                        "SELECT s.key,h.state,h.sequence FROM states s JOIN history h ON h.view=s.view AND h.key=s.key "
                        "AND h.sequence=(SELECT n.sequence FROM history n WHERE n.view=s.view AND n.key=s.key "
                        "AND n.sequence<=? ORDER BY n.sequence DESC LIMIT 1) WHERE s.view=? AND s.key>?" + exact.replace(" AND key=", " AND s.key=") +
                        " ORDER BY s.key LIMIT ?", historical_params)
                rows, last, complete = self._page(view, sql_rows, limit)
                result = Snapshot(rows, cursor,
                                  _token([cursor, last[0], query_key]) if last and not complete else None, complete)
                self._connection.execute("COMMIT")
                return result
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def changes(self, name: str, *, since: str, limit: int = 100) -> ChangePage:
        """Poll bounded committed key postimages after a snapshot/change cursor.

        Multiple updates to a key inside one ingest are coalesced. Intermediate
        change pages can split a transaction; their cursors retain key progress.
        """
        self._limit(limit)
        with self._lock:
            self.refresh(name)
            self._connection.execute("BEGIN")
            try:
                view, row = self._view(name)
                sequence, last_key = self._parse_cursor(name, row, since)
                if last_key is None:
                    condition, params = "sequence>?", [sequence]
                else:
                    condition, params = "(sequence>? OR (sequence=? AND key>?))", [sequence, sequence, last_key]
                sql_rows = self._connection.execute(
                    "SELECT key,state,sequence FROM history WHERE view=? AND " + condition +
                    " ORDER BY sequence,key LIMIT ?", [name, *params, limit + 1])
                rows, last, complete = self._page(view, sql_rows, limit)
                cursor = self._cursor(name, row[0], row[1]) if complete else self._cursor(name, row[0], last[2], last[0])
                result = ChangePage(rows, cursor, complete)
                self._connection.execute("COMMIT")
                return result
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def compact(self, name: str, *, before: str):
        """Discard old view history while preserving a snapshot at ``before``.

        Callers own reader retention. Older cursors expire explicitly; cached
        source documents remain available for a view rebuild.
        """
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                _, row = self._view(name)
                sequence, _ = self._parse_cursor(name, row, before, snapshot=True)
                self._connection.execute("CREATE TEMP TABLE compact_states (key TEXT PRIMARY KEY,state TEXT NOT NULL)")
                self._connection.execute(
                    "INSERT INTO compact_states SELECT h.key,h.state FROM history h WHERE h.view=? AND h.sequence<=? "
                    "AND NOT EXISTS(SELECT 1 FROM history n WHERE n.view=h.view AND n.key=h.key AND n.sequence>h.sequence AND n.sequence<=?)",
                    (name, sequence, sequence))
                self._connection.execute("DELETE FROM history WHERE view=? AND sequence<=?", (name, sequence))
                self._connection.execute("INSERT INTO history SELECT ?,?,key,state FROM compact_states", (name, sequence))
                self._connection.execute("DROP TABLE compact_states")
                self._connection.execute("UPDATE views SET history_floor=? WHERE name=?", (sequence, name))
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def stats(self):
        with self._lock:
            self._check_epoch()
            return {"documents": self._connection.execute("SELECT count(*) FROM documents").fetchone()[0],
                    "sources": self._connection.execute("SELECT count(*) FROM sources").fetchone()[0],
                    "views": self._connection.execute("SELECT count(*) FROM views").fetchone()[0],
                    "states": self._connection.execute("SELECT count(*) FROM states").fetchone()[0],
                    "history": self._connection.execute("SELECT count(*) FROM history").fetchone()[0],
                    "sequence": self._head()}

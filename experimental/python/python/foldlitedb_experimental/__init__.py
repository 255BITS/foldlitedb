"""Optional native numeric projections; the standard Foldlite engine is unchanged.

NativeView maps top-level JSON fields in Rust. It does not execute Python map
callbacks or OrderedFold. SQLite and append-log backends share the same native
projection code. The Python boundary sends bounded JSON batches, then detaches
from Python while Rust parses, maps, reduces, reads and commits them.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from typing import Any, Iterable

from foldlitedb import (
    ChangePage, DatabaseError, Document, IngestResult, Row, Snapshot,
)

from ._native import Engine


_MISSING = object()


def _encode(value):
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
        return json.dumps(value, allow_nan=False, sort_keys=True,
                          separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise DatabaseError(f"Expected finite JSON values: {error}") from error


def _document(value):
    return Document(value["id"], value["payload"], value["position"], value["source"])


def _rows(values):
    return tuple(Row(value["key"], value["result"], value["state"], value["sequence"])
                 for value in values)


@dataclass(frozen=True)
class NativeView:
    """A versioned native field projection, with optional synthetic CPU work.

    Each document emits its key_field/value_field pair `emissions` times.
    Null values do not contribute to numeric reducers. `work` is an experimental
    deterministic computation setting for measuring native map parallelism.
    """
    name: str
    key_field: str
    value_field: str
    reducer: str = "mean/v1"
    version: str = "1"
    work: int = 0
    emissions: int = 1


class NativeDatabase:
    """Experimental single-view numeric database with SQLite or append-log storage.

    Install this optional package separately from experimental/python. Source
    records remain application-owned. NativeView definitions must match when
    reopening; automatic view rebuilding and arbitrary Python callbacks are not
    part of this experimental surface.
    """

    def __init__(self, path, view: NativeView, *, backend="sqlite", workers=None):
        if not isinstance(view, NativeView):
            raise DatabaseError("view must be a NativeView")
        self.view = view
        self.backend = backend
        self.workers = min(os.cpu_count() or 1, 64) if workers is None else workers
        try:
            self._engine = Engine(os.fspath(path), backend, _encode(asdict(view)), self.workers)
        except (ValueError, TypeError, OverflowError) as error:
            raise DatabaseError(str(error)) from error

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        self._engine = None

    def _call(self, request):
        return self._call_json(_encode(request))

    def _call_json(self, request):
        engine = self._engine
        if engine is None:
            raise DatabaseError("Native database is closed")
        try:
            return json.loads(engine.execute_json(request))
        except ValueError as error:
            raise DatabaseError(str(error)) from error

    @staticmethod
    def _bounded(values):
        for index, value in enumerate(values):
            if index >= 1024:
                raise DatabaseError("Batch exceeds max_batch")
            yield value

    def _ingest_records(self, records):
        # Serialize each record before its generator resumes and can mutate it.
        encoded = [_encode(record) for record in self._bounded(records)]
        return self._call_json('{"op":"ingest","documents":[' + ','.join(encoded) + ']}')

    def add(self, payload: Any, *, id=None, source="default", position=None):
        record = {"payload": payload, "source": source}
        if id is not None:
            record["id"] = id
        if position is not None:
            record["position"] = position
        result = self._ingest_records([record])
        return _document(result["documents"][0])

    def add_many(self, payloads: Iterable[Any], *, source="default"):
        records = ({"payload": payload, "source": source} for payload in payloads)
        result = self._ingest_records(records)
        return tuple(_document(document) for document in result["documents"])

    def ingest(self, documents: Iterable[Document]):
        def records():
            for document in documents:
                if not isinstance(document, Document):
                    raise DatabaseError("ingest expects Document instances")
                yield {"id": document.id, "payload": document.payload,
                       "position": document.position, "source": document.source}
        result = self._ingest_records(records())
        return IngestResult(result["inserted"], result["replayed"], result["sequence"])

    def query(self, *, key=_MISSING, limit=100, cursor=None, after=None):
        request = {"op": "query", "limit": limit}
        if key is not _MISSING:
            request["key"] = key
        if cursor is not None:
            request["cursor"] = cursor
        if after is not None:
            request["after"] = after
        result = self._call(request)
        return Snapshot(_rows(result["rows"]), result["cursor"],
                        result.get("after"), result["complete"])

    def rows(self, *, key=_MISSING, limit=100):
        if key is not _MISSING:
            key = json.loads(_encode(key))
        page = self.query(key=key, limit=limit)
        cursor = page.cursor
        while True:
            yield from page.rows
            if page.complete:
                return
            page = self.query(key=key, limit=limit, cursor=cursor, after=page.after)

    def changes(self, *, since, limit=100):
        result = self._call({"op": "changes", "since": since, "limit": limit})
        return ChangePage(_rows(result["rows"]), result["cursor"], result["complete"])

    def checkpoint(self, source="default"):
        result = self._call({"op": "checkpoint", "source": source})
        return result["position"]

    def get(self, id, *, source="default"):
        result = self._call({"op": "get", "id": id, "source": source})
        return _document(result["document"]) if result["document"] is not None else None

    def stats(self):
        return self._call({"op": "stats"})


__all__ = ["NativeDatabase", "NativeView"]

"""JSON value records and errors shared by the native Python API."""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any


class DatabaseError(ValueError):
    """Invalid input, incompatible storage, or an invalid native operation."""


class DocumentConflict(DatabaseError):
    """A source document ID or position was reused with different contents."""


class SourceOrderError(DatabaseError):
    """An unseen document arrived behind a source's committed checkpoint."""


class CursorExpired(DatabaseError):
    """A cursor belongs to a different log, view, or rebuilt cache generation."""


def _json(value: Any) -> str:
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


@dataclass(frozen=True)
class Document:
    """A source-scoped immutable record preserved in original ingestion order.

    Omit ``position`` to allocate the next position for this source when
    ingesting. Identical named retries keep their original resolved position.
    Returned/exported records always have resolved integer positions. Returned
    JSON payloads are independent copies: mutating them cannot change storage.
    """
    id: str
    payload: Any
    position: int | None = None
    source: str = "default"


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

"""JSON value records, validation, fast cloning, and errors shared by the Python API."""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
from typing import Any

_MAX_SAFE_INT = 9_007_199_254_740_992  # 2**53


class DatabaseError(ValueError):
    """Invalid input, incompatible storage, or an invalid native operation."""


class DocumentConflict(DatabaseError):
    """A source document ID or position was reused with different contents."""


class SourceOrderError(DatabaseError):
    """An unseen document arrived behind a source's committed checkpoint."""


class CursorExpired(DatabaseError):
    """A cursor belongs to a different log, view, or rebuilt cache generation."""


class GenerationMismatch(DatabaseError):
    """An existing cache belongs to a different set of source documents."""


def _canonical_json(value: Any, *, ensure_ascii: bool = False) -> str:
    """Serialize finite JSON deterministically with sorted object keys."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=ensure_ascii,
        allow_nan=False,
    )


def _check_dict_keys(item: Any) -> None:
    t = type(item)
    if t is dict:
        for key, child in item.items():
            if type(key) is not str and not isinstance(key, str):
                raise DatabaseError("JSON object keys must be strings")
            ct = type(child)
            if ct is dict or ct is list or ct is tuple or isinstance(child, (dict, list, tuple)):
                _check_dict_keys(child)
    elif t is list or t is tuple:
        for child in item:
            ct = type(child)
            if ct is dict or ct is list or ct is tuple or isinstance(child, (dict, list, tuple)):
                _check_dict_keys(child)
    elif isinstance(item, dict):
        for key, child in item.items():
            if not isinstance(key, str):
                raise DatabaseError("JSON object keys must be strings")
            _check_dict_keys(child)
    elif isinstance(item, (list, tuple)):
        for child in item:
            _check_dict_keys(child)


def _json(value: Any) -> str:
    try:
        _check_dict_keys(value)
        return _canonical_json(value, ensure_ascii=False)
    except DatabaseError:
        raise
    except (TypeError, ValueError, RecursionError) as error:
        raise DatabaseError(f"Expected finite JSON values: {error}") from error


def _transport_json(value: Any) -> str:
    """Serialize an already-validated transport request envelope without sorting keys."""
    try:
        return json.dumps(
            value,
            sort_keys=False,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError, RecursionError) as error:
        raise DatabaseError(f"Expected finite JSON values: {error}") from error


def _check_exact_int(item: int) -> None:
    if -_MAX_SAFE_INT <= item <= _MAX_SAFE_INT:
        return
    try:
        if int(float(item)) != item:
            raise ValueError("integer precision")
    except (ValueError, OverflowError) as error:
        raise DatabaseError(
            "Portable JSON integers must be exactly representable as IEEE-754 doubles; "
            "encode larger identifiers as strings"
        ) from error


def _check_str(item: str) -> None:
    if type(item) is str and item.isascii():
        return
    try:
        item.encode("utf-8")
    except UnicodeError as error:
        raise DatabaseError("Portable JSON strings must contain valid Unicode scalar values") from error


def _validate_value(item: Any, depth: int, max_depth: int) -> None:
    if depth > max_depth:
        raise DatabaseError("Portable JSON nesting exceeds 64")
    t = type(item)
    if t is str:
        if not item.isascii():
            _check_str(item)
        return
    if t is int:
        if not (-_MAX_SAFE_INT <= item <= _MAX_SAFE_INT):
            _check_exact_int(item)
        return
    if t is float:
        if not math.isfinite(item):
            raise DatabaseError("Expected finite JSON values: Out of range float values are not JSON compliant")
        return
    if item is None or t is bool:
        return
    if t is dict:
        next_depth = depth + 1
        for key, child in item.items():
            if next_depth > max_depth:
                raise DatabaseError("Portable JSON nesting exceeds 64")
            kt = type(key)
            if kt is str:
                if not key.isascii():
                    _check_str(key)
            elif isinstance(key, str):
                _check_str(key)
            else:
                raise DatabaseError("JSON object keys must be strings")
            _validate_value(child, next_depth, max_depth)
        return
    if t is list or t is tuple:
        next_depth = depth + 1
        for child in item:
            _validate_value(child, next_depth, max_depth)
        return
    if isinstance(item, int):
        _check_exact_int(item)
    elif isinstance(item, float):
        if not math.isfinite(item):
            raise DatabaseError("Expected finite JSON values: Out of range float values are not JSON compliant")
    elif isinstance(item, str):
        _check_str(item)
    elif isinstance(item, dict):
        next_depth = depth + 1
        for key, child in item.items():
            if next_depth > max_depth:
                raise DatabaseError("Portable JSON nesting exceeds 64")
            if not isinstance(key, str):
                raise DatabaseError("JSON object keys must be strings")
            _check_str(key)
            _validate_value(child, next_depth, max_depth)
    elif isinstance(item, (list, tuple)):
        next_depth = depth + 1
        for child in item:
            _validate_value(child, next_depth, max_depth)
    else:
        raise DatabaseError(
            f"Expected finite JSON values: Object of type {t.__name__} is not JSON serializable"
        )


def _encode(value: Any, *, max_depth: int = 64, sort_keys: bool = True) -> str:
    try:
        _validate_value(value, 0, max_depth)
        return json.dumps(
            value,
            sort_keys=sort_keys,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except DatabaseError:
        raise
    except RecursionError as error:
        raise DatabaseError("Portable JSON nesting is too deep") from error
    except (TypeError, ValueError) as error:
        raise DatabaseError(f"Expected finite JSON values: {error}") from error


def _clone_value(item: Any, depth: int, max_depth: int) -> Any:
    if depth > max_depth:
        raise DatabaseError("Portable JSON nesting exceeds 64")
    t = type(item)
    if t is str:
        if not item.isascii():
            _check_str(item)
        return item
    if t is int:
        if not (-_MAX_SAFE_INT <= item <= _MAX_SAFE_INT):
            _check_exact_int(item)
        return item
    if t is float:
        if not math.isfinite(item):
            raise DatabaseError("Expected finite JSON values: Out of range float values are not JSON compliant")
        return item
    if item is None or t is bool:
        return item
    if t is list or t is tuple:
        next_depth = depth + 1
        return [_clone_value(child, next_depth, max_depth) for child in item]
    if t is dict:
        next_depth = depth + 1
        out: dict[str, Any] = {}
        for key, child in item.items():
            if next_depth > max_depth:
                raise DatabaseError("Portable JSON nesting exceeds 64")
            kt = type(key)
            if kt is str:
                if not key.isascii():
                    _check_str(key)
                out[key] = _clone_value(child, next_depth, max_depth)
            elif isinstance(key, str):
                plain_key = str(key)
                _check_str(plain_key)
                out[plain_key] = _clone_value(child, next_depth, max_depth)
            else:
                raise DatabaseError("JSON object keys must be strings")
        return out
    if isinstance(item, int):
        plain_int = int(item)
        _check_exact_int(plain_int)
        return plain_int
    if isinstance(item, float):
        plain_float = float(item)
        if not math.isfinite(plain_float):
            raise DatabaseError("Expected finite JSON values: Out of range float values are not JSON compliant")
        return plain_float
    if isinstance(item, str):
        plain_str = str(item)
        _check_str(plain_str)
        return plain_str
    if isinstance(item, (list, tuple)):
        next_depth = depth + 1
        return [_clone_value(child, next_depth, max_depth) for child in item]
    if isinstance(item, dict):
        next_depth = depth + 1
        out = {}
        for key, child in item.items():
            if next_depth > max_depth:
                raise DatabaseError("Portable JSON nesting exceeds 64")
            if not isinstance(key, str):
                raise DatabaseError("JSON object keys must be strings")
            plain_key = str(key)
            _check_str(plain_key)
            out[plain_key] = _clone_value(child, next_depth, max_depth)
        return out
    raise DatabaseError(
        f"Expected finite JSON values: Object of type {t.__name__} is not JSON serializable"
    )


def _copy(value: Any, *, max_depth: int = 64) -> Any:
    """Validate portable JSON semantics and return an independent deep copy."""
    t = type(value)
    if t is int:
        if -_MAX_SAFE_INT <= value <= _MAX_SAFE_INT:
            return value
        _check_exact_int(value)
        return value
    if t is str:
        if value.isascii():
            return value
        _check_str(value)
        return value
    if t is float:
        if math.isfinite(value):
            return value
        raise DatabaseError("Expected finite JSON values: Out of range float values are not JSON compliant")
    if value is None or t is bool:
        return value
    try:
        return _clone_value(value, 0, max_depth)
    except RecursionError as error:
        raise DatabaseError("Portable JSON nesting is too deep") from error


def _clone_trusted(value: Any) -> Any:
    """Deep-copy an already-validated JSON value without re-validating primitives."""
    t = type(value)
    if t is list:
        return [_clone_trusted(item) for item in value]
    if t is dict:
        return {k: _clone_trusted(v) for k, v in value.items()}
    return value


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

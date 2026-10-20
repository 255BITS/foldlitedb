"""Foldlite DB: immutable document logs and rebuildable native Wasm views."""

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
)
from .database import Database, OrderedFold, RustReducer, View, portable_assets

PortableDatabase = Database
PortableView = View

__version__ = "0.1.2"

__all__ = [
    "ChangePage", "CursorExpired", "Database", "DatabaseError", "Document",
    "DocumentConflict", "GenerationMismatch", "IngestResult", "OrderedFold",
    "PortableDatabase", "PortableView", "Row", "RustReducer",
    "Snapshot", "SourceOrderError", "View", "portable_assets",
]

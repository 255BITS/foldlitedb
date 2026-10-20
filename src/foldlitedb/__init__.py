"""Foldlite DB: immutable document logs and rebuildable native Wasm views."""

from .types import (
    ChangePage,
    CursorExpired,
    DatabaseError,
    Document,
    DocumentConflict,
    IngestResult,
    Row,
    Snapshot,
    SourceOrderError,
)
from .database import Database, View, portable_assets

PortableDatabase = Database
PortableView = View

__version__ = "0.1.0"

__all__ = [
    "ChangePage", "CursorExpired", "Database", "DatabaseError", "Document",
    "DocumentConflict", "IngestResult", "PortableDatabase", "PortableView", "Row",
    "Snapshot", "SourceOrderError", "View", "portable_assets",
]

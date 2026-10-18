"""Foldlite DB: embedded realtime views over immutable documents."""

from .database import (
    ChangePage,
    CursorExpired,
    Database,
    DatabaseError,
    Document,
    DocumentConflict,
    GenerationMismatch,
    IngestResult,
    OrderedFold,
    Row,
    RustReducer,
    Snapshot,
    SourceOrderError,
    View,
)

__version__ = "0.1.0"

__all__ = [
    "ChangePage", "CursorExpired", "Database", "DatabaseError", "Document",
    "DocumentConflict", "GenerationMismatch", "IngestResult", "OrderedFold",
    "Row", "RustReducer", "Snapshot", "SourceOrderError", "View",
]

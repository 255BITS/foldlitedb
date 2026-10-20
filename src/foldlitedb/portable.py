"""Compatibility names for the native database and portable Wasm views."""
from .database import Database, View, portable_assets

PortableDatabase = Database
PortableView = View

__all__ = ["PortableDatabase", "PortableView", "portable_assets"]

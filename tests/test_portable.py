"""The earlier portable names continue to select the sole native engine."""
from foldlitedb import Database, PortableDatabase, PortableView, View
from foldlitedb.portable import PortableDatabase as ModuleDatabase, PortableView as ModuleView


def test_portable_names_are_aliases_for_the_native_api():
    assert PortableDatabase is ModuleDatabase is Database
    assert PortableView is ModuleView is View


def test_compatibility_names_execute_the_same_bundled_view():
    with PortableDatabase(":memory:", PortableView.bundled()) as database:
        database.add({"metric": "x", "value": 3})
        assert next(database.rows()).result["sum"] == 3

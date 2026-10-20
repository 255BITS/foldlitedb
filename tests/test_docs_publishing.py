"""Generated nested Wasm assets must stay inside their publication checkout."""
import importlib.util
import json
from pathlib import Path
import sys

import pytest


@pytest.fixture
def publisher(monkeypatch):
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location("foldlite_publish_docs", scripts / "publish_docs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def manifest(directory, names):
    (directory / ".foldlitedb-docs").write_text(json.dumps({"generator": "foldlitedb-docs/v1", "files": names}))


def test_nested_assets_are_recognized_and_unmanaged_files_detected(publisher, tmp_path):
    (tmp_path / "portable").mkdir()
    (tmp_path / "portable" / "metrics.wasm").write_bytes(b"wasm")
    names = [".foldlitedb-docs", "portable/metrics.wasm"]
    manifest(tmp_path, names)
    assert publisher.managed_files(tmp_path) == publisher.site_files(tmp_path) == set(names)
    (tmp_path / "unmanaged.txt").touch()
    assert publisher.managed_files(tmp_path) != publisher.site_files(tmp_path)


@pytest.mark.parametrize("name", ["../secret", "/tmp/secret", ".git/config", "portable/../../secret", "portable\\secret", "./index.html", "portable//host.js", ""])
def test_manifest_cannot_escape_or_write_git_metadata(publisher, tmp_path, name):
    manifest(tmp_path, [name])
    with pytest.raises(ValueError, match="manifest"):
        publisher.managed_files(tmp_path)


def test_symlink_ancestor_is_rejected_before_copying(publisher, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    site = tmp_path / "site"
    site.mkdir()
    try:
        (site / "portable").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("This platform does not permit symlink creation")
    manifest(site, ["portable/new.wasm"])
    with pytest.raises(ValueError, match="symlink"):
        publisher.managed_files(site)
    with pytest.raises(ValueError, match="symlink"):
        publisher.validate_generated_path(site, "portable/new.wasm")

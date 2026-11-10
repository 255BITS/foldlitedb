"""Keep package versions and user-facing installation/runtime claims consistent."""
from __future__ import annotations

import ast
import argparse
import json
from pathlib import Path
import re
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def check(root=ROOT, *, tag=None):
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    if project["name"] != "foldlitedb":
        raise ValueError("Release metadata must identify the foldlitedb project")
    version = project["version"]
    if tag is not None and tag != f"v{version}":
        raise ValueError(f"Expected release tag v{version}, got {tag!r}")
    for name in ("native/engine/Cargo.toml", "native/python/Cargo.toml", "native/reducer/Cargo.toml"):
        declared = tomllib.loads((root / name).read_text(encoding="utf-8"))["package"]["version"]
        if declared != version:
            raise ValueError(f"{name}: expected version {version}, found {declared}")
    module = ast.parse((root / "src/foldlitedb/__init__.py").read_text(encoding="utf-8"))
    declared = [ast.literal_eval(node.value) for node in module.body
                if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "__version__"
                        for target in node.targets)]
    if declared != [version]:
        raise ValueError(f"Python __version__ {declared!r} differs from {version}")
    for name in ("README.md", "docs/index.html", "docs/portable.html", "docs/releasing.html"):
        source = (root / name).read_text(encoding="utf-8")
        pinned = re.findall(r"foldlitedb==([0-9]+\.[0-9]+\.[0-9]+)", source)
        if not pinned or any(item != version for item in pinned):
            raise ValueError(f"{name}: installation examples must pin foldlitedb=={version}")
    views_pkg = json.loads((root / "views/package.json").read_text(encoding="utf-8"))
    if views_pkg.get("version") != version:
        raise ValueError(f"views/package.json: expected version {version}, found {views_pkg.get('version')!r}")
    views_lock = json.loads((root / "views/package-lock.json").read_text(encoding="utf-8"))
    if views_lock.get("version") != version or views_lock.get("packages", {}).get("", {}).get("version") != version:
        raise ValueError(f"views/package-lock.json: expected version {version}")
    engines = views_pkg["engines"]["node"]
    for name in ("README.md", "DESIGN.md", "docs/portable.html"):
        if engines not in (root / name).read_text(encoding="utf-8"):
            raise ValueError(f"{name}: document the Node.js engine range {engines!r}")
    print(f"Verified Foldlite DB {version} package and documentation metadata")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="Require a release tag matching the package version")
    check(tag=parser.parse_args().tag)

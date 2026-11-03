"""Keep package versions and user-facing installation/runtime claims consistent."""
from __future__ import annotations

import ast
import json
from pathlib import Path
import re
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def check(root=ROOT):
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    for name in ("native/engine/Cargo.toml", "native/python/Cargo.toml"):
        declared = tomllib.loads((root / name).read_text())["package"]["version"]
        if declared != version:
            raise ValueError(f"{name}: expected version {version}, found {declared}")
    module = ast.parse((root / "src/foldlitedb/__init__.py").read_text())
    declared = [ast.literal_eval(node.value) for node in module.body
                if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "__version__"
                        for target in node.targets)]
    if declared != [version]:
        raise ValueError(f"Python __version__ {declared!r} differs from {version}")
    for name in ("README.md", "docs/index.html", "docs/portable.html", "docs/releasing.html"):
        source = (root / name).read_text()
        pinned = re.findall(r"foldlitedb==([0-9]+\.[0-9]+\.[0-9]+)", source)
        if not pinned or any(item != version for item in pinned):
            raise ValueError(f"{name}: installation examples must pin foldlitedb=={version}")
    engines = json.loads((root / "views/package.json").read_text())["engines"]["node"]
    for name in ("README.md", "DESIGN.md", "docs/portable.html"):
        if engines not in (root / name).read_text():
            raise ValueError(f"{name}: document the Node.js engine range {engines!r}")
    print(f"Verified Foldlite DB {version} package and documentation metadata")


if __name__ == "__main__":
    check()

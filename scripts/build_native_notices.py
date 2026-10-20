"""Preserve installed upstream notices for the native dependency graph."""
import argparse
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / "src/foldlitedb/NATIVE_THIRD_PARTY_LICENSES.txt"


def generate():
    targets = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "x86_64-apple-darwin",
               "aarch64-apple-darwin", "x86_64-pc-windows-msvc")
    packages = {}
    included = set()
    for target in targets:
        metadata = json.loads(subprocess.check_output([
            "cargo", "metadata", "--locked", "--manifest-path", str(ROOT / "native/Cargo.toml"),
            "--format-version", "1", "--filter-platform", target], text=True))
        packages.update((package["id"], package) for package in metadata["packages"])
        nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
        pending = [package["id"] for package in packages.values() if package["name"] == "foldlitedb-portable-python"]
        visited = set()
        while pending:
            identity = pending.pop()
            if identity in visited:
                continue
            visited.add(identity)
            for dependency in nodes[identity]["deps"]:
                if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"]):
                    pending.append(dependency["pkg"])
        included.update(visited)
    output = [
        "Third-party licenses and notices for Foldlite DB's native Python extension\n",
        "Generated from the locked dependency graphs for Linux x86_64/aarch64,\n"
        "macOS x86_64/aarch64 and Windows x86_64. This includes build and\n"
        "platform-specific dependencies for provenance; inclusion does not assert\n"
        "that every listed crate is linked into every wheel. Upstream texts are\n"
        "reproduced with LF line endings. Foldlite DB's own MIT license ships separately.\n",
    ]
    for package in sorted((packages[identity] for identity in included), key=lambda p: (p["name"], p["version"])):
        if package["source"] is None:
            continue
        directory = Path(package["manifest_path"]).parent
        notices = sorted(path for path in directory.rglob("*") if path.is_file() and (
            path.name.upper().startswith(("LICENSE", "LICENCE", "NOTICE", "COPYING", "COPYRIGHT")) or
            any(part.upper() in ("LICENSES", "LICENSE", "LICENSING", "LICENCES")
                for part in path.relative_to(directory).parts[:-1])))
        if package.get("license_file"):
            specified = directory / package["license_file"]
            if specified.is_file() and specified not in notices:
                notices.append(specified)
        if not notices and package["name"] in ("wasmi", "wasmi_core", "wasmi_ir", "wasmi_collections"):
            directory = ROOT / "native/licenses/wasmi"
            notices = sorted(directory.iterdir())
        if not notices and package["name"] == "wasmparser":
            directory = ROOT / "native/licenses/wasmparser"
            notices = sorted(directory.iterdir())
        if not notices:
            raise ValueError(f"Missing upstream license text: {package['name']} {package['version']}")
        output.append(f"\n=== {package['name']} {package['version']} ({package.get('license')}) ===\n")
        for path in notices:
            output.append(f"\n{path.relative_to(directory).as_posix()}\n\n{path.read_text().rstrip()}\n")
    return "\n".join(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    options = parser.parse_args()
    content = generate()
    if options.check:
        if DESTINATION.read_text() != content:
            raise SystemExit("Stale native notices; run python scripts/build_native_notices.py")
    else:
        DESTINATION.write_text(content)
    print(f"{'Verified' if options.check else 'Wrote'} native third-party notices")

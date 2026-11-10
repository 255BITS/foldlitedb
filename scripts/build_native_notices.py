"""Preserve installed upstream notices for the native and Wasm dependency graphs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
NATIVE_DESTINATION = ROOT / "src/foldlitedb/NATIVE_THIRD_PARTY_LICENSES.txt"
REDUCER_WASM_DESTINATION = ROOT / "src/foldlitedb/reducers/assets/THIRD_PARTY_LICENSES.txt"


def _collect_packages(root_crate: str, targets: tuple[str, ...]) -> list[dict]:
    packages: dict[str, dict] = {}
    included: set[str] = set()
    for target in targets:
        metadata = json.loads(
            subprocess.check_output(
                [
                    "cargo",
                    "metadata",
                    "--locked",
                    "--manifest-path",
                    str(ROOT / "native/Cargo.toml"),
                    "--format-version",
                    "1",
                    "--filter-platform",
                    target,
                ],
                text=True,
            )
        )
        packages.update((package["id"], package) for package in metadata["packages"])
        nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
        pending = [
            package["id"]
            for package in packages.values()
            if package["name"] == root_crate
        ]
        visited: set[str] = set()
        while pending:
            identity = pending.pop()
            if identity in visited:
                continue
            visited.add(identity)
            for dependency in nodes[identity]["deps"]:
                if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"]):
                    pending.append(dependency["pkg"])
        included.update(visited)
    return sorted(
        (packages[identity] for identity in included if packages[identity]["source"] is not None),
        key=lambda p: (p["name"], p["version"]),
    )


def _format_notices(header_lines: list[str], crates: list[dict]) -> str:
    output = list(header_lines)
    for package in crates:
        directory = Path(package["manifest_path"]).parent
        notices = sorted(
            path
            for path in directory.rglob("*")
            if path.is_file()
            and (
                path.name.upper().startswith(
                    ("LICENSE", "LICENCE", "NOTICE", "COPYING", "COPYRIGHT")
                )
                or any(
                    part.upper() in ("LICENSES", "LICENSE", "LICENSING", "LICENCES")
                    for part in path.relative_to(directory).parts[:-1]
                )
            )
        )
        if package.get("license_file"):
            specified = directory / package["license_file"]
            if specified.is_file() and specified not in notices:
                notices.append(specified)
        if not notices and package["name"] in (
            "wasmi",
            "wasmi_core",
            "wasmi_ir",
            "wasmi_collections",
        ):
            directory = ROOT / "native/licenses/wasmi"
            notices = sorted(directory.iterdir())
        if not notices and package["name"] == "wasmparser":
            directory = ROOT / "native/licenses/wasmparser"
            notices = sorted(directory.iterdir())
        if not notices:
            raise ValueError(
                f"Missing upstream license text: {package['name']} {package['version']}"
            )
        output.append(
            f"\n=== {package['name']} {package['version']} ({package.get('license')}) ===\n"
        )
        for path in notices:
            text = path.read_text(encoding="utf-8").rstrip()
            output.append(f"\n{path.relative_to(directory).as_posix()}\n\n{text}\n")
    return "\n".join(output)


def generate_native() -> str:
    targets = (
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
        "x86_64-pc-windows-msvc",
    )
    header = [
        "Third-party licenses and notices for Foldlite DB's native Python extension\n",
        "Generated from the locked dependency graphs for Linux x86_64/aarch64,\n"
        "macOS x86_64/aarch64 and Windows x86_64. This includes build and\n"
        "platform-specific dependencies for provenance; inclusion does not assert\n"
        "that every listed crate is linked into every wheel. Upstream texts are\n"
        "reproduced with LF line endings. Foldlite DB's own MIT license ships separately.\n",
    ]
    return _format_notices(header, _collect_packages("foldlitedb-portable-python", targets))


def generate_reducer_wasm() -> str:
    header = [
        "Third-party licenses and notices for Foldlite DB's bundled reducer WebAssembly module\n",
        "Generated from the locked dependency graph of foldlitedb-reducer-wasm\n"
        "(native/Cargo.lock) for target wasm32-unknown-unknown. Build-time\n"
        "dependencies are included for provenance. Upstream texts are reproduced\n"
        "with LF line endings. Foldlite DB's own MIT license ships separately.\n",
    ]
    return _format_notices(
        header,
        _collect_packages("foldlitedb-reducer-wasm", ("wasm32-unknown-unknown",)),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    options = parser.parse_args()
    targets = (
        (NATIVE_DESTINATION, generate_native(), "native extension"),
        (REDUCER_WASM_DESTINATION, generate_reducer_wasm(), "reducer Wasm"),
    )
    if options.check:
        for destination, content, label in targets:
            if not destination.is_file() or destination.read_text(encoding="utf-8") != content:
                raise SystemExit(
                    f"Stale {label} notices ({destination.relative_to(ROOT)}); "
                    "run python scripts/build_native_notices.py"
                )
    else:
        for destination, content, _label in targets:
            destination.write_text(content, encoding="utf-8")
    print(f"{'Verified' if options.check else 'Wrote'} native and reducer Wasm third-party notices")


if __name__ == "__main__":
    main()

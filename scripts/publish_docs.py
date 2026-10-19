"""Publish checked static docs on an isolated gh-pages branch using git and gh.

Requires authenticated repository write/admin access. Only generated static files
are committed; existing unrelated branch files and the source checkout are kept.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from check_docs import check


ROOT = Path(__file__).resolve().parents[1]
MARKER = ".foldlitedb-docs"
GIT = ["git", "-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential"]


def command(arguments, *, cwd=None, capture=False):
    return subprocess.run(arguments, cwd=cwd, check=True, text=True, capture_output=capture)


def managed_files(directory):
    marker = directory / MARKER
    if not marker.is_file() or marker.is_symlink():
        raise ValueError(f"Refusing to replace an unrecognized static branch/directory: {directory}")
    manifest = json.loads(marker.read_text())
    if manifest.get("generator") != "foldlitedb-docs/v1":
        raise ValueError("Unrecognized documentation generator")
    files = manifest["files"]
    if not isinstance(files, list) or any(
        not isinstance(name, str) or Path(name).name != name or name in (".", "..", ".git")
        for name in files
    ):
        raise ValueError("Invalid generated file manifest")
    return set(files)


def publish(site, repository, message):
    site = site.resolve()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("Repository must be owner/name")
    check(site)
    generated = managed_files(site)
    if generated != {path.name for path in site.iterdir()}:
        raise ValueError("Site contains files outside the generated manifest")
    for name in generated:
        if not (site / name).is_file() or (site / name).is_symlink():
            raise ValueError(f"Only regular static files can be published: {name}")
    remote = f"https://github.com/{repository}.git"
    branch = "gh-pages"
    existing = subprocess.run([*GIT, "ls-remote", "--exit-code", "--heads", remote, branch],
                              text=True, capture_output=True)
    if existing.returncode not in (0, 2):
        raise RuntimeError(existing.stderr.strip())
    with tempfile.TemporaryDirectory(prefix="foldlitedb-pages-publish-") as temporary:
        checkout = Path(temporary) / "publish"
        if existing.returncode == 0:
            command([*GIT, "clone", "--depth=1", "--single-branch", "--branch", branch,
                     remote, str(checkout)])
            previous = managed_files(checkout)
        else:
            checkout.mkdir()
            command(["git", "init", f"--initial-branch={branch}"], cwd=checkout)
            command(["git", "remote", "add", "origin", remote], cwd=checkout)
            previous = set()
        for name in previous | generated:
            path = checkout / name
            if path.exists() or path.is_symlink():
                if name not in previous or not path.is_file() or path.is_symlink():
                    raise ValueError(f"Refusing to overwrite an unrelated branch file: {name}")
                path.unlink()
        for name in generated:
            shutil.copy2(site / name, checkout / name)
        for field, fallback in (("user.name", "github-actions[bot]"),
                                ("user.email", "41898282+github-actions[bot]@users.noreply.github.com")):
            configured = subprocess.run(["git", "config", field], cwd=ROOT,
                                        capture_output=True, text=True)
            command(["git", "config", field, configured.stdout.strip() or fallback], cwd=checkout)
        command(["git", "add", "--", *sorted(previous | generated)], cwd=checkout)
        changes = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=checkout)
        if changes.returncode:
            command(["git", "commit", "-m", message], cwd=checkout)
            command([*GIT, "push", "origin", f"HEAD:refs/heads/{branch}"], cwd=checkout)
        commit = command(["git", "rev-parse", "HEAD"], cwd=checkout, capture=True).stdout.strip()
    endpoint = f"repos/{repository}/pages"
    state = subprocess.run(["gh", "api", endpoint], text=True, capture_output=True)
    if state.returncode and "404" not in state.stderr:
        raise RuntimeError(state.stderr.strip())
    method = "PUT" if state.returncode == 0 else "POST"
    current = json.loads(state.stdout) if state.returncode == 0 else {}
    if current.get("build_type") != "legacy" or current.get("source") != {"branch": branch, "path": "/"}:
        command(["gh", "api", "--method", method, endpoint, "-f", "build_type=legacy",
                 "-f", f"source[branch]={branch}", "-f", "source[path]=/"])
    queued = command(["gh", "api", "--method", "POST", endpoint + "/builds"], capture=True)
    pages = json.loads(command(["gh", "api", endpoint], capture=True).stdout)
    print(json.dumps({"commit": commit, "url": pages["html_url"],
                      "build": json.loads(queued.stdout)}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", type=Path, default=ROOT / "_site")
    parser.add_argument("--repository", default="255BITS/foldlitedb")
    parser.add_argument("--message", default="Publish checked development documentation")
    args = parser.parse_args()
    publish(args.site, args.repository, args.message)

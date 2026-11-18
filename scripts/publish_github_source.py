"""Publish the tested source revision after its distributions reach PyPI."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
import tomllib
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen

REPOSITORY = "255BITS/foldlitedb"
PACKAGE = "foldlitedb"
TITLE = "Foldlite DB"


def output(*args, input=None):
    return subprocess.check_output(args, text=True, input=input)


def tag_commit(tag):
    ref = f"refs/tags/{tag}"
    text = output("git", "ls-remote", f"https://github.com/{REPOSITORY}.git", ref, f"{ref}^{{}}")
    refs = {ref: sha for sha, ref in (row.split("\t") for row in text.splitlines())}
    return refs.get(f"{ref}^{{}}", refs.get(ref))


def release_record(tag):
    text = output("gh", "api", "--paginate", f"repos/{REPOSITORY}/releases?per_page=100",
                  "--jq", f"[.[] | select(.tag_name == {json.dumps(tag)})]")
    records = [item for row in text.splitlines() for item in json.loads(row)]
    if len(records) > 1:
        raise RuntimeError(f"Multiple GitHub release records for {tag}")
    return next(iter(records), None)


def verify_pypi(version, artifacts):
    distributions = sorted(path for path in artifacts.iterdir()
                           if path.is_file() and path.name.endswith((".whl", ".tar.gz")))
    if not (any(path.suffix == ".whl" for path in distributions)
            and any(path.name.endswith(".tar.gz") for path in distributions)):
        raise RuntimeError("Expected the tested wheel and source distributions")
    expected = {}
    for path in distributions:
        with path.open("rb") as file:
            expected[path.name] = hashlib.file_digest(file, "sha256").hexdigest()
    url = f"https://pypi.org/pypi/{PACKAGE}/{quote(version, safe='')}/json"
    for attempt in range(5):
        observed = {}
        try:
            with urlopen(url, timeout=15) as response:
                release = json.load(response)
        except HTTPError as error:
            if error.code != 404:
                raise
        else:
            if release["info"]["name"].lower() != PACKAGE or release["info"]["version"] != version:
                raise RuntimeError("PyPI returned a different package or version")
            observed = {file["filename"]: file["digests"]["sha256"]
                        for file in release["urls"] if not file["yanked"]}
        if observed == expected:
            return
        if not observed.items() < expected.items() or attempt == 4:
            break
        time.sleep(3)
    raise RuntimeError(f"PyPI hashes differ from the tested distributions for {PACKAGE} {version}")


def publish(version, source, artifacts):
    verify_pypi(version, artifacts)
    tag = f"v{version}"
    current = tag_commit(tag)
    if current not in (None, source):
        raise RuntimeError(f"{tag} already points to {current}, expected {source}")
    record = release_record(tag)
    is_preview = bool(re.search(r"\d(?:a\d|b\d|rc\d|\.dev\d)", version, re.IGNORECASE))
    if record:
        if current != source or record["draft"] or record["prerelease"] != is_preview:
            raise RuntimeError(f"Review the existing release record for {tag}")
        return record["html_url"]

    if current is None:
        output("gh", "api", "--method", "POST", f"repos/{REPOSITORY}/git/refs",
               "-f", f"ref=refs/tags/{tag}", "-f", f"sha={source}")
    if tag_commit(tag) != source:
        raise RuntimeError(f"{tag} did not resolve to the tested commit")

    payload = {
        "tag_name": tag, "target_commitish": source, "name": f"{TITLE} {version}",
        "generate_release_notes": True, "prerelease": is_preview,
        "body": f"[Tested distributions on PyPI](https://pypi.org/project/{PACKAGE}/{version}/)\n\n"
                f"[Changelog at the tested commit](https://github.com/{REPOSITORY}/blob/{source}/CHANGELOG.md)\n\n"
                f"Tested source commit `{source}`.",
    }
    if not is_preview:
        payload["make_latest"] = "legacy"
    published = json.loads(output("gh", "api", "--method", "POST", f"repos/{REPOSITORY}/releases",
                                  "--input", "-", input=json.dumps(payload)))
    if (published["tag_name"] != tag or published["draft"]
            or published["prerelease"] != is_preview or tag_commit(tag) != source):
        raise RuntimeError(f"Could not verify the published source release for {tag}")
    return published["html_url"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["inspect", "publish"])
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    args = parser.parse_args()
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]
    version = project["version"]
    if project["name"] != PACKAGE or not re.fullmatch(r"[0-9][0-9a-zA-Z.!+]{0,64}", version):
        parser.error(f"Expected a compatible {PACKAGE} package version")

    if args.operation == "inspect":
        tag = f"v{version}"
        release = release_record(tag)
        print(f"{tag} {tag_commit(tag) or 'available'} {release['html_url'] if release else ''}".strip())
        return

    sha = os.environ.get("GITHUB_SHA", "")
    if (os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or os.environ.get("GITHUB_REF") != "refs/heads/master"
            or os.environ.get("GITHUB_EVENT_NAME") != "push"
            or not re.fullmatch(r"[0-9a-f]{40}", sha)
            or output("git", "rev-parse", "HEAD").strip() != sha):
        parser.error(f"Publication requires the tested {REPOSITORY} master push")
    subprocess.run(["git", "fetch", "--no-tags", "origin", "master"], check=True)
    subprocess.run(["git", "merge-base", "--is-ancestor", sha, "FETCH_HEAD"], check=True)
    print(publish(version, sha, args.dist))


if __name__ == "__main__":
    main()

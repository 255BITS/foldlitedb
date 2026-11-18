import hashlib
import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "publish_github_source.py"
SPEC = importlib.util.spec_from_file_location("publish_github_source", SCRIPT)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
VERSION = "1.2.3"
SOURCE = "a" * 40
TAG = f"v{VERSION}"


@pytest.fixture
def manifest(tmp_path):
    files = {
        f"{release.PACKAGE}-{VERSION}-py3-none-any.whl": b"wheel",
        f"{release.PACKAGE}-{VERSION}.tar.gz": b"source",
    }
    for name, content in files.items():
        (tmp_path / name).write_bytes(content)
    receipt = {
        "info": {"name": release.PACKAGE, "version": VERSION},
        "urls": [{"filename": name, "yanked": False, "digests": {"sha256": hashlib.sha256(data).hexdigest()}}
                 for name, data in files.items()],
    }
    return tmp_path, receipt


def test_published_receipt_confirms_every_distribution(manifest):
    dist, receipt = manifest
    with patch.object(release, "urlopen", return_value=io.BytesIO(json.dumps(receipt).encode())):
        release.verify_pypi(VERSION, dist)


def test_wrong_distribution_digest_prevents_tag_creation(manifest):
    dist, receipt = manifest
    receipt["urls"][0]["digests"]["sha256"] = "b" * 64
    with (
        patch.object(release, "urlopen", return_value=io.BytesIO(json.dumps(receipt).encode())),
        patch.object(release, "tag_commit") as tag,
        patch.object(release, "output") as output,
        pytest.raises(RuntimeError, match="PyPI hashes differ"),
    ):
        release.publish(VERSION, SOURCE, dist)
    tag.assert_not_called()
    output.assert_not_called()


def test_creates_a_tag_at_the_tested_commit_then_publishes(manifest):
    dist, _ = manifest
    url = f"https://github.com/{release.REPOSITORY}/releases/tag/{TAG}"
    published = {"tag_name": TAG, "draft": False, "prerelease": False, "html_url": url}
    with (
        patch.object(release, "verify_pypi") as verify,
        patch.object(release, "release_record", return_value=None),
        patch.object(release, "tag_commit", side_effect=[None, SOURCE, SOURCE]),
        patch.object(release, "output", side_effect=["{}", json.dumps(published)]) as output,
    ):
        assert release.publish(VERSION, SOURCE, dist) == url
    verify.assert_called_once_with(VERSION, dist)
    ref, record = output.call_args_list
    assert ref.args[-4:] == ("-f", f"ref=refs/tags/{TAG}", "-f", f"sha={SOURCE}")
    payload = json.loads(record.kwargs["input"])
    assert payload["target_commitish"] == SOURCE
    assert payload["tag_name"] == TAG
    assert payload["generate_release_notes"] is True


def test_retry_handles_a_matching_tag_without_moving_it(manifest):
    dist, _ = manifest
    url = f"https://github.com/{release.REPOSITORY}/releases/tag/{TAG}"
    published = {"tag_name": TAG, "draft": False, "prerelease": False, "html_url": url}
    with (
        patch.object(release, "verify_pypi"),
        patch.object(release, "release_record", side_effect=[None, published]),
        patch.object(release, "tag_commit", return_value=SOURCE),
        patch.object(release, "output", return_value=json.dumps(published)) as output,
    ):
        assert release.publish(VERSION, SOURCE, dist) == url
        assert release.publish(VERSION, SOURCE, dist) == url
    output.assert_called_once()
    assert output.call_args.args[-3] == f"repos/{release.REPOSITORY}/releases"


def test_existing_tag_from_a_different_commit_fails_closed(manifest):
    dist, _ = manifest
    with (
        patch.object(release, "verify_pypi"),
        patch.object(release, "tag_commit", return_value="b" * 40),
        patch.object(release, "output") as output,
        pytest.raises(RuntimeError, match="already points"),
    ):
        release.publish(VERSION, SOURCE, dist)
    output.assert_not_called()

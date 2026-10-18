"""Generic committed document prefixes retain exact bytes independently of schema."""
import hashlib

import pytest

from foldlitedb.fileio import (AppendJournal, PrefixMismatchError, PrefixTruncatedError,
                              validate_jsonl_prefix)


def test_committed_prefix_allows_suffix_and_large_last_document(tmp_path):
    journal = AppendJournal(tmp_path / 'documents.jsonl')
    journal.append({'n': 1})
    document = {'n': 2, 'content': 'x' * 2100000}
    journal.append(document)
    boundary = journal.sync()
    assert boundary['sha256'] == hashlib.sha256(journal.path.read_bytes()).hexdigest()
    journal.append({'n': 3})
    with journal.path.open('ab') as handle:
        handle.write(b'{"unfinished":')
    assert validate_jsonl_prefix(journal.path, **boundary) == document


@pytest.mark.parametrize('damage', ['truncate', 'edit', 'partial'])
def test_committed_prefix_corruption_is_visible(tmp_path, damage):
    journal = AppendJournal(tmp_path / 'documents.jsonl')
    journal.append({'n': 1})
    boundary = journal.sync()
    raw = journal.path.read_bytes()
    if damage == 'truncate':
        journal.path.write_bytes(raw[:-1])
        error = PrefixTruncatedError
    elif damage == 'edit':
        journal.path.write_bytes(raw.replace(b'1', b'2'))
        error = PrefixMismatchError
    else:
        raw = raw[:-1]
        journal.path.write_bytes(raw)
        boundary = {'offset': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
        error = PrefixMismatchError
    with pytest.raises(error):
        validate_jsonl_prefix(journal.path, **boundary)

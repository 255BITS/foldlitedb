"""Generic document logs: budgets, replay, copied projections and corruption."""
import base64
from contextlib import contextmanager
import hashlib
import json

import pytest

from foldlitedb.streams import read_jsonl_page
from foldlitedb.projection import recover_jsonl_tail, append_jsonl_frame


def encode(state):
    return base64.urlsafe_b64encode(json.dumps(state, sort_keys=True).encode()).decode()


def initial():
    return {'offset': 0, 'anchor': hashlib.sha256(b'').hexdigest(), 'file': None, 'last': 0}


def read(path, cursor=None, **kwargs):
    state = json.loads(base64.urlsafe_b64decode(cursor)) if cursor else initial()
    def consume(raw, saved):
        document = json.loads(raw)
        if document['n'] != saved['last'] + 1:
            raise ValueError('Noncontiguous document')
        return document, {'last': document['n']}
    return read_jsonl_page(path, state, consume=consume, encode=encode,
                           include_cursors=True, **kwargs)


def test_documents_resume_partial_unicode_and_byte_budget(tmp_path):
    path = tmp_path / 'documents.jsonl'
    assert read(path)['documents'] == []
    one = json.dumps({'n': 1}).encode() + b'\n'
    two = json.dumps({'n': 2, 'text': 'snowman ☃'}, ensure_ascii=False).encode() + b'\n'
    path.write_bytes(one + two[:-2])
    first = read(path)
    assert first['documents'] == [{'n': 1}] and first['partial_tail']
    assert read(path, first['cursor'])['cursor'] == first['cursor']
    path.write_bytes(one + two)
    with pytest.raises(ValueError, match='exceeds max_bytes'):
        read(path, first['cursor'], max_bytes=4)
    assert read(path, first['cursor'])['documents'] == [{'n': 2, 'text': 'snowman ☃'}]
    assert read(path)['document_cursors'][0] == first['cursor']


def test_file_replacement_anchor_corruption_and_contextmanager_opener(tmp_path):
    path = tmp_path / 'documents.jsonl'
    path.write_bytes(b'{"n":1}\n')
    @contextmanager
    def opener(path):
        with path.open('rb') as source:
            yield source
    cursor = read(path, open_file=opener)['cursor']
    path.write_bytes(b'{"n":9}\n')
    with pytest.raises(ValueError, match='boundary changed'):
        read(path, cursor)
    replacement = tmp_path / 'replacement'
    replacement.write_bytes(b'{"n":1}\n')
    replacement.replace(path)
    with pytest.raises(ValueError, match='replaced'):
        read(path, cursor)


def test_projection_tail_recovery_is_bounded_and_preserves_complete_corruption(tmp_path):
    path = tmp_path / 'projection.jsonl'
    with path.open('w+b') as handle:
        for n in range(500):
            append_jsonl_frame(handle, {'n': n})
        handle.write(b'{"torn":')
        row = recover_jsonl_tail(handle, validate=lambda row: row, max_frame_bytes=64)
        assert row == {'n': 499}
        assert path.read_bytes().endswith(b'{"n":499}\n')
        handle.write(b'{broken}\n')
        handle.flush()
        size = path.stat().st_size
        with pytest.raises(ValueError):
            recover_jsonl_tail(handle, validate=lambda row: row, max_frame_bytes=64)
        assert path.stat().st_size == size


def test_complete_corrupt_documents_fail_and_reads_do_not_mutate(tmp_path):
    path = tmp_path / 'documents.jsonl'
    raw = b'{"n":1}\n{broken}\n'
    path.write_bytes(raw)
    with pytest.raises(ValueError):
        read(path)
    assert path.read_bytes() == raw

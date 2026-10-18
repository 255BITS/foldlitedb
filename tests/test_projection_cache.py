"""Persisted projection jobs resume exact cursor coverage and can be discarded."""
import asyncio

import pytest

from foldlitedb.database import OrderedFold, View
from foldlitedb.projection import materialize_jsonl_view


def view():
    return View('totals', version='1',
                map=lambda document: [(document.payload['document']['key'],
                                       document.payload['document']['value'])],
                reducer=OrderedFold(0, lambda state, value: state + value, version='1'))


def materialize(path, documents, reads, *, generation='one', max_groups=10, invalid=False):
    def read_page(cursor, limit=256):
        reads.append(cursor)
        if invalid:
            raise ValueError('Consumed document boundary changed')
        rows = [row for row in documents if row['n'] > int(cursor or 0)][:limit]
        return rows, [str(row['n']) for row in rows], {}
    return materialize_jsonl_view(path, generation=generation, view=view(),
        read_page=read_page, target_cursor=str(len(documents)), target_position=len(documents),
        document_position=lambda document: document['n'], cursor_position=lambda cursor: int(cursor or 0),
        max_groups=max_groups)


def test_reopen_ingests_only_suffix_and_discard_regenerates(tmp_path):
    path = tmp_path / 'views.sqlite'
    documents = [{'n': n, 'key': 'total', 'value': n} for n in range(1, 4)]
    first = asyncio.run(materialize(path, documents, []))
    assert first[0].state == 6
    reads = []
    reopened = asyncio.run(materialize(path, documents, reads))
    assert reopened[0].state == 6 and None not in reads
    documents.append({'n': 4, 'key': 'total', 'value': 4})
    reads = []
    suffix = asyncio.run(materialize(path, documents, reads))
    assert suffix[0].state == 10 and None not in reads
    path.unlink()
    regenerated = asyncio.run(materialize(path, documents, []))
    assert regenerated[0].state == suffix[0].state


def test_saved_source_boundary_must_validate_before_cached_result(tmp_path):
    path = tmp_path / 'views.sqlite'
    documents = [{'n': 1, 'key': 'total', 'value': 3}]
    asyncio.run(materialize(path, documents, []))
    with pytest.raises(ValueError, match='boundary changed'):
        asyncio.run(materialize(path, documents, [], invalid=True))


def test_generation_change_regenerates_and_group_limit_is_explicit(tmp_path):
    path = tmp_path / 'views.sqlite'
    documents = [{'n': 1, 'key': 'total', 'value': 3}]
    asyncio.run(materialize(path, documents, []))
    documents[0]['value'] = 7
    changed = asyncio.run(materialize(path, documents, [], generation='two'))
    assert changed[0].state == 7
    documents.append({'n': 2, 'key': 'other', 'value': 9})
    with pytest.raises(ValueError, match='exceeds 1 groups'):
        asyncio.run(materialize(path, documents, [], generation='two', max_groups=1))

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


@pytest.mark.parametrize('phase', ['constructor', 'ingest'])
def test_cancellation_drains_owned_database_before_return(tmp_path, monkeypatch, phase):
    """Constructor and queued ingest work cannot outlive the job's close."""
    import threading
    import foldlitedb.database as module
    original = module.Database
    entered, release = threading.Event(), threading.Event()
    instances, events = [], []

    class SlowDatabase(original):
        def __init__(self, *args, **kwargs):
            if phase == 'constructor':
                entered.set()
                assert release.wait(5)
            super().__init__(*args, **kwargs)
            instances.append(self)

        def ingest(self, documents):
            if phase == 'ingest':
                entered.set()
                assert release.wait(5)
            try:
                return super().ingest(documents)
            finally:
                events.append('ingest_settled')

        def close(self):
            events.append('close')
            return super().close()

    monkeypatch.setattr(module, 'Database', SlowDatabase)
    documents = [{'n': 1, 'key': 'total', 'value': 3}]
    async def scenario():
        task = asyncio.create_task(materialize(tmp_path / 'cancel.sqlite', documents, []))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()  # Repeated cancellation must still drain the owner.
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert instances and all(database._closed for database in instances)
        assert events[-1] == 'close'
        if phase == 'ingest':
            assert events.index('ingest_settled') < events.index('close')
    try:
        asyncio.run(scenario())
    finally:
        release.set()


def test_delayed_snapshot_rebuilds_without_exposing_future_state(tmp_path):
    path = tmp_path / 'views.sqlite'
    documents = [{'n': n, 'key': 'total', 'value': n} for n in range(1, 4)]
    latest = asyncio.run(materialize(path, documents, []))
    earlier = asyncio.run(materialize(path, documents[:1], []))
    assert earlier[0].state == 1
    reads = []
    reopened = asyncio.run(materialize(path, documents, reads))
    assert latest[0].state == reopened[0].state == 6
    assert None not in reads  # The durable newer snapshot was preserved.


def test_concurrent_snapshots_hold_a_cache_lock_until_final_query(tmp_path, monkeypatch):
    import threading
    import foldlitedb.database as database_module
    import foldlitedb.fileio as fileio_module
    original_database, original_lock = database_module.Database, fileio_module.file_lock
    final_query, release, lock_attempted = threading.Event(), threading.Event(), threading.Event()
    instances, locks = [], []

    class PausedDatabase(original_database):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.queries = 0
            instances.append(self)

        def query(self, *args, **kwargs):
            self.queries += 1
            if len(instances) == 1 and self.queries == 2:
                final_query.set()
                assert release.wait(5)
            return super().query(*args, **kwargs)

    def traced_lock(*args, **kwargs):
        locks.append(1)
        if len(locks) > 1:
            lock_attempted.set()
        return original_lock(*args, **kwargs)
    monkeypatch.setattr(database_module, 'Database', PausedDatabase)
    monkeypatch.setattr(fileio_module, 'file_lock', traced_lock)
    path = tmp_path / 'shared.sqlite'
    documents = [{'n': n, 'key': 'total', 'value': n} for n in range(1, 4)]
    async def scenario():
        first = asyncio.create_task(materialize(path, documents[:1], []))
        assert await asyncio.to_thread(final_query.wait, 5)
        second = asyncio.create_task(materialize(path, documents, []))
        assert await asyncio.to_thread(lock_attempted.wait, 5)
        assert len(instances) == 1
        release.set()
        earlier, later = await asyncio.gather(first, second)
        assert earlier[0].state == 1
        assert later[0].state == 6
    try:
        asyncio.run(scenario())
    finally:
        release.set()


def test_corrupt_derived_database_has_an_actionable_error(tmp_path):
    path = tmp_path / 'corrupt.sqlite'
    path.write_bytes(b'not a SQLite database')
    documents = [{'n': 1, 'key': 'total', 'value': 3}]
    with pytest.raises(RuntimeError, match='derived view cache.*remove'):
        asyncio.run(materialize(path, documents, []))
    path.unlink()
    result = asyncio.run(materialize(path, documents, []))
    assert result[0].state == 3

"""Application-neutral append-only projection persistence."""
import json
import os
from pathlib import Path


def json_bytes(value):
    try:
        return json.dumps(value, allow_nan=False, sort_keys=True,
                          separators=(',', ':')).encode('utf-8')
    except (ValueError, TypeError, RecursionError) as exc:
        raise ValueError(f'Expected finite JSON: {exc}') from exc


def load_json(raw):
    def reject(value):
        raise ValueError(f'Nonfinite JSON constant: {value}')
    value = json.loads(raw, parse_constant=reject)
    json_bytes(value)  # Overflowing exponents also fail.
    return value


def read_bounded_json(path, *, max_bytes=65536, open_file=None):
    path = Path(path)
    with (open_file(path) if open_file else path.open('rb')) as handle:
        raw = handle.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError('Projection metadata exceeds byte budget')
    return load_json(raw)


def recover_jsonl_tail(handle, *, validate, max_frame_bytes=65536):
    """Recover the last complete document; truncate only an unfinished tail.

    At most two frame budgets are read, independent of total log size. Complete
    corrupt documents fail validation before any truncation. The application
    validator returns the recovered document.
    """
    handle.seek(0, os.SEEK_END)
    size = handle.tell()
    begin = max(0, size - 2 * max_frame_bytes)
    handle.seek(begin)
    tail = handle.read()
    end = tail.rfind(b'\n')
    if end < 0:
        if size > max_frame_bytes:
            raise ValueError('Projection incomplete tail exceeds byte budget')
        handle.truncate(0)
        result = None
    else:
        start = tail.rfind(b'\n', 0, end) + 1
        if begin and not start:
            raise ValueError('Projection final frame exceeds byte budget')
        if len(tail[start:end]) + 1 > max_frame_bytes or len(tail) - end - 1 > max_frame_bytes:
            raise ValueError('Projection tail exceeds byte budget')
        result = validate(load_json(tail[start:end]))
        handle.truncate(begin + end + 1)
    handle.seek(0, os.SEEK_END)
    return result


def append_jsonl_frame(handle, row, *, max_frame_bytes=65536):
    """Append one complete projection document (progress and output together)."""
    encoded = json_bytes(row) + b'\n'
    if len(encoded) > max_frame_bytes:
        raise ValueError(f'Projection frame exceeds {max_frame_bytes} bytes')
    written = handle.write(encoded)
    if written != len(encoded):
        raise OSError('Incomplete projection write; close and reopen to repair its tail')
    handle.flush()


async def materialize_jsonl_view(path, *, generation, view, read_page, target_cursor,
                                 target_position, document_position, cursor_position,
                                 max_groups=2048, timeout=180):
    """Resume a durable view from its committed projection document and cursor.

    ``read_page(cursor)`` returns documents, per-document cursors, and page
    metadata. Document payloads are application-owned; the cached payload wraps
    each as ``{'document': value, 'cursor': cursor}``. The View mapper receives
    this wrapper through Document.payload. Empty mapped documents still commit
    progress. Only the uncommitted suffix is read after reopening.

    The source log remains authoritative: both the requested snapshot and the
    saved source boundary are validated on every call, even without a suffix.
    Group and time budgets make explicit historical jobs bounded. This function
    does not create a mapper process or start a background task.
    """
    import asyncio
    from contextlib import contextmanager
    from dataclasses import replace
    import math
    import threading
    import time
    from .database import Database, Document, GenerationMismatch
    from .fileio import file_lock

    if type(max_groups) is not int or not 1 <= max_groups <= 4095:
        raise ValueError('max_groups must be an integer in 1..4095')
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('timeout must be positive finite seconds')
    cancelled = threading.Event()
    started = time.monotonic()

    class WorkCancelled(Exception):
        pass

    def check_budget():
        if cancelled.is_set():
            raise WorkCancelled()
        if time.monotonic() - started > timeout:
            raise ValueError('Historical reduction exceeded its time budget; retry with a larger explicit history budget or prepare a smaller projection')

    def materialize_locked():
        # One worker owns construction, every operation and close. Cancellation
        # never closes a connection underneath work still running in a thread.
        known = set()
        def bounded_map(document):
            check_budget()
            for key, contribution in view.map(document):
                encoded_key = json_bytes(key)
                if encoded_key not in known:
                    if len(known) >= max_groups:
                        raise ValueError(f'View exceeds {max_groups} groups; narrow the view')
                    known.add(encoded_key)
                yield key, contribution
        check_budget()
        read_page(target_cursor, limit=1)
        check_budget()
        try:
            database = Database(path, generation=generation)
        except GenerationMismatch:
            check_budget()
            database = Database(path, generation=generation, reset=True)
        # A delayed request can capture an older watermark than an already
        # completed job. Rebuild that snapshot transiently without discarding a
        # newer useful durable cache or returning future contributions.
        try:
            checkpoint = database.checkpoint('projection')
        except BaseException:
            database.close()
            raise
        if (checkpoint or 0) > target_position:
            database.close()
            check_budget()
            database = Database(':memory:', generation=generation)
        with database:
            check_budget()
            database.register(replace(view, map=bounded_map))
            snapshot = database.query(view.name, limit=max_groups + 1)
            if not snapshot.complete or len(snapshot.rows) > max_groups:
                raise ValueError(f'View exceeds {max_groups} groups; narrow the view')
            known.update(json_bytes(row.key) for row in snapshot.rows)
            checkpoint = database.checkpoint('projection')
            replay = None
            if checkpoint is not None:
                last = database.get(str(checkpoint), source='projection')
                if last is None:
                    raise ValueError('Projection checkpoint document disappeared; rebuild the view cache')
                replay = last.payload['cursor']
            if replay is not None:
                read_page(replay, limit=1)
            target = cursor_position(target_cursor)
            while cursor_position(replay) < target:
                check_budget()
                documents, cursors, _ = read_page(replay)
                if not documents:
                    raise ValueError('Historical projection watermark disappeared')
                batch = []
                for document, end in zip(documents, cursors):
                    if cursor_position(end) > target:
                        break
                    position = document_position(document)
                    batch.append(Document(str(position), {'document': document, 'cursor': end},
                                          position=position, source='projection'))
                    replay = end
                    checkpoint = position
                if not batch:
                    raise ValueError('Historical snapshot is not a complete document boundary')
                database.ingest(batch)
            check_budget()
            if (checkpoint or 0) != target_position:
                raise ValueError('Projection checkpoint differs from the requested snapshot')
            snapshot = database.query(view.name, limit=max_groups + 1)
            if not snapshot.complete or len(snapshot.rows) > max_groups:
                raise ValueError(f'View exceeds {max_groups} groups; narrow the view')
            return snapshot.rows

    @contextmanager
    def cache_lock():
        cache_path = Path(path)
        while True:
            check_budget()
            lock = file_lock(cache_path.parent, name=cache_path.name + '.lock')
            try:
                lock.__enter__()
            except RuntimeError:
                cancelled.wait(.025)
                continue
            try:
                yield
            finally:
                lock.__exit__(None, None, None)
            return

    def materialize():
        # The lock spans source checkpoint capture through final snapshot, across
        # processes as well as threads. SQLite transaction locks alone do not
        # prevent a second job advancing state between those operations.
        with cache_lock():
            return materialize_locked()

    worker = asyncio.create_task(asyncio.to_thread(materialize))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        cancelled.set()
        # Shielding alone leaves a background task alive. Drain it, including
        # repeated cancellation requests, so the connection is always closed
        # before this job finishes. An in-flight transaction may commit first.
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if worker.done() and not worker.cancelled():
            try:
                worker.result()
            except BaseException:
                pass
        raise

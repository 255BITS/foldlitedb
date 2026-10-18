"""Bounded JSONL document pages with validated continuation boundaries.

Applications supply document decoding, identity validation and cursor encoding.
The reader owns byte budgets, incomplete tails and file generation checks. No
application schema, training runtime or HTTP framework is imported here.
"""
import hashlib
import os
from pathlib import Path

ANCHOR_BYTES = 256
MAX_PAGE_BYTES = 16 * 1024 * 1024


def _integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{name} must be an integer between {low} and {high}')


def read_jsonl_page(path, state, *, consume, encode, limit=100,
                    max_bytes=1048576, include_cursors=False, open_file=None,
                    first_document=None, bind_file=True, allow_missing=True,
                    max_frame_bytes=None, label='document'):
    """Read complete documents without advancing through an unfinished write.

    ``state`` has validated ``offset`` and SHA256 ``anchor`` fields; with
    ``bind_file`` it also has a nullable ``file`` device/inode pair. ``consume``
    receives raw document bytes and the current cursor state and returns a
    decoded document and application cursor fields. ``encode`` makes that state
    an opaque continuation token. ``first_document`` optionally validates the
    bounded first document, for portable generation identities.

    A page reads at most max_bytes plus a 256 byte boundary; first-document
    verification adds at most max_bytes + 1. Complete corrupt documents fail
    through the application validator. File replacement during a read fails.
    """
    _integer(limit, 'limit', 1, 10000)
    _integer(max_bytes, 'max_bytes', 1, MAX_PAGE_BYTES)
    if type(include_cursors) is not bool:
        raise ValueError('include_cursors must be a boolean')
    if max_frame_bytes is not None:
        _integer(max_frame_bytes, 'max_frame_bytes', 1, MAX_PAGE_BYTES)
    state = dict(state)
    path = Path(path)
    prefix = f'Stale {label} cursor: '
    try:
        handle = open_file(path) if open_file else path.open('rb')
    except FileNotFoundError:
        if not allow_missing:
            raise
        if state.get('file') is not None or state['offset']:
            raise ValueError(prefix + 'the log was removed; restart without a cursor')
        return dict(documents=[], document_cursors=[], cursor=encode(state),
                    has_more=False, partial_tail=False)
    with handle as handle:
        stat = os.fstat(handle.fileno())
        identity = [stat.st_dev, stat.st_ino]
        if bind_file and state.get('file') is not None and state['file'] != identity:
            raise ValueError(prefix + 'the log was replaced; restart without a cursor')
        if first_document is not None and stat.st_size:
            raw = handle.readline(max_bytes + 1)
            if len(raw) > max_bytes:
                raise ValueError('Public stream first document exceeds max_bytes')
            if raw.endswith(b'\n'):
                first_document(raw)
        offset = state['offset']
        if offset > stat.st_size:
            raise ValueError(prefix + 'the log was truncated; restart without a cursor')
        handle.seek(max(0, offset - ANCHOR_BYTES))
        anchor = handle.read(min(offset, ANCHOR_BYTES))
        if (hashlib.sha256(anchor).hexdigest() != state['anchor']
                or (offset and not anchor.endswith(b'\n'))):
            raise ValueError(prefix + 'its consumed boundary changed; restart without a cursor')
        handle.seek(offset)
        data = handle.read(max_bytes)
        read_end = offset + len(data)
        documents, cursors, consumed = [], [], 0
        for _ in range(limit):
            end = data.find(b'\n', consumed)
            if end < 0:
                break
            if max_frame_bytes is not None and end + 1 - consumed > max_frame_bytes:
                raise ValueError(f'{label.capitalize()} frame exceeds byte budget')
            document, update = consume(data[consumed:end], state)
            documents.append(document)
            anchor = (anchor + data[consumed:end + 1])[-ANCHOR_BYTES:]
            consumed = end + 1
            state.update(update, offset=offset + consumed, anchor=hashlib.sha256(anchor).hexdigest())
            if bind_file:
                state['file'] = identity
            if include_cursors:
                cursors.append(encode(state))
        if not documents and len(data) == max_bytes and read_end < stat.st_size:
            raise ValueError(f'Next {label} exceeds max_bytes; increase the byte limit (maximum {MAX_PAGE_BYTES})')
        if bind_file:
            state['file'] = identity
        try:
            current = path.stat()
        except FileNotFoundError as exc:
            raise ValueError(prefix + 'the log was removed while reading; retry without a cursor') from exc
        if [current.st_dev, current.st_ino] != identity:
            raise ValueError(prefix + 'the log was replaced while reading; retry without a cursor')
        return dict(documents=documents, document_cursors=cursors, cursor=encode(state),
                    has_more=read_end < stat.st_size or b'\n' in data[consumed:],
                    partial_tail=read_end == stat.st_size and bool(data) and not data.endswith(b'\n'))


def read_jsonl_tail(path, limit=100, max_bytes=1048576, *, decode=None,
                    error_message='Corrupt complete document row in log'):
    """Read a bounded tail, discarding clipped leading and unfinished documents.

    Unlike a forward cursor page, this is a diagnostic snapshot with no replay
    acknowledgement. At most max_bytes are read. Applications can provide their
    document decoder and compatibility error text; the default is json.loads.
    """
    import json
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
        raise ValueError('limit must be between 1 and 10000')
    if not isinstance(max_bytes, int) or not 1 <= max_bytes <= MAX_PAGE_BYTES:
        raise ValueError('max_bytes must be between 1 and 16777216')
    path = Path(path)
    if not path.exists():
        return []
    with path.open('rb') as handle:
        handle.seek(0, 2)
        start = max(0, handle.tell() - max_bytes)
        handle.seek(start)
        data = handle.read(max_bytes)
    if start:
        data = data.partition(b'\n')[2]
    lines = data.split(b'\n')[:-1]
    documents = []
    decode = json.loads if decode is None else decode
    for line in lines[-limit:]:
        try:
            documents.append(decode(line))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError(error_message) from exc
    return documents

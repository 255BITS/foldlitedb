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

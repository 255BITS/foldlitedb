"""Atomic metadata, process-owned writer locks and append-only journals."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import tempfile


def sync_directory(path):
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as output:
            json.dump(value, output, indent=2, allow_nan=False)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def file_lock(directory, *, name=".foldlite.lock"):
    """An OS-owned lock releases on process death; the file itself may remain."""
    path = Path(directory) / name
    with path.open('a+b') as handle:
        # msvcrt permits locking beyond EOF. Do not write an initialization byte
        # that another Windows handle may have locked since the file was opened.
        handle.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError(f'Directory is already locked by another process: {directory}') from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == 'nt':
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def repair_jsonl_tail(path):
    """Drop an incomplete final write before appending new documents."""
    path = Path(path)
    if not path.exists():
        return
    with path.open('r+b') as handle:
        handle.seek(0, 2)
        end = handle.tell()
        while end:
            start = max(0, end - 65536)
            handle.seek(start)
            block = handle.read(end - start)
            last = block.rfind(b'\n')
            if last >= 0:
                handle.truncate(start + last + 1)
                return
            end = start
        handle.truncate(0)


class AppendJournal:
    """One writer's append-only document log with a durable, content-addressed prefix.

    Opening hashes retained history once; appends update that hash incrementally.
    The caller must exclude other writers. Projection readers need no lock.
    """
    def __init__(self, path):
        self.path = Path(path)
        repair_jsonl_tail(self.path)
        self.digest = hashlib.sha256()
        self.offset = 0
        if self.path.exists():
            with self.path.open('rb') as source:
                for block in iter(lambda: source.read(1048576), b''):
                    self.digest.update(block)
                    self.offset += len(block)
        self.last = None
        self.failed = False

    def append(self, row):
        if self.failed:
            raise OSError('Document journal has an incomplete write; resume must repair its tail')
        data = (json.dumps(row, allow_nan=False) + '\n').encode('utf-8')
        try:
            with self.path.open('ab') as output:
                if output.write(data) != len(data):
                    raise OSError('Incomplete document write; reopen to repair its tail')
                output.flush()
        except BaseException:
            self.failed = True
            raise
        self.digest.update(data)
        self.offset += len(data)
        self.last = row

    def sync(self):
        """Flush a committed append boundary and return its offset and digest."""
        if self.failed:
            raise OSError('Cannot commit an incomplete document write')
        with self.path.open('r+b') as source:
            if os.fstat(source.fileno()).st_size != self.offset:
                raise ValueError('Document log changed outside its writer')
            os.fsync(source.fileno())
        sync_directory(self.path.parent)
        return {'offset': self.offset, 'sha256': self.digest.hexdigest()}


class PrefixTruncatedError(ValueError):
    """The source no longer contains all committed prefix bytes."""


class PrefixMismatchError(ValueError):
    """Committed bytes changed or the prefix ends inside a document."""


def validate_jsonl_prefix(path, *, offset, sha256):
    """Verify exact committed bytes and return their last complete document.

    Later documents and unfinished later writes are permitted. Hashing streams
    in 1 MiB blocks and retains the final document for decoding. That document
    may exceed one block; no new document-size limit is imposed here. The caller
    validates application identity against the returned JSON document.
    """
    if type(offset) is not int or offset < 1:
        raise ValueError('Committed prefix offset must be a positive integer')
    digest, remaining, tail = hashlib.sha256(), offset, b''
    with Path(path).open('rb') as source:
        while remaining:
            block = source.read(min(1048576, remaining))
            if not block:
                raise PrefixTruncatedError('Committed prefix is missing documents')
            digest.update(block)
            remaining -= len(block)
            tail += block
            lines = tail.rsplit(b'\n', 2)
            if len(lines) == 3:
                tail = b'\n'.join(lines[-2:])
    if digest.hexdigest() != sha256 or not tail.endswith(b'\n'):
        raise PrefixMismatchError('Committed prefix digest or complete-document mismatch')
    return json.loads(tail.rstrip(b'\n').rsplit(b'\n', 1)[-1])

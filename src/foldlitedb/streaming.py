"""Bounded framework-independent realtime document subscriptions and SSE."""
import asyncio
import base64
from dataclasses import dataclass
import json


def _json(value):
    return json.dumps(value, allow_nan=False, sort_keys=True, separators=(',', ':'))


def cursor_offset(cursor):
    if cursor is None:
        return 0
    if not isinstance(cursor, str) or len(cursor) > 4096:
        raise ValueError('Invalid stream cursor')
    try:
        data = json.loads(base64.b64decode(cursor.encode('ascii'), altchars=b'-_', validate=True))
        offset = data['offset']
        if type(offset) is not int or offset < 0:
            raise ValueError('Invalid stream offset')
        return offset
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ValueError('Invalid stream cursor') from exc


def sse(event, value):
    # No SSE id: native EventSource acknowledgement is not an application commit.
    return ('event: ' + event + '\ndata: ' + _json(value) + '\n\n').encode()


class Subscriber:
    def __init__(self, stream_id, *, max_bytes=1048576, max_items=256):
        if type(max_bytes) is not int or max_bytes < 1 or type(max_items) is not int or max_items < 1:
            raise ValueError('Queue byte and item limits must be positive integers')
        self.stream_id = stream_id
        self.queue = asyncio.Queue(maxsize=max_items)
        self.size = 0
        self.max_bytes = max_bytes
        self.closed = False

    def accepts(self, stream_id):
        return self.stream_id == '*' or self.stream_id == stream_id

    def offer(self, event, data):
        if self.closed:
            return
        encoded = sse(event, data)
        if self.size + len(encoded) > self.max_bytes or self.queue.full():
            self.closed = True
            while not self.queue.empty():
                self.queue.get_nowait()
            encoded = sse('gap', {'reason': 'consumer_queue_exceeded', 'recover': 'reconnect_from_last_applied_cursor'})
            self.size = len(encoded)
            self.queue.put_nowait(encoded)
            return
        self.size += len(encoded)
        self.queue.put_nowait(encoded)

    async def get(self):
        result = await self.queue.get()
        self.size -= len(result)
        return result


@dataclass
class Stream:
    stream_id: str
    cursor: str | None = None
    sequence: int = 0
    caught_up: bool = False
    error: str | None = None
    generation: str | None = None
    projected_offset: int | None = None
    task: object = None


async def stream_events(read_page, subscriber, watermarks, *, stream_id, cursor=None,
                        metadata=lambda: {}, metadata_key="context", replay_limit=4096, heartbeat_seconds=15):
    """Replay through a snapshot watermark, then consume the registered queue.

    Register the subscriber and snapshot watermarks synchronously before calling
    this generator. Duplicate queued suffix documents are skipped at the same
    application cursor. Queue overflow and replay limits are explicit gap frames.
    The caller owns unregistration and application metadata construction.
    """
    try:
        if cursor is not None and stream_id == '*':
            raise ValueError('A replay cursor requires one explicit stream')
        if cursor is not None:
            if stream_id not in watermarks:
                raise ValueError('Cannot replay a missing stream')
            target = cursor_offset(watermarks[stream_id])
            if cursor_offset(cursor) > target:
                # Indexer is behind the acknowledged client. Wait for the
                # shared tail; do not invent a reduced state or lose its suffix.
                target = cursor_offset(cursor)
            replay = cursor
            replay_count = 0
            while cursor_offset(replay) < target:
                frames, cursors, page = await asyncio.to_thread(read_page, stream_id, replay)
                if not frames:
                    raise ValueError('Replay watermark is no longer available')
                for frame, end in zip(frames, cursors):
                    if cursor_offset(end) > target:
                        break
                    replay_count += 1
                    if replay_count > replay_limit:
                        yield sse('gap', {'reason': 'replay_budget_exceeded', 'recover': 'request_fresh_bootstrap', 'retry_after_seconds': 5})
                        return
                    replay = end
                    yield sse('frame', {'stream_id': stream_id, 'cursor': end, 'frame': frame})
                if subscriber.closed:
                    break
            # Validate even an empty replay against the underlying file.
            await asyncio.to_thread(read_page, stream_id, replay, limit=1)
            watermarks[stream_id] = replay
        yield sse('ready', {'streams': watermarks, metadata_key: metadata()})
        while True:
            try:
                encoded = await asyncio.wait_for(subscriber.get(), timeout=heartbeat_seconds)
            except asyncio.TimeoutError:
                yield sse('heartbeat', {metadata_key: metadata()})
                continue
            if encoded.startswith(b'event: frame\n'):
                value = json.loads(encoded.split(b'data: ', 1)[1])
                marker = watermarks.get(value['stream_id'])
                if marker is not None and cursor_offset(value['cursor']) <= cursor_offset(marker):
                    continue
                watermarks[value['stream_id']] = value['cursor']
            yield encoded
            if subscriber.closed:
                return
    except (ValueError, OSError, KeyError) as exc:
        yield sse('reset_required', {'reason': str(exc)})

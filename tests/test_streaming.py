"""Realtime queue backpressure and snapshot/live replay coverage."""
import asyncio
import base64
import json

from foldlitedb.streaming import Subscriber, stream_events


def cursor(offset):
    return base64.urlsafe_b64encode(json.dumps({'offset': offset}).encode()).decode()


def test_overflow_delivers_gap_and_discards_unapplied_frames():
    async def run():
        subscriber = Subscriber('documents', max_bytes=150)
        subscriber.offer('frame', {'value': 'x' * 200})
        frame = await subscriber.get()
        assert subscriber.closed and subscriber.size == 0
        assert frame.startswith(b'event: gap\n')
        assert b'reconnect_from_last_applied_cursor' in frame
    asyncio.run(run())


def test_replay_then_live_skips_queued_duplicate():
    async def run():
        subscriber = Subscriber('documents')
        subscriber.offer('frame', {'stream_id': 'documents', 'cursor': cursor(2), 'frame': {'n': 2}})
        subscriber.offer('frame', {'stream_id': 'documents', 'cursor': cursor(3), 'frame': {'n': 3}})
        def page(stream, saved, limit=256):
            n = json.loads(base64.urlsafe_b64decode(saved))['offset']
            return ([{'n': 2}], [cursor(2)], {}) if n == 1 else ([], [], {})
        generator = stream_events(page, subscriber, {'documents': cursor(2)},
                                  stream_id='documents', cursor=cursor(1))
        replay = await anext(generator)
        assert b'"n":2' in replay
        assert (await anext(generator)).startswith(b'event: ready\n')
        live = await anext(generator)
        assert b'"n":3' in live
        await generator.aclose()
    asyncio.run(run())

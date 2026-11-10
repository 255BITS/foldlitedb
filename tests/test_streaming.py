"""Realtime queue backpressure and snapshot/live replay coverage."""
import asyncio
import base64
import json
import time

import pytest

from foldlitedb.streaming import Subscriber, sse, stream_events


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


def test_overflow_during_replay_delivers_gap_without_ready_and_preserves_caller_watermarks():
    async def run():
        subscriber = Subscriber('documents', max_bytes=150)
        caller_watermarks = {'documents': cursor(3)}

        def page(stream, saved, limit=256):
            n = json.loads(base64.urlsafe_b64decode(saved))['offset']
            if n == 1:
                subscriber.offer('frame', {'value': 'x' * 200})
                return ([{'n': 2}], [cursor(2)], {})
            return ([{'n': 3}], [cursor(3)], {})

        events = [event async for event in stream_events(
            page, subscriber, caller_watermarks,
            stream_id='documents', cursor=cursor(1), heartbeat_seconds=0.01,
        )]
        assert len(events) == 2
        assert events[0].startswith(b'event: frame\n')
        assert events[1].startswith(b'event: gap\n')
        assert caller_watermarks == {'documents': cursor(3)}
    asyncio.run(run())


def test_sse_validates_event_name_and_subscriber_close_wakes_stream_immediately():
    for invalid_event in ['', 'frame\nevent: injected', 'frame\r', 'bad:name', None, 123]:
        with pytest.raises(ValueError, match='Invalid SSE event name'):
            sse(invalid_event, {'ok': True})

    async def run():
        subscriber = Subscriber('documents')
        generator = stream_events(
            lambda *_args, **_kw: ([], [], {}),
            subscriber,
            {'documents': cursor(1)},
            stream_id='documents',
            heartbeat_seconds=30,
        )
        first = await anext(generator)
        assert first.startswith(b'event: ready\n')

        started = time.monotonic()

        async def close_soon():
            await asyncio.sleep(0.02)
            subscriber.close()

        closer = asyncio.create_task(close_soon())
        remaining = [event async for event in generator]
        await closer
        assert remaining == []
        assert time.monotonic() - started < 1.0

    asyncio.run(run())

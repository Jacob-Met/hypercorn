"""Regression tests for flow-controlled HTTP/2 response stream lifetimes."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from h2.events import StreamReset

from hypercorn.asyncio.worker_context import EventWrapper, WorkerContext
from hypercorn.config import Config
from hypercorn.events import Closed
from hypercorn.protocol.h2 import BUFFER_HIGH_WATER, H2Protocol, StreamBuffer
from hypercorn.typing import ConnectionState


@pytest.mark.asyncio
async def test_zero_window_does_not_resume_blocked_producer() -> None:
    """A zero-window pop cannot grant application-side write credit."""
    buffer = StreamBuffer(EventWrapper)
    blocked = asyncio.create_task(buffer.push(b"x" * (BUFFER_HIGH_WATER + 1)))
    await asyncio.sleep(0)
    assert not blocked.done()
    await buffer.pop(0)
    await asyncio.sleep(0)
    assert not blocked.done()
    await buffer.pop(BUFFER_HIGH_WATER)
    await asyncio.wait_for(blocked, timeout=0.5)


@pytest.mark.asyncio
@pytest.mark.parametrize("termination", ["reset", "connection_close"])
async def test_stream_termination_wakes_producer_and_drops_bytes(termination: str) -> None:
    """Neither a stream reset nor full close may retain queued response data."""
    protocol = H2Protocol(
        Mock(),
        Config(),
        WorkerContext(None),
        AsyncMock(),
        ConnectionState({}),
        False,
        None,
        None,
        AsyncMock(),
    )
    stream_id = 1
    buffer = StreamBuffer(EventWrapper)
    protocol.stream_buffers[stream_id] = buffer
    protocol.priority.insert_stream(stream_id)
    protocol.priority.block(stream_id)
    blocked = asyncio.create_task(buffer.push(b"x" * (BUFFER_HIGH_WATER + 1)))
    await asyncio.sleep(0)
    assert not blocked.done()

    if termination == "reset":
        event = StreamReset(stream_id=stream_id)
        await protocol._handle_events([event])
    else:
        await protocol.handle(Closed())

    await asyncio.wait_for(blocked, timeout=0.5)
    assert protocol.stream_buffers == {}
    assert len(buffer.buffer) == 0
    assert buffer.complete


@pytest.mark.asyncio
async def test_stream_termination_wakes_pending_final_drain() -> None:
    """The end-of-response drain waiter must unblock after a peer reset."""
    protocol = H2Protocol(
        Mock(),
        Config(),
        WorkerContext(None),
        AsyncMock(),
        ConnectionState({}),
        False,
        None,
        None,
        AsyncMock(),
    )
    stream_id = 1
    buffer = StreamBuffer(EventWrapper)
    protocol.stream_buffers[stream_id] = buffer
    protocol.priority.insert_stream(stream_id)
    await buffer.push(b"partial response")
    buffer.set_complete()
    drain = asyncio.create_task(buffer.drain())
    await asyncio.sleep(0)
    assert not drain.done()

    event = StreamReset(stream_id=stream_id)
    await protocol._handle_events([event])
    await asyncio.wait_for(drain, timeout=0.5)
    assert not protocol.stream_buffers
    assert not buffer.buffer

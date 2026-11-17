"""HTTP/2 stream flow-control and teardown regressions (issue #390)."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from h2.connection import H2Connection

from hypercorn.asyncio.worker_context import EventWrapper, WorkerContext
from hypercorn.config import Config
from hypercorn.events import Closed, RawData
from hypercorn.protocol.events import Body, EndBody, Response
from hypercorn.protocol.h2 import BUFFER_HIGH_WATER, BufferCompleteError, H2Protocol, StreamBuffer
from hypercorn.typing import ConnectionState


@pytest.mark.asyncio
async def test_zero_window_cannot_release_backpressured_producer() -> None:
    buffer = StreamBuffer(EventWrapper)
    producer = asyncio.create_task(buffer.push(b"a" * BUFFER_HIGH_WATER))
    await asyncio.sleep(0)
    assert not producer.done()

    # H2Protocol._send_data calls pop(0) when the HTTP/2 send window is zero.
    for _ in range(5):
        assert await buffer.pop(0) == b""
        await asyncio.sleep(0)
    assert not producer.done()
    assert len(buffer.buffer) == BUFFER_HIGH_WATER

    await buffer.pop(BUFFER_HIGH_WATER)
    await asyncio.wait_for(producer, 1)


@pytest.mark.asyncio
async def test_closed_buffer_wakes_producer_and_drops_pending_write() -> None:
    buffer = StreamBuffer(EventWrapper)
    producer = asyncio.create_task(buffer.push(b"a" * BUFFER_HIGH_WATER))
    await asyncio.sleep(0)
    await buffer.close()
    # An already pending send may complete as a no-op or raise after close.
    try:
        await asyncio.wait_for(producer, 1)
    except BufferCompleteError:
        pass
    assert not buffer.buffer


async def protocol_with_requests(*ids: int) -> tuple[H2Protocol, H2Connection]:
    group = Mock()
    group.spawn_app = AsyncMock(return_value=AsyncMock())
    protocol = H2Protocol(
        Mock(),
        Config(),
        WorkerContext(None),
        group,
        ConnectionState({}),
        False,
        None,
        None,
        AsyncMock(),
    )
    await protocol.initiate()
    client = H2Connection()
    client.initiate_connection()
    for stream_id in ids:
        client.send_headers(
            stream_id,
            [
                (":method", "GET"),
                (":path", "/"),
                (":authority", "example.test"),
                (":scheme", "http"),
            ],
            end_stream=True,
        )
    await protocol.handle(RawData(client.data_to_send()))
    for stream_id in ids:
        await protocol.stream_send(Response(stream_id=stream_id, status_code=200, headers=[]))
    return protocol, client


@pytest.mark.asyncio
async def test_reset_retires_buffer_and_wakes_pending_end_without_touching_sibling() -> None:
    protocol, client = await protocol_with_requests(1, 3)
    await protocol.stream_send(Body(stream_id=1, data=b"a" * 123))
    pending_end = asyncio.create_task(protocol.stream_send(EndBody(stream_id=1)))
    await asyncio.sleep(0)
    assert not pending_end.done()

    client.reset_stream(1)
    await protocol.handle(RawData(client.data_to_send()))
    await asyncio.wait_for(pending_end, 1)
    assert 1 not in protocol.stream_buffers
    assert 3 in protocol.stream_buffers

    await protocol.stream_send(Body(stream_id=3, data=b"sibling"))
    await protocol._send_data(3)
    assert protocol.stream_buffers[3].buffer == bytearray()


@pytest.mark.asyncio
async def test_connection_close_retires_all_buffers_and_wakes_pending_end() -> None:
    protocol, _ = await protocol_with_requests(1)
    await protocol.stream_send(Body(stream_id=1, data=b"a" * 123))
    pending_end = asyncio.create_task(protocol.stream_send(EndBody(stream_id=1)))
    await asyncio.sleep(0)
    assert not pending_end.done()

    await protocol.handle(Closed())
    await asyncio.wait_for(pending_end, 1)
    assert not protocol.stream_buffers
    assert not protocol.streams


@pytest.mark.asyncio
async def test_one_large_write_never_exceeds_high_water() -> None:
    buffer = StreamBuffer(EventWrapper)
    body = b"z" * (BUFFER_HIGH_WATER * 5 + 17)
    producer = asyncio.create_task(buffer.push(body))
    await asyncio.sleep(0)
    assert len(buffer.buffer) <= BUFFER_HIGH_WATER

    received = bytearray()
    for _ in range(20):
        received.extend(await buffer.pop(BUFFER_HIGH_WATER))
        await asyncio.sleep(0)
        assert len(buffer.buffer) <= BUFFER_HIGH_WATER
        if producer.done() and not buffer.buffer:
            break
    await asyncio.wait_for(producer, 1)
    assert received == body


@pytest.mark.asyncio
async def test_zero_peer_window_blocks_production_until_credit_returns() -> None:
    from h2.settings import SettingCodes

    protocol, client = await protocol_with_requests(1)
    # The client reduces the response stream's window to zero.
    client.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 0})
    await protocol.handle(RawData(client.data_to_send()))
    producer = asyncio.create_task(
        protocol.stream_send(Body(stream_id=1, data=b"x" * BUFFER_HIGH_WATER))
    )
    await asyncio.sleep(0)
    assert not producer.done()

    for _ in range(3):
        await protocol._send_data(1)
        await asyncio.sleep(0)
    assert not producer.done()

    client.increment_flow_control_window(BUFFER_HIGH_WATER, stream_id=1)
    await protocol.handle(RawData(client.data_to_send()))
    for _ in range(3):
        await protocol._send_data(1)
    await asyncio.wait_for(producer, 1)


@pytest.mark.asyncio
async def test_reset_wakes_pending_body_producer() -> None:
    protocol, client = await protocol_with_requests(1, 3)
    body = asyncio.create_task(
        protocol.stream_send(Body(stream_id=1, data=b"x" * (BUFFER_HIGH_WATER * 2)))
    )
    await asyncio.sleep(0)
    assert not body.done()
    client.reset_stream(1)
    await protocol.handle(RawData(client.data_to_send()))
    await asyncio.wait_for(body, 1)
    assert 1 not in protocol.stream_buffers
    assert 3 in protocol.stream_buffers


@pytest.mark.trio
async def test_trio_zero_window_and_disconnect() -> None:
    import trio

    from hypercorn.trio.worker_context import EventWrapper as TrioEventWrapper

    buffer = StreamBuffer(TrioEventWrapper)
    finished = trio.Event()

    async def producer() -> None:
        try:
            await buffer.push(b"p" * BUFFER_HIGH_WATER)
        except BufferCompleteError:
            pass
        finally:
            finished.set()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(producer)
        await trio.lowlevel.checkpoint()
        assert not finished.is_set()
        assert await buffer.pop(0) == b""
        await trio.lowlevel.checkpoint()
        assert not finished.is_set()
        await buffer.close()
        with trio.fail_after(1):
            await finished.wait()


@pytest.mark.asyncio
async def test_peer_goaway_retires_buffer_and_wakes_pending_sender() -> None:
    protocol, client = await protocol_with_requests(1, 3)
    await protocol.stream_send(Body(stream_id=1, data=b"a" * 100))
    pending_end = asyncio.create_task(protocol.stream_send(EndBody(stream_id=1)))
    await asyncio.sleep(0)
    assert not pending_end.done()

    client.close_connection()
    await protocol.handle(RawData(client.data_to_send()))
    await asyncio.wait_for(pending_end, 1)
    assert not protocol.stream_buffers
    assert not protocol.streams
    assert protocol.closed


@pytest.mark.asyncio
async def test_malformed_frame_retires_buffer_and_wakes_sender() -> None:
    protocol, _ = await protocol_with_requests(1)
    await protocol.stream_send(Body(stream_id=1, data=b"x" * 101))
    pending_end = asyncio.create_task(protocol.stream_send(EndBody(stream_id=1)))
    await asyncio.sleep(0)
    assert not pending_end.done()

    # DATA frame targeting stream ID zero is a connection protocol error.
    await protocol.handle(RawData(b"\x00" * 9))
    await asyncio.wait_for(pending_end, 1)
    assert protocol.closed
    assert not protocol.stream_buffers
    assert not protocol.streams

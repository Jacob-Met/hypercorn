"""HTTP/2 peer-abort regression exercised through a real ASGI server socket."""

from __future__ import annotations

import asyncio
import socket

import h2.config
import h2.connection
import h2.errors
import h2.events
import pytest

from hypercorn.asyncio import serve
from hypercorn.config import Config


@pytest.mark.asyncio
@pytest.mark.parametrize("terminate_with", ["reset", "abort"])
async def test_peer_disconnect_releases_flow_controlled_asgi_send(terminate_with: str) -> None:
    """A peer withholding WINDOW_UPDATE cannot consume unbounded app memory."""
    accepted = 0
    finished = asyncio.Event()
    stopping = asyncio.Event()

    async def app(scope, receive, send) -> None:
        nonlocal accepted
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return

        await send({"type": "http.response.start", "status": 200, "headers": []})
        if scope["path"] == "/second":
            await send({"type": "http.response.body", "body": b"ok"})
            return
        for _ in range(256):
            await send({"type": "http.response.body", "body": b"x" * 16384, "more_body": True})
            accepted += 1
        await send({"type": "http.response.body", "body": b"", "more_body": False})
        finished.set()

    with socket.socket() as port_socket:
        port_socket.bind(("127.0.0.1", 0))
        port = port_socket.getsockname()[1]

    config = Config()
    config.bind = [f"127.0.0.1:{port}"]
    config.graceful_timeout = 2
    server = asyncio.create_task(serve(app, config, shutdown_trigger=stopping.wait))
    writer = None
    try:
        await asyncio.sleep(0.2)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        client = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
        client.initiate_connection()
        client.send_headers(
            1,
            [
                (b":method", b"GET"),
                (b":path", b"/"),
                (b":scheme", b"http"),
                (b":authority", b"localhost"),
            ],
            end_stream=True,
        )
        writer.write(client.data_to_send())
        await writer.drain()

        await asyncio.sleep(0.2)
        assert 0 < accepted < 16, f"ASGI send accepted {accepted} chunks despite zero stream credit"
        assert not finished.is_set(), "App must be blocked until peer disconnect"

        if terminate_with == "reset":
            client.reset_stream(1, error_code=h2.errors.ErrorCodes.CANCEL)
            client.send_headers(
                3,
                [
                    (b":method", b"GET"),
                    (b":path", b"/second"),
                    (b":scheme", b"http"),
                    (b":authority", b"localhost"),
                ],
                end_stream=True,
            )
            writer.write(client.data_to_send())
            await writer.drain()
            saw_second_response = False
            for _ in range(10):
                received = await asyncio.wait_for(reader.read(65536), timeout=1)
                assert received, "HTTP/2 connection closed before its next request"
                for event in client.receive_data(received):
                    if isinstance(event, h2.events.ResponseReceived) and event.stream_id == 3:
                        saw_second_response = True
                        assert dict(event.headers)[b":status"] == b"200"
                    if isinstance(event, h2.events.DataReceived):
                        client.acknowledge_received_data(
                            event.flow_controlled_length, event.stream_id
                        )
                if saw_second_response:
                    break
                writer.write(client.data_to_send())
                await writer.drain()
            assert saw_second_response, "Unrelated stream was blocked by reset response"
        else:
            writer.transport.abort()

        await asyncio.wait_for(finished.wait(), timeout=2)
        assert accepted == 256
    finally:
        if writer is not None and not writer.is_closing():
            writer.close()
        stopping.set()
        await asyncio.wait_for(server, timeout=4)

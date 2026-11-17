"""HTTP/2 early-response clients must not block other multiplexed requests."""

from __future__ import annotations

import asyncio
import socket
import ssl

import h2.config
import h2.connection
import h2.events
import pytest

from hypercorn.asyncio import serve
from hypercorn.config import Config


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["unread", "removed"])
@pytest.mark.parametrize("tls", [False, True], ids=["h2c", "h2-tls-alpn"])
async def test_early_response_keeps_other_h2_streams_alive(case: str, tls: bool) -> None:
    """Full unread request queues and late body frames are isolated per stream."""
    stopping = asyncio.Event()

    async def app(scope, receive, send) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        else:
            status = 413 if scope["path"] == "/early" else 200
            await send(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": [(b"content-length", b"2")],
                }
            )
            await send({"type": "http.response.body", "body": b"ok"})

    with socket.socket() as port_socket:
        port_socket.bind(("127.0.0.1", 0))
        port = port_socket.getsockname()[1]

    config = Config()
    config.bind = [f"127.0.0.1:{port}"]
    config.graceful_timeout = 1
    if tls:
        config.certfile = "tests/assets/cert.pem"
        config.keyfile = "tests/assets/key.pem"
    server = asyncio.create_task(serve(app, config, shutdown_trigger=stopping.wait))
    writer = None
    try:
        await asyncio.sleep(0.2)
        ssl_context = None
        if tls:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_context.set_alpn_protocols(["h2"])
        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=ssl_context)
        if tls:
            ssl_object = writer.get_extra_info("ssl_object")
            assert ssl_object.selected_alpn_protocol() == "h2"

        client = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
        client.initiate_connection()

        def request(stream: int, method: bytes, path: bytes, end: bool) -> None:
            client.send_headers(
                stream,
                [
                    (b":method", method),
                    (b":scheme", b"https" if tls else b"http"),
                    (b":authority", b"localhost"),
                    (b":path", path),
                ],
                end_stream=end,
            )

        request(1, b"POST", b"/early", False)
        if case == "unread":
            for _ in range(40):
                client.send_data(1, b"x" * 1024)
            request(3, b"GET", b"/next", True)
        else:
            client.send_data(1, b"x" * 1024)
        writer.write(client.data_to_send())
        await writer.drain()

        statuses: dict[int, bytes] = {}
        ended: set[int] = set()
        bodies: dict[int, bytearray] = {}
        late_sent = False
        deadline = asyncio.get_running_loop().time() + 2.5
        while 3 not in ended and asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                chunk = await asyncio.wait_for(reader.read(65536), max(0.01, remaining))
            except asyncio.TimeoutError:
                break
            assert chunk, "Connection unexpectedly closed after an early response"
            for event in client.receive_data(chunk):
                if isinstance(event, h2.events.ResponseReceived):
                    statuses[event.stream_id] = dict(event.headers)[b":status"]
                elif isinstance(event, h2.events.DataReceived):
                    bodies.setdefault(event.stream_id, bytearray()).extend(event.data)
                    client.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                elif isinstance(event, h2.events.StreamEnded):
                    ended.add(event.stream_id)
            if case == "removed" and 1 in ended and not late_sent:
                await asyncio.sleep(0.05)
                client.send_data(1, b"x" * 4096, end_stream=True)
                request(3, b"GET", b"/next", True)
                late_sent = True
            writer.write(client.data_to_send())
            await writer.drain()
        assert statuses.get(1) == b"413"
        assert statuses.get(3) == b"200", f"Next stream stalled after {case}: {statuses}"
        assert 3 in ended
        assert bodies.get(3) == b"ok"
    finally:
        if writer is not None:
            writer.transport.abort()
        stopping.set()
        try:
            await asyncio.wait_for(server, timeout=3)
        except asyncio.TimeoutError:
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

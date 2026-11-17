from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from hypercorn.app_wrappers import ASGIWrapper
from hypercorn.asyncio.task_group import TaskGroup
from hypercorn.config import Config
from hypercorn.typing import HTTPScope, Scope


@pytest.mark.asyncio
async def test_spawn_app(http_scope: HTTPScope) -> None:
    event_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    async def _echo_app(scope: Scope, receive: Callable, send: Callable) -> None:
        while True:
            message = await receive()
            if message is None:
                return
            await send(message)

    app_queue: asyncio.Queue = asyncio.Queue()
    async with TaskGroup(event_loop) as task_group:
        put = await task_group.spawn_app(
            ASGIWrapper(_echo_app), Config(), http_scope, app_queue.put
        )
        await put({"type": "http.disconnect"})
        assert (await app_queue.get()) == {"type": "http.disconnect"}
        await put(None)


@pytest.mark.asyncio
async def test_spawn_app_error(http_scope: HTTPScope) -> None:
    event_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    async def _error_app(scope: Scope, receive: Callable, send: Callable) -> None:
        raise Exception()

    app_queue: asyncio.Queue = asyncio.Queue()
    async with TaskGroup(event_loop) as task_group:
        await task_group.spawn_app(ASGIWrapper(_error_app), Config(), http_scope, app_queue.put)
    assert (await app_queue.get()) is None


@pytest.mark.asyncio
async def test_discard_pending_releases_blocked_app_queue(http_scope: HTTPScope) -> None:
    """A completed response must release a writer blocked by an unread body."""
    event_loop = asyncio.get_running_loop()
    started = asyncio.Event()
    may_receive = asyncio.Event()
    received = []

    async def app(scope: Scope, receive: Callable, send: Callable) -> None:
        started.set()
        await may_receive.wait()
        received.append(await receive())

    config = Config()
    config.max_app_queue_size = 1
    sink = asyncio.Queue()
    async with TaskGroup(event_loop) as task_group:
        put = await task_group.spawn_app(ASGIWrapper(app), config, http_scope, sink.put)
        await started.wait()
        await put({"type": "http.request", "body": b"one", "more_body": True})
        blocked = asyncio.create_task(
            put({"type": "http.request", "body": b"two", "more_body": True})
        )
        await asyncio.sleep(0)
        assert not blocked.done()
        try:
            await asyncio.wait_for(put({"type": "http.disconnect"}, discard_pending=True), 1)
            await asyncio.wait_for(blocked, 0.4)
        finally:
            may_receive.set()
            if not blocked.done():
                blocked.cancel()
                await asyncio.gather(blocked, return_exceptions=True)
    assert received == [{"type": "http.disconnect"}]

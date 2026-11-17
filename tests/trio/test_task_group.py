"""Trio application mailbox lifecycle across early response closure."""

from __future__ import annotations

from collections.abc import Callable

import pytest
import trio

from hypercorn.app_wrappers import ASGIWrapper
from hypercorn.config import Config
from hypercorn.trio.task_group import TaskGroup
from hypercorn.typing import HTTPScope, Scope


@pytest.mark.trio
async def test_discard_pending_releases_blocked_app_queue(http_scope: HTTPScope) -> None:
    started = trio.Event()
    may_receive = trio.Event()
    sender_done = trio.Event()
    received = []

    async def app(scope: Scope, receive: Callable, send: Callable) -> None:
        started.set()
        await may_receive.wait()
        received.append(await receive())

    async def sink(message) -> None:
        return

    config = Config()
    config.max_app_queue_size = 1
    async with TaskGroup() as task_group:
        put = await task_group.spawn_app(ASGIWrapper(app), config, http_scope, sink)
        await started.wait()
        await put({"type": "http.request", "body": b"one", "more_body": True})

        async def blocked_sender() -> None:
            await put({"type": "http.request", "body": b"two", "more_body": True})
            sender_done.set()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(blocked_sender)
            await trio.sleep(0)
            with trio.fail_after(0.5):
                await put({"type": "http.disconnect"}, discard_pending=True)
                await sender_done.wait()
                may_receive.set()
    assert received == [{"type": "http.disconnect"}]

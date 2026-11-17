from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Any

import trio

from ..config import Config
from ..typing import (
    AppQueuePut,
    AppWrapper,
    ASGIReceiveCallable,
    ASGIReceiveEvent,
    ASGISendEvent,
    Scope,
)

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


async def _handle(
    app: AppWrapper,
    config: Config,
    scope: Scope,
    receive: ASGIReceiveCallable,
    send: Callable[[ASGISendEvent | None], Awaitable[None]],
    sync_spawn: Callable,
    call_soon: Callable,
) -> None:
    try:
        await app(scope, receive, send, sync_spawn, call_soon)
    except trio.Cancelled:
        raise
    except BaseExceptionGroup as error:
        _, other_errors = error.split(trio.Cancelled)
        if other_errors is not None:
            await config.log.exception("Error in ASGI Framework")
            await send(None)
        else:
            raise
    except Exception:
        await config.log.exception("Error in ASGI Framework")
    finally:
        await send(None)


class TaskGroup:
    def __init__(self) -> None:
        self._nursery: trio.Nursery | None = None
        self._nursery_manager: AbstractAsyncContextManager[trio.Nursery] | None = None

    async def spawn_app(
        self,
        app: AppWrapper,
        config: Config,
        scope: Scope,
        send: Callable[[ASGISendEvent | None], Awaitable[None]],
    ) -> AppQueuePut:
        app_send_channel, app_receive_channel = trio.open_memory_channel[ASGIReceiveEvent](
            config.max_app_queue_size
        )

        async def app_receive() -> ASGIReceiveEvent:
            try:
                return await app_receive_channel.receive()
            except trio.EndOfChannel:
                return {"type": "http.disconnect"}

        async def app_put(message: ASGIReceiveEvent, *, discard_pending: bool = False) -> None:
            if discard_pending:
                assert message["type"] == "http.disconnect"
                # Close without waiting on a bounded, unread request queue.
                # The receiver observes EOF as http.disconnect; blocked
                # producers are released even when the application is done.
                while True:
                    try:
                        app_receive_channel.receive_nowait()
                    except trio.WouldBlock:
                        break
                app_send_channel.close()
            else:
                try:
                    await app_send_channel.send(message)
                except trio.ClosedResourceError:
                    pass  # A completed response retired this mailbox.

        self._nursery.start_soon(
            _handle,
            app,
            config,
            scope,
            app_receive,
            send,
            trio.to_thread.run_sync,
            trio.from_thread.run,
        )
        return app_put

    def spawn(self, func: Callable, *args: Any) -> None:
        self._nursery.start_soon(func, *args)

    async def __aenter__(self) -> TaskGroup:
        self._nursery_manager = trio.open_nursery()
        self._nursery = await self._nursery_manager.__aenter__()
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self._nursery_manager.__aexit__(exc_type, exc_value, tb)
        self._nursery_manager = None
        self._nursery = None

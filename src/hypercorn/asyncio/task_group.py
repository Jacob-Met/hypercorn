from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from types import TracebackType
from typing import Any

from ..config import Config
from ..typing import (
    AppQueuePut,
    AppWrapper,
    ASGIReceiveCallable,
    ASGIReceiveEvent,
    ASGISendEvent,
    Scope,
)

try:
    from asyncio import TaskGroup as AsyncioTaskGroup
except ImportError:
    from taskgroup import TaskGroup as AsyncioTaskGroup  # type: ignore


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
    except asyncio.CancelledError:
        raise
    except Exception:
        await config.log.exception("Error in ASGI Framework")
    finally:
        await send(None)


class TaskGroup:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._task_group = AsyncioTaskGroup()

    async def spawn_app(
        self,
        app: AppWrapper,
        config: Config,
        scope: Scope,
        send: Callable[[ASGISendEvent | None], Awaitable[None]],
    ) -> AppQueuePut:
        app_queue: asyncio.Queue[ASGIReceiveEvent] = asyncio.Queue(config.max_app_queue_size)
        disconnected = False
        mailbox_closed = asyncio.Event()

        async def app_receive() -> ASGIReceiveEvent:
            message = await app_queue.get()
            if disconnected and message is not None and message["type"] == "http.request":
                return {"type": "http.disconnect"}
            return message

        async def app_put(message: ASGIReceiveEvent, *, discard_pending: bool = False) -> None:
            nonlocal disconnected
            if discard_pending:
                assert message["type"] == "http.disconnect"
                # A completed early response may leave an unread, full
                # request mailbox. The disconnect must never wait for an
                # application which is no longer receiving request data.
                disconnected = True
                mailbox_closed.set()
                while not app_queue.empty():
                    app_queue.get_nowait()
                app_queue.put_nowait(message)
            elif not disconnected:
                if not app_queue.full():
                    app_queue.put_nowait(message)
                else:
                    # A full request mailbox normally applies backpressure,
                    # but must be cancellable when the application finishes
                    # its response without draining the request body.
                    writer = asyncio.create_task(app_queue.put(message))
                    closing = asyncio.create_task(mailbox_closed.wait())
                    try:
                        done, _ = await asyncio.wait(
                            {writer, closing}, return_when=asyncio.FIRST_COMPLETED
                        )
                        if closing in done:
                            writer.cancel()
                        else:
                            await writer
                    finally:
                        writer.cancel()
                        closing.cancel()
                        await asyncio.gather(writer, closing, return_exceptions=True)

        def _call_soon(func: Callable, *args: Any) -> Any:
            future = asyncio.run_coroutine_threadsafe(func(*args), self._loop)
            return future.result()

        self.spawn(
            _handle,
            app,
            config,
            scope,
            app_receive,
            send,
            partial(self._loop.run_in_executor, None),
            _call_soon,
        )
        return app_put

    def spawn(self, func: Callable, *args: Any) -> None:
        self._task_group.create_task(func(*args))

    async def __aenter__(self) -> TaskGroup:
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self._task_group.__aexit__(exc_type, exc_value, tb)

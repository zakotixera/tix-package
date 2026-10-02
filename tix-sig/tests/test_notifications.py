"""
Cancellation fan-out with one notification per event loop.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from sig import CancellationToken


async def test_waiters_share_notification_and_cancelled_futures_are_skipped() -> None:
    """
    A large waiter group wakes together even if one task cancels before delivery.
    """
    token: CancellationToken = CancellationToken()
    tasks: list[asyncio.Task[str]] = [asyncio.create_task(token.wait_async()) for _ in range(128)]
    await asyncio.sleep(0)
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    scheduled: MagicMock
    with patch.object(loop, "call_soon_threadsafe", wraps=loop.call_soon_threadsafe) as scheduled:
        assert token.cancel("stopped")
        assert scheduled.call_count == 1
        tasks[0].cancel()
        results: list[str | BaseException] = await asyncio.gather(*tasks, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    assert results[1:] == ["stopped"] * 127
    assert not token._waiters
    assert not token.cancel("later")


async def test_already_cancelled_wait_avoids_future_allocation() -> None:
    """
    An already-cancelled token returns its cause without creating waiter state.
    """
    token: CancellationToken = CancellationToken()
    token.cancel("already stopped")
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    created: MagicMock
    with patch.object(loop, "create_future", wraps=loop.create_future) as created:
        assert await token.wait_async(timeout=0) == "already stopped"
        created.assert_not_called()

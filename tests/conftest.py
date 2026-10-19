import asyncio
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from typing import Any

import httpx

from app.api import Services
from app.config import Settings
from app.domain import ClaimedEvent, EventProcessor
from app.main import create_app

TERMINAL = {"completed", "dead"}


def make_settings(tmp_path: Any, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "db_path": str(tmp_path / "test.db"),
        "processing_min": 0.01,
        "processing_max": 0.02,
        "lease_seconds": 1.0,
        "poll_interval": 0.02,
        "recovery_interval": 0.1,
        "stats_interval": 0.05,
        "backoff_base": 0.01,
        "backoff_max": 0.05,
        "shutdown_grace": 0.5,
        "log_level": "WARNING",
    }
    values.update(overrides)
    return Settings(**values)


class CountingProcessor:
    """Test double for ``EventProcessor`` that records every invocation."""

    def __init__(self, delay: float = 0.01, fail: bool = False) -> None:
        self.calls: Counter[str] = Counter()
        self.delay = delay
        self.fail = fail
        self.running = 0
        self.max_running = 0

    async def __call__(self, event: ClaimedEvent) -> dict[str, Any]:
        self.calls[event.idempotency_key] += 1
        self.running += 1
        self.max_running = max(self.max_running, self.running)
        try:
            await asyncio.sleep(self.delay)
        finally:
            self.running -= 1
        if self.fail:
            raise RuntimeError("boom")
        return {"ok": True}


class Clock:
    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@asynccontextmanager
async def live_app(
    settings: Settings,
    processor: EventProcessor | None = None,
    clock: Callable[[], float] = time.time,
) -> AsyncIterator[tuple[httpx.AsyncClient, Services]]:
    app = create_app(settings, processor=processor, clock=clock)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app.state.services


@asynccontextmanager
async def running_app(
    settings: Settings,
    processor: EventProcessor | None = None,
    clock: Callable[[], float] = time.time,
) -> AsyncIterator[httpx.AsyncClient]:
    async with live_app(settings, processor, clock) as (client, _):
        yield client


def webhook(key: str, **payload: Any) -> dict[str, Any]:
    return {
        "idempotency_key": key,
        "event_type": "order.created",
        "payload": payload or {"order_id": "ORD-1", "amount": 299.99},
    }


async def wait_terminal(
    client: httpx.AsyncClient, keys: Iterable[str], limit: float = 10.0
) -> dict[str, dict[str, Any]]:
    pending = set(keys)
    seen: dict[str, dict[str, Any]] = {}
    deadline = time.monotonic() + limit
    while pending:
        if time.monotonic() > deadline:
            raise AssertionError(f"not terminal in time: {sorted(pending)}")
        for key in list(pending):
            body = (await client.get(f"/webhooks/{key}")).json()
            if body.get("status") in TERMINAL:
                seen[key] = body
                pending.discard(key)
        await asyncio.sleep(0.02)
    return seen

import asyncio
from collections.abc import Mapping
from random import Random
from typing import Any

from app.domain import ClaimedEvent

POISON_EVENT_TYPE = "test.poison"


class SimulatedProcessor:
    """Stand-in for business logic: sleep 2-5 s and return a result.

    ``test.poison`` always fails and ``failure_rate`` fails at random; both
    exist to demonstrate retries and dead-lettering.
    """

    def __init__(
        self,
        min_seconds: float,
        max_seconds: float,
        failure_rate: float = 0.0,
        rng: Random | None = None,
    ) -> None:
        self._min = min_seconds
        self._max = max_seconds
        self._failure_rate = failure_rate
        self._rng = rng or Random()  # nosec B311

    async def __call__(self, event: ClaimedEvent) -> Mapping[str, Any]:
        if event.event_type == POISON_EVENT_TYPE:
            raise RuntimeError("poison event always fails")
        duration = self._rng.uniform(self._min, self._max)
        await asyncio.sleep(duration)
        if self._rng.random() < self._failure_rate:
            raise RuntimeError("simulated processing failure")
        return {"processed": True, "duration_ms": round(duration * 1000)}

"""Dispatcher: one loop claims work, up to N tasks process it.

A single claimer instead of N polling workers means one ``UPDATE`` per
wake-up (no thundering herd on the writer) and an exact bound on
concurrency: the loop never claims more than the number of free slots.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from random import Random

from app.domain import (
    ClaimedEvent,
    EventProcessor,
    RetryPolicy,
    after_failure,
)
from app.log_config import bind, log, unbind
from app.metrics import Metrics
from app.repository import DatabaseUnavailable, Repository

logger = logging.getLogger("app.worker")


class Dispatcher:
    def __init__(
        self,
        *,
        repo: Repository,
        processor: EventProcessor,
        metrics: Metrics,
        concurrency: int,
        lease_seconds: float,
        processing_timeout: float,
        poll_interval: float,
        retry_policy: RetryPolicy,
        worker_id: str,
        clock: Callable[[], float] = time.time,
        rng: Random | None = None,
    ) -> None:
        self._repo = repo
        self._processor = processor
        self._metrics = metrics
        self._concurrency = concurrency
        self._lease_seconds = lease_seconds
        self._timeout = processing_timeout
        self._poll_interval = poll_interval
        self._retry_policy = retry_policy
        self._worker_id = worker_id
        self._clock = clock
        self._rng = rng or Random()  # nosec B311
        self._wake = asyncio.Event()
        self._inflight: set[asyncio.Task[None]] = set()
        self._loop_task: asyncio.Task[None] | None = None
        self._stopping = False

    @property
    def alive(self) -> bool:
        return self._loop_task is not None and not self._loop_task.done()

    @property
    def busy(self) -> int:
        return len(self._inflight)

    def notify(self) -> None:
        """Wake the claim loop now instead of at the next poll."""
        self._wake.set()

    def start(self) -> None:
        self._loop_task = asyncio.create_task(self._run(), name="dispatcher")

    async def stop(self, grace: float) -> None:
        """Stop claiming, give in-flight work one shared deadline, release.

        The deadline is computed once: waiting ``grace`` per task would let
        64 tasks stretch shutdown to 64 x ``grace``.
        """
        self._stopping = True
        self._wake.set()
        if self._loop_task is not None:
            await self._loop_task
        if not self._inflight:
            return
        _, pending = await asyncio.wait(set(self._inflight), timeout=grace)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stopping:
            # Clear before claiming so a notify() during the claim is kept.
            self._wake.clear()
            free = self._concurrency - len(self._inflight)
            if free > 0:
                try:
                    events = await self._repo.claim(
                        free, self._lease_seconds, self._worker_id
                    )
                except DatabaseUnavailable as exc:
                    log(
                        logger,
                        logging.WARNING,
                        "claim.failed",
                        error=str(exc),
                    )
                    events = []
                for event in events:
                    self._spawn(event)
            try:
                async with asyncio.timeout(self._poll_interval):
                    await self._wake.wait()
            except TimeoutError:
                pass

    def _spawn(self, event: ClaimedEvent) -> None:
        task = asyncio.create_task(self._handle(event))
        self._inflight.add(task)
        self._metrics.workers_busy.set(len(self._inflight))
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        self._inflight.discard(task)
        self._metrics.workers_busy.set(len(self._inflight))
        self._wake.set()

    async def _handle(self, event: ClaimedEvent) -> None:
        token = bind(
            idempotency_key=event.idempotency_key,
            event_type=event.event_type,
            attempt=event.attempts,
            worker_id=self._worker_id,
        )
        log(logger, logging.INFO, "webhook.claimed")
        started = time.monotonic()
        try:
            async with asyncio.timeout(self._timeout):
                result = await self._processor(event)
        except asyncio.CancelledError:
            await asyncio.shield(self._release(event))
            raise
        except Exception as exc:
            elapsed = time.monotonic() - started
            await self._record_failure(event, exc, elapsed)
        else:
            elapsed = time.monotonic() - started
            await self._record_success(event, result, elapsed)
        finally:
            unbind(token)

    async def _record_success(
        self, event: ClaimedEvent, result: dict, duration: float
    ) -> None:
        try:
            committed = await self._repo.complete(event, result)
        except DatabaseUnavailable as exc:
            # The lease expires and recovery re-queues the event.
            log(logger, logging.ERROR, "commit.failed", error=str(exc))
            return
        if not committed:
            self._stale()
            return
        self._metrics.processed.labels(event.event_type, "completed").inc()
        self._metrics.processing_seconds.labels(event.event_type).observe(
            duration
        )
        self._metrics.end_to_end_seconds.observe(
            self._clock() - event.received_at
        )
        log(
            logger,
            logging.INFO,
            "webhook.completed",
            duration_ms=round(duration * 1000),
        )

    async def _record_failure(
        self, event: ClaimedEvent, exc: BaseException, duration: float
    ) -> None:
        error = f"{type(exc).__name__}: {exc}" if str(exc) else repr(exc)
        transition = after_failure(
            event.attempts,
            event.max_attempts,
            self._clock(),
            self._retry_policy,
            self._rng,
        )
        try:
            committed = await self._repo.fail(event, transition, error)
        except DatabaseUnavailable as db_exc:
            log(logger, logging.ERROR, "commit.failed", error=str(db_exc))
            return
        if not committed:
            self._stale()
            return
        result = transition.status.value
        self._metrics.processed.labels(event.event_type, result).inc()
        self._metrics.processing_seconds.labels(event.event_type).observe(
            duration
        )
        level = logging.ERROR if result == "dead" else logging.WARNING
        log(
            logger,
            level,
            f"webhook.{result}",
            error=error,
            next_attempt_at=transition.next_attempt_at,
        )

    async def _release(self, event: ClaimedEvent) -> None:
        try:
            await self._repo.release(event)
            log(logger, logging.INFO, "webhook.released")
        except DatabaseUnavailable as exc:
            log(logger, logging.WARNING, "release.failed", error=str(exc))

    def _stale(self) -> None:
        self._metrics.lease_stale.inc()
        log(logger, logging.WARNING, "lease.stale")

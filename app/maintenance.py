"""Background upkeep: lease recovery, queue statistics, retention."""

import asyncio
import logging
import os
import shutil
import time
from collections.abc import Awaitable, Callable

from app.domain import Status, after_lease_expiry
from app.log_config import log
from app.metrics import Metrics
from app.repository import DatabaseUnavailable, Repository

logger = logging.getLogger("app.maintenance")

_DAY = 86_400.0


class Maintenance:
    def __init__(
        self,
        *,
        repo: Repository,
        metrics: Metrics,
        db_path: str,
        recovery_interval: float,
        stats_interval: float,
        retention_interval: float,
        retention_completed_days: float,
        retention_dead_days: float,
        workers_alive: Callable[[], bool],
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._repo = repo
        self._metrics = metrics
        self._db_dir = os.path.dirname(os.path.abspath(db_path))
        self._recovery_interval = recovery_interval
        self._stats_interval = stats_interval
        self._retention_interval = retention_interval
        self._completed_ttl = retention_completed_days * _DAY
        self._dead_ttl = retention_dead_days * _DAY
        self._workers_alive = workers_alive
        self._clock = clock
        self._tasks: list[asyncio.Task[None]] = []
        self.queue_depth = 0

    @property
    def alive(self) -> bool:
        return bool(self._tasks) and not any(t.done() for t in self._tasks)

    def start(self) -> None:
        loops = (
            (self.recover_leases, self._recovery_interval),
            (self.refresh_stats, self._stats_interval),
            (self.purge, self._retention_interval),
        )
        self._tasks = [
            asyncio.create_task(self._every(job, interval))
            for job, interval in loops
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def _every(
        self, job: Callable[[], Awaitable[object]], interval: float
    ) -> None:
        while True:
            try:
                await job()
            except DatabaseUnavailable as exc:
                log(
                    logger,
                    logging.WARNING,
                    "maintenance.failed",
                    error=str(exc),
                )
            await asyncio.sleep(interval)

    async def recover_leases(self, *, startup: bool = False) -> int:
        """Re-queue (or bury) events whose worker died or stalled."""
        leases = await self._repo.stale_leases(expired_only=not startup)
        reason = "startup" if startup else "expired"
        recovered = 0
        for lease in leases:
            transition = after_lease_expiry(
                lease.attempts, lease.max_attempts, self._clock()
            )
            if await self._repo.recover(lease, transition):
                recovered += 1
                self._metrics.lease_recovered.labels(reason).inc()
                log(
                    logger,
                    logging.WARNING,
                    "lease.recovered",
                    idempotency_key=lease.idempotency_key,
                    reason=reason,
                    status=transition.status.value,
                    attempt=lease.attempts,
                )
        return recovered

    async def refresh_stats(self) -> None:
        stats = await self._repo.queue_stats()
        self.queue_depth = stats.depth
        for status in (
            Status.PENDING,
            Status.RETRY_SCHEDULED,
            Status.PROCESSING,
        ):
            self._metrics.queue_depth.labels(status.value).set(
                stats.by_status.get(status, 0)
            )
        lag = 0.0
        if stats.oldest_ready_at is not None:
            lag = max(0.0, self._clock() - stats.oldest_ready_at)
        self._metrics.dispatch_lag.set(lag)
        self._metrics.workers_alive.set(1 if self._workers_alive() else 0)
        usage = shutil.disk_usage(self._db_dir)
        self._metrics.db_disk_free_ratio.set(usage.free / usage.total)

    async def purge(self) -> int:
        now = self._clock()
        total = 0
        for status, ttl in (
            (Status.COMPLETED, self._completed_ttl),
            (Status.DEAD, self._dead_ttl),
        ):
            deleted = await self._repo.purge(status, now - ttl)
            if deleted:
                self._metrics.retention_purged.labels(status.value).inc(
                    deleted
                )
                log(
                    logger,
                    logging.INFO,
                    "retention.purged",
                    status=status.value,
                    deleted=deleted,
                )
            total += deleted
        deleted_keys = await self._repo.purge_expired_keys(now)
        if deleted_keys:
            self._metrics.retention_purged.labels("idempotency_key").inc(
                deleted_keys
            )
            log(
                logger,
                logging.INFO,
                "retention.purged",
                status="idempotency_key",
                deleted=deleted_keys,
            )
            total += deleted_keys
        return total

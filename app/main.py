"""Composition root: the only module that knows every other module."""

import logging
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app import log_config
from app.api import Services, error, router, validation_error_payload
from app.config import Settings, parse_csv, parse_secrets
from app.domain import EventProcessor, RetryPolicy
from app.log_config import log
from app.maintenance import Maintenance
from app.metrics import Metrics
from app.processor import SimulatedProcessor
from app.ratelimit import TokenBucketLimiter
from app.repository import DatabaseUnavailable, Repository
from app.worker import Dispatcher

logger = logging.getLogger("app.main")


def create_app(
    settings: Settings | None = None,
    processor: EventProcessor | None = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    settings = settings or Settings()
    secrets = parse_secrets(settings.webhook_secrets)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log_config.configure(settings.log_level)
        if not secrets:
            log(
                logger,
                logging.WARNING,
                "auth.disabled",
                hint="set WEBHOOK_SECRETS to require HMAC signatures",
            )
        repo = Repository(
            settings.db_path,
            clock=clock,
            idempotency_ttl_seconds=(
                settings.retention_idempotency_days * 86_400
            ),
        )
        await repo.open()
        metrics = Metrics()
        dispatcher = Dispatcher(
            repo=repo,
            processor=processor
            or SimulatedProcessor(
                settings.processing_min,
                settings.processing_max,
                settings.failure_rate,
            ),
            metrics=metrics,
            concurrency=settings.worker_concurrency,
            lease_seconds=settings.lease_seconds,
            processing_timeout=settings.processing_timeout,
            poll_interval=settings.poll_interval,
            retry_policy=RetryPolicy(
                settings.backoff_base, settings.backoff_max
            ),
            worker_id=f"worker-{uuid.uuid4().hex[:8]}",
            clock=clock,
        )
        maintenance = Maintenance(
            repo=repo,
            metrics=metrics,
            db_path=settings.db_path,
            recovery_interval=settings.recovery_interval,
            stats_interval=settings.stats_interval,
            retention_interval=settings.retention_interval,
            retention_completed_days=settings.retention_completed_days,
            retention_dead_days=settings.retention_dead_days,
            workers_alive=lambda: dispatcher.alive,
            clock=clock,
        )
        # Before any worker runs, every 'processing' row is an orphan of
        # the previous process: requeue it now, not after the lease.
        recovered = await maintenance.recover_leases(startup=True)
        await maintenance.refresh_stats()
        services = Services(
            settings=settings,
            repo=repo,
            dispatcher=dispatcher,
            maintenance=maintenance,
            limiter=TokenBucketLimiter(
                settings.rate_limit_rps, settings.rate_limit_burst
            ),
            metrics=metrics,
            secrets=secrets,
            source_allowlist=parse_csv(settings.allowed_sources),
            clock=clock,
        )
        app.state.services = services
        dispatcher.start()
        maintenance.start()
        log(
            logger,
            logging.INFO,
            "service.started",
            recovered=recovered,
            queue_depth=maintenance.queue_depth,
            concurrency=settings.worker_concurrency,
        )
        try:
            yield
        finally:
            services.begin_shutdown()
            await dispatcher.stop(settings.shutdown_grace)
            await maintenance.stop()
            await repo.close()
            log(logger, logging.INFO, "service.stopped")

    app = FastAPI(title="Webhook Receiver", version="0.1.0", lifespan=lifespan)
    app.include_router(router)

    @app.exception_handler(RequestValidationError)
    async def _invalid(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            validation_error_payload(list(exc.errors())), status_code=422
        )

    @app.exception_handler(DatabaseUnavailable)
    async def _db_down(_: Request, exc: DatabaseUnavailable) -> JSONResponse:
        # Any route the database fails under answers 503, never 500.
        log(logger, logging.ERROR, "db.unavailable", error=str(exc))
        return error(503, "db_unavailable", "database unavailable", 1)

    return app

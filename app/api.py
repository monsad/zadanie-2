"""HTTP adapter: maps requests to repository calls and back."""

import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import APIRouter, Header, Path, Query, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import ValidationError

from app.auth import AuthError, same_secret, verify
from app.config import Settings
from app.domain import Status, canonical_hash, canonical_json
from app.log_config import log
from app.maintenance import Maintenance
from app.metrics import Metrics
from app.ratelimit import TokenBucketLimiter
from app.repository import DatabaseUnavailable, Repository
from app.schemas import KEY_PATTERN, WebhookIn, status_view
from app.worker import Dispatcher

logger = logging.getLogger("app.api")

OTHER_SOURCE = "other"
UNKNOWN_SOURCE = "unknown"


@dataclass(slots=True, kw_only=True)
class Services:
    """Everything a request handler needs, built once in ``main``."""

    settings: Settings
    repo: Repository
    dispatcher: Dispatcher
    maintenance: Maintenance
    limiter: TokenBucketLimiter
    metrics: Metrics
    secrets: dict[str, bytes]
    source_allowlist: frozenset[str]
    clock: Callable[[], float] = time.time
    shutting_down: bool = False

    def begin_shutdown(self) -> None:
        self.shutting_down = True

    @property
    def auth_enabled(self) -> bool:
        return bool(self.secrets)

    def source_label(self, raw: str | None) -> str:
        """Bounded label for an untrusted ``X-Source``.

        Unknown values collapse into one bucket, so rotating the header
        neither bypasses the rate limit nor grows metric cardinality.
        """
        if not raw:
            return UNKNOWN_SOURCE
        if raw in self.source_allowlist or raw in self.secrets:
            return raw
        return OTHER_SOURCE


router = APIRouter()

KeyPath = Annotated[str, Path(pattern=KEY_PATTERN)]


class _BodyTooLargeError(Exception):
    pass


def _services(request: Request) -> Services:
    return request.app.state.services


def error(
    status_code: int,
    code: str,
    message: str,
    retry_after: float | None = None,
) -> JSONResponse:
    headers = {}
    if retry_after is not None:
        headers["Retry-After"] = str(max(1, math.ceil(retry_after)))
    return JSONResponse(
        {"error": {"code": code, "message": message}},
        status_code=status_code,
        headers=headers,
    )


def validation_error_payload(errors: list[Any]) -> dict[str, Any]:
    first = errors[0] if errors else {"loc": (), "msg": "invalid request"}
    where = ".".join(str(part) for part in first["loc"]) or "request"
    message = f"{where}: {first['msg']}"
    return {"error": {"code": "invalid_request", "message": message}}


async def _read_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise _BodyTooLargeError
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise _BodyTooLargeError
        chunks.append(chunk)
    return b"".join(chunks)


def _authenticate(svc: Services, request: Request, signed: bytes) -> str:
    """Return the request's source; verified when auth is enabled."""
    if not svc.auth_enabled:
        return svc.source_label(request.headers.get("x-source"))
    return verify(
        secrets=svc.secrets,
        source=request.headers.get("x-source"),
        timestamp=request.headers.get("x-timestamp"),
        signature=request.headers.get("x-signature"),
        body=signed,
        now=svc.clock(),
        tolerance=svc.settings.signature_tolerance,
    )


def _describe(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        return validation_error_payload(exc.errors())["error"]["message"]
    return str(exc)


@router.post("/webhooks")
async def receive_webhook(request: Request) -> Response:
    svc = _services(request)
    started = time.perf_counter()
    try:
        return await _receive(svc, request)
    finally:
        svc.metrics.ingest_seconds.observe(time.perf_counter() - started)


async def _receive(svc: Services, request: Request) -> Response:
    metrics = svc.metrics
    label = svc.source_label(request.headers.get("x-source"))
    if svc.shutting_down:
        return error(503, "shutting_down", "service is stopping", 5)

    try:
        body = await _read_body(request, svc.settings.max_body_bytes)
    except _BodyTooLargeError:
        metrics.received.labels(label, "invalid").inc()
        return error(413, "payload_too_large", "request body too large")

    try:
        source = _authenticate(svc, request, body)
    except AuthError as exc:
        metrics.received.labels(label, "unauthorized").inc()
        log(logger, logging.WARNING, "webhook.unauthorized", reason=str(exc))
        return error(401, "unauthorized", str(exc))

    wait = svc.limiter.acquire(source)
    if wait:
        metrics.received.labels(source, "rate_limited").inc()
        return error(429, "rate_limited", "too many requests", wait)

    try:
        webhook = WebhookIn.model_validate_json(body)
        payload_json = canonical_json(webhook.payload)
    except (ValidationError, ValueError) as exc:
        metrics.received.labels(source, "invalid").inc()
        return error(422, "invalid_webhook", _describe(exc))
    payload_hash = canonical_hash(webhook.event_type, webhook.payload)
    key = webhook.idempotency_key

    try:
        inserted = await svc.repo.insert(
            idempotency_key=key,
            source=source,
            event_type=webhook.event_type,
            payload_json=payload_json,
            payload_hash=payload_hash,
            max_attempts=svc.settings.max_attempts,
            max_active=svc.settings.max_queue_depth,
        )
        outcome, record = inserted.outcome, inserted.record
    except DatabaseUnavailable as exc:
        log(logger, logging.ERROR, "db.unavailable", error=str(exc))
        return error(503, "db_unavailable", "database unavailable", 1)

    metrics.received.labels(source, outcome).inc()
    if outcome == "rejected":
        # Only new work is refused; duplicates never reach the cap.
        log(logger, logging.WARNING, "webhook.rejected", key=key)
        return error(503, "queue_full", "queue is full", 5)
    if outcome == "accepted":
        event = "webhook.received"
    else:
        event = f"webhook.{outcome}"
    log(
        logger,
        logging.INFO,
        event,
        idempotency_key=key,
        source=source,
        event_type=webhook.event_type,
    )
    if outcome == "conflict":
        return error(
            409,
            "payload_conflict",
            "idempotency_key reused with a different payload",
        )
    if outcome == "accepted":
        svc.maintenance.queue_depth += 1
        svc.dispatcher.notify()
        return JSONResponse(
            status_view(record),
            status_code=202,
            headers={"Location": f"/webhooks/{key}"},
        )
    return JSONResponse(status_view(record), status_code=200)


@router.get("/webhooks/{key}")
async def get_status(request: Request, key: KeyPath) -> Response:
    svc = _services(request)
    try:
        source = _authenticate(svc, request, request.url.path.encode())
    except AuthError as exc:
        return error(401, "unauthorized", str(exc))
    try:
        record = await svc.repo.get(key)
    except DatabaseUnavailable:
        return error(503, "db_unavailable", "database unavailable", 1)
    # With auth on, other sources' events look exactly like missing ones.
    # Without auth X-Source is untrusted, so scoping by it would be theatre.
    if record is None or (svc.auth_enabled and record["source"] != source):
        return error(404, "not_found", "unknown idempotency_key")
    return JSONResponse(status_view(record))


def _admin_denied(svc: Services, token: str | None) -> JSONResponse | None:
    expected = svc.settings.admin_token
    if not expected or not token or not same_secret(expected, token):
        return error(401, "unauthorized", "admin token required")
    return None


@router.get("/webhooks")
async def list_webhooks(
    request: Request,
    status: Status | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    after: Annotated[int, Query(ge=0)] = 0,
    x_admin_token: Annotated[str | None, Header()] = None,
) -> Response:
    svc = _services(request)
    if denied := _admin_denied(svc, x_admin_token):
        return denied
    rows = await svc.repo.list_events(
        status=status, limit=limit, after_id=after
    )
    items = [{"id": row["id"], **status_view(row)} for row in rows]
    next_after = rows[-1]["id"] if len(rows) == limit else None
    return JSONResponse({"items": items, "next_after": next_after})


@router.post("/webhooks/{key}/replay")
async def replay_webhook(
    request: Request,
    key: KeyPath,
    x_admin_token: Annotated[str | None, Header()] = None,
) -> Response:
    svc = _services(request)
    if denied := _admin_denied(svc, x_admin_token):
        return denied
    record = await svc.repo.replay(key)
    if record is None:
        if await svc.repo.get_event(key) is None:
            return error(404, "not_found", "unknown idempotency_key")
        return error(409, "not_dead", "only dead events can be replayed")
    svc.dispatcher.notify()
    log(logger, logging.WARNING, "webhook.replayed", idempotency_key=key)
    return JSONResponse(status_view(record), status_code=202)


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process answers. Deliberately no database check."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> Response:
    svc = _services(request)
    checks: dict[str, Any] = {
        "db": "ok",
        "dispatcher": svc.dispatcher.alive,
        "maintenance": svc.maintenance.alive,
        "shutting_down": svc.shutting_down,
        "workers_busy": svc.dispatcher.busy,
        "queue_depth": svc.maintenance.queue_depth,
    }
    try:
        await svc.repo.ping()
    except DatabaseUnavailable as exc:
        checks["db"] = str(exc)
    healthy = (
        checks["db"] == "ok"
        and checks["dispatcher"]
        and checks["maintenance"]
        and not svc.shutting_down
    )
    return JSONResponse(checks, status_code=200 if healthy else 503)


@router.get("/metrics")
async def metrics(request: Request) -> Response:
    registry = _services(request).metrics.registry
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

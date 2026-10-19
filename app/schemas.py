"""HTTP boundary models. Pydantic never leaves the API layer."""

import json
from datetime import UTC, datetime
from typing import Annotated, Any, Self

from pydantic import BaseModel, StringConstraints, field_validator

KEY_PATTERN = r"^[A-Za-z0-9._:-]{1,128}$"
EVENT_TYPE_PATTERN = r"^[a-z0-9_.]{1,64}$"
MAX_PAYLOAD_DEPTH = 32

IdempotencyKey = Annotated[str, StringConstraints(pattern=KEY_PATTERN)]
EventType = Annotated[str, StringConstraints(pattern=EVENT_TYPE_PATTERN)]


def json_depth(value: Any) -> int:
    """Nesting depth of a decoded JSON value, computed without recursion."""
    deepest = 0
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        node, depth = stack.pop()
        if isinstance(node, dict):
            children = list(node.values())
        elif isinstance(node, list):
            children = node
        else:
            continue
        deepest = max(deepest, depth + 1)
        stack.extend((child, depth + 1) for child in children)
    return deepest


class WebhookIn(BaseModel):
    idempotency_key: IdempotencyKey
    event_type: EventType
    payload: dict[str, Any]

    @field_validator("payload")
    @classmethod
    def _limit_depth(cls, value: dict[str, Any]) -> dict[str, Any]:
        if json_depth(value) > MAX_PAYLOAD_DEPTH:
            raise ValueError(f"payload nesting exceeds {MAX_PAYLOAD_DEPTH}")
        return value


def _iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    moment = datetime.fromtimestamp(epoch, UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class WebhookStatus(BaseModel):
    idempotency_key: str
    event_type: str
    status: str
    attempts: int
    received_at: str | None
    started_at: str | None
    completed_at: str | None
    next_attempt_at: str | None
    last_error: str | None
    result: dict[str, Any] | None

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> Self:
        waiting = record["status"] in ("pending", "retry_scheduled")
        return cls(
            idempotency_key=record["idempotency_key"],
            event_type=record["event_type"],
            status=record["status"],
            attempts=record["attempts"],
            received_at=_iso(record["received_at"]),
            started_at=_iso(record["started_at"]),
            completed_at=_iso(record["completed_at"]),
            next_attempt_at=(
                _iso(record["next_attempt_at"]) if waiting else None
            ),
            last_error=record["last_error"],
            result=json.loads(record["result"]) if record["result"] else None,
        )


def status_view(record: dict[str, Any]) -> dict[str, Any]:
    """Public projection of a row; the payload is deliberately omitted."""
    return WebhookStatus.from_record(record).model_dump()

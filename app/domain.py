"""Domain rules of the webhook lifecycle.

Pure Python: no I/O, no frameworks, no event loop. Every time-dependent
function takes ``now`` as an argument so it can be tested without sleeping.
"""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from random import Random
from typing import Any, Protocol


class Status(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY_SCHEDULED = "retry_scheduled"
    COMPLETED = "completed"
    DEAD = "dead"


TERMINAL_STATUSES = frozenset({Status.COMPLETED, Status.DEAD})


@dataclass(frozen=True, slots=True, kw_only=True)
class ClaimedEvent:
    """An event leased by a worker; ``lease_token`` fences every write.

    Frozen is shallow: ``payload`` is typed as a read-only ``Mapping`` and
    processors must treat it as such.
    """

    id: int
    idempotency_key: str
    event_type: str
    payload: Mapping[str, Any]
    attempts: int
    max_attempts: int
    lease_token: str
    received_at: float


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    base: float
    cap: float
    jitter: float = 0.2


@dataclass(frozen=True, slots=True)
class Transition:
    status: Status
    next_attempt_at: float
    completed_at: float | None


class EventProcessor(Protocol):
    async def __call__(self, event: ClaimedEvent) -> Mapping[str, Any]: ...


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def canonical_hash(event_type: str, payload: Mapping[str, Any]) -> str:
    """Fingerprint used to tell a duplicate from a conflicting reuse."""
    document = canonical_json({"event_type": event_type, "payload": payload})
    return hashlib.sha256(document.encode()).hexdigest()


def is_exhausted(attempts: int, max_attempts: int) -> bool:
    return attempts >= max_attempts


def backoff_delay(attempts: int, policy: RetryPolicy, rng: Random) -> float:
    """Exponential delay capped at ``policy.cap``, then jittered upwards."""
    delay = min(policy.base * 2 ** (attempts - 1), policy.cap)
    return delay * (1 + rng.uniform(0, policy.jitter))


def after_failure(
    attempts: int,
    max_attempts: int,
    now: float,
    policy: RetryPolicy,
    rng: Random,
) -> Transition:
    if is_exhausted(attempts, max_attempts):
        return Transition(Status.DEAD, now, completed_at=now)
    delay = backoff_delay(attempts, policy, rng)
    return Transition(Status.RETRY_SCHEDULED, now + delay, completed_at=None)


def after_lease_expiry(
    attempts: int, max_attempts: int, now: float
) -> Transition:
    """A lost lease counts as a failed attempt; it is never retried forever.

    Without the exhaustion check an event that crashes the whole process
    would come back after every restart.
    """
    if is_exhausted(attempts, max_attempts):
        return Transition(Status.DEAD, now, completed_at=now)
    return Transition(Status.PENDING, now, completed_at=None)

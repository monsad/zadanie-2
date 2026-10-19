import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(slots=True)
class _Bucket:
    tokens: float
    updated: float


class TokenBucketLimiter:
    """In-memory token bucket per key.

    Callers must pass a bounded set of keys (see ``source_label`` in the
    API); keys taken verbatim from an untrusted header would grow without
    limit.
    """

    def __init__(
        self,
        rate: float,
        burst: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._rate = rate
        self._burst = burst
        self._clock = clock
        self._buckets: dict[str, _Bucket] = {}

    def __len__(self) -> int:
        return len(self._buckets)

    def acquire(self, key: str) -> float:
        """Take one token. Returns 0 on success, else seconds to wait."""
        now = self._clock()
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = self._buckets[key] = _Bucket(self._burst, now)
        bucket.tokens = min(
            self._burst, bucket.tokens + (now - bucket.updated) * self._rate
        )
        bucket.updated = now
        if bucket.tokens >= 1:
            bucket.tokens -= 1
            return 0.0
        return (1 - bucket.tokens) / self._rate

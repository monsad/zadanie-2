from random import Random

from app.domain import (
    RetryPolicy,
    Status,
    after_failure,
    after_lease_expiry,
    backoff_delay,
    canonical_hash,
)

POLICY = RetryPolicy(base=2.0, cap=300.0)


def test_failure_schedules_retry_until_exhausted():
    retry = after_failure(1, 5, now=100.0, policy=POLICY, rng=Random(1))
    assert retry.status is Status.RETRY_SCHEDULED
    assert retry.next_attempt_at > 100.0
    assert retry.completed_at is None

    dead = after_failure(5, 5, now=100.0, policy=POLICY, rng=Random(1))
    assert dead.status is Status.DEAD
    assert dead.completed_at == 100.0


def test_backoff_grows_exponentially_with_bounded_jitter():
    rng = Random(42)
    for attempt, base in [(1, 2.0), (2, 4.0), (3, 8.0), (4, 16.0)]:
        delay = backoff_delay(attempt, POLICY, rng)
        assert base <= delay <= base * 1.2


def test_backoff_is_capped():
    delay = backoff_delay(20, POLICY, Random(0))
    assert 300.0 <= delay <= 360.0


def test_lost_lease_requeues_then_buries():
    assert after_lease_expiry(2, 5, 10.0).status is Status.PENDING
    assert after_lease_expiry(5, 5, 10.0).status is Status.DEAD


def test_canonical_hash_ignores_key_order_only():
    a = canonical_hash("order.created", {"a": 1, "b": {"x": 1, "y": 2}})
    b = canonical_hash("order.created", {"b": {"y": 2, "x": 1}, "a": 1})
    assert a == b
    assert a != canonical_hash("order.created", {"a": 2, "b": {}})
    assert a != canonical_hash("order.paid", {"a": 1, "b": {"x": 1, "y": 2}})

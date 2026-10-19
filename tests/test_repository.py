import pytest
from pydantic import ValidationError

from app.config import Settings
from app.domain import Status, Transition, after_lease_expiry
from app.repository import _CLAIM_SQL, Repository


class Clock:
    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@pytest.fixture
async def repo(tmp_path):
    clock = Clock()
    repository = Repository(str(tmp_path / "repo.db"), clock=clock)
    await repository.open()
    repository.clock = clock
    yield repository
    await repository.close()


async def insert(repo: Repository, key: str, max_attempts: int = 5):
    return await repo.insert(
        idempotency_key=key,
        source="test",
        event_type="order.created",
        payload_json='{"a":1}',
        payload_hash="h-" + key,
        max_attempts=max_attempts,
    )


async def test_insert_is_idempotent(repo):
    assert (await insert(repo, "k1")).outcome == "accepted"
    assert (await insert(repo, "k1")).outcome == "duplicate"
    conflict = await repo.insert(
        idempotency_key="k1",
        source="test",
        event_type="order.created",
        payload_json='{"a":2}',
        payload_hash="other",
        max_attempts=5,
    )
    assert conflict.outcome == "conflict"


async def test_claim_respects_limit_and_leases(repo):
    for i in range(5):
        await insert(repo, f"k{i}")
    first = await repo.claim(3, lease_seconds=30, worker_id="w")
    second = await repo.claim(10, lease_seconds=30, worker_id="w")
    assert len(first) == 3
    assert len(second) == 2
    assert len({e.lease_token for e in first + second}) == 5
    assert await repo.claim(10, lease_seconds=30, worker_id="w") == []


async def test_fencing_rejects_writes_from_a_lost_lease(repo):
    await insert(repo, "k")
    (old,) = await repo.claim(1, lease_seconds=30, worker_id="slow")
    repo.clock.now += 31
    (lease,) = await repo.stale_leases(expired_only=True)
    now = repo.clock.now
    transition = after_lease_expiry(lease.attempts, lease.max_attempts, now)
    assert await repo.recover(lease, transition)
    (new,) = await repo.claim(1, lease_seconds=30, worker_id="fast")

    assert not await repo.complete(old, {"from": "slow"})
    assert await repo.complete(new, {"from": "fast"})
    record = await repo.get("k")
    assert record["status"] == "completed"
    assert record["result"] == '{"from": "fast"}'
    assert record["attempts"] == 2


async def test_release_returns_attempt(repo):
    await insert(repo, "k")
    (event,) = await repo.claim(1, lease_seconds=30, worker_id="w")
    assert await repo.release(event)
    record = await repo.get("k")
    assert record["status"] == "pending"
    assert record["attempts"] == 0


async def test_retention_purges_only_old_terminal_rows(repo):
    for key in ("old", "fresh", "waiting"):
        await insert(repo, key)
    events = await repo.claim(2, lease_seconds=30, worker_id="w")
    by_key = {e.idempotency_key: e for e in events}
    await repo.complete(by_key["old"], {})
    repo.clock.now += 8 * 86_400
    await repo.complete(by_key["fresh"], {})

    deleted = await repo.purge(Status.COMPLETED, repo.clock.now - 7 * 86_400)
    assert deleted == 1
    assert await repo.get_event("old") is None
    remnant = await repo.get("old")
    assert remnant is not None
    assert remnant["status"] == "completed"
    assert remnant["result"] is None
    assert await repo.get("fresh") is not None
    assert await repo.get("waiting") is not None
    assert await repo.get_event("fresh") is not None
    assert await repo.get_event("waiting") is not None
    with pytest.raises(ValueError):
        await repo.purge(Status.PENDING, repo.clock.now)


async def test_claim_query_uses_ready_index(repo):
    cursor = await repo._writer_conn.execute(
        "EXPLAIN QUERY PLAN " + _CLAIM_SQL,
        {"now": 1.0, "lease_seconds": 1, "worker_id": "w", "limit": 10},
    )
    plan = " ".join(row[3] for row in await cursor.fetchall())
    assert "ix_events_ready" in plan
    assert "TEMP B-TREE" not in plan


async def _key_count(repo: Repository, key: str) -> int:
    cursor = await repo._writer_conn.execute(
        "SELECT COUNT(*) AS n FROM webhook_idempotency_keys"
        " WHERE idempotency_key = ?",
        (key,),
    )
    return (await cursor.fetchone())["n"]


async def test_insert_after_event_delete_keeps_idempotency(repo):
    assert (await insert(repo, "k1")).outcome == "accepted"
    assert await repo.delete_event("k1")
    again = await insert(repo, "k1")
    assert again.outcome == "duplicate"
    assert await repo.get_event("k1") is None
    conflict = await repo.insert(
        idempotency_key="k1",
        source="test",
        event_type="order.created",
        payload_json='{"a":2}',
        payload_hash="other",
        max_attempts=5,
    )
    assert conflict.outcome == "conflict"
    assert await repo.get_event("k1") is None


async def test_retention_of_completed_events_keeps_key(repo):
    await insert(repo, "kept")
    (event,) = await repo.claim(1, lease_seconds=30, worker_id="w")
    await repo.complete(event, {})
    created = repo.clock.now
    repo.clock.now += 8 * 86_400
    assert await repo.purge(Status.COMPLETED, repo.clock.now - 7 * 86_400) == 1
    assert await repo.get_event("kept") is None
    remnant = await repo.get("kept")
    assert remnant is not None
    assert remnant["status"] == "completed"
    assert await _key_count(repo, "kept") == 1
    assert await repo.purge_expired_keys(repo.clock.now) == 0
    repo.clock.now = created + 90 * 86_400 + 1
    assert await repo.purge_expired_keys(repo.clock.now) == 1
    assert await repo.get("kept") is None
    reused = await insert(repo, "kept")
    assert reused.outcome == "accepted"
    assert await repo.get_event("kept") is not None


async def test_replay_does_not_create_a_second_key(repo):
    await insert(repo, "dead-k", max_attempts=1)
    (event,) = await repo.claim(1, lease_seconds=30, worker_id="w")
    now = repo.clock.now
    assert await repo.fail(event, Transition(Status.DEAD, now, now), "x")
    replayed = await repo.replay("dead-k")
    assert replayed is not None
    assert replayed["status"] == "pending"
    assert await _key_count(repo, "dead-k") == 1
    key_row = await repo._fetch_key(repo._writer_conn, "dead-k")
    assert key_row["last_status"] == "pending"
    assert key_row["payload_hash"] == "h-dead-k"


def test_idempotency_ttl_must_exceed_payload_retention():
    with pytest.raises(ValidationError):
        Settings(retention_idempotency_days=30, retention_dead_days=30)
    Settings(retention_idempotency_days=31, retention_dead_days=30)

"""One test per business requirement, plus the HTTP contract."""

import asyncio

from app.repository import DatabaseUnavailable, Repository
from tests.conftest import (
    Clock,
    CountingProcessor,
    live_app,
    make_settings,
    running_app,
    wait_terminal,
    webhook,
)


async def test_accepted_webhook_is_processed(tmp_path):
    async with running_app(make_settings(tmp_path)) as client:
        response = await client.post("/webhooks", json=webhook("evt-001"))
        assert response.status_code == 202
        assert response.headers["location"] == "/webhooks/evt-001"

        (status,) = (await wait_terminal(client, ["evt-001"])).values()
        assert status["status"] == "completed"
        assert status["attempts"] == 1
        assert status["result"]["processed"] is True
        assert status["started_at"] and status["completed_at"]


async def test_duplicate_returns_existing_status_without_reprocessing(
    tmp_path,
):
    processor = CountingProcessor()
    async with running_app(make_settings(tmp_path), processor) as client:
        await client.post("/webhooks", json=webhook("evt-002"))
        await wait_terminal(client, ["evt-002"])

        again = await client.post("/webhooks", json=webhook("evt-002"))
        assert again.status_code == 200
        assert again.json()["status"] == "completed"
        await asyncio.sleep(0.1)
    assert processor.calls["evt-002"] == 1


async def test_concurrent_duplicates_are_processed_once(tmp_path):
    processor = CountingProcessor()
    async with running_app(make_settings(tmp_path), processor) as client:
        responses = await asyncio.gather(
            *(
                client.post("/webhooks", json=webhook("evt-x"))
                for _ in range(50)
            )
        )
        codes = sorted(r.status_code for r in responses)
        assert codes == [200] * 49 + [202]
        await wait_terminal(client, ["evt-x"])
    assert processor.calls["evt-x"] == 1


async def test_same_key_different_payload_is_conflict(tmp_path):
    async with running_app(make_settings(tmp_path)) as client:
        await client.post("/webhooks", json=webhook("evt-003", a=1))
        response = await client.post("/webhooks", json=webhook("evt-003", a=2))
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "payload_conflict"


async def test_key_order_does_not_make_a_conflict(tmp_path):
    async with running_app(make_settings(tmp_path)) as client:
        first = webhook("evt-004", a=1, b=2)
        second = {**first, "payload": {"b": 2, "a": 1}}
        assert (await client.post("/webhooks", json=first)).status_code == 202
        assert (await client.post("/webhooks", json=second)).status_code == 200


async def test_status_lookup_and_validation(tmp_path):
    async with running_app(make_settings(tmp_path)) as client:
        assert (await client.get("/webhooks/nope")).status_code == 404
        assert (await client.get("/webhooks/bad key!")).status_code == 422
        bad = {"idempotency_key": "", "event_type": "x", "payload": {}}
        response = await client.post("/webhooks", json=bad)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "invalid_webhook"
        not_json = await client.post("/webhooks", content=b"{oops")
        assert not_json.status_code == 422


async def test_many_concurrent_webhooks(tmp_path):
    processor = CountingProcessor(delay=0.05)
    settings = make_settings(
        tmp_path, worker_concurrency=16, rate_limit_burst=1000
    )
    keys = [f"evt-{i}" for i in range(200)]
    async with running_app(settings, processor) as client:
        responses = await asyncio.gather(
            *(client.post("/webhooks", json=webhook(k)) for k in keys)
        )
        assert {r.status_code for r in responses} == {202}
        statuses = await wait_terminal(client, keys, limit=20)
    assert {s["status"] for s in statuses.values()} == {"completed"}
    assert set(processor.calls.values()) == {1}
    assert processor.max_running <= 16


async def test_failures_are_retried_then_dead(tmp_path):
    processor = CountingProcessor(fail=True)
    settings = make_settings(tmp_path, max_attempts=3)
    async with running_app(settings, processor) as client:
        await client.post("/webhooks", json=webhook("evt-005"))
        (status,) = (await wait_terminal(client, ["evt-005"])).values()
    assert status["status"] == "dead"
    assert status["attempts"] == 3
    assert "boom" in status["last_error"]
    assert processor.calls["evt-005"] == 3


async def test_graceful_restart_finishes_accepted_work(tmp_path):
    keys = [f"evt-r{i}" for i in range(10)]
    slow = make_settings(tmp_path, lease_seconds=30, shutdown_grace=0.05)
    async with running_app(slow, CountingProcessor(delay=5)) as client:
        for key in keys:
            response = await client.post("/webhooks", json=webhook(key))
            assert response.status_code == 202
        await asyncio.sleep(0.1)

    processor = CountingProcessor()
    async with running_app(make_settings(tmp_path), processor) as client:
        statuses = await wait_terminal(client, keys)
    assert {s["status"] for s in statuses.values()} == {"completed"}
    # Released on shutdown, so the interrupted attempt is not counted.
    assert {s["attempts"] for s in statuses.values()} == {1}


async def test_crash_mid_processing_is_recovered_on_start(tmp_path):
    settings = make_settings(tmp_path, lease_seconds=300)
    repo = Repository(settings.db_path)
    await repo.open()
    await repo.insert(
        idempotency_key="evt-crash",
        source="test",
        event_type="order.created",
        payload_json="{}",
        payload_hash="h",
        max_attempts=5,
    )
    await repo.claim(1, lease_seconds=300, worker_id="dead-process")
    await repo.close()  # "kill -9": the lease is never completed or released

    processor = CountingProcessor()
    async with running_app(settings, processor) as client:
        (status,) = (await wait_terminal(client, ["evt-crash"], 3)).values()
    assert status["status"] == "completed"
    assert status["attempts"] == 2


async def test_backpressure_rejects_without_storing(tmp_path):
    settings = make_settings(tmp_path, max_queue_depth=0)
    async with running_app(settings) as client:
        response = await client.post("/webhooks", json=webhook("evt-full"))
        assert response.status_code == 503
        assert "retry-after" in response.headers
        assert (await client.get("/webhooks/evt-full")).status_code == 404


async def test_queue_limit_holds_under_concurrent_requests(tmp_path):
    settings = make_settings(tmp_path, max_queue_depth=1)
    processor = CountingProcessor(delay=5)
    async with running_app(settings, processor) as client:
        responses = await asyncio.gather(
            *(
                client.post("/webhooks", json=webhook(f"evt-cap-{i}"))
                for i in range(10)
            )
        )
        codes = sorted(r.status_code for r in responses)
        assert codes == [202] + [503] * 9
        # A duplicate of the stored event is still answered when full.
        accepted = next(r for r in responses if r.status_code == 202)
        key = accepted.json()["idempotency_key"]
        again = await client.post("/webhooks", json=webhook(key))
        assert again.status_code == 200


async def test_admin_endpoints_map_db_errors_to_503(tmp_path, monkeypatch):
    settings = make_settings(tmp_path, admin_token="t0ken")
    async with live_app(settings) as (client, services):

        async def down(*_args, **_kwargs):
            raise DatabaseUnavailable("disk I/O error")

        monkeypatch.setattr(services.repo, "list_events", down)
        monkeypatch.setattr(services.repo, "replay", down)
        headers = {"X-Admin-Token": "t0ken"}
        listing = await client.get("/webhooks", headers=headers)
        replay = await client.post("/webhooks/evt-x/replay", headers=headers)
        assert listing.status_code == 503
        assert replay.status_code == 503
        monkeypatch.undo()


async def test_rate_limit_per_source(tmp_path):
    settings = make_settings(
        tmp_path, rate_limit_burst=2, rate_limit_rps=0.1, allowed_sources="a,b"
    )
    async with running_app(settings) as client:
        for i in range(2):
            ok = await client.post(
                "/webhooks", json=webhook(f"a{i}"), headers={"X-Source": "a"}
            )
            assert ok.status_code == 202
        limited = await client.post(
            "/webhooks", json=webhook("a2"), headers={"X-Source": "a"}
        )
        assert limited.status_code == 429
        assert int(limited.headers["retry-after"]) >= 1
        other = await client.post(
            "/webhooks", json=webhook("b0"), headers={"X-Source": "b"}
        )
        assert other.status_code == 202


async def test_health_ready_and_metrics(tmp_path):
    async with running_app(make_settings(tmp_path)) as client:
        assert (await client.get("/health")).json() == {"status": "ok"}
        ready = await client.get("/ready")
        assert ready.status_code == 200
        assert ready.json()["dispatcher"] is True
        await client.post("/webhooks", json=webhook("evt-m"))
        await wait_terminal(client, ["evt-m"])
        text = (await client.get("/metrics")).text
        assert 'webhooks_received_total{outcome="accepted"' in text
        assert "webhooks_dispatch_lag_seconds" in text


async def test_duplicate_after_event_deleted_does_not_reprocess(tmp_path):
    processor = CountingProcessor()
    settings = make_settings(tmp_path)
    async with live_app(settings, processor) as (client, services):
        assert (
            await client.post("/webhooks", json=webhook("evt-gone"))
        ).status_code == 202
        await wait_terminal(client, ["evt-gone"])
        assert await services.repo.delete_event("evt-gone")

        again = await client.post("/webhooks", json=webhook("evt-gone"))
        assert again.status_code == 200
        assert again.json()["status"] == "completed"
        assert again.json()["result"] is None
        assert "payload" not in again.json()
        await asyncio.sleep(0.1)
        assert await services.repo.get_event("evt-gone") is None
    assert processor.calls["evt-gone"] == 1


async def test_conflict_after_event_deleted(tmp_path):
    settings = make_settings(tmp_path)
    async with live_app(settings) as (client, services):
        await client.post("/webhooks", json=webhook("evt-gone", a=1))
        await wait_terminal(client, ["evt-gone"])
        assert await services.repo.delete_event("evt-gone")
        response = await client.post(
            "/webhooks", json=webhook("evt-gone", a=2)
        )
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "payload_conflict"
        assert await services.repo.get_event("evt-gone") is None


async def test_get_returns_remnant_after_event_deleted(tmp_path):
    settings = make_settings(tmp_path)
    async with live_app(settings) as (client, services):
        await client.post("/webhooks", json=webhook("evt-gone"))
        await wait_terminal(client, ["evt-gone"])
        assert await services.repo.delete_event("evt-gone")
        remnant = await client.get("/webhooks/evt-gone")
        assert remnant.status_code == 200
        body = remnant.json()
        assert body["status"] == "completed"
        assert body["idempotency_key"] == "evt-gone"
        assert body["result"] is None
        assert "payload" not in body


async def test_expired_idempotency_key_accepts_new_event(tmp_path):
    processor = CountingProcessor()
    clock = Clock()
    settings = make_settings(
        tmp_path,
        retention_completed_days=1,
        retention_dead_days=2,
        retention_idempotency_days=3,
    )
    async with live_app(settings, processor, clock) as (client, services):
        assert (
            await client.post("/webhooks", json=webhook("evt-ttl"))
        ).status_code == 202
        await wait_terminal(client, ["evt-ttl"])
        assert await services.repo.delete_event("evt-ttl")
        clock.now += 3 * 86_400 + 1
        again = await client.post("/webhooks", json=webhook("evt-ttl"))
        assert again.status_code == 202
        await wait_terminal(client, ["evt-ttl"])
        assert await services.repo.get_event("evt-ttl") is not None
    assert processor.calls["evt-ttl"] == 2

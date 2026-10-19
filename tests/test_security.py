import json
import time

from app.auth import sign
from app.domain import canonical_json
from tests.conftest import make_settings, running_app, wait_terminal, webhook

SECRETS = "shop:s3cr3t,crm:other-secret"


def signed(source: str, secret: bytes, body: bytes, ts: float | None = None):
    timestamp = str(int(ts if ts is not None else time.time()))
    return {
        "X-Source": source,
        "X-Timestamp": timestamp,
        "X-Signature": sign(secret, timestamp, body),
        "Content-Type": "application/json",
    }


def body_of(key: str, **payload) -> bytes:
    return json.dumps(webhook(key, **payload)).encode()


def get_headers(source: str, secret: bytes, key: str):
    return signed(source, secret, f"/webhooks/{key}".encode())


async def test_s1_missing_wrong_or_foreign_signature(tmp_path):
    settings = make_settings(tmp_path, webhook_secrets=SECRETS)
    body = body_of("s1")
    async with running_app(settings) as client:
        none = await client.post("/webhooks", content=body)
        wrong = await client.post(
            "/webhooks", content=body, headers=signed("shop", b"nope", body)
        )
        foreign = await client.post(
            "/webhooks",
            content=body,
            headers={
                **signed("crm", b"other-secret", body),
                "X-Source": "shop",
            },
        )
        statuses = {
            none.status_code,
            wrong.status_code,
            foreign.status_code,
        }
        assert statuses == {401}
        lookup = await client.get(
            "/webhooks/s1", headers=get_headers("shop", b"s3cr3t", "s1")
        )
        assert lookup.status_code == 404


async def test_s2_stale_timestamp(tmp_path):
    settings = make_settings(tmp_path, webhook_secrets=SECRETS)
    body = body_of("s2")
    headers = signed("shop", b"s3cr3t", body, ts=time.time() - 360)
    async with running_app(settings) as client:
        response = await client.post(
            "/webhooks", content=body, headers=headers
        )
        assert response.status_code == 401


async def test_s3_replay_in_window_is_a_duplicate(tmp_path):
    settings = make_settings(tmp_path, webhook_secrets=SECRETS)
    body = body_of("s3")
    headers = signed("shop", b"s3cr3t", body)
    async with running_app(settings) as client:
        first = await client.post("/webhooks", content=body, headers=headers)
        replay = await client.post("/webhooks", content=body, headers=headers)
        assert (first.status_code, replay.status_code) == (202, 200)


async def test_s4_tampered_body(tmp_path):
    settings = make_settings(tmp_path, webhook_secrets=SECRETS)
    body = body_of("s4", amount=10)
    headers = signed("shop", b"s3cr3t", body)
    tampered = body.replace(b"10", b"99")
    async with running_app(settings) as client:
        response = await client.post(
            "/webhooks", content=tampered, headers=headers
        )
        assert response.status_code == 401


async def test_s5_oversized_and_deeply_nested(tmp_path):
    settings = make_settings(tmp_path, max_body_bytes=1024)
    async with running_app(settings) as client:
        big = webhook("s5", blob="x" * 2000)
        assert (await client.post("/webhooks", json=big)).status_code == 413
        deep: dict = {}
        node = deep
        for _ in range(40):
            node["n"] = {}
            node = node["n"]
        nested = {"idempotency_key": "s5b", "event_type": "a", "payload": deep}
        response = await client.post("/webhooks", content=json.dumps(nested))
        assert response.status_code == 422


async def test_s6_rotating_source_cannot_bypass_limits(tmp_path):
    settings = make_settings(tmp_path, rate_limit_burst=3, rate_limit_rps=0.1)
    async with running_app(settings) as client:
        codes = [
            (
                await client.post(
                    "/webhooks",
                    json=webhook(f"s6-{i}"),
                    headers={"X-Source": f"random-{i}"},
                )
            ).status_code
            for i in range(6)
        ]
        assert codes == [202, 202, 202, 429, 429, 429]
        metrics = (await client.get("/metrics")).text
        assert "random-" not in metrics


async def test_s7_scoping_and_admin_token(tmp_path):
    settings = make_settings(
        tmp_path, webhook_secrets=SECRETS, admin_token="adm1n"
    )
    body = body_of("s7")
    async with running_app(settings) as client:
        await client.post(
            "/webhooks", content=body, headers=signed("shop", b"s3cr3t", body)
        )
        own = await client.get(
            "/webhooks/s7", headers=get_headers("shop", b"s3cr3t", "s7")
        )
        foreign = await client.get(
            "/webhooks/s7", headers=get_headers("crm", b"other-secret", "s7")
        )
        assert (own.status_code, foreign.status_code) == (200, 404)

        assert (await client.get("/webhooks")).status_code == 401
        assert (await client.post("/webhooks/s7/replay")).status_code == 401
        listed = await client.get(
            "/webhooks", headers={"X-Admin-Token": "adm1n"}
        )
        assert listed.status_code == 200
        assert listed.json()["items"][0]["idempotency_key"] == "s7"


async def test_s8_sql_characters(tmp_path):
    async with running_app(make_settings(tmp_path)) as client:
        injected = webhook("x'; DROP TABLE webhook_events;--")
        rejected = await client.post("/webhooks", json=injected)
        assert rejected.status_code == 422
        literal = webhook("s8", note="'); DELETE FROM webhook_events; --")
        accepted = await client.post("/webhooks", json=literal)
        assert accepted.status_code == 202
        await wait_terminal(client, ["s8"])
        services = client._transport.app.state.services
        record = await services.repo.get("s8")
        assert record["payload"] == canonical_json(literal["payload"])


async def test_s9_payload_never_logged(tmp_path, capsys):
    settings = make_settings(tmp_path, log_level="INFO")
    payload = {"user_id": "USR-9876", "email": "jan@example.com"}
    async with running_app(settings) as client:
        await client.post("/webhooks", json=webhook("evt-004", **payload))
        await wait_terminal(client, ["evt-004"])
    out = capsys.readouterr().out
    assert "webhook.completed" in out
    assert "jan@example.com" not in out


async def test_s10_non_finite_timestamp_is_rejected(tmp_path):
    # NaN compares False with everything, so it would pass the window check
    # and let a signed request be replayed forever.
    settings = make_settings(tmp_path, webhook_secrets=SECRETS)
    body = body_of("s10")
    async with running_app(settings) as client:
        for value in ("nan", "NaN", "inf", "-inf"):
            headers = {
                "X-Source": "shop",
                "X-Timestamp": value,
                "X-Signature": sign(b"s3cr3t", value, body),
                "Content-Type": "application/json",
            }
            response = await client.post(
                "/webhooks", content=body, headers=headers
            )
            assert response.status_code == 401, value


async def test_s11_non_ascii_credentials_are_401_not_500(tmp_path):
    # hmac.compare_digest raises TypeError on non-ASCII str arguments.
    settings = make_settings(
        tmp_path, webhook_secrets=SECRETS, admin_token="t0ken"
    )
    body = body_of("s11")
    timestamp = str(int(time.time())).encode()
    async with running_app(settings) as client:
        post = await client.post(
            "/webhooks",
            content=body,
            headers=[
                (b"x-source", b"shop"),
                (b"x-timestamp", timestamp),
                (b"x-signature", b"sha256=\xe9"),
                (b"content-type", b"application/json"),
            ],
        )
        admin = await client.get(
            "/webhooks", headers=[(b"x-admin-token", b"\xe9")]
        )
        assert post.status_code == 401
        assert admin.status_code == 401

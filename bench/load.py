"""Open-loop load test: constant arrival rate, a share of duplicates.

Usage: python bench/load.py --rate 1000 --duration 60 --duplicates 0.1

Open loop means requests are sent on schedule whether or not earlier ones
returned, which is how webhook senders behave (a closed loop would hide
latency by slowing down with the server).
"""

import argparse
import asyncio
import random
import statistics
import time
import uuid
from collections import Counter

import httpx

TERMINAL = {"completed", "dead"}


async def send(client, body, latencies, codes):
    started = time.perf_counter()
    try:
        response = await client.post("/webhooks", json=body)
        codes[response.status_code] += 1
    except httpx.HTTPError as exc:
        codes[type(exc).__name__] += 1
    latencies.append(time.perf_counter() - started)


def percentile(values, q):
    return statistics.quantiles(values, n=100)[q - 1] * 1000 if values else 0


async def drain(client, keys, limit):
    deadline = time.monotonic() + limit
    pending = set(keys)
    while pending and time.monotonic() < deadline:
        sample = list(pending)[:200]
        results = await asyncio.gather(
            *(client.get(f"/webhooks/{k}") for k in sample)
        )
        for key, response in zip(sample, results, strict=True):
            if response.json().get("status") in TERMINAL:
                pending.discard(key)
        await asyncio.sleep(0.5)
    return pending


async def main(args):
    rate = args.rate / 60
    total = int(rate * args.duration)
    run = uuid.uuid4().hex[:6]
    sent_keys: list[str] = []
    latencies: list[float] = []
    codes: Counter = Counter()
    limits = httpx.Limits(max_connections=200)
    async with httpx.AsyncClient(
        base_url=args.url, limits=limits, timeout=10
    ) as client:
        start = time.perf_counter()
        tasks = []
        for i in range(total):
            if sent_keys and random.random() < args.duplicates:
                key = random.choice(sent_keys)
            else:
                key = f"bench-{run}-{i}"
                sent_keys.append(key)
            body = {
                "idempotency_key": key,
                "event_type": "order.created",
                "payload": {"order_id": key, "amount": 299.99},
            }
            delay = start + i / rate - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(
                asyncio.create_task(send(client, body, latencies, codes))
            )
        await asyncio.gather(*tasks)
        ingest_done = time.perf_counter() - start
        unfinished = await drain(client, sent_keys, args.drain_timeout)
        drained = time.perf_counter() - start

    print(
        f"sent {total} requests ({len(sent_keys)} unique) "
        f"at {args.rate}/min in {ingest_done:.1f}s"
    )
    print("status codes:", dict(sorted(codes.items(), key=str)))
    print(
        f"ingest latency ms: p50={percentile(latencies, 50):.1f} "
        f"p95={percentile(latencies, 95):.1f} "
        f"p99={percentile(latencies, 99):.1f}"
    )
    print(f"all terminal after {drained:.1f}s; unfinished={len(unfinished)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--rate", type=float, default=1000, help="per minute")
    parser.add_argument("--duration", type=float, default=60, help="seconds")
    parser.add_argument("--duplicates", type=float, default=0.1)
    parser.add_argument("--drain-timeout", type=float, default=120)
    asyncio.run(main(parser.parse_args()))

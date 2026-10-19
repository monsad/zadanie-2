#!/usr/bin/env bash
# End-to-end check of the running service in Docker.
#
# Usage: scripts/e2e.sh [--bench] [--keep]
#   --bench  also run the 60 s load test (needs uv and just)
#   --keep   leave the containers running at the end
#
# Every step prints PASS or FAIL; the full output goes to e2e-<time>.log.
# It kills and restarts the app container, so do not run anything else
# against the same docker compose project at the same time.
# Exit code is the number of failed steps.

set -u
cd "$(dirname "$0")/.."

BASE=http://localhost:8000
RUN_BENCH=0
KEEP=0
for arg in "$@"; do
    case "$arg" in
        --bench) RUN_BENCH=1 ;;
        --keep) KEEP=1 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

LOG="e2e-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1

PASSED=0
FAILED=0
pass() { PASSED=$((PASSED + 1)); printf '  PASS  %s\n' "$1"; }
fail() { FAILED=$((FAILED + 1)); printf '  FAIL  %s\n' "$1"; }
step() { printf '\n== %s\n' "$1"; }

# POST a webhook; prints the HTTP status code.
post() {
    curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/webhooks" \
        -H 'Content-Type: application/json' -d "$1"
}

# Prints the status of one event, or "none".
status_of() {
    curl -s "$BASE/webhooks/$1" | python3 -c \
        'import json,sys
try: print(json.load(sys.stdin).get("status","none"))
except Exception: print("none")'
}

# Waits until every given key is completed; prints how many are not.
wait_completed() {
    local limit=$1; shift
    local deadline=$((SECONDS + limit)) left
    while :; do
        left=0
        for key in "$@"; do
            [ "$(status_of "$key")" = completed ] || left=$((left + 1))
        done
        if [ "$left" -eq 0 ] || [ $SECONDS -ge $deadline ]; then
            echo "$left"
            return
        fi
        sleep 1
    done
}

wait_ready() {
    local deadline=$((SECONDS + $1))
    until curl -sf "$BASE/ready" >/dev/null; do
        [ $SECONDS -ge $deadline ] && return 1
        sleep 1
    done
}

step "1. Static checks (just check)"
if command -v just >/dev/null && command -v uv >/dev/null; then
    if just check >/dev/null 2>&1; then
        pass "lint, tests, bandit, pip-audit"
    else
        fail "just check failed; run it by hand to see why"
    fi
else
    echo "  SKIP  uv or just not installed"
fi

step "2. Start the service"
docker compose down -v >/dev/null 2>&1
if docker compose up -d --build >/dev/null 2>&1 && wait_ready 90; then
    pass "service is ready on $BASE"
else
    fail "service did not become ready"
    docker compose logs --tail 30 app
    exit 1
fi

step "3. New webhook is accepted and processed"
EVENT='{"idempotency_key":"evt-001","event_type":"order.created","payload":{"order_id":"ORD-1","amount":299.99}}'
code=$(post "$EVENT")
[ "$code" = 202 ] && pass "POST returns 202" || fail "POST returned $code, expected 202"
left=$(wait_completed 15 evt-001)
[ "$left" -eq 0 ] && pass "status becomes completed" \
    || fail "status is $(status_of evt-001), expected completed"

step "4. Duplicate is not processed again"
code=$(post "$EVENT")
[ "$code" = 200 ] && pass "same webhook returns 200" \
    || fail "duplicate returned $code, expected 200"
attempts=$(curl -s "$BASE/webhooks/evt-001" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["attempts"])')
[ "$attempts" = 1 ] && pass "processed exactly once (attempts=1)" \
    || fail "attempts=$attempts, expected 1"

step "5. Same key with a different payload"
code=$(post '{"idempotency_key":"evt-001","event_type":"order.created","payload":{"order_id":"OTHER"}}')
[ "$code" = 409 ] && pass "returns 409 conflict" || fail "returned $code, expected 409"

step "6. Invalid input"
code=$(post '{"oops"')
[ "$code" = 422 ] && pass "broken JSON returns 422" || fail "returned $code, expected 422"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/webhooks/does-not-exist")
[ "$code" = 404 ] && pass "unknown key returns 404" || fail "returned $code, expected 404"

step "7. Many concurrent webhooks"
pids=()
for i in $(seq 1 50); do
    post "{\"idempotency_key\":\"load-$i\",\"event_type\":\"order.paid\",\"payload\":{\"n\":$i}}" >/dev/null &
    pids+=($!)
done
wait "${pids[@]}"
keys=(); for i in $(seq 1 50); do keys+=("load-$i"); done
left=$(wait_completed 30 "${keys[@]}")
[ "$left" -eq 0 ] && pass "50 parallel webhooks all completed" \
    || fail "$left of 50 not completed"

step "8. Crash during processing loses nothing"
keys=()
for i in $(seq 1 20); do
    post "{\"idempotency_key\":\"crash-$i\",\"event_type\":\"order.created\",\"payload\":{\"n\":$i}}" >/dev/null
    keys+=("crash-$i")
done
docker compose kill app >/dev/null 2>&1
docker compose up -d app >/dev/null 2>&1
if wait_ready 60; then
    pass "service restarted after kill"
else
    fail "service did not come back after kill"
fi
left=$(wait_completed 40 "${keys[@]}")
[ "$left" -eq 0 ] && pass "all 20 events completed after the crash" \
    || fail "$left of 20 lost or unfinished"
recovered=$(docker compose logs --no-color app 2>/dev/null \
    | grep '"service.started"' | tail -1 \
    | sed -n 's/.*"recovered": \([0-9]*\).*/\1/p')
echo "        recovered on restart: $recovered"

step "9. Health and metrics"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")
[ "$code" = 200 ] && pass "/health returns 200" || fail "/health returned $code"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/ready")
[ "$code" = 200 ] && pass "/ready returns 200" || fail "/ready returned $code"
# Counters reset on restart, so send one webhook before reading them.
post '{"idempotency_key":"metrics-1","event_type":"order.created","payload":{}}' >/dev/null
if curl -s "$BASE/metrics" | grep -q '^webhooks_received_total'; then
    pass "/metrics exposes webhooks_received_total"
else
    fail "/metrics is missing webhooks_received_total"
fi

if [ "$RUN_BENCH" = 1 ]; then
    step "10. Load test: 1000 webhooks/min for 60 s"
    if just bench; then
        pass "benchmark finished (check unfinished=0 above)"
    else
        fail "benchmark failed"
    fi
fi

if [ "$KEEP" = 0 ]; then
    docker compose down -v >/dev/null 2>&1
fi

printf '\n== Result: %d passed, %d failed (log: %s)\n' "$PASSED" "$FAILED" "$LOG"
exit "$FAILED"

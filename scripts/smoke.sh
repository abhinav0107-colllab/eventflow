#!/usr/bin/env bash
# Submits one job of each type to a running stack and waits for the
# expected terminal state. Usage: BASE_URL=... API_KEY=... scripts/smoke.sh
set -euo pipefail
BASE_URL=${BASE_URL:-http://localhost:8080}
: "${API_KEY:?API_KEY must be set}"
command -v jq >/dev/null || { echo "jq is required"; exit 1; }

hdr=(-H "X-API-Key: ${API_KEY}" -H "Content-Type: application/json")

for i in $(seq 1 60); do
  curl -fsS "${BASE_URL}/ready" >/dev/null 2>&1 && break
  [ "$i" = 60 ] && { echo "API not ready"; exit 1; }
  sleep 1
done

submit() { curl -fsS "${hdr[@]}" -X POST "${BASE_URL}/api/v1/jobs" -d "$1" | jq -r .id; }

declare -A expect
expect[$(submit '{"type":"EMAIL_SIMULATION","payload":{"to":"smoke@example.com","subject":"smoke"}}')]=succeeded
expect[$(submit '{"type":"WEBHOOK_SIMULATION","payload":{"url":"https://example.com/hook"}}')]=succeeded
expect[$(submit '{"type":"CPU_INTENSIVE_COMPUTATION","payload":{"operation":"prime_count","n":100000}}')]=succeeded
expect[$(submit '{"type":"DELAYED_TASK","payload":{"duration_ms":500},"delay_seconds":1}')]=succeeded
expect[$(submit '{"type":"DELIBERATE_FAILURE","payload":{"mode":"permanent"}}')]=failed

for id in "${!expect[@]}"; do
  want=${expect[$id]}
  for i in $(seq 1 60); do
    status=$(curl -fsS "${hdr[@]}" "${BASE_URL}/api/v1/jobs/${id}" | jq -r .status)
    [ "$status" = "$want" ] && { echo "ok   $id -> $status"; break; }
    [ "$i" = 60 ] && { echo "FAIL $id: status $status, want $want"; exit 1; }
    sleep 0.5
  done
done
echo "smoke test passed"

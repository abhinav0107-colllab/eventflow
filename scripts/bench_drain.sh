#!/usr/bin/env bash
# Measures pure worker throughput: with workers stopped, submit JOBS jobs and
# wait until all are queued in Redis; then start WORKERS worker replicas and
# time how long they take to drain the queue.
#
#   JOBS=20000 WORKERS=2 API_KEY=... scripts/bench_drain.sh
#
# Requires the Docker Compose stack, jq, and the k6 image. The measured time
# includes worker start-up (about one second), so the result is conservative.
set -euo pipefail
cd "$(dirname "$0")/.."
JOBS=${JOBS:-20000}
WORKERS=${WORKERS:-2}
BASE_URL=${BASE_URL:-http://localhost:8080}
: "${API_KEY:?API_KEY must be set}"

counts() {
  curl -fsS -H "X-API-Key: ${API_KEY}" "${BASE_URL}/api/v1/queues" |
    jq -r '.queues[].jobs | [.pending, .queued, .running] | @tsv'   # one line per queue
}
outstanding() { counts | awk '{print $1+$2+$3}' | paste -sd+ | bc; }

[ "$(outstanding)" = "0" ] || { echo "stack is not idle; aborting"; exit 1; }

docker compose stop worker >/dev/null
echo "submitting ${JOBS} jobs with workers stopped..."
make --no-print-directory loadtest-processing K6_ARGS="-e JOBS=${JOBS} -e DRAIN=false" >/dev/null

until counts | awk -v n="$JOBS" '{p+=$1; q+=$2} END {exit !(p==0 && q==n)}'; do sleep 0.5; done
echo "all ${JOBS} jobs queued; starting ${WORKERS} workers"

start=$(date +%s.%N)
docker compose up -d --no-build --scale worker="${WORKERS}" worker >/dev/null 2>&1
until [ "$(outstanding)" = "0" ]; do sleep 0.25; done
end=$(date +%s.%N)

secs=$(echo "$end - $start" | bc -l)
printf 'drained %d jobs with %d workers in %.1fs => %.0f jobs/s\n' "$JOBS" "$WORKERS" "$secs" "$(echo "$JOBS / $secs" | bc -l)"

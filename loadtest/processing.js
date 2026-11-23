// End-to-end queue processing throughput.
//
// Phase 1 ("burst"): submit JOBS jobs (default 10000) as fast as possible.
// Phase 2 ("drain"): poll GET /api/v1/queues until no job is pending,
// queued or running, then report jobs/s = JOBS / (drain end - burst start).
//
// Run against an otherwise idle stack: the drain check considers all jobs.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Gauge } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE_URL, params, submit, summary } from './lib.js';

const JOBS = Number(__ENV.JOBS || 10000);
const processed = new Gauge('jobs_processed_per_second');
const drainSeconds = new Gauge('drain_seconds');

// DRAIN=false only submits (used by scripts/bench_drain.sh, which measures
// pure worker throughput against a pre-filled queue).
const scenarios = {
  burst: { executor: 'shared-iterations', vus: 50, iterations: JOBS, maxDuration: '10m', exec: 'burst' },
};
if (__ENV.DRAIN !== 'false') {
  scenarios.drain = { executor: 'per-vu-iterations', vus: 1, iterations: 1, startTime: '1s', maxDuration: '15m', exec: 'drain' };
}

export const options = {
  scenarios,
  thresholds: { 'checks{scenario:burst}': ['rate>0.99'] },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function burst() {
  submit({
    type: 'EMAIL_SIMULATION',
    payload: { to: 'throughput@example.com', subject: 'k6', simulate_latency_ms: Number(__ENV.JOB_LATENCY_MS || 10) },
  });
}

function outstanding() {
  const res = http.get(`${BASE_URL}/api/v1/queues`, params({ name: 'GET /api/v1/queues' }));
  if (res.status !== 200) return -1;
  return res.json('queues').reduce((n, q) => n + q.jobs.pending + q.jobs.queued + q.jobs.running, 0);
}

export function drain() {
  const start = Date.now() - 1000; // the burst scenario started one second earlier
  // Wait until the burst has finished submitting before trusting zero.
  while (exec.instance.iterationsCompleted < JOBS && Date.now() - start < 600000) {
    sleep(0.5);
  }
  let left = outstanding();
  while (left !== 0 && Date.now() - start < 900000) {
    sleep(0.25);
    left = outstanding();
  }
  const secs = (Date.now() - start) / 1000;
  drainSeconds.add(secs);
  processed.add(JOBS / secs);
  check(left, { 'drained: no outstanding jobs': (n) => n === 0 });
  console.log(`processed ${JOBS} jobs in ${secs.toFixed(1)}s => ${(JOBS / secs).toFixed(1)} jobs/s`);
}

export const handleSummary = summary('processing');

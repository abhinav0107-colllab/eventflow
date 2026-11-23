// Shared helpers for EventFlow k6 scripts.
import http from 'k6/http';
import { check } from 'k6';

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const API_KEY = __ENV.API_KEY;
if (!API_KEY) {
  throw new Error('API_KEY environment variable is required');
}

export const params = (tags = {}) => ({
  headers: { 'Content-Type': 'application/json', 'X-API-Key': API_KEY },
  tags,
});

// A cheap job so that submission benchmarks measure the API, not handlers.
export function emailJob(extra = {}) {
  return Object.assign({
    type: 'EMAIL_SIMULATION',
    payload: { to: 'loadtest@example.com', subject: 'k6', body: 'load test', simulate_latency_ms: 5 },
  }, extra);
}

const mix = [
  () => emailJob({ priority: 8 }),
  () => ({ type: 'WEBHOOK_SIMULATION', payload: { url: 'https://example.com/hook', simulate_latency_ms: 10 } }),
  () => ({ type: 'CPU_INTENSIVE_COMPUTATION', payload: { operation: 'prime_count', n: 50000 }, priority: 3 }),
  () => ({ type: 'DELAYED_TASK', payload: { duration_ms: 20 } }),
];

export function mixedJob() {
  return mix[Math.floor(Math.random() * mix.length)]();
}

export function submit(body, tags = { name: 'POST /api/v1/jobs' }) {
  const res = http.post(`${BASE_URL}/api/v1/jobs`, JSON.stringify(body), params(tags));
  check(res, { 'submit: 201 Created': (r) => r.status === 201 });
  return res;
}

// Self-contained text summary (no remote jslib import, so the scripts run
// in air-gapped CI). One line per metric with its most useful statistics.
function fmt(v, unit) {
  if (typeof v !== 'number') return String(v);
  if (unit === 'ms') return v >= 1000 ? `${(v / 1000).toFixed(2)}s` : `${v.toFixed(2)}ms`;
  if (unit === '%') return `${(v * 100).toFixed(2)}%`;
  return Number.isInteger(v) ? String(v) : v.toFixed(2);
}

function textSummary(data) {
  const lines = [];
  for (const name of Object.keys(data.metrics).sort()) {
    const m = data.metrics[name];
    const v = m.values;
    const unit = m.contains === 'time' ? 'ms' : '';
    let body;
    if (m.type === 'trend') {
      body = ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'].filter((k) => k in v).map((k) => `${k}=${fmt(v[k], unit)}`).join(' ');
    } else if (m.type === 'rate') {
      body = `rate=${fmt(v.rate, '%')} (${v.passes} of ${v.passes + v.fails})`;
    } else if (m.type === 'counter') {
      body = `count=${fmt(v.count)} rate=${fmt(v.rate)}/s`;
    } else {
      body = `value=${fmt(v.value)}`;
    }
    const failed = m.thresholds && Object.values(m.thresholds).some((t) => !t.ok);
    lines.push(`${failed ? '✗' : ' '} ${name.padEnd(58)} ${body}`);
  }
  return lines.join('\n') + '\n';
}

// Writes a human summary to stdout and the full metrics JSON to
// loadtest/results/<name>.json (mounted at /scripts in the k6 container).
export function summary(name) {
  return (data) => ({
    stdout: textSummary(data),
    [`/scripts/results/${name}.json`]: JSON.stringify(data, null, 2),
  });
}

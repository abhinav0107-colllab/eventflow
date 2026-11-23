// Sustained load: a steady mixed workload (default 150 submissions/s plus
// 150 reads/s) for DURATION (default 10m) to surface leaks, backlog growth
// and latency drift. Watch the Grafana dashboard while it runs.
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, params, submit, mixedJob, summary } from './lib.js';

const duration = __ENV.DURATION || '10m';

export const options = {
  scenarios: {
    writes: {
      executor: 'constant-arrival-rate', exec: 'write', rate: Number(__ENV.WRITE_RATE || 150),
      timeUnit: '1s', duration, preAllocatedVUs: 50, maxVUs: 300,
    },
    reads: {
      executor: 'constant-arrival-rate', exec: 'read', rate: Number(__ENV.READ_RATE || 150),
      timeUnit: '1s', duration, preAllocatedVUs: 50, maxVUs: 300,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{scenario:writes}': ['p(95)<300'],
    'http_req_duration{scenario:reads}': ['p(95)<200'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function write() {
  submit(mixedJob());
}

export function read() {
  const res = http.get(`${BASE_URL}/api/v1/jobs?limit=20`, params({ name: 'GET /api/v1/jobs' }));
  check(res, { 'list: 200': (r) => r.status === 200 });
}

export const handleSummary = summary('sustained');

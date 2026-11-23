// Concurrent API requests: ramps up to VUS (default 100) closed-loop virtual
// users each submitting a job and immediately reading it back and listing.
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, params, submit, mixedJob, summary } from './lib.js';

const vus = Number(__ENV.VUS || 100);

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: vus },
        { duration: __ENV.HOLD || '45s', target: vus },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const res = submit(mixedJob());
  if (res.status !== 201) return;
  const id = res.json('id');
  const get = http.get(`${BASE_URL}/api/v1/jobs/${id}`, params({ name: 'GET /api/v1/jobs/{id}' }));
  check(get, { 'get: 200': (r) => r.status === 200 });
  const list = http.get(`${BASE_URL}/api/v1/jobs?limit=20`, params({ name: 'GET /api/v1/jobs' }));
  check(list, { 'list: 200': (r) => r.status === 200 });
}

export const handleSummary = summary('concurrent');

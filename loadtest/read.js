// Read API performance: seeds SEED jobs (default 500), then drives a
// constant arrival rate of reads across get, list, filtered list, attempts
// and queue statistics.
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, params, emailJob, summary } from './lib.js';

export const options = {
  setupTimeout: '120s',
  scenarios: {
    reads: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 1000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 100,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:GET /api/v1/jobs/{id}}': ['p(95)<100'],
    'http_req_duration{name:GET /api/v1/jobs}': ['p(95)<250'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const n = Number(__ENV.SEED || 500);
  const ids = [];
  const batch = [];
  for (let i = 0; i < n; i++) {
    batch.push(['POST', `${BASE_URL}/api/v1/jobs`, JSON.stringify(emailJob()), params({ name: 'seed' })]);
    if (batch.length === 50 || i === n - 1) {
      for (const r of http.batch(batch.splice(0))) {
        if (r.status === 201) ids.push(r.json('id'));
      }
    }
  }
  return { ids };
}

export default function (data) {
  const id = data.ids[Math.floor(Math.random() * data.ids.length)];
  const r = Math.random();
  let res;
  if (r < 0.6) {
    res = http.get(`${BASE_URL}/api/v1/jobs/${id}`, params({ name: 'GET /api/v1/jobs/{id}' }));
  } else if (r < 0.8) {
    res = http.get(`${BASE_URL}/api/v1/jobs?limit=50`, params({ name: 'GET /api/v1/jobs' }));
  } else if (r < 0.9) {
    res = http.get(`${BASE_URL}/api/v1/jobs?status=succeeded&type=EMAIL_SIMULATION&limit=50`,
      params({ name: 'GET /api/v1/jobs (filtered)' }));
  } else if (r < 0.97) {
    res = http.get(`${BASE_URL}/api/v1/jobs/${id}/attempts`, params({ name: 'GET /api/v1/jobs/{id}/attempts' }));
  } else {
    res = http.get(`${BASE_URL}/api/v1/queues`, params({ name: 'GET /api/v1/queues' }));
  }
  check(res, { 'read: 200': (x) => x.status === 200 });
}

export const handleSummary = summary('read');

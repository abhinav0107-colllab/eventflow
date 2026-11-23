// Job submission throughput: a constant arrival rate of POST /api/v1/jobs.
// RATE (req/s, default 300) for DURATION (default 60s).
import { submit, emailJob, summary } from './lib.js';

export const options = {
  scenarios: {
    submit: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 300),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50,
      maxVUs: 400,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:POST /api/v1/jobs}': ['p(95)<250', 'p(99)<500'],
    dropped_iterations: ['count<100'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  submit(emailJob());
}

export const handleSummary = summary('submit');

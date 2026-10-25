// Steady load: 1,000 events/s for 10 minutes across three tenants (10 events per request).
// Targets: ingest p99 < 50 ms (server side, see Grafana), no failed requests.
import { sendBatch } from './lib.js';

const RATE = Number(__ENV.EVENTS_PER_SEC || 1000);
const BATCH = 10;

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE / BATCH,
      timeUnit: '1s',
      duration: __ENV.DURATION || '10m',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    checks: ['rate>0.999'],
    http_req_failed: ['rate<0.001'],
  },
};

const tenants = ['acme', 'globex', 'initech'];
export default function () {
  sendBatch(tenants[__ITER % tenants.length], BATCH);
}

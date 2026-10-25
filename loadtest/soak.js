// Soak: 300 events/s for 1 hour, single-event requests (exercises the per-request path).
import { sendOne } from './lib.js';

export const options = {
  scenarios: {
    soak: { executor: 'constant-arrival-rate', rate: Number(__ENV.EVENTS_PER_SEC || 300), timeUnit: '1s', duration: __ENV.DURATION || '1h', preAllocatedVUs: 50, maxVUs: 200 },
  },
  thresholds: { checks: ['rate>0.999'], 'http_req_duration{expected_response:true}': ['p(99)<250'] },
};

const tenants = ['acme', 'globex', 'initech'];
export default function () {
  sendOne(tenants[__ITER % tenants.length]);
}

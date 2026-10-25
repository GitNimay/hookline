// Spike: ramp to 2,000 events/s, hold, ramp down.
import { sendBatch } from './lib.js';

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 400,
      stages: [
        { target: 200, duration: '30s' }, // 2,000 events/s at 10 per request
        { target: 200, duration: '2m' },
        { target: 10, duration: '30s' },
      ],
    },
  },
  thresholds: { checks: ['rate>0.999'] },
};

const tenants = ['acme', 'globex', 'initech'];
export default function () {
  sendBatch(tenants[__ITER % tenants.length], 10);
}

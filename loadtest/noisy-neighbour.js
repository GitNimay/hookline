// Noisy neighbour: acme floods (default 1M events as fast as allowed), globex sends 10 events/s.
// Pass/fail: globex's p99 delivery latency, read from the chaos customer afterwards
// (`hookctl chaos stats`, hook globex-a), must stay under 5 s.
import { sendBatch, sendOne } from './lib.js';

const FLOOD = Number(__ENV.FLOOD_EVENTS || 1000000);

export const options = {
  scenarios: {
    flood: { executor: 'shared-iterations', exec: 'flood', vus: 40, iterations: FLOOD / 100, maxDuration: '30m' },
    victim: { executor: 'constant-arrival-rate', exec: 'victim', rate: 10, timeUnit: '1s', duration: __ENV.DURATION || '10m', preAllocatedVUs: 5 },
  },
};

export function flood() {
  sendBatch('acme', 100);
}

export function victim() {
  sendOne('globex');
}

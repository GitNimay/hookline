// Shared helpers for the k6 scripts. Keys come from `hookctl seed` (.hookline/seed.json).
import http from 'k6/http';
import { check } from 'k6';

export const INGEST = __ENV.HOOKLINE_INGEST_URL || 'http://localhost:8080';
const seed = JSON.parse(open(__ENV.SEED_FILE || '../.hookline/seed.json'));
export const keys = Object.fromEntries(Object.entries(seed.tenants).map(([n, t]) => [n, t.key]));

const types = ['order.created', 'order.paid', 'user.signed_up'];

export function event(i) {
  return {
    event_type: types[i % types.length],
    payload: { id: `obj_${Date.now()}_${i}`, amount: Math.floor(Math.random() * 50000), currency: 'EUR' },
  };
}

// sendBatch posts `size` events for one tenant in one request.
export function sendBatch(tenant, size) {
  const events = Array.from({ length: size }, (_, i) => event(__ITER * size + i));
  const res = http.post(`${INGEST}/v1/events:batch`, JSON.stringify({ events }), {
    headers: { Authorization: `Bearer ${keys[tenant]}`, 'Content-Type': 'application/json' },
    tags: { tenant },
  });
  check(res, { accepted: (r) => r.status === 202 || r.status === 200 });
  return res;
}

export function sendOne(tenant) {
  const res = http.post(`${INGEST}/v1/events`, JSON.stringify(event(__ITER)), {
    headers: { Authorization: `Bearer ${keys[tenant]}`, 'Content-Type': 'application/json' },
    tags: { tenant },
  });
  check(res, { accepted: (r) => r.status === 202 });
  return res;
}

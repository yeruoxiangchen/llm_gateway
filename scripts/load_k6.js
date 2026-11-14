import http from 'k6/http';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import { Counter, Rate } from 'k6/metrics';

const clients = new SharedArray('clients', () => JSON.parse(open(__ENV.LLMGW_CLIENTS)).clients);
const valid = new Rate('valid_usage');
const accepted = new Counter('accepted_responses');
const tokens = new Counter('reported_tokens');

export const options = {
  scenarios: { requests: {
    executor: 'constant-arrival-rate', rate: Number(__ENV.LLMGW_RATE), timeUnit: '1s',
    duration: `${__ENV.LLMGW_DURATION}s`, preAllocatedVUs: Number(__ENV.LLMGW_VUS),
    maxVUs: Number(__ENV.LLMGW_VUS), gracefulStop: '15s',
  } },
  thresholds: {
    http_req_failed: ['rate==0'], valid_usage: ['rate==1'],
    dropped_iterations: ['count==0'], http_req_duration: ['p(95)<1000', 'p(99)<2000'],
  },
  summaryTrendStats: ['min', 'avg', 'p(50)', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const iteration = exec.scenario.iterationInTest;
  const client = clients[iteration % clients.length];
  const response = http.post(`${__ENV.LLMGW_TARGET}/v1/chat/completions`, JSON.stringify({
    model: __ENV.LLMGW_ALIAS, messages: [{ role: 'user', content: 'hello' }], max_tokens: 16, stream: false,
  }), { headers: { Authorization: `Bearer ${client.key}`, 'Content-Type': 'application/json',
                  'Idempotency-Key': `${__ENV.LLMGW_CASE}-${iteration}` }, timeout: '10s' });
  let usage = null;
  try { usage = response.json('usage'); } catch (_) {}
  const ok = response.status === 200 && usage !== null && usage.total_tokens === 12;
  valid.add(ok);
  if (ok) { accepted.add(1); tokens.add(usage.total_tokens); }
}

export function handleSummary(data) {
  return { [__ENV.LLMGW_SUMMARY]: JSON.stringify(data, null, 2) };
}

import { test, expect } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';
import {
  validate, schemas, SAMPLES, EVENT_TYPES, CONSUMERS, BUSINESS_ACTIONS,
} from '../mock-server/index.js';

const COMMON = ['event_id', 'event_type', 'version', 'occurred_at', 'org_id', 'site_id', 'payload'];
const CONSUMER_KEYS = ['sop_dispatch', 'room_status_update', 'payment_plan_generation'];

// Each test starts from a clean in-memory bus via the documented hook.
test.beforeEach(async ({ request }) => {
  const res = await request.post('/api/v1/_test/reset');
  expect(res.ok()).toBeTruthy();
});

// ---------- Criterion 1: runnable service, seven contracts, demo UI ----------
test('service health, metadata and the seven contracts are available', async ({ request, page }) => {
  const health = await (await request.get('/api/v1/health')).json();
  expect(health.ok).toBe(true);
  expect(health.service).toBe('domain-event-contract');
  expect(health.contracts).toBe(7);

  const meta = await (await request.get('/api/v1/meta')).json();
  expect(meta.event_types).toHaveLength(7);
  expect(meta.common_fields).toEqual(COMMON);
  expect(meta.consumers).toHaveLength(3);

  const contracts = await (await request.get('/api/v1/contracts')).json();
  expect(contracts.count).toBe(7);
  expect(contracts.contracts.map((c) => c.event_type).sort()).toEqual([...EVENT_TYPES].sort());

  page.on('pageerror', (e) => { throw e; });
  await page.goto('/');
  await expect(page.getByTestId('tab-contracts')).toBeVisible();
  await expect(page.getByTestId('panel-contracts')).toBeVisible();
});

// ---------- Criterion 1: contract file shape ----------
test('seven schema files each declare the common fields and a typed payload', () => {
  const dir = path.join(process.cwd(), 'contracts', 'events');
  const files = fs.readdirSync(dir).filter((f) => f.endsWith('.schema.json'));
  expect(files).toHaveLength(7);
  for (const file of files) {
    const s = JSON.parse(fs.readFileSync(path.join(dir, file), 'utf8'));
    expect(s.type).toBe('object');
    expect(s.required).toEqual(COMMON);
    expect(s.properties.payload.type).toBe('object');
    expect(s.properties.payload.required.length).toBeGreaterThanOrEqual(4);
    expect(s.additionalProperties).toBe(true);
    expect(s.properties.payload.additionalProperties).toBe(true);
  }
  expect(Object.keys(schemas).sort()).toEqual([...EVENT_TYPES].sort());
});

// ---------- Validator: accepts valid/forward-compatible, rejects invalid ----------
test('self-written validator accepts valid and unknown fields, rejects missing or wrong fields', () => {
  const sample = SAMPLES.events.find((e) => e.event_type === 'contract.signed');
  expect(validate(sample, schemas['contract.signed']).valid).toBe(true);

  const unknown = JSON.parse(JSON.stringify(sample));
  unknown.future_header_field = 'from-a-later-version';
  unknown.payload.new_metric = { score: 99 };
  expect(validate(unknown, schemas['contract.signed']).valid).toBe(true);

  const missingPayloadField = JSON.parse(JSON.stringify(sample));
  delete missingPayloadField.payload.contract_id;
  let result = validate(missingPayloadField, schemas['contract.signed']);
  expect(result.valid).toBe(false);
  expect(result.errors.some((e) => e.path === '$.payload.contract_id' && e.code === 'REQUIRED')).toBe(true);

  const missingOrg = JSON.parse(JSON.stringify(sample));
  delete missingOrg.org_id;
  result = validate(missingOrg, schemas['contract.signed']);
  expect(result.valid).toBe(false);
  expect(result.errors.some((e) => e.path === '$.org_id' && e.code === 'REQUIRED')).toBe(true);

  const wrongType = JSON.parse(JSON.stringify(sample));
  wrongType.event_type = 'unknown.event';
  expect(validate(wrongType, schemas['contract.signed']).valid).toBe(false);

  const wrongDate = JSON.parse(JSON.stringify(sample));
  wrongDate.occurred_at = 'not-a-date';
  expect(validate(wrongDate, schemas['contract.signed']).errors.some((e) => e.code === 'FORMAT')).toBe(true);
});

// ---------- Criterion 1: producers for all seven event types ----------
test('all seven business-action producers emit schema-valid events into the stream', async ({ request }) => {
  const emitted = [];
  expect(Object.keys(BUSINESS_ACTIONS)).toHaveLength(7);
  for (const action of Object.keys(BUSINESS_ACTIONS)) {
    const res = await request.post('/api/v1/business/' + action, { data: {} });
    expect(res.ok(), action).toBeTruthy();
    const body = await res.json();
    expect(body.ok, action).toBe(true);
    expect(body.event.event_type).toBe(BUSINESS_ACTIONS[action].eventType);
    expect(validate(body.event, schemas[body.event.event_type]).valid).toBe(true);
    emitted.push(body.event);
  }
  const stream = await (await request.get('/api/v1/stream')).json();
  expect(stream.count).toBe(7 + emitted.length);
  const ids = stream.entries.map((e) => e.data.event_id);
  for (const e of emitted) expect(ids).toContain(e.event_id);
  expect(new Set(ids).size).toBe(ids.length);
});

// ---------- Criterion 2: one signed contract handled once by each consumer ----------
test('contract.signed is handled exactly once by each of the three consumers', async ({ request }) => {
  const res = await request.post('/api/v1/consume/all', { data: { sample: 'contract.signed' } });
  expect(res.ok()).toBeTruthy();
  const body = await res.json();
  expect(body.results).toHaveLength(3);
  expect(body.results.every((r) => r.ok)).toBe(true);

  const consumers = await (await request.get('/api/v1/consumers')).json();
  const handled = consumers.processed.filter((r) => r.event_type === 'contract.signed');
  expect(handled).toHaveLength(3);
  expect(handled.map((r) => r.consumer).sort()).toEqual([...CONSUMER_KEYS].sort());
  for (const key of CONSUMER_KEYS) {
    expect(handled.filter((r) => r.consumer === key)).toHaveLength(1);
    expect(handled.find((r) => r.consumer === key).outcome.kind).toBeTruthy();
  }
  expect((await (await request.get('/api/v1/dlq')).json()).count).toBe(0);
});

// ---------- Criterion 3: v1 consumers ignore unknown fields ----------
test('an event with unknown fields is processed normally by v1 consumers', async ({ request }) => {
  const res = await request.post('/api/v1/consume/all', { data: { sample: 'unknown_fields' } });
  expect(res.ok()).toBeTruthy();
  const body = await res.json();
  expect(body.results).toHaveLength(3);
  expect(body.results.every((r) => r.ok)).toBe(true);

  const consumers = await (await request.get('/api/v1/consumers')).json();
  expect(consumers.processed.filter((r) => r.event_id === SAMPLES.edge_cases.unknown_fields.event_id)).toHaveLength(3);
  expect((await (await request.get('/api/v1/dlq')).json()).count).toBe(0);
});

// ---------- Criterion 4: missing org_id -> DLQ with reason, later events unaffected ----------
test('missing org_id goes to the dead-letter queue with a reason and does not block later events', async ({ request }) => {
  const bad = SAMPLES.edge_cases.missing_org_id;
  const good = SAMPLES.events.find((e) => e.event_type === 'contract.signed');
  const res = await request.post('/api/v1/consume/batch', { data: { events: [bad, good] } });
  expect(res.ok()).toBeTruthy();
  const body = await res.json();
  expect(body.submitted).toBe(2);

  const dlq = await (await request.get('/api/v1/dlq')).json();
  expect(dlq.count).toBeGreaterThan(0);
  expect(dlq.entries.every((d) => typeof d.reason === 'string' && d.reason.length > 0)).toBe(true);
  expect(dlq.entries.some((d) => d.reason.includes('org_id'))).toBe(true);
  expect(dlq.entries.some((d) => d.event && d.event.event_id === bad.event_id)).toBe(true);

  // The later, valid event is still handled once by every consumer.
  const consumers = await (await request.get('/api/v1/consumers')).json();
  const later = consumers.processed.filter((r) => r.event_id === good.event_id);
  expect(later).toHaveLength(3);
  expect(later.map((r) => r.consumer).sort()).toEqual([...CONSUMER_KEYS].sort());
});

// ---------- Each consumer is individually addressable ----------
test('each consumer can be addressed individually', async ({ request }) => {
  for (const key of CONSUMER_KEYS) {
    const res = await request.post('/api/v1/consume/' + key, { data: { sample: 'contract.signed' } });
    expect(res.ok(), key).toBeTruthy();
    expect((await res.json()).ok).toBe(true);
  }
  const consumers = await (await request.get('/api/v1/consumers')).json();
  expect(consumers.count).toBe(3);
  for (const key of CONSUMER_KEYS) {
    expect(consumers.processed.filter((r) => r.consumer === key)).toHaveLength(1);
  }
});

// ---------- Bundled samples and honest self-test summary ----------
test('bundled samples cover seven valid events plus two edge cases', () => {
  expect(SAMPLES.events).toHaveLength(7);
  expect(new Set(SAMPLES.events.map((e) => e.event_type)).size).toBe(7);
  for (const e of SAMPLES.events) expect(validate(e, schemas[e.event_type]).valid).toBe(true);
  expect(SAMPLES.edge_cases.unknown_fields).toBeTruthy();
  expect(SAMPLES.edge_cases.missing_org_id.org_id).toBeUndefined();
  expect(validate(SAMPLES.edge_cases.unknown_fields, schemas['contract.signed']).valid).toBe(true);
  expect(validate(SAMPLES.edge_cases.missing_org_id, schemas['lead.assigned']).valid).toBe(false);
});

test('self-test summary reports all checks green', async ({ request }) => {
  const s = await (await request.get('/api/v1/selftest')).json();
  expect(s.ok).toBe(true);
  expect(s.schema_count).toBe(7);
  expect(s.producer_actions).toBe(7);
  expect(s.consumer_count).toBe(3);
  expect(s.checks.all_contracts_loaded).toBe(true);
  expect(s.checks.all_samples_valid).toBe(true);
  expect(s.checks.dlq_reasons_nonempty).toBe(true);
  expect(s.sample_validation.every((v) => v.valid)).toBe(true);
});

// ---------- UI demos ----------
test('demo UI lists contracts and shows the selected schema', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByTestId('tab-contracts')).toBeVisible();
  await expect(page.locator('[data-testid^="contract-row-"]')).toHaveCount(7);
  await page.getByTestId('contract-schema-btn-contract.signed').click();
  await expect(page.getByTestId('contract-schema-json')).toContainText('contract.signed');
});

test('demo UI produces events, fans out to consumers and routes invalid events to the DLQ', async ({ page }) => {
  await page.goto('/');
  await page.getByTestId('tab-stream').click();
  await page.getByTestId('action-sign-contract').click();
  await expect(page.getByTestId('produced-result')).toContainText('contract.signed');

  await page.getByTestId('tab-consumers').click();
  await page.getByTestId('consume-contract-all').click();
  await expect(page.getByTestId('processed-count')).toContainText('3');

  await page.getByTestId('tab-dlq').click();
  await page.getByTestId('dlq-inject').click();
  await expect(page.getByTestId('first-dlq-reason')).toContainText('org_id');
});

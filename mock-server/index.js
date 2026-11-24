/**
 * Domain event contract verification service.
 *
 * Provides: a self-written JSON Schema validator (no extra packages), an
 * in-memory Redis Stream simulation, seven business-action producers, three
 * consumers (SOP dispatch / room status update / payment plan generation),
 * and a dead-letter queue with a non-empty reason for invalid events.
 *
 * IMPORTANT: every identifier is synthetic. No real personal information.
 * The stream is in-memory; swapping it for a real Redis Stream is the
 * documented production replacement point (see README).
 */
import express from 'express';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const PORT = Number(process.env.PORT || 3001);
const STREAM = 'domain.events';

const EVENT_TYPES = [
  'contract.signed', 'lead.assigned', 'lead.recycled', 'quote.approved',
  'room.status_changed', 'demand.published', 'certification.approved',
];

// ------------------------------------------------------------
// 1. Contract loading
// ------------------------------------------------------------
const contractsDir = path.join(ROOT, 'contracts', 'events');
const schemas = {};
for (const file of fs.readdirSync(contractsDir).filter((f) => f.endsWith('.schema.json')).sort()) {
  const schema = JSON.parse(fs.readFileSync(path.join(contractsDir, file), 'utf8'));
  schemas[schema.properties.event_type.const] = schema;
}
const SAMPLES = JSON.parse(fs.readFileSync(path.join(ROOT, 'data', 'samples.json'), 'utf8'));

// ------------------------------------------------------------
// 2. Self-written JSON Schema validator (jsonschema-equivalent subset)
//    Supports: type, const, enum, required, properties,
//    additionalProperties, minLength, pattern, format(date-time),
//    minimum, items. Unknown fields are accepted by default (v1 tolerant).
// ------------------------------------------------------------
const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const join = (p, k) => `${p}.${k}`;
const DATE_TIME_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;
const isDateTime = (v) => typeof v === 'string' && DATE_TIME_RE.test(v) && !Number.isNaN(Date.parse(v));
const kindOf = (v) => (Array.isArray(v) ? 'array' : v === null ? 'null' : typeof v);

function validate(instance, schema, at = '$') {
  const errors = [];
  const err = (p, message, code) => errors.push({ path: p, message, code });
  if (!isPlainObject(schema)) return { valid: true, errors };

  if (schema.type) {
    let ok = true;
    switch (schema.type) {
      case 'object': ok = isPlainObject(instance); break;
      case 'array': ok = Array.isArray(instance); break;
      case 'integer': ok = Number.isInteger(instance); break;
      case 'number': ok = typeof instance === 'number' && Number.isFinite(instance); break;
      default: ok = typeof instance === schema.type;
    }
    if (!ok) {
      err(at, `expected ${schema.type}, got ${kindOf(instance)}`, 'TYPE');
      return { valid: false, errors };
    }
  }
  if ('const' in schema && instance !== schema.const) {
    err(at, `expected const ${JSON.stringify(schema.const)}, got ${JSON.stringify(instance)}`, 'CONST');
  }
  if (Array.isArray(schema.enum) && !schema.enum.includes(instance)) {
    err(at, `value ${JSON.stringify(instance)} is not one of ${JSON.stringify(schema.enum)}`, 'ENUM');
  }
  if (typeof instance === 'string') {
    if (schema.minLength != null && instance.length < schema.minLength) {
      err(at, `string length ${instance.length} < minLength ${schema.minLength}`, 'MIN_LENGTH');
    }
    if (schema.pattern) {
      try { if (!new RegExp(schema.pattern).test(instance)) err(at, `does not match pattern ${schema.pattern}`, 'PATTERN'); }
      catch { /* invalid schema pattern is ignored, not an instance failure */ }
    }
    if (schema.format === 'date-time' && !isDateTime(instance)) {
      err(at, `expected ISO date-time, got ${JSON.stringify(instance)}`, 'FORMAT');
    }
  }
  if (typeof instance === 'number' && schema.minimum != null && instance < schema.minimum) {
    err(at, `value ${instance} < minimum ${schema.minimum}`, 'MINIMUM');
  }
  if (isPlainObject(instance)) {
    for (const key of schema.required || []) {
      if (!Object.prototype.hasOwnProperty.call(instance, key)) {
        err(join(at, key), 'missing required property', 'REQUIRED');
      }
    }
    const props = schema.properties || {};
    for (const [key, value] of Object.entries(instance)) {
      if (Object.prototype.hasOwnProperty.call(props, key)) {
        errors.push(...validate(value, props[key], join(at, key)).errors);
      } else if (schema.additionalProperties === false) {
        err(join(at, key), 'additional property is not allowed', 'ADDITIONAL_PROPERTY');
      }
      // additionalProperties === true / undefined: unknown fields ignored.
    }
  }
  if (Array.isArray(instance) && schema.items) {
    instance.forEach((item, i) => errors.push(...validate(item, schema.items, `${at}[${i}]`).errors));
  }
  return { valid: errors.length === 0, errors };
}

const reasonFrom = (errors = []) => errors.map((e) => `${e.path}: ${e.message}`).join('; ') || 'validation failed';

// ------------------------------------------------------------
// 3. In-memory Redis Stream simulation + dead-letter queue
// ------------------------------------------------------------
const streams = new Map();
let streamSeq = 0;
const dlq = [];
const processed = [];

function xadd(stream, data) {
  if (!streams.has(stream)) streams.set(stream, []);
  const entry = { id: `${Date.now()}-${streamSeq++}`, data: JSON.parse(JSON.stringify(data)) };
  streams.get(stream).push(entry);
  return entry;
}
function xrange(stream) { return (streams.get(stream) || []).map((e) => ({ ...e })); }
function xlen(stream) { return (streams.get(stream) || []).length; }

function toDLQ(event, errors, source) {
  const rec = {
    id: `dlq_${crypto.randomBytes(5).toString('hex')}`,
    at: new Date().toISOString(),
    source,
    event: event ?? null,
    reason: reasonFrom(errors),
    errors,
  };
  dlq.push(rec);
  return rec;
}

// ------------------------------------------------------------
// 4. Producers: one business action per event type
// ------------------------------------------------------------
const nowIso = () => new Date().toISOString();
const pick = (obj, key, fallback) => (obj && obj[key] !== undefined ? obj[key] : fallback);

const PAYLOAD_BUILDERS = {
  'contract.signed': (o = {}) => ({
    contract_id: pick(o, 'contract_id', 'ctr-10001'),
    rider_id: pick(o, 'rider_id', 'rider-20001'),
    signed_at: pick(o, 'signed_at', nowIso()),
    channel: pick(o, 'channel', 'online'),
    amount: pick(o, 'amount', 1280),
    currency: pick(o, 'currency', 'CNY'),
  }),
  'lead.assigned': (o = {}) => ({
    lead_id: pick(o, 'lead_id', 'lead-30001'),
    assignee_id: pick(o, 'assignee_id', 'staff-40001'),
    station_id: pick(o, 'station_id', 'st-sh-001'),
    assigned_at: pick(o, 'assigned_at', nowIso()),
    source: pick(o, 'source', 'web_form'),
  }),
  'lead.recycled': (o = {}) => ({
    lead_id: pick(o, 'lead_id', 'lead-30001'),
    reason: pick(o, 'reason', 'no_response'),
    recycled_at: pick(o, 'recycled_at', nowIso()),
    previous_assignee_id: pick(o, 'previous_assignee_id', 'staff-40001'),
  }),
  'quote.approved': (o = {}) => ({
    quote_id: pick(o, 'quote_id', 'quote-50001'),
    contract_id: pick(o, 'contract_id', 'ctr-10001'),
    amount: pick(o, 'amount', 2400),
    currency: pick(o, 'currency', 'CNY'),
    approved_by: pick(o, 'approved_by', 'manager-60001'),
    approved_at: pick(o, 'approved_at', nowIso()),
  }),
  'room.status_changed': (o = {}) => ({
    room_id: pick(o, 'room_id', 'room-70001'),
    from_status: pick(o, 'from_status', 'reserved'),
    to_status: pick(o, 'to_status', 'occupied'),
    changed_at: pick(o, 'changed_at', nowIso()),
    changed_by: pick(o, 'changed_by', 'staff-60002'),
  }),
  'demand.published': (o = {}) => ({
    demand_id: pick(o, 'demand_id', 'demand-80001'),
    title: pick(o, 'title', 'Weekend delivery riders'),
    headcount: pick(o, 'headcount', 12),
    city: pick(o, 'city', '示例市A'),
    published_at: pick(o, 'published_at', nowIso()),
    channel: pick(o, 'channel', 'partner'),
  }),
  'certification.approved': (o = {}) => ({
    certification_id: pick(o, 'certification_id', 'cert-90001'),
    subject_id: pick(o, 'subject_id', 'rider-20001'),
    cert_type: pick(o, 'cert_type', 'rider_safety'),
    approved_by: pick(o, 'approved_by', 'auditor-10001'),
    approved_at: pick(o, 'approved_at', nowIso()),
    expires_at: pick(o, 'expires_at', new Date(Date.now() + 365 * 86400000).toISOString()),
  }),
};

function makeEvent(eventType, payload) {
  return {
    event_id: `evt_${crypto.randomBytes(8).toString('hex')}`,
    event_type: eventType,
    version: 'v1',
    occurred_at: nowIso(),
    org_id: 'org-demo-001',
    site_id: 'site-demo-sh-01',
    payload,
  };
}

/** Validate first, then append to the stream; invalid events go to the DLQ. */
function produceEvent(eventType, overrides = {}) {
  const schema = schemas[eventType];
  if (!schema) {
    const rec = toDLQ(overrides.event || { event_type: eventType },
      [{ path: '$.event_type', message: `no contract for ${eventType}` }], 'producer');
    return { ok: false, code: 'UNKNOWN_EVENT_TYPE', reason: rec.reason, dlq_id: rec.id };
  }
  const event = overrides.event
    ? JSON.parse(JSON.stringify(overrides.event))
    : makeEvent(eventType, PAYLOAD_BUILDERS[eventType](overrides.payload || {}));
  const result = validate(event, schema);
  if (!result.valid) {
    const rec = toDLQ(event, result.errors, 'producer');
    return { ok: false, code: 'SCHEMA_VIOLATION', reason: rec.reason, dlq_id: rec.id, event, errors: result.errors };
  }
  const entry = xadd(STREAM, event);
  return { ok: true, event, stream_id: entry.id, validation: result };
}

const BUSINESS_ACTIONS = {
  'sign-contract': { eventType: 'contract.signed', label: 'Sign service contract' },
  'assign-lead': { eventType: 'lead.assigned', label: 'Assign candidate lead' },
  'recycle-lead': { eventType: 'lead.recycled', label: 'Recycle candidate lead' },
  'approve-quote': { eventType: 'quote.approved', label: 'Approve service quote' },
  'change-room-status': { eventType: 'room.status_changed', label: 'Change room status' },
  'publish-demand': { eventType: 'demand.published', label: 'Publish hiring demand' },
  'approve-certification': { eventType: 'certification.approved', label: 'Approve certification' },
};

function runBusinessAction(action, overrides = {}) {
  const spec = BUSINESS_ACTIONS[action];
  if (!spec) return { ok: false, code: 'UNKNOWN_ACTION', reason: `unknown business action: ${action}` };
  const produced = produceEvent(spec.eventType, overrides);
  return { ...produced, action, label: spec.label };
}

// ------------------------------------------------------------
// 5. Consumers: each validates, then records a handled outcome
// ------------------------------------------------------------
const CONSUMERS = {
  sop_dispatch: {
    name: 'SOP dispatch',
    label: 'SOP派发',
    description: 'Creates SOP to-dos for lead/contract/certification events; logs other events.',
    handle(event) {
      const p = event.payload || {};
      return {
        kind: 'sop_todo',
        ref: `sop-${String(event.event_id).slice(4, 12)}`,
        related_type: event.event_type,
        subject: p.lead_id || p.contract_id || p.certification_id || event.event_type,
      };
    },
  },
  room_status_update: {
    name: 'Room status update',
    label: '房间状态更新',
    description: 'Applies room state transitions; records association for other event types.',
    handle(event) {
      const p = event.payload || {};
      return {
        kind: 'room_snapshot',
        room_id: p.room_id || 'n/a',
        to_status: p.to_status || 'unchanged',
        related_type: event.event_type,
      };
    },
  },
  payment_plan_generation: {
    name: 'Payment plan generation',
    label: '付款计划生成',
    description: 'Generates payment plans for approved quotes and signed contracts.',
    handle(event) {
      const p = event.payload || {};
      return {
        kind: 'payment_plan',
        plan_id: `plan-${String(event.event_id).slice(4, 12)}`,
        amount: p.amount ?? null,
        currency: p.currency || null,
        related_type: event.event_type,
      };
    },
  },
};

function deliverToConsumer(consumerKey, event) {
  const consumer = CONSUMERS[consumerKey];
  if (!consumer) return { ok: false, code: 'UNKNOWN_CONSUMER', reason: `unknown consumer: ${consumerKey}` };
  const schema = schemas[event && event.event_type];
  if (!schema) {
    const rec = toDLQ(event, [{ path: '$.event_type', message: `no contract for ${event && event.event_type}` }],
      `consumer:${consumerKey}`);
    return { ok: false, code: 'UNKNOWN_EVENT_TYPE', reason: rec.reason, dlq_id: rec.id };
  }
  const result = validate(event, schema);
  if (!result.valid) {
    const rec = toDLQ(event, result.errors, `consumer:${consumerKey}`);
    return { ok: false, code: 'SCHEMA_VIOLATION', reason: rec.reason, dlq_id: rec.id, errors: result.errors };
  }
  const record = {
    id: `prc_${crypto.randomBytes(5).toString('hex')}`,
    at: nowIso(),
    consumer: consumerKey,
    consumer_name: consumer.name,
    event_id: event.event_id,
    event_type: event.event_type,
    outcome: consumer.handle(event),
  };
  processed.push(record);
  return { ok: true, record };
}

function deliverToAll(event) {
  return Object.keys(CONSUMERS).map((key) => ({ consumer: key, ...deliverToConsumer(key, event) }));
}

/** Invalid events are skipped and recorded; subsequent events keep flowing. */
function consumeBatch(events) {
  return (events || []).map((event) => ({
    event_id: event && event.event_id,
    results: deliverToAll(event),
  }));
}

function resolveEvent(body = {}) {
  if (body.event) return body.event;
  if (body.sample) {
    if (SAMPLES.edge_cases[body.sample]) return SAMPLES.edge_cases[body.sample];
    const found = SAMPLES.events.find((e) => e.event_type === body.sample);
    if (found) return found;
  }
  return null;
}

function resetAll() {
  streams.clear();
  dlq.length = 0;
  processed.length = 0;
  streamSeq = 0;
  for (const event of SAMPLES.events) xadd(STREAM, event);
  return {
    contracts: Object.keys(schemas).length,
    stream_events: xlen(STREAM),
    dlq: dlq.length,
    processed: processed.length,
    producer_actions: Object.keys(BUSINESS_ACTIONS).length,
    consumers: Object.keys(CONSUMERS).length,
  };
}
resetAll();

// ------------------------------------------------------------
// 6. Express API
// ------------------------------------------------------------
const app = express();
app.use(express.json());

app.get('/api/v1/health', (_req, res) => res.json({
  ok: true,
  service: 'domain-event-contract',
  ts: Date.now(),
  contracts: Object.keys(schemas).length,
  stream_events: xlen(STREAM),
  dlq: dlq.length,
}));

app.get('/api/v1/meta', (_req, res) => res.json({
  version: 'v1',
  event_types: EVENT_TYPES,
  common_fields: ['event_id', 'event_type', 'version', 'occurred_at', 'org_id', 'site_id', 'payload'],
  producers: BUSINESS_ACTIONS,
  consumers: Object.entries(CONSUMERS).map(([key, c]) => ({ key, name: c.name, label: c.label, description: c.description })),
  stream: { name: STREAM, kind: 'in-memory Redis Stream simulation' },
  demo_boundary: 'In-memory stream, DLQ and consumer records; swap for real Redis Stream and durable stores in production.',
}));

app.get('/api/v1/contracts', (_req, res) => res.json({
  count: Object.keys(schemas).length,
  contracts: Object.entries(schemas).map(([eventType, schema]) => ({
    event_type: eventType,
    schema_file: `${eventType}.schema.json`,
    title: schema.title,
    required: schema.required,
    payload_required: schema.properties.payload.required,
  })),
}));

app.get('/api/v1/contracts/:type', (req, res) => {
  const schema = schemas[req.params.type];
  if (!schema) return res.status(404).json({ code: 'CONTRACT_NOT_FOUND', message: `no contract for ${req.params.type}` });
  res.json({ event_type: req.params.type, schema });
});

app.get('/api/v1/samples', (_req, res) => res.json(SAMPLES));

app.post('/api/v1/business/:action', (req, res) => {
  const result = runBusinessAction(req.params.action, req.body || {});
  res.status(result.ok ? 200 : 400).json(result);
});

app.get('/api/v1/stream', (_req, res) => res.json({ name: STREAM, count: xlen(STREAM), entries: xrange(STREAM) }));
app.get('/api/v1/stream/:name', (req, res) => res.json({
  name: req.params.name, count: xlen(req.params.name), entries: xrange(req.params.name),
}));

app.post('/api/v1/consume/all', (req, res) => {
  const event = resolveEvent(req.body || {});
  if (!event) return res.status(400).json({ code: 'EVENT_REQUIRED', message: 'provide event or sample' });
  const results = deliverToAll(event);
  // A business-level rejection is expressed in results[].ok, not in the HTTP status.
  res.json({ event_id: event.event_id, results });
});

app.post('/api/v1/consume/batch', (req, res) => {
  const events = Array.isArray(req.body?.events) ? req.body.events : [];
  const results = consumeBatch(events);
  res.json({ submitted: events.length, results, dlq_count: dlq.length });
});

app.post('/api/v1/consume/:consumer', (req, res) => {
  const event = resolveEvent(req.body || {});
  if (!event) return res.status(400).json({ code: 'EVENT_REQUIRED', message: 'provide event or sample' });
  const result = deliverToConsumer(req.params.consumer, event);
  res.json(result);
});

app.get('/api/v1/consumers', (_req, res) => res.json({
  definitions: Object.entries(CONSUMERS).map(([key, c]) => ({ key, name: c.name, label: c.label, description: c.description })),
  processed,
  count: processed.length,
}));

app.get('/api/v1/dlq', (_req, res) => res.json({ count: dlq.length, entries: dlq }));

app.get('/api/v1/selftest', (_req, res) => {
  const sampleValidation = SAMPLES.events.map((e) => ({
    event_type: e.event_type,
    valid: validate(e, schemas[e.event_type]).valid,
  }));
  res.json({
    ok: sampleValidation.every((s) => s.valid) && Object.keys(schemas).length === EVENT_TYPES.length,
    schema_count: Object.keys(schemas).length,
    event_types: EVENT_TYPES,
    producer_actions: Object.keys(BUSINESS_ACTIONS).length,
    consumer_count: Object.keys(CONSUMERS).length,
    stream: { name: STREAM, count: xlen(STREAM) },
    processed_count: processed.length,
    dlq_count: dlq.length,
    sample_validation: sampleValidation,
    checks: {
      all_contracts_loaded: Object.keys(schemas).length === EVENT_TYPES.length,
      all_samples_valid: sampleValidation.every((s) => s.valid),
      dlq_reasons_nonempty: dlq.every((d) => typeof d.reason === 'string' && d.reason.length > 0),
    },
    demo_boundary: 'In-memory Redis Stream simulation; no real database or Redis connection.',
  });
});

const testHooksEnabled = () => process.env.NODE_ENV !== 'production' || process.env.ALLOW_TEST_HOOKS === '1';
app.post('/api/v1/_test/reset', (_req, res) => {
  if (!testHooksEnabled()) {
    return res.status(404).json({ code: 'TEST_HOOKS_DISABLED', message: 'test hooks are disabled in production' });
  }
  res.json(resetAll());
});

// ------------------------------------------------------------
// 7. Exports and boot (import-safe for tests)
// ------------------------------------------------------------
function startServer(port = PORT) {
  return app.listen(port, '0.0.0.0', () => {
    console.log(`[event-contract-mock] listening on http://0.0.0.0:${port}  stream=${STREAM} contracts=${Object.keys(schemas).length}`);
  });
}

export {
  app, schemas, EVENT_TYPES, SAMPLES, STREAM, CONSUMERS, BUSINESS_ACTIONS,
  streams, dlq, processed,
  validate, xadd, xrange, xlen, produceEvent, runBusinessAction,
  deliverToConsumer, deliverToAll, consumeBatch, toDLQ, resetAll, startServer,
};

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectRun) startServer();

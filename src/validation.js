import { createHash } from 'node:crypto';

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export function object(value, name = 'Body') {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new ApiError(400, `${name} must be an object.`);
  }
  return value;
}

export function integer(value, min, max, name) {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new ApiError(400, `${name} must be an integer from ${min} to ${max}.`);
  }
  return value;
}

export function identifier(value, name) {
  if (typeof value !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(value)) {
    throw new ApiError(400, `${name} must be 1–64 characters using letters, digits, dots, hyphens or underscores.`);
  }
  return value;
}

function canonical(value, depth = 0) {
  if (depth > 16) throw new ApiError(400, 'Payload nesting exceeds 16 levels.');
  if (Array.isArray(value)) return value.map((entry) => canonical(entry, depth + 1));
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [
    key, canonical(value[key], depth + 1),
  ]));
}

export function normalizeJob(input) {
  object(input);
  const {
    tenant_id, kind, payload = {}, priority = 1, cost = 1, max_attempts = 3,
    delay_ms = 0, idempotency_key = null,
  } = input;
  identifier(tenant_id, 'tenant_id');
  if (!['echo', 'hash', 'wait', 'flaky'].includes(kind)) {
    throw new ApiError(400, 'kind must be echo, hash, wait or flaky.');
  }
  object(payload, 'payload');
  if (kind === 'wait' || kind === 'flaky') {
    integer(payload.duration_ms ?? 250, 0, 60_000, 'payload.duration_ms');
  }
  if (kind === 'flaky') integer(payload.fail_attempts ?? 1, 0, 10, 'payload.fail_attempts');
  integer(priority, 0, 2, 'priority');
  integer(cost, 1, 1_000, 'cost');
  integer(max_attempts, 1, 10, 'max_attempts');
  integer(delay_ms, 0, 86_400_000, 'delay_ms');
  if (idempotency_key !== null) identifier(idempotency_key, 'idempotency_key');

  const job = { tenant_id, kind, payload: JSON.stringify(canonical(payload)), priority, cost, max_attempts, delay_ms };
  return {
    ...job,
    idempotency_key,
    request_hash: idempotency_key === null ? null : createHash('sha256').update(JSON.stringify(job)).digest('hex'),
  };
}

export function normalizeTenant(input) {
  object(input);
  const { id, name, weight = 1, max_running = 2 } = input;
  identifier(id, 'id');
  if (typeof name !== 'string' || name.trim().length < 1 || name.trim().length > 60) {
    throw new ApiError(400, 'name must be 1–60 characters.');
  }
  return {
    id, name: name.trim(), weight: integer(weight, 1, 100, 'weight'),
    max_running: integer(max_running, 1, 64, 'max_running'),
  };
}

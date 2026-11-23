import { randomUUID } from 'node:crypto';
import { chargeService, selectTenant } from './policy.js';
import { SQLite } from './sqlite.js';
import { ApiError } from './validation.js';

function publicJob(row) {
  if (!row) return null;
  const { seq, lease_token, request_hash, payload, result, ...job } = row;
  return { ...job, payload: JSON.parse(payload), result: result === null ? null : JSON.parse(result) };
}

export class Ledger {
  constructor(database, { leaseMs = 6_000, agingMs = 30_000 } = {}) {
    this.database = database;
    this.leaseMs = leaseMs;
    this.agingMs = agingMs;
  }

  close() {
    this.closing ??= this.database.close();
    return this.closing;
  }

  addTenant(tenant) {
    return this.database.transaction(async (db) => {
      const { id, name, weight, max_running } = tenant;
      const changes = await db.run(`
        INSERT INTO tenants (id, name, weight, max_running) VALUES (?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
      `, id, name, weight, max_running);
      if (!changes) throw new ApiError(409, 'Tenant already exists.');
      return tenant;
    });
  }

  event(db, id, now, event, details) {
    return db.run('INSERT INTO job_events (job_id, at, event, details) VALUES (?, ?, ?, ?)',
      id, now, event, JSON.stringify(details));
  }

  jobRow(db, id) {
    return db.one('SELECT * FROM jobs WHERE id = ?', id);
  }

  getJob(id) {
    return this.database.transaction(async (db) => {
      const job = publicJob(await this.jobRow(db, id));
      if (!job) throw new ApiError(404, 'Job not found.');
      const events = await db.all('SELECT at, event, details FROM job_events WHERE job_id = ? ORDER BY seq', id);
      return { ...job, events: events.map((row) => ({ ...row, details: JSON.parse(row.details) })) };
    }, { readOnly: true });
  }

  async insert(db, input, now) {
    const { tenant_id, kind, payload, priority, cost, max_attempts, delay_ms, idempotency_key, request_hash } = input;
    if (idempotency_key !== null) {
      const existing = await db.one('SELECT * FROM jobs WHERE tenant_id = ? AND idempotency_key = ?',
        tenant_id, idempotency_key);
      if (existing) {
        if (existing.request_hash !== request_hash) throw new ApiError(409, 'Idempotency key already used with another request.');
        return { created: false, job: publicJob(existing) };
      }
    }

    if (!await db.one('SELECT 1 FROM tenants WHERE id = ?', tenant_id)) {
      throw new ApiError(404, 'Tenant not found.');
    }
    const id = randomUUID();
    const available = now + delay_ms;
    const job = await db.one(`
      INSERT INTO jobs (id, tenant_id, kind, payload, status, priority, cost, max_attempts,
        created_at, available_at, updated_at, idempotency_key, request_hash)
      VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)
      RETURNING *
    `, id, tenant_id, kind, payload, priority, cost, max_attempts, now, available, now, idempotency_key, request_hash);
    await this.event(db, id, now, 'submitted', { available_at: available });
    return { created: true, job: publicJob(job) };
  }

  submit(input) {
    return this.database.transaction(async (db) => this.insert(db, input, await db.now()));
  }

  submitBatch(inputs) {
    return this.database.transaction(async (db) => {
      const now = await db.now();
      const jobs = [];
      for (const input of inputs) jobs.push(await this.insert(db, input, now));
      return jobs;
    });
  }

  registerWorker(worker, slots) {
    return this.database.transaction(async (db) => {
      const now = await db.now();
      await db.run(`
        INSERT INTO workers (id, last_seen, slots) VALUES (?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET last_seen = excluded.last_seen, slots = excluded.slots
      `, worker, now, slots);
      return { worker_id: worker, slots, protocol_version: 1 };
    });
  }

  seen(db, worker, now) {
    return db.run(`
      INSERT INTO workers (id, last_seen) VALUES (?, ?)
      ON CONFLICT (id) DO UPDATE SET last_seen = excluded.last_seen
      WHERE workers.last_seen < excluded.last_seen - 750
    `, worker, now);
  }

  claim(worker) {
    return this.database.transaction(async (db) => {
      const now = await db.now();
      await this.seen(db, worker, now);
      await this.recover(db, now);
      const state = await db.one('SELECT floor, dispatches FROM scheduler_state WHERE id = 1');
      const candidates = (await db.all(`
        SELECT t.*, (SELECT count(*) FROM jobs r WHERE r.tenant_id = t.id AND r.status = 'running') AS running
        FROM tenants t
        WHERE EXISTS (
          SELECT 1 FROM jobs q WHERE q.tenant_id = t.id AND q.status = 'queued' AND q.available_at <= ?
        )
      `, now)).filter((tenant) => tenant.running < tenant.max_running);
      const winner = selectTenant(candidates, state.floor);
      if (!winner) return null;

      const { tenant, service } = winner;
      const aged = await db.one(`
        SELECT * FROM jobs WHERE tenant_id = ? AND status = 'queued' AND available_at <= ?
        ORDER BY available_at, seq LIMIT 1
      `, tenant.id, now - this.agingMs);
      const byPriority = `
        SELECT * FROM jobs WHERE tenant_id = ? AND status = 'queued' AND priority = ? AND available_at <= ?
        ORDER BY available_at, seq LIMIT 1
      `;
      const job = aged ?? await db.one(byPriority, tenant.id, 2, now) ??
        await db.one(byPriority, tenant.id, 1, now) ?? await db.one(byPriority, tenant.id, 0, now);
      const charged = chargeService(service, job.cost, tenant.weight);
      const token = randomUUID();
      const dispatch = state.dispatches + 1;
      await db.run('UPDATE scheduler_state SET floor = ?, dispatches = ? WHERE id = 1', service, dispatch);
      await db.run('UPDATE tenants SET virtual_service = ?, last_dispatch = ? WHERE id = ?', charged, dispatch, tenant.id);
      const claimed = await db.one(`
        UPDATE jobs SET status = 'running', attempts = attempts + 1,
          first_started_at = COALESCE(first_started_at, ?), updated_at = ?, worker_id = ?,
          lease_token = ?, lease_until = ?, error = NULL WHERE id = ?
        RETURNING *
      `, now, now, worker, token, now + this.leaseMs, job.id);
      await this.event(db, job.id, now, 'claimed', {
        worker_id: worker, attempt: job.attempts + 1, dispatch,
        eligible_tenants: candidates.length, service_before: service, service_after: charged,
        weight: tenant.weight, cost: job.cost, priority: job.priority,
        running: tenant.running + 1, max_running: tenant.max_running,
        rule: aged ? 'aging' : 'priority',
      });
      return { ...publicJob(claimed), lease_token: token, lease_ms: this.leaseMs };
    });
  }

  async currentLease(db, id, token, now) {
    const job = await this.jobRow(db, id);
    if (!job || job.status !== 'running' || job.lease_token !== token || job.lease_until <= now) {
      throw new ApiError(409, 'Lease is expired or no longer current.');
    }
    return job;
  }

  heartbeat(id, token) {
    return this.database.transaction(async (db) => {
      const now = await db.now();
      const job = await this.currentLease(db, id, token, now);
      await this.seen(db, job.worker_id, now);
      await db.run('UPDATE jobs SET lease_until = ? WHERE id = ?', now + this.leaseMs, id);
      return { lease_until: now + this.leaseMs };
    });
  }

  finish(id, token, outcome) {
    return this.database.transaction(async (db) => {
      const now = await db.now();
      const job = await this.currentLease(db, id, token, now);
      await this.seen(db, job.worker_id, now);
      if (outcome.ok) {
        await db.run(`
          UPDATE jobs SET status = 'succeeded', updated_at = ?, result = ?,
            lease_token = NULL, lease_until = NULL, worker_id = NULL WHERE id = ?
        `, now, JSON.stringify(outcome.result ?? null), id);
        await this.event(db, id, now, 'succeeded', { attempt: job.attempts, worker_id: job.worker_id });
      } else {
        await this.fail(db, job, now, outcome.error, outcome.retryable, 'handler_failed');
      }
      return publicJob(await this.jobRow(db, id));
    });
  }

  async fail(db, job, now, error, retryable, reason) {
    const retry = retryable && job.attempts < job.max_attempts;
    const available = retry ? now + Math.min(30_000, 500 * 2 ** (job.attempts - 1)) : job.available_at;
    await db.run(`
      UPDATE jobs SET status = ?, updated_at = ?, error = ?, available_at = ?,
        lease_token = NULL, lease_until = NULL, worker_id = NULL WHERE id = ?
    `, retry ? 'queued' : 'dead_letter', now, error, available, job.id);
    await this.event(db, job.id, now, retry ? 'retry_scheduled' : 'dead_lettered', {
      attempt: job.attempts, worker_id: job.worker_id, reason, error,
      ...(retry ? { available_at: available } : {}),
    });
  }

  async recover(db, now) {
    const expired = await db.all(`
      SELECT * FROM jobs WHERE status = 'running' AND lease_until <= ?
      ORDER BY lease_until, seq LIMIT 100
    `, now);
    for (const job of expired) await this.fail(db, job, now, 'Worker lease expired', true, 'lease_expired');
  }

  async sweep() {
    const expired = await this.database.transaction(async (db) => db.one(`
      SELECT 1 FROM jobs WHERE status = 'running' AND lease_until <= ? LIMIT 1
    `, await db.now()), { readOnly: true });
    if (expired) await this.database.transaction(async (db) => this.recover(db, await db.now()));
  }

  dashboard(status) {
    return this.database.transaction(async (db) => {
      const now = await db.now();
      const grouped = await db.all(`
        SELECT tenant_id, status, count(*) AS total,
          sum(CASE WHEN available_at <= ? THEN 1 ELSE 0 END) AS eligible,
          min(CASE WHEN available_at <= ? THEN available_at END) AS earliest
        FROM jobs GROUP BY tenant_id, status
      `, now, now);
      const counts = { queued: 0, running: 0, succeeded: 0, dead_letter: 0 };
      const records = await db.all(`SELECT id, name, weight, max_running FROM tenants ORDER BY ${db.tenantOrder}`);
      const byId = new Map(records.map((tenant) => [tenant.id, Object.assign(tenant, {
        ...counts, ready: 0, oldest_ready_ms: null,
      })]));
      for (const { tenant_id, status: state, total, eligible, earliest } of grouped) {
        const tenant = byId.get(tenant_id);
        counts[state] += total;
        tenant[state] = total;
        if (state === 'queued') {
          tenant.ready = eligible;
          tenant.oldest_ready_ms = earliest === null ? null : now - earliest;
        }
      }
      const columns = 'id, tenant_id, kind, status, priority, attempts, max_attempts, created_at, updated_at, worker_id';
      const jobs = status
        ? await db.all(`SELECT ${columns} FROM jobs WHERE status = ? ORDER BY seq DESC LIMIT 25`, status)
        : await db.all(`SELECT ${columns} FROM jobs ORDER BY seq DESC LIMIT 25`);
      const workers = await db.all(`
        SELECT w.id, w.last_seen, w.slots,
          (SELECT count(*) FROM jobs j WHERE j.worker_id = w.id AND j.status = 'running') AS running
        FROM workers w WHERE w.last_seen >= ? ORDER BY w.id
      `, now - Math.max(10_000, this.leaseMs * 2));
      const { dispatches } = await db.one('SELECT dispatches FROM scheduler_state WHERE id = 1');
      return { at: now, counts, tenants: records, jobs, workers, lease_ms: this.leaseMs, dispatches };
    }, { readOnly: true });
  }
}

export class Store extends Ledger {
  constructor(filename, { clock = Date.now, ...options } = {}) {
    super(new SQLite(filename, clock), options);
  }
}

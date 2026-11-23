import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { describe } from 'node:test';
import { postgresFixture } from '../scripts/postgres-fixture.js';
import { simulate } from '../src/scenario.js';
import { Store } from '../src/store.js';
import { normalizeJob } from '../src/validation.js';

async function engine(t, open) {
  let now = 10_000;
  const database = await open(t);
  const store = await database({ clock: () => now, leaseMs: 1_000, agingMs: 2_000 });
  return {
    store,
    submit: async (input = {}) => (await store.submit(normalizeJob({ tenant_id: 'northstar', kind: 'echo', ...input }))).job,
    advance: (ms) => { now += ms; },
  };
}

function contract(name, open) {
  describe(name, () => {
    test('dispatch spreads a burst across eligible tenants and persists the decision', async (t) => {
      const { store, submit } = await engine(t, open);
      for (let n = 0; n < 10; n++) await submit();
      for (const tenant_id of ['lumen', 'orbit', 'terrace']) await submit({ tenant_id });
      const jobs = [];
      for (let n = 0; n < 4; n++) jobs.push(await store.claim('worker'));
      assert.equal(new Set(jobs.map((job) => job.tenant_id)).size, 4);
      const { events, lease_token } = await store.getJob(jobs[0].id);
      assert.equal(events[1].event, 'claimed');
      assert.equal(events[1].details.eligible_tenants, 4);
      assert.equal(lease_token, undefined);
    });

    test('weighted share tracks estimated cost under continuously eligible demand', async (t) => {
      const { store, submit } = await engine(t, open);
      await store.addTenant({ id: 'a', name: 'A', weight: 2, max_running: 1 });
      await store.addTenant({ id: 'b', name: 'B', weight: 1, max_running: 1 });
      for (let n = 0; n < 100; n++) {
        await submit({ tenant_id: 'a', cost: 3 });
        await submit({ tenant_id: 'b', cost: 1 });
      }
      const service = { a: 0, b: 0 };
      for (let n = 0; n < 100; n++) {
        const job = await store.claim('worker');
        service[job.tenant_id] += job.cost;
        await store.finish(job.id, job.lease_token, { ok: true });
      }
      assert.ok(Math.abs(service.a - 2 * service.b) <= 3, JSON.stringify(service));
    });

    test('new tenants join at the global floor and cannot spend accumulated idle credit', async (t) => {
      const { store, submit } = await engine(t, open);
      for (let n = 0; n < 35; n++) await submit();
      for (let n = 0; n < 20; n++) {
        const job = await store.claim('worker');
        await store.finish(job.id, job.lease_token, { ok: true });
      }
      for (let n = 0; n < 10; n++) await submit({ tenant_id: 'lumen' });
      const ids = [];
      for (let n = 0; n < 6; n++) {
        const job = await store.claim('worker');
        ids.push(job.tenant_id);
        await store.finish(job.id, job.lease_token, { ok: true });
      }
      assert.equal(ids.filter((id) => id === 'lumen').length, 3);
    });

    test('concurrent limits, eligibility, priority, and aging', async (t) => {
      const { store, submit, advance } = await engine(t, open);
      const older = await submit({ tenant_id: 'orbit', priority: 0 });
      const high = await submit({ tenant_id: 'orbit', priority: 2 });
      const delayed = await submit({ tenant_id: 'orbit', priority: 2, delay_ms: 5_000 });
      const first = await store.claim('worker-1');
      assert.equal(first.id, high.id);
      assert.equal(await store.claim('worker-2'), null);
      await store.finish(first.id, first.lease_token, { ok: true });
      advance(2_000);
      const later = await submit({ tenant_id: 'orbit', priority: 2 });
      const second = await store.claim('worker-2');
      assert.equal(second.id, older.id);
      assert.equal((await store.getJob(second.id)).events.at(-1).details.rule, 'aging');
      await store.finish(second.id, second.lease_token, { ok: true });
      const third = await store.claim('worker-3');
      assert.equal(third.id, later.id);
      await store.finish(third.id, third.lease_token, { ok: true });
      assert.equal(await store.claim('worker-3'), null);
      advance(3_000);
      assert.equal((await store.claim('worker-3')).id, delayed.id);
    });

    test('heartbeat postpones recovery, expired leases are fenced, and exhausted jobs dead-letter', async (t) => {
      const { store, submit, advance } = await engine(t, open);
      const job = await submit({ max_attempts: 2 });
      const first = await store.claim('worker-1');
      advance(700);
      await store.heartbeat(job.id, first.lease_token);
      advance(350);
      await store.sweep();
      assert.equal((await store.getJob(job.id)).status, 'running');

      advance(650);
      await assert.rejects(store.finish(job.id, first.lease_token, { ok: true }), { status: 409 });
      await store.sweep();
      assert.equal((await store.getJob(job.id)).events.at(-1).details.reason, 'lease_expired');
      assert.equal(await store.claim('worker-2'), null);
      advance(500);
      const second = await store.claim('worker-2');
      assert.equal(second.id, first.id);
      assert.notEqual(second.lease_token, first.lease_token);
      await assert.rejects(store.heartbeat(job.id, first.lease_token), { status: 409 });
      await store.finish(job.id, second.lease_token, { ok: false, error: 'Still failing', retryable: true });
      assert.equal((await store.getJob(job.id)).status, 'dead_letter');
    });

    test('a restart preserves job state, dispatch history, and tenant-scoped idempotency', async (t) => {
      const database = await open(t, true);
      const original = await database();
      const submitted = normalizeJob({
        tenant_id: 'northstar', kind: 'echo', payload: { z: 2, a: 1 }, idempotency_key: 'order-42',
      });
      const { job } = await original.submit(submitted);
      const lease = await original.claim('worker');
      await original.finish(job.id, lease.lease_token, { ok: true, result: { accepted: true } });
      await original.close();

      const reopened = await database();
      const repeated = normalizeJob({
        tenant_id: 'northstar', kind: 'echo', payload: { a: 1, z: 2 }, idempotency_key: 'order-42',
      });
      const restored = await reopened.getJob(job.id);
      assert.deepEqual(restored.result, { accepted: true });
      assert.deepEqual(restored.events.map((e) => e.event), ['submitted', 'claimed', 'succeeded']);
      assert.equal((await reopened.dashboard()).dispatches, 1);
      assert.equal((await reopened.submit(repeated)).created, false);
      await assert.rejects(reopened.submit(normalizeJob({ ...repeated, payload: {} })), { status: 409 });
      assert.equal((await reopened.submit(normalizeJob({ ...repeated, tenant_id: 'lumen', payload: {} }))).created, true);
    });
  });
}

contract('SQLite ledger', (t, persistent) => {
  const directory = persistent ? mkdtempSync(join(tmpdir(), 'fairslot-')) : null;
  const stores = [];
  t.after(async () => {
    for (const store of stores) await store.close();
    if (directory) rmSync(directory, { recursive: true, force: true });
  });
  return (options) => {
    const store = new Store(directory ? join(directory, 'ledger.db') : ':memory:', options);
    stores.push(store);
    return store;
  };
});

if (process.env.FAIRSLOT_TEST_DATABASE_URL) {
  const { openPostgresStore } = await import('../src/postgres.js');
  contract('PostgreSQL ledger', async (t) => {
    const { url, cleanups } = await postgresFixture(t);
    return async (options) => {
      const store = await openPostgresStore(url, options);
      cleanups.push(() => store.close());
      return store;
    };
  });
}

test('the reference simulation isolates smaller tenants from the bulk queue', () => {
  const result = simulate();
  assert.equal(result.total, 101_000);
  assert.equal(new Set(result.fairslot.first_24.slice(0, 4)).size, 4);
  assert.equal(new Set(result.fifo.first_24).size, 1);
  for (const tenant of result.fifo.tenants.slice(1)) assert.ok(tenant.first_ms >= 6_250_000);
  for (const tenant of result.fairslot.tenants.slice(1)) assert.equal(tenant.first_ms, 0);
});

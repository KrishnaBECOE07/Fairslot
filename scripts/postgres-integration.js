import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import test from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { postgresFixture } from './postgres-fixture.js';

test('PostgreSQL coordinators share claims and a worker survives the loss of its coordinator', { timeout: 25_000 }, async (t) => {
  const { url, cleanups } = await postgresFixture(t);

  function launch(file, env, stdout = 'ignore') {
    const processEnv = { ...process.env, ...env };
    const child = spawn(process.execPath, [file], { env: processEnv, stdio: ['ignore', stdout, 'inherit'] });
    const exit = once(child, 'exit');
    exit.catch(() => {});
    const stop = async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      await exit;
    };
    cleanups.push(stop);
    return { child, exit, stop };
  }

  async function coordinator() {
    const running = launch('src/server.js', {
      FAIRSLOT_DATABASE_URL: url, FAIRSLOT_HOST: '127.0.0.1', FAIRSLOT_PORT: '0',
      FAIRSLOT_KEY: '', FAIRSLOT_LEASE_MS: '2400', FAIRSLOT_AGING_MS: '30000',
    }, 'pipe');
    const lines = createInterface({ input: running.child.stdout });
    try {
      const base = await Promise.race([
        once(lines, 'line', { signal: AbortSignal.timeout(8_000) }).then(([line]) => {
          const match = /^FairSlot coordinator listening on (http:\/\/127\.0\.0\.1:\d+)$/.exec(line);
          assert.ok(match, line);
          return match[1];
        }),
        running.exit.then(([code]) => assert.fail(`Coordinator exited before it was ready (code ${code}).`)),
      ]);
      return { base, stop: running.stop };
    } finally {
      lines.close();
      running.child.stdout.resume();
    }
  }

  async function http(base, path, body) {
    const response = await fetch(`${base}${path}`, {
      signal: AbortSignal.timeout(4_000),
      ...(body && { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
    });
    return { status: response.status, data: response.status === 204 ? null : await response.json() };
  }

  async function expect(base, path, body, status = 200) {
    const response = await http(base, path, body);
    assert.equal(response.status, status, JSON.stringify(response.data));
    return response.data;
  }

  async function waitForJob(base, id, status) {
    for (let i = 0; i < 120; i++) {
      const job = await expect(base, `/api/jobs/${id}`);
      if (job.status === status) return job;
      await sleep(75);
    }
    assert.fail(`Job ${id} did not reach ${status}.`);
  }

  const [first, second] = await Promise.all([coordinator(), coordinator()]);
  const a = first.base;
  const b = second.base;
  const echo = { tenant_id: 'orbit', kind: 'echo', idempotency_key: 'concurrent-submit' };
  const submissions = await Promise.all([http(a, '/api/jobs', echo), http(b, '/api/jobs', echo)]);
  assert.deepEqual(submissions.map(({ status }) => status).sort(), [200, 201]);
  assert.equal(submissions[0].data.id, submissions[1].data.id);
  await expect(a, '/api/jobs', { tenant_id: 'orbit', kind: 'echo' }, 201);

  const claims = await Promise.all(Array.from({ length: 8 }, (_, i) =>
    http(i % 2 ? a : b, '/api/worker/v1/claim', { worker_id: `manual-${i}` })));
  assert.deepEqual(claims.map(({ status }) => status).sort(), [200, 204, 204, 204, 204, 204, 204, 204]);
  const winner = claims.findIndex(({ status }) => status === 200);
  const leased = claims[winner].data;
  assert.equal(leased.id, submissions[0].data.id);
  const finish = { lease_token: leased.lease_token, ok: true };
  const other = winner % 2 ? b : a;
  await expect(other, `/api/worker/v1/jobs/${leased.id}/finish`, finish);
  await expect(a, `/api/worker/jobs/${leased.id}/finish`, finish, 409);

  for (const tenant_id of ['northstar', 'lumen', 'terrace']) {
    await expect(a, '/api/jobs', { tenant_id, kind: 'echo' }, 201);
  }
  const wave = await Promise.all(Array.from({ length: 4 }, (_, i) =>
    expect(i % 2 ? a : b, '/api/worker/v1/claim', { worker_id: `wave-${i}` })));
  assert.equal(new Set(wave.map(({ id }) => id)).size, 4);
  assert.equal(new Set(wave.map(({ tenant_id }) => tenant_id)).size, 4);
  for (const job of wave) {
    await expect(b, `/api/worker/v1/jobs/${job.id}/finish`, { ok: true, lease_token: job.lease_token });
  }

  const work = await expect(a, '/api/jobs', { tenant_id: 'orbit', kind: 'wait', payload: { duration_ms: 4_200 } }, 201);
  launch('src/worker.js', { FAIRSLOT_URLS: `${a},${b}`, FAIRSLOT_WORKER_ID: 'failover-worker', FAIRSLOT_SLOTS: '1', FAIRSLOT_KEY: '' });
  const running = await waitForJob(b, work.id, 'running');
  assert.equal(running.worker_id, 'failover-worker');
  const dashboard = await expect(b, '/api/dashboard');
  assert.equal(dashboard.workers.find(({ id }) => id === 'failover-worker')?.slots, 1);
  await first.stop();
  const finished = await waitForJob(b, work.id, 'succeeded');
  assert.equal(finished.attempts, 1);
  assert.deepEqual(finished.events.map(({ event }) => event), ['submitted', 'claimed', 'succeeded']);

  const next = await expect(b, '/api/jobs', { tenant_id: 'lumen', kind: 'echo' }, 201);
  assert.equal((await waitForJob(b, next.id, 'succeeded')).attempts, 1);

  const restarted = await coordinator();
  assert.deepEqual(await expect(restarted.base, `/api/jobs/${work.id}`), finished);
  const final = await expect(restarted.base, '/api/dashboard');
  assert.deepEqual(final.counts, { queued: 0, running: 0, succeeded: 7, dead_letter: 0 });
  assert.equal(final.dispatches, 7);
  const jobs = await Promise.all(final.jobs.map(({ id }) => expect(restarted.base, `/api/jobs/${id}`)));
  const dispatches = jobs.flatMap(({ events }) => events.filter(({ event }) => event === 'claimed')
    .map(({ details }) => details.dispatch)).sort((left, right) => left - right);
  assert.deepEqual(dispatches, [1, 2, 3, 4, 5, 6, 7]);
});

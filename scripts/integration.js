import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { startCoordinator } from '../src/server.js';

test('a replacement HTTP worker recovers a crashed attempt and its result survives restart', { timeout: 18_000 }, async (t) => {
  const directory = mkdtempSync(join(tmpdir(), 'fairslot-http-'));
  const filename = join(directory, 'ledger.db');
  let coordinator;
  let baseUrl;
  const children = [];

  async function listen() {
    coordinator = startCoordinator({ port: 0, filename, leaseMs: 2_400 });
    await once(coordinator, 'listening');
    baseUrl = `http://127.0.0.1:${coordinator.address().port}`;
  }

  async function stopCoordinator() {
    if (!coordinator?.listening) return;
    const closed = once(coordinator, 'close');
    coordinator.close();
    coordinator.closeAllConnections();
    await closed;
  }

  function worker(id) {
    const child = spawn(process.execPath, ['src/worker.js'], {
      env: { ...process.env, FAIRSLOT_URLS: baseUrl, FAIRSLOT_WORKER_ID: id, FAIRSLOT_SLOTS: '1', FAIRSLOT_KEY: '' },
      stdio: ['ignore', 'ignore', 'inherit'],
    });
    const exit = once(child, 'exit');
    children.push({ child, exit });
    return { child, exit };
  }

  async function http(path, body, status = 200) {
    const response = await fetch(`${baseUrl}${path}`, {
      signal: AbortSignal.timeout(2_000),
      ...(body && { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
    });
    const output = await response.json();
    assert.equal(response.status, status, JSON.stringify(output));
    return output;
  }

  async function waitForJob(id, predicate) {
    for (let n = 0; n < 120; n++) {
      const job = await http(`/api/jobs/${id}`);
      if (predicate(job)) return job;
      await sleep(100);
    }
    assert.fail('Job did not reach the expected state.');
  }

  t.after(async () => {
    for (const { child, exit } of children) {
      child.kill();
      await exit;
    }
    await stopCoordinator();
    rmSync(directory, { recursive: true, force: true });
  });

  await listen();
  assert.equal((await http('/health')).status, 'ok');
  const submission = { tenant_id: 'orbit', kind: 'wait', payload: { duration_ms: 3_200 }, idempotency_key: 'crash-demo' };
  const submitted = await http('/api/jobs', submission, 201);
  assert.equal((await http('/api/jobs', submission)).id, submitted.id);
  const first = worker('crash-test-1');
  await waitForJob(submitted.id, (job) => job.worker_id === 'crash-test-1');
  first.child.kill();
  await first.exit;

  worker('crash-test-2');
  const finished = await waitForJob(submitted.id, (job) => job.status === 'succeeded');
  assert.equal(finished.attempts, 2);
  assert.deepEqual(finished.result, { duration_ms: 3_200 });
  assert.deepEqual(finished.events.map((event) => event.event),
    ['submitted', 'claimed', 'retry_scheduled', 'claimed', 'succeeded']);
  assert.equal(finished.events[2].details.reason, 'lease_expired');
  assert.equal(finished.events[3].details.worker_id, 'crash-test-2');

  await stopCoordinator();
  await listen();
  const restored = await http(`/api/jobs/${submitted.id}`);
  assert.deepEqual(restored, finished);
});

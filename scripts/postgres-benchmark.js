import assert from 'node:assert/strict';
import { availableParallelism, cpus, platform, release } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { tenants } from '../src/scenario.js';
import { launchProcess, openPostgresFixture, startPostgresCoordinator } from './postgres-fixture.js';

const bulk = Number(process.argv[2] ?? 360);
if (!Number.isSafeInteger(bulk) || bulk < 20 || bulk > 5_000 || process.argv.length > 3) {
  throw new Error('Usage: npm run benchmark:postgres -- [bulk jobs, 20–5000]');
}

const workerCount = 4;
const slotsPerWorker = 2;
const durationMs = 25;
const counts = [bulk, 48, 36, 36];
const workload = tenants.flatMap(({ id }, index) => Array.from({ length: counts[index] }, () => ({
  tenant_id: id, kind: 'wait', payload: { duration_ms: durationMs },
})));

async function request(base, path, body) {
  const response = await fetch(`${base}${path}`, {
    signal: AbortSignal.timeout(5_000),
    ...(body && { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  });
  const data = await response.json();
  assert.equal(response.status, body ? 201 : 200, `${path}: ${JSON.stringify(data)}`);
  return data;
}

async function mapInBatches(items, operation) {
  const results = [];
  for (let i = 0; i < items.length; i += 8) {
    const batch = items.slice(i, i + 8);
    results.push(...await Promise.all(batch.map((item, index) => operation(item, i + index))));
  }
  return results;
}

function summarize(jobs, elapsed, parallelism) {
  const dispatchOrder = jobs.map((job) => {
    assert.equal(job.status, 'succeeded');
    assert.equal(job.attempts, 1);
    assert.deepEqual(job.result, { duration_ms: durationMs });
    assert.deepEqual(job.events.map(({ event }) => event), ['submitted', 'claimed', 'succeeded']);
    const { dispatch, running, max_running } = job.events[1].details;
    assert.ok(running <= max_running, `Tenant cap exceeded for ${job.tenant_id}`);
    return { dispatch, job };
  }).sort((left, right) => left.dispatch - right.dispatch);
  for (const [index, { dispatch }] of dispatchOrder.entries()) assert.equal(dispatch, index + 1);

  const firstFour = dispatchOrder.slice(0, tenants.length).map(({ job }) => job.tenant_id);
  assert.equal(new Set(firstFour).size, tenants.length);
  const start = dispatchOrder[0].job.first_started_at;
  const rows = tenants.map(({ id }, index) => {
    const waits = jobs.filter(({ tenant_id }) => tenant_id === id)
      .map(({ first_started_at }) => first_started_at - start).sort((left, right) => left - right);
    assert.equal(waits.length, counts[index]);
    const percentile = (p) => waits[Math.ceil(p * waits.length) - 1];
    return { tenant: id, completed: waits.length, first_ms: waits[0], p50_ms: percentile(0.5),
      p95_ms: percentile(0.95), p99_ms: percentile(0.99) };
  });
  const finish = jobs.reduce((latest, job) => Math.max(latest, job.events[2].at), start);
  const drainMs = finish - start;
  assert.ok(drainMs > 0);
  return { coordinators: parallelism, completed: jobs.length, drain_ms: drainMs,
    jobs_per_second: Number((jobs.length * 1_000 / drainMs).toFixed(1)),
    observed_ms_from_worker_launch: Math.round(elapsed), first_dispatches: firstFour, tenants: rows };
}

async function benchmark(coordinatorCount) {
  const { url, cleanups, close } = await openPostgresFixture();
  try {
    const coordinators = await Promise.all(Array.from({ length: coordinatorCount }, () =>
      startPostgresCoordinator(url, cleanups)));
    const selectCoordinator = (index) => coordinators[index % coordinatorCount].base;
    const submitted = await mapInBatches(workload, (job, index) => request(selectCoordinator(index), '/api/jobs', job));
    const poll = () => request(selectCoordinator(0), '/api/dashboard');
    const start = performance.now();
    const workers = Array.from({ length: workerCount }, (_, index) => launchProcess('src/worker.js', {
      FAIRSLOT_URLS: selectCoordinator(index), FAIRSLOT_WORKER_ID: `benchmark-${index + 1}`,
      FAIRSLOT_SLOTS: String(slotsPerWorker), FAIRSLOT_KEY: '',
    }, cleanups));

    const elapsed = await Promise.race([
      (async () => {
        while (performance.now() - start < 120_000) {
          const { counts, dispatches } = await poll();
          assert.equal(counts.dead_letter, 0, 'A benchmark job exhausted its attempts.');
          if (counts.succeeded === workload.length) {
            assert.deepEqual(counts, { queued: 0, running: 0, succeeded: workload.length, dead_letter: 0 });
            assert.equal(dispatches, workload.length, 'A job was dispatched more than once.');
            return performance.now() - start;
          }
          await sleep(100);
        }
        throw new Error(`Timed out waiting for ${workload.length} jobs to finish.`);
      })(),
      ...[...workers, ...coordinators].map(({ exit }) => exit.then(([code, signal]) => {
        throw new Error(`Benchmark process exited early (${signal ?? `code ${code}`}).`);
      })),
    ]);
    const jobs = await mapInBatches(submitted, ({ id }, index) => request(selectCoordinator(index), `/api/jobs/${id}`));
    return summarize(jobs, elapsed, coordinatorCount);
  } finally {
    await close();
  }
}

console.log(JSON.stringify({ node: process.version, os: `${platform()} ${release()}`, cpu: cpus()[0]?.model,
  logical_cpus_available: availableParallelism(), worker_processes: workerCount, slots_per_worker: slotsPerWorker,
  handler_duration_ms: durationMs, jobs_by_tenant: Object.fromEntries(tenants.map(({ id }, index) => [id, counts[index]])),
}, null, 2));
for (const coordinatorCount of [1, 2]) console.log(JSON.stringify(await benchmark(coordinatorCount), null, 2));

import { createHash, randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { identifier, integer } from './validation.js';

const coordinators = (process.env.FAIRSLOT_URLS ?? process.env.FAIRSLOT_URL ?? 'http://127.0.0.1:4317')
  .split(',').map((url) => {
    const coordinator = new URL(url.trim());
    if (!['http:', 'https:'].includes(coordinator.protocol)) throw new Error('Coordinator URLs must use HTTP or HTTPS.');
    return coordinator;
  });
let coordinatorIndex = 0;
let workerApi = '/api/worker/v1';
const worker = identifier(process.env.FAIRSLOT_WORKER_ID ??
  `${hostname().replace(/[^a-zA-Z0-9-]/g, '-').slice(0, 20) || 'worker'}-${randomUUID().slice(0, 8)}`, 'worker ID');
const slots = integer(Number(process.env.FAIRSLOT_SLOTS ?? 1), 1, 64, 'worker slots');
const headers = { 'Content-Type': 'application/json' };
if (process.env.FAIRSLOT_KEY) headers.Authorization = `Bearer ${process.env.FAIRSLOT_KEY}`;
const shutdown = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => shutdown.abort());

async function post(path, body, timeout = 3_000) {
  const index = coordinatorIndex;
  try {
    const response = await fetch(new URL(`${workerApi}${path}`, coordinators[index]), {
      method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeout),
    });
    if (response.status === 204) return null;
    const result = await response.json();
    if (!response.ok) throw Object.assign(new Error(result.error), { status: response.status });
    return result;
  } catch (error) {
    // A failed response can hide a committed claim. Rotate for the next call and let any orphaned lease expire.
    if (coordinatorIndex === index && (!error.status || error.status >= 500)) {
      coordinatorIndex = (index + 1) % coordinators.length;
    }
    throw error;
  }
}

async function handle(job, signal) {
  if (job.kind === 'echo') return { ok: true, result: job.payload };
  if (job.kind === 'hash') {
    return { ok: true, result: { sha256: createHash('sha256').update(JSON.stringify(job.payload)).digest('hex') } };
  }
  if (job.kind === 'wait' || job.kind === 'flaky') {
    const duration = job.payload.duration_ms ?? 250;
    await sleep(duration, undefined, { signal });
    if (job.kind === 'flaky' && job.attempts <= (job.payload.fail_attempts ?? 1)) {
      return { ok: false, error: 'Simulated transient handler failure', retryable: true };
    }
    return { ok: true, result: { duration_ms: duration } };
  }
  return { ok: false, error: 'Handler unavailable on this worker', retryable: false };
}

async function processJob(job) {
  const path = `/jobs/${job.id}`;
  const lease = { lease_token: job.lease_token };
  const controller = new AbortController();
  const interval = Math.floor(job.lease_ms / 3);
  const heartbeat = (async () => {
    let delay = interval;
    while (!controller.signal.aborted) {
      await sleep(delay, undefined, { signal: controller.signal });
      try {
        await post(`${path}/heartbeat`, lease, interval);
        delay = interval;
      } catch (error) {
        if (error.status === 409 || error.status === 401) controller.abort();
        delay = Math.floor(interval / 2);
      }
    }
  })().catch((error) => {
    if (error.name !== 'AbortError') console.error(error.message);
  });

  try {
    const outcome = await handle(job, controller.signal);
    while (!controller.signal.aborted) {
      try {
        await post(`${path}/finish`, { ...lease, ...outcome }, interval);
        return;
      } catch (error) {
        if (error.status && error.status < 500) throw error;
        await sleep(interval, undefined, { signal: controller.signal });
      }
    }
  } catch (error) {
    if (error.name !== 'AbortError' && error.status !== 409) console.error(`${job.id}: ${error.message}`);
  } finally {
    controller.abort();
    await heartbeat;
  }
}

async function work() {
  let delay = 200;
  while (!shutdown.signal.aborted) {
    try {
      const job = await post('/claim', { worker_id: worker });
      if (job) {
        delay = 200;
        await processJob(job);
        continue;
      }
    } catch (error) {
      if (delay === 200 || error.status === 401) console.error(`${worker}: ${error.message}`);
      if (error.status === 401) {
        process.exitCode = 1;
        shutdown.abort();
        return;
      }
    }
    await sleep(delay, undefined, { signal: shutdown.signal }).catch((error) => {
      if (error.name !== 'AbortError') throw error;
    });
    delay = Math.min(delay * 2, 1_600);
  }
}

async function register() {
  while (!shutdown.signal.aborted) {
    try {
      await post('/register', { worker_id: worker, slots });
      return true;
    } catch (error) {
      if (error.status === 404) {
        workerApi = '/api/worker';
        return true;
      }
      console.error(`${worker}: ${error.message}`);
      if (error.status >= 400 && error.status < 500) {
        process.exitCode = 1;
        return false;
      }
      await sleep(800, undefined, { signal: shutdown.signal }).catch((error) => {
        if (error.name !== 'AbortError') throw error;
      });
    }
  }
  return false;
}

if (await register()) {
  console.log(`${worker} online with ${slots} ${slots === 1 ? 'slot' : 'slots'}`);
  await Promise.all(Array.from({ length: slots }, () => work()));
}

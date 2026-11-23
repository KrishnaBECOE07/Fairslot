import { createHash, timingSafeEqual } from 'node:crypto';
import { mkdirSync, readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, resolve } from 'node:path';
import { simulate } from './scenario.js';
import { Store } from './store.js';
import { ApiError, identifier, integer, normalizeJob, normalizeTenant, object } from './validation.js';

const staticFiles = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/styles.css', ['styles.css', 'text/css; charset=utf-8']],
]);
const statuses = ['queued', 'running', 'succeeded', 'dead_letter'];
const localHosts = ['127.0.0.1', 'localhost', '::1', '[::1]'];

function json(response, status, body) {
  response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(body));
}

async function readJson(request, limit = 65_536) {
  if (request.headers['content-type']?.split(';')[0].trim().toLowerCase() !== 'application/json') {
    throw new ApiError(415, 'Content-Type must be application/json.');
  }
  if (Number(request.headers['content-length']) > limit) throw new ApiError(413, 'Request body is too large.');
  let size = 0;
  const chunks = [];
  for await (const chunk of request.iterator({ destroyOnReturn: false })) {
    size += chunk.length;
    if (size > limit) throw new ApiError(413, 'Request body is too large.');
    chunks.push(chunk);
  }
  let body;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new ApiError(400, 'Invalid JSON.');
  }
  return object(body);
}

const sha256 = (value) => createHash('sha256').update(value).digest();

export function makeServer(store, { key = '', localOnly = true } = {}) {
  const expectedKey = key ? sha256(`Bearer ${key}`) : null;
  return createServer({ requestTimeout: 15_000 }, async (request, response) => {
    response.setHeader('Cache-Control', 'no-store');
    response.setHeader('X-Content-Type-Options', 'nosniff');
    response.setHeader('Referrer-Policy', 'no-referrer');
    response.setHeader('Content-Security-Policy', "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'");

    try {
      const origin = new URL(`http://${request.headers.host}`);
      if (localOnly && !localHosts.includes(origin.hostname)) throw new ApiError(403, 'Host is not allowed.');
      if (request.headers.origin && request.headers.origin !== origin.origin) {
        throw new ApiError(403, 'Cross-origin requests are disabled.');
      }
      const url = new URL(request.url, origin);
      const { pathname } = url;
      const method = request.method;

      if (method === 'GET' && staticFiles.has(pathname)) {
        const [file, mime] = staticFiles.get(pathname);
        response.writeHead(200, { 'Content-Type': mime });
        return response.end(readFileSync(new URL(`../web/${file}`, import.meta.url)));
      }
      if (method === 'GET' && pathname === '/health') return json(response, 200, { status: 'ok' });
      if (expectedKey && !timingSafeEqual(expectedKey, sha256(request.headers.authorization ?? ''))) {
        throw new ApiError(401, 'A coordinator key is required.');
      }

      if (method === 'GET' && pathname === '/api/dashboard') {
        const filter = url.searchParams.get('status');
        if (filter !== null && !statuses.includes(filter)) throw new ApiError(400, 'Unknown status filter.');
        return json(response, 200, await store.dashboard(filter));
      }
      if (method === 'GET' && pathname === '/api/lab') {
        const bulk = integer(Number(url.searchParams.get('bulk') ?? 100_000), 100, 200_000, 'bulk');
        const workers = integer(Number(url.searchParams.get('workers') ?? 4), 1, 64, 'workers');
        return json(response, 200, simulate({ bulk, workers }));
      }
      if (method === 'POST' && pathname === '/api/tenants') {
        return json(response, 201, await store.addTenant(normalizeTenant(await readJson(request))));
      }
      if (method === 'POST' && pathname === '/api/jobs') {
        const result = await store.submit(normalizeJob(await readJson(request)));
        return json(response, result.created ? 201 : 200, result.job);
      }
      const detail = pathname.match(/^\/api\/jobs\/([a-zA-Z0-9-]+)$/);
      if (method === 'GET' && detail) return json(response, 200, await store.getJob(detail[1]));

      if (method === 'POST' && pathname === '/api/demo') {
        await readJson(request);
        const jobs = [
          ['northstar', 90, 'wait', 0],
          ['lumen', 12, 'wait', 2],
          ['terrace', 8, 'flaky', 1],
          ['orbit', 5, 'wait', 2],
        ].flatMap(([tenant_id, count, kind, priority]) =>
          Array.from({ length: count }, (_, index) => normalizeJob({
            tenant_id, kind, priority,
            max_attempts: 3,
            payload: {
              duration_ms: 340 + (index % 6) * 65,
              ...(kind === 'flaky' ? { fail_attempts: index % 4 === 0 ? 3 : 1 } : {}),
            },
          })),
        );
        return json(response, 201, { queued: (await store.submitBatch(jobs)).length });
      }
      if (method === 'POST' && pathname === '/api/worker/v1/register') {
        const body = await readJson(request);
        const worker = identifier(body.worker_id, 'worker_id');
        const slots = integer(body.slots, 1, 64, 'slots');
        return json(response, 200, await store.registerWorker(worker, slots));
      }
      const workerRoute = pathname.match(/^\/api\/worker\/(?:v1\/)?(.+)$/)?.[1];
      if (method === 'POST' && workerRoute === 'claim') {
        const body = await readJson(request);
        const job = await store.claim(identifier(body.worker_id, 'worker_id'));
        if (job) return json(response, 200, job);
        response.writeHead(204);
        return response.end();
      }
      const lease = workerRoute?.match(/^jobs\/([a-zA-Z0-9-]+)\/(heartbeat|finish)$/);
      if (method === 'POST' && lease) {
        const [, id, operation] = lease;
        const body = await readJson(request, 131_072);
        const token = identifier(body.lease_token, 'lease_token');
        if (operation === 'heartbeat') return json(response, 200, await store.heartbeat(id, token));
        if (typeof body.ok !== 'boolean') throw new ApiError(400, 'ok must be a boolean.');
        if (!body.ok && (typeof body.error !== 'string' || !body.error.trim() || body.error.length > 500 ||
            typeof body.retryable !== 'boolean')) {
          throw new ApiError(400, 'A failure requires an error message up to 500 characters and a retryable boolean.');
        }
        return json(response, 200, await store.finish(id, token, body));
      }
      throw new ApiError(404, 'Route not found.');
    } catch (error) {
      if (error instanceof ApiError) return json(response, error.status, { error: error.message });
      console.error(error);
      return json(response, 500, { error: 'Coordinator could not process the request.' });
    }
  });
}

export function startCoordinator({
  host = '127.0.0.1', port = 4317, filename = resolve('data/fairslot.db'),
  key = '', leaseMs = 6_000, agingMs = 30_000, store,
} = {}) {
  const localOnly = localHosts.includes(host);
  if (!localOnly && !key) throw new Error('A FAIRSLOT_KEY is required for a non-loopback bind.');
  if (!store) {
    if (filename !== ':memory:') mkdirSync(dirname(resolve(filename)), { recursive: true });
    store = new Store(filename, { leaseMs, agingMs });
  }
  const server = makeServer(store, { key, localOnly });
  let sweeping = false;
  const sweeper = setInterval(async () => {
    if (sweeping) return;
    sweeping = true;
    try {
      await store.sweep();
    } catch (error) {
      console.error('Lease sweep failed:', error);
    } finally {
      sweeping = false;
    }
  }, 500);
  sweeper.unref();
  server.once('close', () => {
    clearInterval(sweeper);
    store.close().catch((error) => console.error('Ledger close failed:', error));
  });
  server.listen(port, host);
  return server;
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(import.meta.filename)) {
  const config = {
    host: process.env.FAIRSLOT_HOST,
    port: integer(Number(process.env.FAIRSLOT_PORT ?? 4317), 0, 65_535, 'FAIRSLOT_PORT'),
    filename: process.env.FAIRSLOT_DB,
    key: process.env.FAIRSLOT_KEY,
    leaseMs: integer(Number(process.env.FAIRSLOT_LEASE_MS ?? 6_000), 900, 300_000, 'FAIRSLOT_LEASE_MS'),
    agingMs: integer(Number(process.env.FAIRSLOT_AGING_MS ?? 30_000), 0, 3_600_000, 'FAIRSLOT_AGING_MS'),
  };
  if (process.env.FAIRSLOT_DATABASE_URL) {
    const { openPostgresStore } = await import('./postgres.js');
    config.store = await openPostgresStore(process.env.FAIRSLOT_DATABASE_URL, config);
  }
  const server = startCoordinator(config);
  server.once('listening', () => {
    const { address, family, port } = server.address();
    const host = family === 'IPv6' ? `[${address}]` : address;
    console.log(`FairSlot coordinator listening on http://${host}:${port}`);
  });
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => server.close());
}

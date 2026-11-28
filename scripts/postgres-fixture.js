import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createInterface } from 'node:readline';

export async function openPostgresFixture() {
  const databaseUrl = process.env.FAIRSLOT_TEST_DATABASE_URL;
  if (!databaseUrl) throw new Error('Set FAIRSLOT_TEST_DATABASE_URL to a PostgreSQL test database.');
  const url = new URL(databaseUrl);
  const { default: { Pool } } = await import('pg');
  const pool = new Pool({ connectionString: databaseUrl, connectionTimeoutMillis: 3_000 });
  const schema = `fairslot_test_${randomUUID().replaceAll('-', '')}`;
  try {
    await pool.query(`CREATE SCHEMA ${schema}`);
  } catch (error) {
    await pool.end();
    throw error;
  }
  const options = url.searchParams.get('options');
  url.searchParams.set('options', `${options ? `${options} ` : ''}-csearch_path=${schema}`);
  const cleanups = [];
  async function close() {
    try {
      const results = await Promise.allSettled(cleanups.map((cleanup) => cleanup()));
      const failed = results.find((result) => result.status === 'rejected');
      if (failed) throw failed.reason;
    } finally {
      try {
        await pool.query(`DROP SCHEMA ${schema} CASCADE`);
      } finally {
        await pool.end();
      }
    }
  }
  return { url: url.toString(), cleanups, close };
}

export async function postgresFixture(t) {
  const fixture = await openPostgresFixture();
  t.after(fixture.close);
  return fixture;
}

export function launchProcess(file, env, cleanups, stdout = 'ignore') {
  const child = spawn(process.execPath, [file], {
    env: { ...process.env, ...env }, stdio: ['ignore', stdout, 'inherit'],
  });
  const exit = once(child, 'exit');
  exit.catch(() => {});
  const stop = async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exit;
  };
  cleanups.push(stop);
  return { child, exit, stop };
}

export async function startPostgresCoordinator(url, cleanups, { leaseMs = 6_000 } = {}) {
  const running = launchProcess('src/server.js', {
    FAIRSLOT_DATABASE_URL: url, FAIRSLOT_HOST: '127.0.0.1', FAIRSLOT_PORT: '0',
    FAIRSLOT_KEY: '', FAIRSLOT_LEASE_MS: String(leaseMs), FAIRSLOT_AGING_MS: '30000',
  }, cleanups, 'pipe');
  const lines = createInterface({ input: running.child.stdout });
  try {
    const base = await Promise.race([
      once(lines, 'line', { signal: AbortSignal.timeout(8_000) }).then(([line]) => {
        const match = /^FairSlot coordinator listening on (http:\/\/127\.0\.0\.1:\d+)$/.exec(line);
        if (!match) throw new Error(`Unexpected coordinator output: ${line}`);
        return match[1];
      }),
      running.exit.then(([code, signal]) => {
        throw new Error(`Coordinator exited before it was ready (${signal ?? `code ${code}`}).`);
      }),
    ]);
    return { base, stop: running.stop, exit: running.exit };
  } finally {
    lines.close();
    running.child.stdout.resume();
  }
}

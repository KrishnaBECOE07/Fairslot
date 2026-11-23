import { readFileSync } from 'node:fs';
import { tenants } from './scenario.js';
import { Ledger } from './store.js';

const migrations = [new URL('./migrations/001-initial.sql', import.meta.url)];

class PostgresSession {
  tenantOrder = 'seq';

  constructor(client, clock) {
    this.client = client;
    if (clock) this.now = clock;
  }

  query(sql, params) {
    // The shared ledger uses ? for parameters in its static SQL statements.
    let index = 0;
    return this.client.query(sql.replace(/\?/g, () => `$${++index}`), params);
  }

  async one(sql, ...params) { return (await this.query(sql, params)).rows[0]; }
  async all(sql, ...params) { return (await this.query(sql, params)).rows; }
  async run(sql, ...params) { return (await this.query(sql, params)).rowCount; }

  async now() {
    const { ms } = await this.one('SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS ms');
    return ms;
  }
}

async function transact(pool, begin, work) {
  const client = await pool.connect();
  let releaseError;
  try {
    await client.query(begin);
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      releaseError = rollbackError;
    }
    throw error;
  } finally {
    client.release(releaseError);
  }
}

async function migrate(pool) {
  await transact(pool, 'BEGIN ISOLATION LEVEL READ COMMITTED', async (client) => {
    // Concurrent coordinator startups must agree which migrations have run.
    await client.query('SELECT pg_advisory_xact_lock(1179861836, 1)');
    await client.query('CREATE TABLE IF NOT EXISTS fairslot_migrations (version INTEGER PRIMARY KEY)');
    const { rows } = await client.query('SELECT version FROM fairslot_migrations');
    const applied = new Set(rows.map(({ version }) => version));
    if (rows.some(({ version }) => version > migrations.length)) {
      throw new Error('The FairSlot database was migrated by a newer coordinator.');
    }
    for (const [index, file] of migrations.entries()) {
      const version = index + 1;
      if (applied.has(version)) continue;
      await client.query(readFileSync(file, 'utf8'));
      await client.query('INSERT INTO fairslot_migrations VALUES ($1)', [version]);
    }
    for (const { id, name, weight, max_running } of tenants) {
      await client.query(`INSERT INTO tenants (id, name, weight, max_running) VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO NOTHING`, [id, name, weight, max_running]);
    }
  });
}

export async function openPostgresStore(connectionString, { clock, ...options } = {}) {
  const { default: pg } = await import('pg').catch((error) => {
    if (error.code !== 'ERR_MODULE_NOT_FOUND') throw error;
    throw new Error('Install the PostgreSQL driver with npm ci --include=optional.', { cause: error });
  });
  const pool = new pg.Pool({
    connectionString,
    connectionTimeoutMillis: 3_000,
    statement_timeout: 5_000,
    types: {
      getTypeParser(oid, format) {
        if (oid !== pg.types.builtins.INT8 || format === 'binary') return pg.types.getTypeParser(oid, format);
        return (value) => {
          const number = Number(value);
          if (!Number.isSafeInteger(number)) throw new RangeError('FairSlot database integer exceeds safe precision.');
          return number;
        };
      },
    },
  });
  pool.on('error', (error) => console.error('Idle PostgreSQL connection failed:', error.message));
  try {
    await migrate(pool);
  } catch (error) {
    await pool.end();
    throw error;
  }

  return new Ledger({
    transaction(work, { readOnly = false } = {}) {
      const begin = readOnly ? 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY' : 'BEGIN ISOLATION LEVEL READ COMMITTED';
      return transact(pool, begin, async (client) => {
        // Serialize ledger writes across replicas, then sample database time for lease decisions.
        if (!readOnly) await client.query('SELECT id FROM scheduler_state WHERE id = 1 FOR UPDATE');
        return work(new PostgresSession(client, clock));
      });
    },
    close: () => pool.end(),
  }, options);
}

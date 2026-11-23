import { randomUUID } from 'node:crypto';

export async function postgresFixture(t) {
  const databaseUrl = process.env.FAIRSLOT_TEST_DATABASE_URL;
  if (!databaseUrl) throw new Error('Set FAIRSLOT_TEST_DATABASE_URL to a PostgreSQL test database.');
  const { default: { Pool } } = await import('pg');
  const pool = new Pool({ connectionString: databaseUrl, connectionTimeoutMillis: 3_000 });
  const schema = `fairslot_test_${randomUUID().replaceAll('-', '')}`;
  try {
    await pool.query(`CREATE SCHEMA ${schema}`);
  } catch (error) {
    await pool.end();
    throw error;
  }
  const url = new URL(databaseUrl);
  const options = url.searchParams.get('options');
  url.searchParams.set('options', `${options ? `${options} ` : ''}-csearch_path=${schema}`);
  const cleanups = [];
  t.after(async () => {
    try {
      await Promise.all(cleanups.map((cleanup) => cleanup()));
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
    } finally {
      await pool.end();
    }
  });
  return { url: url.toString(), cleanups };
}

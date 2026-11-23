import { readFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { tenants } from './scenario.js';

const schema = readFileSync(new URL('./schema.sql', import.meta.url), 'utf8');

export class SQLite {
  constructor(filename, clock) {
    this.db = new DatabaseSync(filename, { timeout: 2_000 });
    this.statements = new Map();
    this.now = clock;
    this.tenantOrder = 'rowid';
    this.pending = Promise.resolve();
    try {
      this.db.exec('BEGIN IMMEDIATE');
      this.db.exec(schema);
      if (!this.all('PRAGMA table_info(workers)').some(({ name }) => name === 'slots')) {
        this.db.exec('ALTER TABLE workers ADD COLUMN slots INTEGER CHECK (slots BETWEEN 1 AND 64)');
      }
      for (const { id, name, weight, max_running } of tenants) {
        this.run('INSERT OR IGNORE INTO tenants (id, name, weight, max_running) VALUES (?, ?, ?, ?)',
          id, name, weight, max_running);
      }
      this.db.exec('COMMIT');
    } catch (error) {
      this.db.close();
      throw error;
    }
  }

  statement(sql) {
    if (!this.statements.has(sql)) this.statements.set(sql, this.db.prepare(sql));
    return this.statements.get(sql);
  }

  one(sql, ...params) { return this.statement(sql).get(...params); }
  all(sql, ...params) { return this.statement(sql).all(...params); }
  run(sql, ...params) { return this.statement(sql).run(...params).changes; }

  transaction(work, { readOnly = false } = {}) {
    // Awaited ledger operations must hold exclusive use of this connection until commit.
    const result = this.pending.then(async () => {
      this.db.exec(readOnly ? 'BEGIN' : 'BEGIN IMMEDIATE');
      try {
        const value = await work(this);
        this.db.exec('COMMIT');
        return value;
      } catch (error) {
        this.db.exec('ROLLBACK');
        throw error;
      }
    });
    this.pending = result.catch(() => {});
    return result;
  }

  async close() {
    await this.pending;
    this.db.close();
  }
}

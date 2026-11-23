CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  weight REAL NOT NULL CHECK (weight > 0),
  max_running INTEGER NOT NULL CHECK (max_running > 0),
  virtual_service REAL NOT NULL DEFAULT 0,
  last_dispatch INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE IF NOT EXISTS jobs (
  seq INTEGER PRIMARY KEY,
  id TEXT NOT NULL UNIQUE,
  tenant_id TEXT NOT NULL REFERENCES tenants(id),
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'dead_letter')),
  priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 2),
  cost INTEGER NOT NULL CHECK (cost > 0),
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
  created_at INTEGER NOT NULL,
  available_at INTEGER NOT NULL,
  first_started_at INTEGER,
  updated_at INTEGER NOT NULL,
  worker_id TEXT,
  lease_token TEXT,
  lease_until INTEGER,
  result TEXT,
  error TEXT,
  idempotency_key TEXT,
  request_hash TEXT,
  UNIQUE (tenant_id, idempotency_key),
  CHECK (
    (status = 'running' AND worker_id IS NOT NULL AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
    OR (status != 'running' AND worker_id IS NULL AND lease_token IS NULL AND lease_until IS NULL)
  )
) STRICT;

CREATE INDEX IF NOT EXISTS jobs_tenant_state ON jobs(tenant_id, status, available_at);
CREATE INDEX IF NOT EXISTS jobs_ready_priority ON jobs(tenant_id, priority, available_at, seq) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_running_expiry ON jobs(lease_until) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_running_worker ON jobs(worker_id) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_latest_state ON jobs(status, seq DESC);

CREATE TABLE IF NOT EXISTS job_events (
  seq INTEGER PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id),
  at INTEGER NOT NULL,
  event TEXT NOT NULL,
  details TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS job_events_history ON job_events(job_id, seq);

CREATE TABLE IF NOT EXISTS scheduler_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  floor REAL NOT NULL,
  dispatches INTEGER NOT NULL
) STRICT;

INSERT OR IGNORE INTO scheduler_state (id, floor, dispatches) VALUES (1, 0, 0);

CREATE TABLE IF NOT EXISTS workers (
  id TEXT PRIMARY KEY,
  last_seen INTEGER NOT NULL,
  slots INTEGER CHECK (slots BETWEEN 1 AND 64)
) STRICT;

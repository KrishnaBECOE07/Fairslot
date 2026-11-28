# FairSlot

FairSlot is a multi-tenant background job engine. A tenant with a large backlog can use spare capacity while smaller tenants continue to get worker slots. Every dispatch records the scheduler decision alongside the job's attempts and outcome.

In a **101,000-job simulation**, FairSlot reduced p95 queue wait for the three smaller tenants from about 105 minutes under FIFO to 47–91 seconds. A separate [live PostgreSQL benchmark](#live-postgresql-benchmark) runs the HTTP coordinator and worker processes and checks the durable claim history. [Project plan and architecture](docs/PROJECT-PLAN.md)

## Run locally

Requires Node.js 22.20 or newer. The default demo uses built-in Node.js modules and a local SQLite database.

```sh
npm run demo
```

Open [the operator console](http://127.0.0.1:4317). Select **Run live workload** to submit 115 jobs across four tenants. The demo starts one coordinator and three independent worker processes. The sample includes transient and exhausted failures.

- **Overview** shows queued and running jobs, successes, dead letters, per-tenant caps, and recent worker activity. Filter by status and open a job to inspect the scheduler decision and attempt history.
- **Fairness Lab** compares global FIFO against FairSlot using the same input jobs and worker slots. Change the bulk backlog or worker count and rerun the comparison.

The default database is `data/fairslot.db`. Jobs, results, and dispatch history persist across runs. For separate processes, run `npm start` in one terminal and `npm run worker` in each worker terminal.

For a quick walkthrough, open **Fairness Lab** to see how the first dispatches are allocated. Then run the live workload from **Overview** and inspect a dead-lettered job to see its retries and final failure.

## Run two coordinators with PostgreSQL

Use a dedicated PostgreSQL database with a role that can create tables in its schema. Install the optional database driver with `npm ci --include=optional`.

In the first PowerShell 7 terminal, provide a PostgreSQL connection URL and start the coordinator. The first startup applies the schema migrations. A second startup loads the same ledger.

```powershell
$env:FAIRSLOT_DATABASE_URL = Read-Host 'PostgreSQL connection URL' -MaskInput
$env:FAIRSLOT_PORT = '4317'
npm start
```

Repeat in a second terminal with the same URL and port `4318`. Set the same `FAIRSLOT_LEASE_MS`, `FAIRSLOT_AGING_MS`, and `FAIRSLOT_KEY` on both if you override the defaults. In a third terminal, start a worker with both coordinator addresses.

```powershell
$env:FAIRSLOT_URLS = 'http://127.0.0.1:4317,http://127.0.0.1:4318'
npm run worker
```

Open both consoles to inspect the same jobs. Submit work from either console, then stop one coordinator. The worker switches addresses after a connection or server failure and can renew or finish an active lease through the surviving coordinator. If a claim commits and its response is lost, the claim expires and the job follows its normal retry policy.

PostgreSQL starts with its own ledger. Existing SQLite jobs and events stay in `data/fairslot.db`.

## Submit a job

PowerShell example

```powershell
$job = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:4317/api/jobs' -ContentType 'application/json' -Body '{"tenant_id":"lumen","kind":"echo","payload":{"order_id":"42"},"priority":2,"idempotency_key":"order-42"}'
Invoke-RestMethod -Uri "http://127.0.0.1:4317/api/jobs/$($job.id)"
```

| Endpoint | Behavior |
| --- | --- |
| `POST /api/jobs` | Submit a job. Returns 201 for a new job, 200 for a matching idempotent replay, or 409 if the key was reused with a changed request. |
| `GET /api/jobs/{id}` | Inspect the job, stored result, and ordered event timeline. |
| `POST /api/tenants` | Create a tenant using `id`, `name`, integer `weight`, and `max_running`. |
| `GET /api/dashboard?status=running` | Read counts, tenants, workers, and up to 25 recent jobs. The status filter is optional and applies to the jobs list. |
| `GET /api/lab?bulk=100000&workers=4` | Run the fixed-duration queue comparison. |
| `POST /api/worker/v1/register` | Record a `worker_id` and its advertised `slots` (1–64). |
| `POST /api/worker/v1/claim` | Claim with a `worker_id`. Returns 204 when no eligible job is available. |
| `POST /api/worker/v1/jobs/{id}/heartbeat` | Renew a job's current `lease_token`. |
| `POST /api/worker/v1/jobs/{id}/finish` | Finish using `lease_token` and `ok`. Success takes an optional `result`. Failure requires `error` and `retryable`. |

Requests with a body use `application/json`. Built-in handlers are `echo`, `hash`, `wait`, and `flaky`. Optional job fields include `priority` (0–2), estimated `cost` (1–1,000), `max_attempts` (1–10), and `delay_ms` (0–86,400,000).

Existing workers can continue using the unversioned claim, heartbeat, and finish routes.

## Verification

```sh
npm test
npm run test:integration
npm run benchmark
# Override the bulk backlog and worker slots
npm run benchmark -- 50000 8
```

The integration test uses a temporary database and a loopback port. It kills an active worker, verifies that the replacement recovers the job, and reopens the coordinator to check the persisted outcome. GitHub Actions is configured to run the SQLite and PostgreSQL suites and a small live benchmark on pushes and pull requests to `main`.

To run the same ledger contract against PostgreSQL and test failover between two coordinator processes, set `FAIRSLOT_TEST_DATABASE_URL` to a test database whose role can create schemas, then run `npm run test:postgres`. The tests create and remove their own schemas.

With 101,000 simulated jobs on four worker slots, all tenants start in the first dispatch wave under FairSlot. The smaller tenants' p95 queue waits range from 47.25 to 90.5 seconds, compared with 104.7 to 105.2 minutes under global FIFO. The comparison uses virtual time and fixed-duration jobs. It measures dispatch policy under that workload.

### Live PostgreSQL benchmark

Set `FAIRSLOT_TEST_DATABASE_URL` as above and run:

```sh
npm ci --include=optional
npm run benchmark:postgres
# Override the bulk tenant's backlog. The other tenants still submit 120 jobs total.
npm run benchmark:postgres -- 60
```

Each case preloads a fresh schema through the HTTP API, then starts four workers with two slots each. The same 480 jobs run with one coordinator and with two coordinators sharing PostgreSQL. The benchmark checks that every job succeeded on its first attempt, dispatch numbers are unique and contiguous, each tenant stayed within its configured concurrency cap, and the first four claims include all four tenants. Its JSON output reports throughput and per-tenant p50, p95, and p99 dispatch wait.

Dispatch wait starts at the first claim for the preloaded backlog. Drain time ends at the last recorded success. Both use timestamps from PostgreSQL, excluding submission and worker startup. The workload uses the 25 ms wait handler with the default tenant caps. The coordinator failover test separately stops a coordinator while a worker lease is active and verifies completion through its peer.

Two local runs per configuration on an Intel Core i5-12500H with Windows, Node.js 22.20.0, and PostgreSQL 17.4 produced these ranges. The database, coordinators, and workers shared one machine.

| Coordinators | Jobs per run | Drain time | Throughput | Smaller tenants' p95 dispatch wait |
| --- | ---: | ---: | ---: | ---: |
| 1 | 480 | 6.22–6.48 s | 74.1–77.1 jobs/s | 1.19–2.04 s |
| 2 | 480 | 6.34–6.98 s | 68.8–75.7 jobs/s | 1.32–2.07 s |

All four tenants appeared in the first four claims in every run. At fixed worker capacity, the measured throughput ranges overlap. Both coordinators still share one scheduler lock, so these runs provide a local baseline for comparing coordination overhead.

## Configuration

| Environment variable | Applies to | Default |
| --- | --- | --- |
| `FAIRSLOT_HOST` | Coordinator | `127.0.0.1` |
| `FAIRSLOT_PORT` | Coordinator and demo | `4317` |
| `FAIRSLOT_DB` | Coordinator | `data/fairslot.db` |
| `FAIRSLOT_DATABASE_URL` | Coordinator | Unset. Set a PostgreSQL URL to use PostgreSQL. |
| `FAIRSLOT_LEASE_MS` | Coordinator | `6000` |
| `FAIRSLOT_AGING_MS` | Coordinator | `30000` |
| `FAIRSLOT_URL` | Worker | `http://127.0.0.1:4317` |
| `FAIRSLOT_URLS` | Worker | Optional comma-separated list of coordinators sharing one PostgreSQL ledger. Takes precedence over `FAIRSLOT_URL`. |
| `FAIRSLOT_WORKER_ID` | Worker | Generated process identifier |
| `FAIRSLOT_SLOTS` | Worker | `1` |
| `FAIRSLOT_KEY` | Coordinator and worker | Optional shared API bearer key |

Use SQLite for a single local coordinator and PostgreSQL for multiple coordinator processes. The browser console is for the default loopback setup. A non-loopback bind requires a shared key and is intended for API access behind a private TLS endpoint. Per-role credentials, adaptive capacity, and production load validation are covered in the project plan.

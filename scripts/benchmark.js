import { simulate } from '../src/scenario.js';
import { integer } from '../src/validation.js';

const [bulk = 100_000, workers = 4] = process.argv.slice(2);
const result = simulate({
  bulk: integer(Number(bulk), 100, 200_000, 'bulk'),
  workers: integer(Number(workers), 1, 64, 'workers'),
});

console.log(`Fixed-duration simulation • ${result.total.toLocaleString('en-US')} jobs • ${result.workers} slots • ${result.duration_ms} ms per job`);
console.table(result.fairslot.tenants.map((tenant, index) => ({
  Tenant: tenant.name,
  Jobs: tenant.count,
  'FIFO first (ms)': result.fifo.tenants[index].first_ms,
  'FairSlot first (ms)': tenant.first_ms,
  'FIFO p95 (ms)': result.fifo.tenants[index].p95_ms,
  'FairSlot p95 (ms)': tenant.p95_ms,
})));

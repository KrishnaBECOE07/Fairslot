import { chargeService, selectTenant } from './policy.js';

export const tenants = [
  { id: 'northstar', name: 'Northstar', weight: 1, max_running: 3 },
  { id: 'lumen', name: 'Lumen', weight: 1, max_running: 2 },
  { id: 'terrace', name: 'Terrace', weight: 1, max_running: 2 },
  { id: 'orbit', name: 'Orbit', weight: 1, max_running: 1 },
];

export function simulate({ bulk = 100_000, workers = 4 } = {}) {
  const counts = [bulk, 500, 300, 200];
  const duration_ms = 250;
  const total = counts.reduce((sum, count) => sum + count, 0);

  function run(fair) {
    const groups = tenants.map((tenant, index) => ({
      ...tenant, count: counts[index], dispatched: 0,
      virtual_service: 0, last_dispatch: 0, first_ms: null, p95_ms: null,
    }));
    const first_24 = [];
    let floor = 0;

    for (let index = 0; index < total; index++) {
      const active = groups.filter((group) => group.dispatched < group.count);
      const chosen = fair ? selectTenant(active, floor) : null;
      const group = chosen ? chosen.tenant : active[0];
      const wait = Math.floor(index / workers) * duration_ms;

      group.dispatched++;
      if (group.dispatched === 1) group.first_ms = wait;
      if (group.dispatched === Math.ceil(group.count * 0.95)) group.p95_ms = wait;
      if (index < 24) first_24.push(group.id);
      if (chosen) {
        floor = chosen.service;
        group.virtual_service = chargeService(floor, 1, group.weight);
        group.last_dispatch = index + 1;
      }
    }

    return {
      first_24,
      tenants: groups.map(({ id, name, count, first_ms, p95_ms }) => ({
        id, name, count, first_ms, p95_ms,
      })),
    };
  }

  return { bulk, workers, duration_ms, total, fairslot: run(true), fifo: run(false) };
}

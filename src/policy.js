export function selectTenant(candidates, floor) {
  let winner = null;
  for (const tenant of candidates) {
    const service = Math.max(tenant.virtual_service, floor);
    if (
      !winner ||
      service < winner.service ||
      (service === winner.service && tenant.last_dispatch < winner.tenant.last_dispatch) ||
      (service === winner.service && tenant.last_dispatch === winner.tenant.last_dispatch &&
        tenant.id < winner.tenant.id)
    ) {
      winner = { tenant, service };
    }
  }
  return winner;
}

export function chargeService(service, cost, weight) {
  return service + cost / weight;
}

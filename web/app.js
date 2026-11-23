const byId = (id) => document.getElementById(id);
const numbers = new Intl.NumberFormat();
const time = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
const labels = { queued: 'Queued', running: 'Running', succeeded: 'Succeeded', dead_letter: 'Dead letter' };
const priorities = ['Low', 'Normal', 'High'];
const tenantNames = new Map();
let dashboardRequest = 0;
let toastTimer;
let previousJobs = '';
let labLoaded = false;
let detailRequest = 0;

function element(tag, content = '', className = '') {
  const node = document.createElement(tag);
  if (content !== '') node.textContent = content;
  if (className) node.className = className;
  return node;
}

function duration(ms) {
  if (ms === null || ms === undefined) return '—';
  if (ms === 0) return '0 ms';
  if (ms < 1_000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(ms < 10_000 ? 1 : 0).replace(/\.0$/, '')} s`;
  const seconds = Math.round(ms / 1_000);
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3_600)}h ${Math.floor((seconds % 3_600) / 60)}m`;
}

async function request(path, options) {
  const response = await fetch(path, { ...options, signal: AbortSignal.timeout(5_000) });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error ?? 'Request failed.');
  return data;
}

function connection(state, message) {
  const node = byId('connection');
  node.dataset.state = state;
  node.lastElementChild.textContent = message;
}

function tenantLabel(id, name = tenantNames.get(id) ?? id) {
  const label = element('span', '', 'tenant-name');
  label.dataset.tenant = id;
  label.append(element('i', '', 'tenant-dot'), document.createTextNode(name));
  return label;
}

function badge(status) {
  const label = element('span', labels[status], 'status');
  label.dataset.status = status;
  return label;
}

function row(...values) {
  const tr = element('tr');
  for (const value of values) {
    const td = element('td');
    td.append(value);
    tr.append(td);
  }
  return tr;
}

function emptyRow(message, columns) {
  const cell = element('td', message, 'empty');
  cell.colSpan = columns;
  const tr = element('tr');
  tr.append(cell);
  return tr;
}

function renderTenants(tenants) {
  const max = Math.max(1, ...tenants.map((tenant) => tenant.queued));
  byId('tenant-rows').replaceChildren(...tenants.map((tenant) => {
    tenantNames.set(tenant.id, tenant.name);
    const backlog = element('span', '', 'backlog');
    backlog.dataset.tenant = tenant.id;
    const progress = element('progress');
    progress.max = max;
    progress.value = tenant.queued;
    progress.setAttribute('aria-hidden', 'true');
    backlog.append(progress, numbers.format(tenant.queued));
    return row(tenantLabel(tenant.id), backlog, `${tenant.running} / ${tenant.max_running}`,
      numbers.format(tenant.weight), duration(tenant.oldest_ready_ms));
  }));
}

function renderWorkers(workers, leaseMs) {
  byId('worker-count').textContent = `${workers.length} online`;
  byId('lease-duration').textContent = duration(leaseMs);
  byId('workers').replaceChildren(...(workers.length ? workers.map((worker) => {
    const li = element('li');
    const icon = element('span', '›_', 'worker-icon');
    icon.setAttribute('aria-hidden', 'true');
    li.append(icon, element('span', worker.id, 'worker-id'),
      element('span', worker.running ? `${worker.running} running` : 'Idle', 'worker-state'));
    return li;
  }) : [element('li', 'No workers checked in recently.', 'empty')]));
}

function renderJobs(jobs, filter) {
  const signature = JSON.stringify([jobs, filter]);
  if (signature === previousJobs) return;
  previousJobs = signature;
  const focused = document.activeElement?.dataset.jobId;
  let matchingButton;
  const rows = jobs.map((job) => {
    const link = element('button', job.id.slice(0, 8), 'job-link');
    link.type = 'button';
    link.dataset.jobId = job.id;
    link.setAttribute('aria-label', `Inspect job ${job.id}`);
    if (focused === job.id) matchingButton = link;
    return row(link, tenantLabel(job.tenant_id), job.kind, badge(job.status), priorities[job.priority],
      `${job.attempts} / ${job.max_attempts}`, time.format(job.updated_at));
  });
  if (!rows.length) rows.push(emptyRow(filter ? 'No jobs in this status.' : 'Run a live workload to see jobs here.', 7));
  byId('job-rows').replaceChildren(...rows);
  matchingButton?.focus({ preventScroll: true });
}

async function refreshDashboard() {
  const version = ++dashboardRequest;
  const filter = byId('job-filter').value;
  try {
    const data = await request(`/api/dashboard${filter ? `?status=${filter}` : ''}`);
    if (version !== dashboardRequest) return;
    connection('online', 'Coordinator online');
    for (const [status, count] of Object.entries(data.counts)) byId(`count-${status}`).textContent = numbers.format(count);
    const ready = data.tenants.reduce((sum, tenant) => sum + tenant.ready, 0);
    byId('queue-subtitle').textContent = `${numbers.format(ready)} ready · ${numbers.format(data.counts.queued - ready)} scheduled`;
    renderTenants(data.tenants);
    renderWorkers(data.workers, data.lease_ms);
    renderJobs(data.jobs, filter);
    byId('last-updated').textContent = `Updated ${time.format(data.at)}`;
  } catch (error) {
    if (version !== dashboardRequest) return;
    connection('offline', 'Coordinator unavailable');
    byId('last-updated').textContent = error.message;
  }
}

function announce(message) {
  const toast = byId('toast');
  toast.textContent = message;
  toast.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { toast.hidden = true; }, 4_000);
}

function describeEvent({ event, details }) {
  if (event === 'claimed') {
    const rule = details.rule === 'aging' ? 'Aging selected the oldest eligible job.' :
      `${priorities[details.priority]} priority within the tenant.`;
    const display = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
    return `${details.worker_id} claimed attempt ${details.attempt}. Lowest virtual service among ${details.eligible_tenants} eligible tenants. Estimated cost ${details.cost} ÷ weight ${details.weight} moved service from ${display.format(details.service_before)} to ${display.format(details.service_after)}. Tenant leases ${details.running}/${details.max_running}. ${rule}`;
  }
  if (event === 'submitted') return `Eligible at ${time.format(details.available_at)}.`;
  if (event === 'succeeded') return `Attempt ${details.attempt} completed on ${details.worker_id}.`;
  if (event === 'retry_scheduled') return `${details.error}. Available again at ${time.format(details.available_at)}.`;
  return `${details.error}. Attempt ${details.attempt} ended on ${details.worker_id}.`;
}

async function inspectJob(id) {
  const version = ++detailRequest;
  const modal = byId('job-detail');
  const content = byId('detail-content');
  byId('detail-heading').textContent = `Job ${id.slice(0, 8)}`;
  content.replaceChildren(element('p', 'Loading job history…', 'tone-muted'));
  if (!modal.open) modal.showModal();
  try {
    const job = await request(`/api/jobs/${id}`);
    if (version !== detailRequest || !modal.open) return;
    const meta = element('dl', '', 'detail-meta');
    for (const [label, value] of [['Tenant', tenantLabel(job.tenant_id)], ['Handler', job.kind],
      ['Status', badge(job.status)], ['Attempts', `${job.attempts} / ${job.max_attempts}`]]) {
      const dd = element('dd');
      dd.append(value);
      const group = element('div');
      group.append(element('dt', label), dd);
      meta.append(group);
    }
    const events = element('ol', '', 'event-list');
    const names = {
      submitted: 'Submitted', claimed: 'Dispatch decision', succeeded: 'Succeeded',
      retry_scheduled: 'Retry scheduled', dead_lettered: 'Moved to dead letter',
    };
    for (const entry of job.events) {
      const event = element('li');
      event.append(element('strong', names[entry.event]), element('p', describeEvent(entry)),
        element('span', time.format(entry.at), 'event-time'));
      events.append(event);
    }
    content.replaceChildren(meta, element('h3', 'Attempt history'), events);
    if (job.result !== null) {
      const result = element('details');
      result.append(element('summary', 'Result'), element('pre', JSON.stringify(job.result, null, 2)));
      content.append(result);
    }
  } catch (error) {
    if (version === detailRequest && modal.open) content.replaceChildren(element('p', error.message));
  }
}

function drawTrack(id, sequence, names) {
  const track = byId(id);
  track.setAttribute('role', 'img');
  track.setAttribute('aria-label', sequence.map((tenant) => names.get(tenant)).join(', '));
  track.replaceChildren(...sequence.map((tenant, index) => {
    const segment = element('span', '', 'track-segment');
    segment.dataset.tenant = tenant;
    segment.title = `Dispatch ${index + 1} · ${names.get(tenant)}`;
    return segment;
  }));
}

async function runLab() {
  const button = byId('simulate');
  if (button.disabled) return;
  button.disabled = true;
  const status = byId('lab-status');
  status.textContent = 'Running comparison…';
  try {
    const params = new URLSearchParams({ bulk: byId('bulk').value, workers: byId('slots').value });
    const data = await request(`/api/lab?${params}`);
    const names = new Map(data.fairslot.tenants.map(({ id, name }) => [id, name]));
    drawTrack('fifo-track', data.fifo.first_24, names);
    drawTrack('fair-track', data.fairslot.first_24, names);
    byId('legend').replaceChildren(...data.fairslot.tenants.map(({ id, name }) => {
      const item = element('li');
      item.append(tenantLabel(id, name));
      return item;
    }));
    byId('dispatch-note').textContent = `${numbers.format(data.total)} jobs across ${data.workers} worker ${data.workers === 1 ? 'slot' : 'slots'}.`;
    byId('lab-rows').replaceChildren(...data.fairslot.tenants.map((tenant, index) => {
      const fifo = data.fifo.tenants[index];
      const fairFirst = element('span', duration(tenant.first_ms), tenant.first_ms < fifo.first_ms ? 'improved' : '');
      const fairP95 = element('span', duration(tenant.p95_ms), tenant.p95_ms < fifo.p95_ms ? 'improved' : '');
      return row(tenantLabel(tenant.id, tenant.name), numbers.format(tenant.count),
        duration(fifo.first_ms), fairFirst, duration(fifo.p95_ms), fairP95);
    }));
    byId('lab-results').hidden = false;
    status.textContent = '';
    labLoaded = true;
  } catch (error) {
    status.textContent = error.message;
  } finally {
    button.disabled = false;
  }
}

function navigate() {
  const lab = location.hash === '#lab';
  byId('overview').hidden = lab;
  byId('lab').hidden = !lab;
  byId(lab ? 'nav-lab' : 'nav-overview').setAttribute('aria-current', 'page');
  byId(lab ? 'nav-overview' : 'nav-lab').removeAttribute('aria-current');
  if (lab && !labLoaded) void runLab();
  if (!lab) void refreshDashboard();
}

byId('job-filter').addEventListener('change', refreshDashboard);
byId('job-rows').addEventListener('click', (event) => {
  const button = event.target.closest('[data-job-id]');
  if (button) void inspectJob(button.dataset.jobId);
});
byId('seed').addEventListener('click', async (event) => {
  const button = event.currentTarget;
  button.disabled = true;
  try {
    const result = await request('/api/demo', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
    });
    announce(`${numbers.format(result.queued)} jobs submitted across four tenants.`);
    void refreshDashboard();
  } catch (error) {
    announce(error.message);
  } finally {
    button.disabled = false;
  }
});
byId('lab-form').addEventListener('submit', (event) => {
  event.preventDefault();
  void runLab();
});
window.addEventListener('hashchange', navigate);
window.addEventListener('visibilitychange', () => {
  if (!document.hidden && !byId('overview').hidden) void refreshDashboard();
});
setInterval(() => {
  if (!document.hidden && !byId('overview').hidden) void refreshDashboard();
}, 1_500);
navigate();
if (byId('overview').hidden) void refreshDashboard();

import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';

const port = Number(process.env.FAIRSLOT_PORT ?? 4317);
if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) throw new Error('Set a valid FAIRSLOT_PORT.');
const url = `http://127.0.0.1:${port}`;
const children = [];
let closing = false;

function stop(code = 0) {
  if (closing) return;
  closing = true;
  process.exitCode = code;
  for (const child of children) child.kill();
}

function launch(file, options) {
  const child = spawn(process.execPath, [file], { env: { ...process.env, ...options }, stdio: 'inherit' });
  children.push(child);
  child.once('exit', () => stop(1));
  child.once('error', (error) => {
    console.error(error.message);
    stop(1);
  });
}

for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => stop());
launch('src/server.js', { FAIRSLOT_HOST: '127.0.0.1', FAIRSLOT_PORT: String(port) });

let ready = false;
for (let attempt = 0; attempt < 50 && !closing; attempt++) {
  try {
    const response = await fetch(`${url}/health`, { signal: AbortSignal.timeout(500) });
    await response.arrayBuffer();
    if (response.ok) {
      ready = true;
      break;
    }
  } catch {
    await sleep(100);
  }
}
if (ready && !closing) {
  for (let index = 1; index <= 3; index++) {
    launch('src/worker.js', { FAIRSLOT_URL: url, FAIRSLOT_URLS: url,
      FAIRSLOT_WORKER_ID: `worker-0${index}`, FAIRSLOT_SLOTS: '1' });
  }
  console.log(`Operator console: ${url}\nPress Run live workload to submit a sample batch.`);
} else if (!closing) {
  console.error('The coordinator did not become ready.');
  stop(1);
}

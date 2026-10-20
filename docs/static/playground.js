import {BrowserDatabase} from './portable/client.js';

const $ = id => document.getElementById(id);
const example = [
  {metric: 'latency_ms', value: 42}, {metric: 'cpu_percent', value: 28}, {metric: 'error_rate', value: 0.02},
  {metric: 'latency_ms', value: 68}, {metric: 'cpu_percent', value: 43}, {metric: 'error_rate', value: 0.01},
  {metric: 'latency_ms', value: 35}, {metric: 'cpu_percent', value: 37}, {metric: 'error_rate', value: 0.04},
  {metric: 'latency_ms', value: 51}, {metric: 'cpu_percent', value: 52}, {metric: 'error_rate', value: 0.01},
];
let database, busy = false, stream = null, liveIndex = 0;
const format = value => value === null ? '—' : new Intl.NumberFormat('en', {maximumFractionDigits: 3}).format(value);
const status = message => { $('playground-status').textContent = message; };
const setBusy = value => {
  busy = value; $('playground').setAttribute('aria-busy', String(value));
  document.querySelectorAll('[data-work]').forEach(button => { button.disabled = value; });
  $('document-file').disabled = value;
};
async function run(action) {
  if (busy) return;
  setBusy(true); $('playground-error').hidden = true;
  try { await action(); }
  catch (error) {
    $('playground-error').textContent = error.message || String(error); $('playground-error').hidden = false;
    status('The batch was not committed. Your previous documents and views are intact.');
  } finally { setBusy(false); }
}
function stopStream() {
  if (stream) clearInterval(stream);
  stream = null; $('toggle-stream').setAttribute('aria-pressed', 'false'); $('stream-label').textContent = 'Start live stream';
}
function range(row) {
  const wrapper = document.createElement('div'); wrapper.className = 'metric-range';
  const label = document.createElement('span'); label.textContent = String(row.key); label.title = String(row.key);
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.setAttribute('viewBox', '0 0 200 17'); svg.setAttribute('aria-hidden', 'true');
  const {min, max, mean} = row.result;
  const scale = Math.max(1, Math.abs(min), Math.abs(max));
  const dot = min === max ? 100 : 12 + ((mean / scale - min / scale) / (max / scale - min / scale)) * 176;
  const element = (tag, attrs) => { const node = document.createElementNS('http://www.w3.org/2000/svg', tag); Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, value)); svg.append(node); };
  element('line', {x1: 12, y1: 8, x2: 188, y2: 8, stroke: '#e2e9da', 'stroke-width': 5, 'stroke-linecap': 'round'});
  element('line', {x1: 12, y1: 8, x2: dot, y2: 8, stroke: '#b2c8a1', 'stroke-width': 5, 'stroke-linecap': 'round'});
  element('circle', {cx: dot, cy: 8, r: 4, fill: '#397249', stroke: '#fff', 'stroke-width': 2});
  wrapper.append(label, svg); return wrapper;
}
async function render() {
  const [rows, info] = await Promise.all([database.rows(), database.stats()]);
  $('document-count').textContent = format(info.documents); $('group-count').textContent = format(rows.length); $('change-count').textContent = format(info.sequence);
  $('module-name').textContent = `${info.descriptor.name}@${info.descriptor.version} · state ${info.descriptor.state_version}`;
  $('module-digest').textContent = info.module_sha256;
  $('runtime-state').textContent = 'WebAssembly worker · in memory';
  const table = $('view-rows'), ranges = $('metric-ranges'); table.replaceChildren(); ranges.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement('tr'), th = document.createElement('th'); th.scope = 'row'; th.textContent = String(row.key); tr.append(th);
    for (const key of ['count', 'mean', 'min', 'max']) { const td = document.createElement('td'); td.textContent = format(row.result[key]); tr.append(td); }
    table.append(tr); ranges.append(range(row));
  }
  if (!rows.length) { const tr = document.createElement('tr'), td = document.createElement('td'); td.colSpan = 5; td.className = 'empty-state'; td.textContent = 'Add a metric document to create a group.'; tr.append(td); table.append(tr); }
  $('replay-state').textContent = 'Ready'; $('replay-state').classList.remove('verified');
}
async function reset() {
  stopStream(); database?.close(); database = await BrowserDatabase.connect({base: new URL('./portable/', location.href)});
  await database.ingest(example.map((payload, index) => ({id: `example-${index}`, source: 'example', position: index, payload})));
  await render(); status('12 sample documents, 3 live groups. Add a measurement or start the simulated stream.');
}
function parseDocuments(text) {
  let data;
  try { data = JSON.parse(text); }
  catch {
    data = text.split(/\r?\n/).filter(line => line.trim()).map((line, index) => {
      try { return JSON.parse(line); } catch { throw new Error(`Invalid JSON on line ${index + 1}. Use a JSON array or one document per line.`); }
    });
  }
  const records = Array.isArray(data) ? data : [data];
  if (!records.length || records.length > 1024) throw new Error('Import between 1 and 1,024 documents at a time.');
  const isRetained = value => value && typeof value === 'object' && Object.hasOwn(value, 'payload') && typeof value.id === 'string' && typeof value.source === 'string' && Number.isSafeInteger(value.position);
  return records.map(record => isRetained(record) ? record : {payload: record, source: 'import'});
}
async function importText(text, label) {
  if (new TextEncoder().encode(text).length > 4 * 1024 * 1024) throw new Error('Choose a file or pasted input of 4 MiB or less.');
  const result = await database.ingest(parseDocuments(text)); await render();
  status(`${label}: ${format(result.inserted)} documents added${result.replayed ? `, ${format(result.replayed)} identical replays` : ''}. Views updated atomically.`);
}
async function importFile(file) {
  if (!file) return;
  if (file.size > 4 * 1024 * 1024) throw new Error('Choose a file of 4 MiB or less.');
  await importText(await file.text(), file.name);
}
$('measurement-form').addEventListener('submit', event => {
  event.preventDefault(); run(async () => {
    const metric = $('metric-input').value.trim(), value = Number($('value-input').value);
    if (!metric || !Number.isFinite(value)) throw new Error('Enter a metric name and a finite numeric value.');
    await database.add({metric, value}, {source: 'manual'}); await render(); status(`Added ${metric} = ${format(value)}. The worker updated its live group.`);
  });
});
$('reset-button').addEventListener('click', () => run(reset));
$('import-text').addEventListener('click', () => run(() => importText($('document-text').value, 'Pasted documents')));
$('document-file').addEventListener('change', event => { const file = event.target.files[0]; run(() => importFile(file)); event.target.value = ''; });
const drop = $('file-drop');
drop.addEventListener('dragover', event => { event.preventDefault(); drop.classList.add('drag-over'); });
drop.addEventListener('dragleave', () => drop.classList.remove('drag-over'));
drop.addEventListener('drop', event => { event.preventDefault(); drop.classList.remove('drag-over'); if (!busy) run(() => importFile(event.dataTransfer.files[0])); });
$('toggle-stream').addEventListener('click', () => {
  if (stream) { stopStream(); status('Live simulation paused. Your documents and views are retained.'); return; }
  $('toggle-stream').setAttribute('aria-pressed', 'true'); $('stream-label').textContent = 'Pause live stream';
  status('Simulating a local stream: one measurement every 700 ms.');
  stream = setInterval(() => {
    if (busy) return;
    run(async () => {
      const metric = ['latency_ms', 'cpu_percent', 'error_rate'][liveIndex++ % 3];
      const value = metric === 'error_rate' ? Math.round(Math.random() * 5) / 100 : Math.round(20 + Math.random() * 65);
      await database.add({metric, value}, {source: 'simulation'}); await render();
      status(`Live: ${metric} = ${format(value)}. Every measurement is retained for replay.`);
    });
  }, 700);
});
$('replay-button').addEventListener('click', () => run(async () => {
  const result = await database.replay();
  if (!result.equivalent) throw new Error('Replay results did not match the current view. Export the documents for inspection.');
  $('replay-state').textContent = 'Verified ✓'; $('replay-state').classList.add('verified');
  status(`Replay verified: ${format(result.documents)} retained documents rebuilt ${format(result.groups)} groups with identical states and results.`);
}));
$('export-button').addEventListener('click', () => run(async () => {
  const documents = await database.export(), blob = new Blob([JSON.stringify(documents, null, 2) + '\n'], {type: 'application/json'});
  const url = URL.createObjectURL(blob), link = document.createElement('a'); link.href = url; link.download = 'foldlite-documents.json'; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000); status(`Exported ${format(documents.length)} documents with identities, sources, and positions. Import them here or replay them in Python.`);
}));
window.addEventListener('pagehide', () => { stopStream(); database?.close(); });
window.foldliteDemo = {get database() {return database;}, get ready() {return Boolean(database) && !busy;}};
setBusy(true);
reset().catch(error => {
  $('runtime-state').textContent = 'Could not start the worker'; $('playground-error').textContent = error.message; $('playground-error').hidden = false;
  status('Serve this page over HTTP or HTTPS in a browser with WebAssembly and module worker support.');
}).finally(() => setBusy(false));

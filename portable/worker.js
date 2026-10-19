/* One view, one isolated worker. Retained documents and committed view state are
   in memory; reload the page to discard them, or export documents for replay. */
import {ViewModule, ViewError, canonical, finiteJSON} from './host.js';

const utf8 = new TextEncoder();
const MAX_DOCUMENTS = 20000, MAX_BATCH = 1024, MAX_GROUPS = 4096, MAX_EVENTS = 100000;
const MAX_RETAINED_BYTES = 16 * 1024 * 1024, MAX_HISTORY_BYTES = 32 * 1024 * 1024;
const clone = value => JSON.parse(JSON.stringify(value));
const bytes = value => utf8.encode(canonical(value)).length;
function nonnegative(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) throw new ViewError(`${label} must be a nonnegative safe integer`);
  return value;
}
function name(value, label) {
  if (typeof value !== 'string' || !value || utf8.encode(value).length > 4096) throw new ViewError(`${label} must contain 1–4096 UTF-8 bytes`);
  return value;
}
/* SQLite's binary UTF-8 order is Unicode scalar order, unlike JS's UTF-16 <. */
function keyCompare(a, b) {
  const left = Array.from(a, c => c.codePointAt(0)), right = Array.from(b, c => c.codePointAt(0));
  for (let i = 0; i < Math.min(left.length, right.length); i++) if (left[i] !== right[i]) return left[i] - right[i];
  return left.length - right.length;
}

class MemoryDatabase {
  constructor(view) {
    this.view = view;
    this.epoch = globalThis.crypto?.randomUUID?.() || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
    this.sequence = 0; this.documents = []; this.batches = []; this.identities = new Map(); this.positions = new Map();
    this.checkpoints = new Map(); this.states = new Map(); this.history = []; this.documentBytes = 0; this.historyBytes = 0;
  }
  cursor(sequence = this.sequence) { return `${this.epoch}:${this.view.spec.sha256}:${sequence}`; }
  readCursor(cursor = this.cursor()) {
    if (cursor === 0 || cursor === null) return 0;
    if (typeof cursor !== 'string' || !cursor.startsWith(`${this.epoch}:${this.view.spec.sha256}:`)) throw new ViewError('Cursor belongs to another database or view module');
    const tail = cursor.slice(cursor.lastIndexOf(':') + 1);
    if (!/^(0|[1-9][0-9]*)$/.test(tail)) throw new ViewError('Invalid cursor');
    const sequence = nonnegative(Number(tail), 'Cursor sequence');
    if (sequence > this.sequence) throw new ViewError('Cursor is ahead of committed state');
    return sequence;
  }
  ingest(input) {
    if (!Array.isArray(input) || input.length > MAX_BATCH) throw new ViewError(`A batch must be an array of at most ${MAX_BATCH} documents`);
    finiteJSON(input);
    const identities = new Map(this.identities), positions = new Map(this.positions), checkpoints = new Map(this.checkpoints), states = new Map(this.states);
    const appended = [], events = [], resolved = [];
    let sequence = this.sequence, documentBytes = this.documentBytes, historyBytes = this.historyBytes, replayed = 0;
    for (const supplied of input) {
      if (!supplied || Array.isArray(supplied) || typeof supplied !== 'object' || !Object.hasOwn(supplied, 'payload')) throw new ViewError('Each document needs a payload');
      const source = name(supplied.source === undefined ? 'default' : supplied.source, 'Source');
      const payload = clone(supplied.payload), payloadText = canonical(payload);
      if (utf8.encode(payloadText).length > 262144) throw new ViewError('Document exceeds 262144 bytes');
      if (supplied.id !== undefined) {
        name(supplied.id, 'Document id');
        const previous = identities.get(canonical([source, supplied.id]));
        if (previous) {
          if (canonical(previous.payload) !== payloadText || (supplied.position !== undefined && supplied.position !== previous.position)) throw new ViewError('DocumentConflict: document identity already has different contents or position');
          resolved.push(previous); replayed++; continue;
        }
      }
      const position = supplied.position === undefined ? (checkpoints.get(source) ?? -1) + 1 : nonnegative(supplied.position, 'Source position');
      nonnegative(position, 'Source position');
      const atPosition = positions.get(canonical([source, position]));
      if (atPosition) {
        if (canonical(atPosition.payload) !== payloadText || (supplied.id !== undefined && supplied.id !== atPosition.id)) throw new ViewError('DocumentConflict: source position already has different contents');
        resolved.push(atPosition); replayed++; continue;
      }
      if (position <= (checkpoints.get(source) ?? -1)) throw new ViewError('Source positions must increase; an older position is not a retained replay');
      const id = supplied.id === undefined ? (globalThis.crypto?.randomUUID?.() || `doc-${this.epoch}-${position}-${Math.random().toString(36).slice(2)}`) : supplied.id;
      if (this.documents.length + appended.length >= MAX_DOCUMENTS) throw new ViewError('In-memory document limit reached; export your documents');
      const document = {id, source, position, payload, sequence: ++sequence};
      documentBytes += bytes(document);
      if (documentBytes > MAX_RETAINED_BYTES) throw new ViewError('Retained documents exceed 16 MiB');
      identities.set(canonical([source, id]), document); positions.set(canonical([source, position]), document); checkpoints.set(source, position);
      appended.push(document); resolved.push(document);
    }
    const groups = new Map();
    if (appended.length) {
      const mapped = this.view.map(appended.map(document => document.payload));
      if (!Array.isArray(mapped) || mapped.length !== appended.length) throw new ViewError('Map must return one contribution array per document');
      let emissions = 0;
      for (const contributions of mapped) {
        if (!Array.isArray(contributions)) throw new ViewError('Map must return contribution arrays');
        emissions += contributions.length;
        if (emissions > 4096) throw new ViewError('Map exceeds 4096 contributions per batch');
        for (const contribution of contributions) {
          if (!contribution || !Object.hasOwn(contribution, 'key') || !Object.hasOwn(contribution, 'value')) throw new ViewError('Map emissions need a key and value');
          const key = canonical(contribution.key);
          if (!groups.has(key)) groups.set(key, {key: contribution.key, values: []});
          groups.get(key).values.push(contribution.value);
        }
      }
      for (const [encoded, group] of [...groups.entries()].sort(([a], [b]) => keyCompare(a, b))) {
        const previous = states.get(encoded);
        const state = this.view.add(previous ? previous.state : this.view.identity(), group.values);
        const row = {key: group.key, state, result: this.view.finalize(state), sequence: ++sequence};
        states.set(encoded, row); events.push(row); historyBytes += bytes(row);
      }
      if (states.size > MAX_GROUPS || this.history.length + events.length > MAX_EVENTS || historyBytes > MAX_HISTORY_BYTES) throw new ViewError('In-memory view history limit reached; export your documents');
    }
    // Commit every retained record, checkpoint, state, and change together.
    this.identities = identities; this.positions = positions; this.checkpoints = checkpoints; this.states = states;
    this.documents.push(...appended); this.history.push(...events); this.sequence = sequence;
    if (appended.length) this.batches.push(appended);
    this.documentBytes = documentBytes; this.historyBytes = historyBytes;
    return {documents: resolved, inserted: appended.length, replayed, sequence, cursor: this.cursor()};
  }
  limit(request) {
    const limit = request.limit ?? 100;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1024) throw new ViewError('Page limit must be between 1 and 1024');
    return limit;
  }
  page(request, kind) {
    let cursor = request.cursor ?? this.cursor(), last = kind === 'query' ? null : -1;
    if (request.after !== undefined && request.after !== null) {
      let token;
      try { token = JSON.parse(request.after); } catch { throw new ViewError('Invalid page continuation'); }
      if (!Array.isArray(token) || token.length !== 2 || typeof token[0] !== 'string') throw new ViewError('Invalid page continuation');
      if (request.cursor !== undefined && request.cursor !== token[0]) throw new ViewError('Page cursor mismatch');
      cursor = token[0]; last = token[1];
      if (kind === 'query' && typeof last !== 'string') throw new ViewError('Invalid query continuation');
      if (kind !== 'query') nonnegative(last, 'Document continuation');
    }
    if (typeof cursor !== 'string') throw new ViewError('Page cursor must be an opaque string');
    const snapshot = this.readCursor(cursor), limit = this.limit(request);
    if (kind === 'documents') {
      const remaining = this.documents.filter(d => d.sequence <= snapshot && d.sequence > last);
      const documents = remaining.slice(0, limit), complete = remaining.length <= limit;
      return {documents, cursor, after: complete ? null : JSON.stringify([cursor, documents.at(-1).sequence]), complete};
    }
    const atSnapshot = new Map();
    for (const row of this.history) if (row.sequence <= snapshot) atSnapshot.set(canonical(row.key), row);
    const key = Object.hasOwn(request, 'key') ? canonical(request.key) : undefined;
    const remaining = [...atSnapshot.entries()].filter(([encoded]) => (last === null || keyCompare(encoded, last) > 0) && (key === undefined || encoded === key)).sort(([a], [b]) => keyCompare(a, b));
    const page = remaining.slice(0, limit), complete = remaining.length <= limit;
    return {rows: page.map(([, row]) => row), cursor, after: complete ? null : JSON.stringify([cursor, page.at(-1)[0]]), complete};
  }
  changes(request) {
    const since = this.readCursor(request.since ?? 0), limit = this.limit(request);
    const remaining = this.history.filter(row => row.sequence > since), rows = remaining.slice(0, limit), complete = remaining.length <= limit;
    return {rows, cursor: this.cursor(complete ? this.sequence : rows.at(-1).sequence), complete};
  }
  stats() {
    return {documents: this.documents.length, sequence: this.sequence, cursor: this.cursor(), module_sha256: this.view.spec.sha256, descriptor: this.view.descriptor, checkpoints: Object.fromEntries(this.checkpoints), storage: 'memory', memory_bytes: this.view.exports.memory.buffer.byteLength};
  }
  replay() {
    const rebuilt = new MemoryDatabase(this.view);
    for (const batch of this.batches) rebuilt.ingest(batch);
    const values = database => [...database.states.entries()].sort(([a], [b]) => keyCompare(a, b)).map(([, row]) => ({key: row.key, state: row.state, result: row.result}));
    return {equivalent: canonical(values(this)) === canonical(values(rebuilt)), documents: this.documents.length, groups: this.states.size, module_sha256: this.view.spec.sha256};
  }
  request(request) {
    finiteJSON(request);
    switch (request.op) {
      case 'ingest': return this.ingest(request.documents);
      case 'query': return this.page(request, 'query');
      case 'documents': return this.page(request, 'documents');
      case 'changes': return this.changes(request);
      case 'stats': return this.stats();
      case 'replay': return this.replay();
      case 'export': return this.documents;
      case 'view': return this.view.request(request.request);
      default: throw new ViewError('Unknown browser database operation');
    }
  }
}

let database = null;
self.onmessage = async ({data}) => {
  const {id} = data;
  try {
    let result;
    if (data.op === 'init') {
      if (database) throw new ViewError('Database is already initialized');
      const view = data.bytes ? await ViewModule.fromBytes(data.bytes, data.spec) : await ViewModule.load(new URL(data.base), data.name ?? 'metrics');
      database = new MemoryDatabase(view); result = database.stats();
    } else {
      if (!database) throw new ViewError('Initialize the browser database first');
      result = database.request(data);
    }
    self.postMessage({id, ok: result});
  } catch (error) { self.postMessage({id, error: error.message || String(error)}); }
};

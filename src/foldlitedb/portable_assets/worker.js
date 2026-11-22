/* One view, one isolated worker. Retained documents and committed view state are
   in memory; reload the page to discard them, or export documents for replay. */
import {ViewModule, ViewError, canonical, canonicalValidated, compareKeys, scalarCompare, databaseRequestJSON} from './host.js';
import {DocumentConflict, SourceOrderError, CursorExpired, serializeError} from './errors.js';

const utf8 = new TextEncoder();
const MAX_DOCUMENTS = 20000, MAX_BATCH = 1024, MAX_GROUPS = 4096, MAX_EVENTS = 100000;
const MAX_RETAINED_BYTES = 16 * 1024 * 1024, MAX_HISTORY_BYTES = 32 * 1024 * 1024;
const bytes = value => utf8.encode(JSON.stringify(value)).length;
function nonnegative(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) throw new ViewError(`${label} must be a nonnegative safe integer`);
  return value;
}
function name(value, label) {
  if (typeof value !== 'string' || !value || utf8.encode(value).length > 4096) throw new ViewError(`${label} must contain 1–4096 UTF-8 bytes`);
  return value;
}
function partitionPoint(items, predicate) {
  let lo = 0, hi = items.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (predicate(items[mid])) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

// Preview and ingestion validate the same map output. Empty emissions can be
// intentional; preview makes filtering visible before retaining source facts.
function mapPayloads(view, documents) {
  const mapped = view.map(documents);
  if (!Array.isArray(mapped) || mapped.length !== documents.length) throw new ViewError('Map must return one contribution array per document');
  let emissions = 0;
  return mapped.map(contributions => {
    if (!Array.isArray(contributions)) throw new ViewError('Map must return contribution arrays');
    emissions += contributions.length;
    if (emissions > 4096) throw new ViewError('Map exceeds 4096 contributions per batch');
    return contributions.map(contribution => {
      if (!contribution || typeof contribution !== 'object' || Array.isArray(contribution) || !Object.hasOwn(contribution, 'key') || !Object.hasOwn(contribution, 'value')) throw new ViewError('Map emissions need a key and value');
      return {key: contribution.key, value: contribution.value};
    });
  });
}

const cursorRecovery = ' Obtain a fresh snapshot with await db.query() and replace the previous results before resuming changes.';
const QUERY_FORMAT = 'foldlite-query-v1';

function querySelector(request) {
  const selector = {};
  for (const flag of ['inclusive_start', 'inclusive_end']) {
    const value = Object.hasOwn(request, flag) ? request[flag] : true;
    if (typeof value !== 'boolean') throw new ViewError(`${flag} must be a boolean`);
    selector[flag] = value;
  }
  const exact = Object.hasOwn(request, 'key'), prefix = Object.hasOwn(request, 'prefix');
  const range = Object.hasOwn(request, 'start_key') || Object.hasOwn(request, 'end_key');
  if (Number(exact) + Number(prefix) + Number(range) > 1) throw new ViewError('key, range bounds, and prefix are mutually exclusive');
  if (prefix && typeof request.prefix !== 'string' && !Array.isArray(request.prefix)) throw new ViewError('prefix must be a string or array');
  for (const name of ['key', 'start_key', 'end_key', 'prefix']) {
    if (Object.hasOwn(request, name)) selector[name] = request[name];
  }
  return selector;
}
function queryMatches(key, selector) {
  if (Object.hasOwn(selector, 'key')) return compareKeys(key, selector.key) === 0;
  if (Object.hasOwn(selector, 'prefix')) {
    const prefix = selector.prefix;
    if (typeof prefix === 'string') return typeof key === 'string' && key.startsWith(prefix);
    return Array.isArray(key) && key.length >= prefix.length && prefix.every((value, i) => compareKeys(value, key[i]) === 0);
  }
  if (Object.hasOwn(selector, 'start_key')) {
    const order = compareKeys(key, selector.start_key);
    if (order < 0 || (order === 0 && !selector.inclusive_start)) return false;
  }
  if (Object.hasOwn(selector, 'end_key')) {
    const order = compareKeys(key, selector.end_key);
    if (order > 0 || (order === 0 && !selector.inclusive_end)) return false;
  }
  return true;
}

class MemoryDatabase {
  constructor(view) {
    this.view = view;
    this.epoch = globalThis.crypto?.randomUUID?.() || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
    this.sequence = 0; this.documents = []; this.batches = []; this.identities = new Map(); this.positions = new Map();
    this.checkpoints = new Map(); this.states = new Map(); this.history = []; this.historyEntries = []; this.documentBytes = 0; this.historyBytes = 0;
  }
  cursor(sequence = this.sequence) { return `${this.epoch}:${this.view.spec.sha256}:${sequence}`; }
  readCursor(cursor, allowStart = false) {
    if (cursor === undefined || cursor === null) return allowStart ? 0 : this.sequence;
    if (allowStart && cursor === 0) return 0;
    if (typeof cursor !== 'string') throw new ViewError('Cursor must be an opaque cursor string');
    if (!cursor.startsWith(`${this.epoch}:${this.view.spec.sha256}:`)) throw new CursorExpired('Cursor belongs to another database or view module.' + cursorRecovery);
    const tail = cursor.slice(cursor.lastIndexOf(':') + 1);
    if (!/^(0|[1-9][0-9]*)$/.test(tail)) throw new ViewError('Invalid cursor sequence');
    const sequence = Number(tail);
    if (!Number.isSafeInteger(sequence)) throw new ViewError('Invalid cursor sequence');
    if (sequence > this.sequence) throw new CursorExpired('Cursor is ahead of committed state.' + cursorRecovery);
    return sequence;
  }
  ingest(input, validated = false) {
    if (!Array.isArray(input) || input.length > MAX_BATCH) throw new ViewError(`A batch must be an array of at most ${MAX_BATCH} documents`);
    if (!validated) databaseRequestJSON({op: 'ingest', documents: input});
    const stagedIdentities = new Map(), stagedPositions = new Map(), stagedCheckpoints = new Map(), stagedStates = new Map();
    const getIdentity = key => stagedIdentities.get(key) ?? this.identities.get(key);
    const getPosition = key => stagedPositions.get(key) ?? this.positions.get(key);
    const getCheckpoint = source => stagedCheckpoints.has(source) ? stagedCheckpoints.get(source) : this.checkpoints.get(source);
    const getState = key => stagedStates.get(key) ?? this.states.get(key);
    const appended = [], events = [], eventEntries = [], resolved = [];
    let sequence = this.sequence, documentBytes = this.documentBytes, historyBytes = this.historyBytes, replayed = 0;
    for (const supplied of input) {
      if (!supplied || Array.isArray(supplied) || typeof supplied !== 'object' || !Object.hasOwn(supplied, 'payload')) throw new ViewError('Each document needs a payload');
      const source = name(supplied.source === undefined ? 'default' : supplied.source, 'Source');
      const payloadText = canonicalValidated(supplied.payload);
      if (utf8.encode(payloadText).length > 262144) throw new ViewError('Document exceeds 262144 bytes');
      const encodedSource = JSON.stringify(source);
      let idKey = null;
      if (supplied.id !== undefined) {
        name(supplied.id, 'Document id');
        idKey = `[${encodedSource},${JSON.stringify(supplied.id)}]`;
        const previous = getIdentity(idKey);
        if (previous) {
          if (canonicalValidated(previous.payload) !== payloadText || (supplied.position !== undefined && supplied.position !== previous.position)) throw new DocumentConflict('DocumentConflict: document identity already has different contents or position');
          resolved.push(previous); replayed++; continue;
        }
      }
      const checkpoint = getCheckpoint(source) ?? -1;
      const position = supplied.position === undefined ? checkpoint + 1 : nonnegative(supplied.position, 'Source position');
      nonnegative(position, 'Source position');
      const posKey = `[${encodedSource},${position}]`;
      const atPosition = getPosition(posKey);
      if (atPosition) {
        if (canonicalValidated(atPosition.payload) !== payloadText || (supplied.id !== undefined && supplied.id !== atPosition.id)) throw new DocumentConflict('DocumentConflict: source position already has different contents');
        resolved.push(atPosition); replayed++; continue;
      }
      if (position <= checkpoint) throw new SourceOrderError('Source positions must increase; an older position is not a retained replay');
      const id = supplied.id === undefined ? (globalThis.crypto?.randomUUID?.() || `doc-${this.epoch}-${position}-${Math.random().toString(36).slice(2)}`) : supplied.id;
      if (this.documents.length + appended.length >= MAX_DOCUMENTS) throw new ViewError('In-memory document limit reached; export your documents');
      const payload = JSON.parse(payloadText);
      const document = {id, source, position, payload, sequence: ++sequence};
      documentBytes += bytes(document);
      if (documentBytes > MAX_RETAINED_BYTES) throw new ViewError('Retained documents exceed 16 MiB');
      stagedIdentities.set(idKey ?? `[${encodedSource},${JSON.stringify(id)}]`, document);
      stagedPositions.set(posKey, document);
      stagedCheckpoints.set(source, position);
      appended.push(document); resolved.push(document);
    }
    const groups = new Map();
    // Underscore-prefixed sources are retained internal metadata in both hosts.
    const mappedDocuments = appended.filter(document => !document.source.startsWith('_'));
    if (mappedDocuments.length) {
      const mapped = mapPayloads(this.view, mappedDocuments.map(document => document.payload));
      for (const contributions of mapped) {
        for (const contribution of contributions) {
          const key = canonicalValidated(contribution.key);
          if (!groups.has(key)) groups.set(key, {key: JSON.parse(key), values: []});
          groups.get(key).values.push(contribution.value);
        }
      }
      const ordered = [...groups.entries()].sort(([a], [b]) => scalarCompare(a, b));
      const reduced = this.view.reduceGroups(ordered.map(([encoded, group]) => {
        const previous = getState(encoded);
        return previous ? {state: previous.state, values: group.values} : {values: group.values};
      }));
      let totalGroups = this.states.size;
      for (let index = 0; index < ordered.length; index++) {
        const [encoded, group] = ordered[index], {state, result} = reduced[index];
        const row = {key: group.key, state, result, sequence: ++sequence};
        if (!this.states.has(encoded) && !stagedStates.has(encoded)) totalGroups++;
        stagedStates.set(encoded, row); events.push(row); eventEntries.push([encoded, row]); historyBytes += bytes(row);
      }
      if (totalGroups > MAX_GROUPS || this.history.length + events.length > MAX_EVENTS || historyBytes > MAX_HISTORY_BYTES) throw new ViewError('In-memory view history limit reached; export your documents');
    }
    // Commit every retained record, checkpoint, state, and change together.
    for (const [key, value] of stagedIdentities) this.identities.set(key, value);
    for (const [key, value] of stagedPositions) this.positions.set(key, value);
    for (const [key, value] of stagedCheckpoints) this.checkpoints.set(key, value);
    for (const [key, value] of stagedStates) this.states.set(key, value);
    this.documents.push(...appended); this.history.push(...events); this.historyEntries.push(...eventEntries); this.sequence = sequence;
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
    if (kind === 'query') return this.query(request);
    let cursor = request.cursor ?? this.cursor(), last = -1;
    if (request.after !== undefined && request.after !== null) {
      let token;
      try { token = JSON.parse(request.after); } catch { throw new ViewError('Invalid page continuation'); }
      if (!Array.isArray(token) || token.length !== 2 || typeof token[0] !== 'string') throw new ViewError('Invalid page continuation');
      if (request.cursor !== undefined && request.cursor !== token[0]) throw new ViewError('Page cursor mismatch');
      cursor = token[0]; last = token[1];
      nonnegative(last, 'Document continuation');
    }
    if (typeof cursor !== 'string') throw new ViewError('Page cursor must be an opaque string');
    const snapshot = this.readCursor(cursor), limit = this.limit(request);
    const start = partitionPoint(this.documents, d => d.sequence <= last);
    const end = snapshot === this.sequence ? this.documents.length : partitionPoint(this.documents, d => d.sequence <= snapshot);
    const stop = Math.min(end, start + limit);
    const documents = start < stop ? this.documents.slice(start, stop) : [];
    const complete = end <= start + limit;
    return {documents, cursor, after: complete ? null : JSON.stringify([cursor, documents.at(-1).sequence]), complete};
  }
  query(request) {
    const cursor = request.cursor ?? this.cursor();
    if (typeof cursor !== 'string') throw new ViewError('Page cursor must be an opaque string');
    const snapshot = this.readCursor(cursor), limit = this.limit(request);
    const selector = querySelector(request);
    // Validate each key at its own depth budget, without charging the selector
    // envelope as another JSON level (the request validator does the same).
    const selection = '{' + Object.keys(selector).sort().map(name => JSON.stringify(name) + ':' + canonicalValidated(selector[name])).join(',') + '}';
    let last;
    if (request.after !== undefined && request.after !== null) {
      let token;
      try { token = JSON.parse(request.after); } catch { throw new ViewError('Invalid query continuation'); }
      if (!Array.isArray(token) || token.length !== 4 || token[0] !== QUERY_FORMAT || typeof token[1] !== 'string'
        || typeof token[2] !== 'string' || typeof token[3] !== 'string') throw new ViewError('Invalid or obsolete query continuation; obtain a fresh query page');
      if (token[1] !== cursor || token[2] !== selection) throw new ViewError('Query continuation belongs to a different snapshot or selector');
      try {
        last = JSON.parse(token[3]);
        if (canonical(last) !== token[3] || !queryMatches(last, selector)) throw new Error();
      } catch { throw new ViewError('Invalid query continuation key'); }
    }
    let atSnapshot = this.states;
    if (snapshot !== this.sequence) {
      atSnapshot = new Map();
      for (const [encoded, row] of this.historyEntries) {
        if (row.sequence > snapshot) break;
        atSnapshot.set(encoded, row);
      }
    }
    const remaining = [...atSnapshot.entries()].filter(([, row]) => (last === undefined || compareKeys(row.key, last) > 0)
      && queryMatches(row.key, selector)).sort(([, a], [, b]) => compareKeys(a.key, b.key));
    const page = remaining.slice(0, limit), complete = remaining.length <= limit;
    return {rows: page.map(([, row]) => row), cursor,
      after: complete ? null : JSON.stringify([QUERY_FORMAT, cursor, selection, page.at(-1)[0]]), complete};
  }
  changes(request) {
    const since = this.readCursor(request.since ?? 0, true), limit = this.limit(request);
    const start = partitionPoint(this.history, row => row.sequence <= since);
    const rows = this.history.slice(start, start + limit);
    const complete = this.history.length - start <= limit;
    return {rows, cursor: this.cursor(complete ? this.sequence : rows.at(-1).sequence), complete};
  }
  stats() {
    return {documents: this.documents.length, sequence: this.sequence, cursor: this.cursor(), module_sha256: this.view.spec.sha256, descriptor: this.view.descriptor, checkpoints: Object.fromEntries(this.checkpoints), storage: 'memory', memory_bytes: this.view.exports.memory.buffer.byteLength};
  }
  replay() {
    const rebuilt = new MemoryDatabase(this.view);
    for (const batch of this.batches) rebuilt.ingest(batch, true);
    const values = database => [...database.states.entries()].sort(([a], [b]) => scalarCompare(a, b)).map(([, row]) => ({key: row.key, state: row.state, result: row.result}));
    return {equivalent: canonicalValidated(values(this)) === canonicalValidated(values(rebuilt)), documents: this.documents.length, groups: this.states.size, module_sha256: this.view.spec.sha256};
  }
  request(request) {
    databaseRequestJSON(request);
    switch (request.op) {
      case 'ingest': return this.ingest(request.documents, true);
      case 'preview': {
        if (!Object.hasOwn(request, 'payload')) throw new ViewError('Preview requires a payload');
        return mapPayloads(this.view, [request.payload])[0];
      }
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
  } catch (error) {
    const failure = serializeError(error);
    // Keep the text field readable by earlier clients during an asset refresh.
    self.postMessage({id, error: failure.message, errorCode: failure.code});
  }
};

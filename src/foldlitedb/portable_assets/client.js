/* BrowserDatabase owns a dedicated worker. Every request has a wall-clock
   deadline; a stalled Wasm module is terminated without blocking the page.

   const db = await BrowserDatabase.connect({name: 'metrics'});
   const document = await db.add({metric: 'latency', value: 42});
   const rows = await db.rows(); // [{key, state, result, sequence}]
   const snapshot = await db.query({limit: 100});
   await db.add({metric: 'latency', value: 12});
   const updates = await db.changes(snapshot.cursor);
   const retained = await db.export(); // {id, source, position, payload, sequence}[]
   console.assert((await db.replay()).equivalent);
   db.close();

   addMany(payloads) and ingest(resolvedDocuments) return
   {documents, inserted, replayed, sequence, cursor}. Named ids are source-scoped;
   positions start at zero and advance within a source. All batch writes commit
   together. Query/document pages accept {limit, cursor, after}; changes accepts
   a prior cursor and {limit}. Cursors belong to one worker and one module.
   Storage is in memory: export before closing/reloading. */
import {finiteJSON} from './host.js';
import {DatabaseError, deserializeError} from './errors.js';
export {DatabaseError, DocumentConflict, SourceOrderError, CursorExpired} from './errors.js';

export class BrowserDatabase {
  static async connect(options = {}) {
    const database = new BrowserDatabase(options);
    try {
      const init = options.bytes
        ? {op: 'init', bytes: options.bytes, spec: options.spec}
        : {op: 'init', base: new URL(options.base ?? './', import.meta.url).href, name: options.name ?? 'metrics'};
      database.info = await database._call(init);
      return database;
    } catch (error) { database.close(); throw error; }
  }
  constructor({timeoutMs = 15000, workerURL = new URL('./worker.js', import.meta.url)} = {}) {
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60000) throw new DatabaseError('Worker timeout must be between 1 and 60000 ms');
    this.worker = new Worker(workerURL, {type: 'module'});
    this.timeoutMs = timeoutMs; this.nextID = 0; this.pending = null; this.closed = false;
    this.queue = Promise.resolve();
    this.worker.onmessage = ({data}) => {
      if (!this.pending || data.id !== this.pending.id) return;
      const {resolve, reject, timer} = this.pending; clearTimeout(timer); this.pending = null;
      if (Object.hasOwn(data, 'error')) {
        const failure = typeof data.error === 'string' && typeof data.errorCode === 'string'
          ? {code: data.errorCode, message: data.error} : data.error;
        reject(deserializeError(failure));
      } else resolve(data.ok);
    };
    this.worker.onerror = event => this.close(new DatabaseError(event.message || 'Browser database worker failed'));
    this.worker.onmessageerror = () => this.close(new DatabaseError('Cannot decode browser worker response'));
  }
  _call(message) {
    const send = () => {
      if (this.closed) throw new DatabaseError('Browser database worker is closed');
      const id = ++this.nextID;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => this.close(new DatabaseError('Browser database worker deadline exceeded')), this.timeoutMs);
        this.pending = {id, resolve, reject, timer};
        try { this.worker.postMessage({...message, id}); } catch (error) { this.close(new DatabaseError(error.message)); }
      });
    };
    const result = this.queue.then(send); this.queue = result.catch(() => {}); return result;
  }
  request(request) {
    try {
      finiteJSON(request);
      if (new TextEncoder().encode(JSON.stringify(request)).length > 4 * 1024 * 1024) throw new DatabaseError('Browser request exceeds 4 MiB');
    } catch (error) { return Promise.reject(error instanceof DatabaseError ? error : new DatabaseError(error.message)); }
    return this._call(structuredClone(request));
  }
  add(payload, options = {}) { return this.addMany([payload], options).then(result => result.documents[0]); }
  addMany(payloads, options = {}) {
    if (!Array.isArray(payloads)) return Promise.reject(new DatabaseError('Payloads must be an array'));
    const documents = payloads.map((payload, index) => ({...options, payload, ...(options.position === undefined ? {} : {position: options.position + index})}));
    return this.ingest(documents);
  }
  ingest(documents) { return this.request({op: 'ingest', documents}); }
  // Map one payload without retaining it or running its reducer.
  preview(payload) { return this.request({op: 'preview', payload}); }
  query(options = {}) { return this.request({...options, op: 'query'}); }
  async rows(options = {}) {
    let page = await this.query({...options, limit: options.limit ?? 1000}), rows = page.rows;
    while (!page.complete) {
      page = await this.query({...options, after: page.after, cursor: page.cursor, limit: options.limit ?? 1000}); rows.push(...page.rows);
    }
    return rows;
  }
  changes(since = 0, options = {}) { return this.request({...options, op: 'changes', since}); }
  export() { return this.request({op: 'export'}); }
  stats() { return this.request({op: 'stats'}); }
  replay() { return this.request({op: 'replay'}); }
  close(error = new DatabaseError('Browser database worker closed')) {
    if (this.closed) return;
    this.closed = true; this.worker.terminate();
    if (this.pending) { clearTimeout(this.pending.timer); this.pending.reject(error); this.pending = null; }
  }
}

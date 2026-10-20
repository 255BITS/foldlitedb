/* Stable application errors shared by the worker and public browser client. */
export class DatabaseError extends Error {
  constructor(message, code = 'DatabaseError') {
    super(message);
    this.name = code;
    this.code = code;
  }
}

export class DocumentConflict extends DatabaseError {
  constructor(message) { super(message, 'DocumentConflict'); }
}

export class SourceOrderError extends DatabaseError {
  constructor(message) { super(message, 'SourceOrderError'); }
}

export class CursorExpired extends DatabaseError {
  constructor(message) { super(message, 'CursorExpired'); }
}

const types = new Map([
  ['DatabaseError', DatabaseError], ['DocumentConflict', DocumentConflict],
  ['SourceOrderError', SourceOrderError], ['CursorExpired', CursorExpired],
]);

export function serializeError(error) {
  return {code: error instanceof DatabaseError ? error.code : 'DatabaseError',
    message: error?.message || String(error)};
}

export function deserializeError(value) {
  const message = typeof value === 'string' ? value : String(value?.message ?? 'Browser database request failed');
  // Accept older workers' conflict prefix while assets are being refreshed.
  const code = typeof value === 'string'
    ? (value.startsWith('DocumentConflict:') ? 'DocumentConflict' : 'DatabaseError')
    : value?.code;
  const Type = types.get(code) ?? DatabaseError;
  return new Type(message);
}

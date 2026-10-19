/* Portable view ABI v1. Maps and reductions execute exclusively in WebAssembly. */
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', {fatal: true});
export class ViewError extends Error {}
export const MAX_REQUEST_BYTES = 262144;
export const MAX_MEMORY_BYTES = 16 * 1024 * 1024;
function unicode(value) {
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new ViewError('JSON strings must contain well-formed Unicode');
    } else if (unit >= 0xdc00 && unit <= 0xdfff) throw new ViewError('JSON strings must contain well-formed Unicode');
  }
}
export function finiteJSON(value, depth = 0) {
  if (depth > 64) throw new ViewError('JSON nesting exceeds 64');
  if (typeof value === 'string') { unicode(value); return; }
  if (value === null || typeof value === 'boolean') return;
  if (typeof value === 'number' && Number.isFinite(value)) return;
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      if (!Object.hasOwn(value, i)) throw new ViewError('Sparse arrays are not finite JSON');
      finiteJSON(value[i], depth + 1);
    }
    return;
  }
  if (value && Object.getPrototypeOf(value) === Object.prototype) {
    Object.entries(value).forEach(([key, v]) => { unicode(key); finiteJSON(v, depth + 1); }); return;
  }
  throw new ViewError('Expected finite JSON values');
}
export function canonical(value) {
  finiteJSON(value);
  const encode = v => Array.isArray(v) ? '[' + v.map(encode).join(',') + ']'
    : v !== null && typeof v === 'object' ? '{' + Object.keys(v).sort().map(k => JSON.stringify(k) + ':' + encode(v[k])).join(',') + '}'
    : JSON.stringify(v);
  return encode(value);
}
const SHA256_K = Uint32Array.of(
  0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
  0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
  0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
  0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
  0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
  0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
  0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
  0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2);
export function sha256Hex(data) {
  const input = ArrayBuffer.isView(data)
    ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : new Uint8Array(data);
  const padded = new Uint8Array(((input.length + 9 + 63) >>> 6) << 6);
  padded.set(input); padded[input.length] = 0x80;
  const block = new DataView(padded.buffer);
  block.setUint32(padded.length - 8, Math.floor(input.length / 0x20000000));
  block.setUint32(padded.length - 4, (input.length % 0x20000000) * 8);
  const w = new Uint32Array(64);
  let h0=0x6a09e667,h1=0xbb67ae85,h2=0x3c6ef372,h3=0xa54ff53a;
  let h4=0x510e527f,h5=0x9b05688c,h6=0x1f83d9ab,h7=0x5be0cd19;
  for (let at = 0; at < padded.length; at += 64) {
    for (let i = 0; i < 16; i++) w[i] = block.getUint32(at + i * 4);
    for (let i = 16; i < 64; i++) {
      const x = w[i-15], y = w[i-2];
      const s0 = ((x>>>7)|(x<<25)) ^ ((x>>>18)|(x<<14)) ^ (x>>>3);
      const s1 = ((y>>>17)|(y<<15)) ^ ((y>>>19)|(y<<13)) ^ (y>>>10);
      w[i] = (w[i-16] + s0 + w[i-7] + s1) >>> 0;
    }
    let a=h0,b=h1,c=h2,d=h3,e=h4,f=h5,g=h6,h=h7;
    for (let i = 0; i < 64; i++) {
      const S1 = ((e>>>6)|(e<<26)) ^ ((e>>>11)|(e<<21)) ^ ((e>>>25)|(e<<7));
      const t1 = (h + S1 + ((e & f) ^ (~e & g)) + SHA256_K[i] + w[i]) >>> 0;
      const S0 = ((a>>>2)|(a<<30)) ^ ((a>>>13)|(a<<19)) ^ ((a>>>22)|(a<<10));
      const t2 = (S0 + ((a & b) ^ (a & c) ^ (b & c))) >>> 0;
      h=g; g=f; f=e; e=(d+t1)>>>0; d=c; c=b; b=a; a=(t1+t2)>>>0;
    }
    h0=(h0+a)>>>0; h1=(h1+b)>>>0; h2=(h2+c)>>>0; h3=(h3+d)>>>0;
    h4=(h4+e)>>>0; h5=(h5+f)>>>0; h6=(h6+g)>>>0; h7=(h7+h)>>>0;
  }
  return [h0,h1,h2,h3,h4,h5,h6,h7].map(v => v.toString(16).padStart(8, '0')).join('');
}
export async function moduleDigest(binary) {
  const subtle = globalThis.crypto?.subtle;
  if (subtle) {
    try {
      return [...new Uint8Array(await subtle.digest('SHA-256', binary))]
        .map(v => v.toString(16).padStart(2, '0')).join('');
    } catch (error) { /* Web Crypto refused; the portable digest below is authoritative. */ }
  }
  try { return sha256Hex(binary); }
  catch (error) {
    throw new ViewError(`Cannot compute the bundled reducer digest: ${error.message || error}`);
  }
}
/* Reject an unbounded memory before instantiation. The browser API does not expose
   declared limits, so read the standard Wasm memory section directly. */
function memoryLimits(binary) {
  const bytes = new Uint8Array(binary); let at = 8;
  const uint = () => {
    let value = 0, factor = 1;
    for (let i = 0; i < 5; i++) {
      if (at >= bytes.length) throw new ViewError('Truncated WebAssembly module');
      const byte = bytes[at++]; value += (byte & 127) * factor;
      if (!(byte & 128)) {
        if (value > 0xffffffff) throw new ViewError('Invalid WebAssembly integer');
        return value;
      }
      factor *= 128;
    }
    throw new ViewError('Invalid WebAssembly integer');
  };
  let found = false;
  while (at < bytes.length) {
    const section = bytes[at++], length = uint(), end = at + length;
    if (end > bytes.length) throw new ViewError('Truncated WebAssembly section');
    if (section === 5) {
      if (found || uint() !== 1 || uint() !== 1) throw new ViewError('View must declare one bounded, unshared memory');
      const minimum = uint(), maximum = uint();
      if (minimum > maximum || maximum > 256) throw new ViewError('View memory exceeds 16 MiB');
      found = true;
    }
    if (section === 4) {
      const count = uint();
      if (count > 1) throw new ViewError('View may declare at most one bounded table');
      if (count) {
        const element = bytes[at++];
        if ((element !== 0x70 && element !== 0x6f) || uint() !== 1) throw new ViewError('View must declare a bounded table');
        const minimum = uint(), maximum = uint();
        if (minimum > maximum || maximum > 100000) throw new ViewError('View table exceeds 100000 elements');
      }
    }
    at = end;
  }
  if (!found) throw new ViewError('View must declare a bounded memory');
}

export class ViewModule {
  static async load(base = new URL('./', import.meta.url), name = 'metrics') {
    if (!/^[a-zA-Z0-9_-]+$/.test(name)) throw new ViewError('Invalid module name');
    const response = await fetch(new URL(`${name}.json`, base));
    if (!response.ok) throw new ViewError('Cannot load view manifest');
    const spec = await response.json();
    if (typeof spec.module !== 'string' || !/^[a-zA-Z0-9_.-]+\.wasm$/.test(spec.module)) throw new ViewError('Invalid view module filename');
    const binary = await fetch(new URL(spec.module, base));
    if (!binary.ok) throw new ViewError('Cannot load view module');
    return ViewModule.fromBytes(await binary.arrayBuffer(), spec);
  }
  static async fromBytes(bytes, spec) {
    const binary = bytes instanceof ArrayBuffer ? bytes.slice(0)
      : ArrayBuffer.isView(bytes) ? bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) : null;
    if (!binary || binary.byteLength > MAX_MEMORY_BYTES) throw new ViewError('Invalid or oversized view binary');
    finiteJSON(spec);
    if (spec.abi !== 1 || !/^[a-f0-9]{64}$/.test(spec.sha256)) throw new ViewError('Unsupported view manifest');
    if (await moduleDigest(binary) !== spec.sha256) throw new ViewError('View module digest mismatch');
    memoryLimits(binary);
    const module = await WebAssembly.compile(binary);
    if (WebAssembly.Module.imports(module).length) throw new ViewError('View must have no host imports');
    const instance = await WebAssembly.instantiate(module, {}), exports = instance.exports;
    for (const name of ['abi_version', 'input_ptr', 'input_capacity', 'output_ptr', 'execute']) {
      if (typeof exports[name] !== 'function') throw new ViewError(`Missing view export: ${name}`);
    }
    if (!(exports.memory instanceof WebAssembly.Memory) || exports.abi_version() !== 1 || exports.input_capacity() !== MAX_REQUEST_BYTES) throw new ViewError('Unsupported view ABI');
    const view = new ViewModule(exports, JSON.parse(JSON.stringify(spec)), module);
    const descriptor = view.request({op: 'descriptor'});
    if (!descriptor || descriptor.abi !== 1 || typeof descriptor.name !== 'string' || !descriptor.name || typeof descriptor.version !== 'string' || !descriptor.version || typeof descriptor.state_version !== 'string' || !descriptor.state_version || typeof descriptor.mergeable !== 'boolean') throw new ViewError('Invalid view descriptor');
    if (canonical(descriptor) !== canonical(spec.descriptor)) throw new ViewError('View descriptor does not match manifest');
    view.descriptor = descriptor;
    return view;
  }
  constructor(exports, spec, module) { this.exports = exports; this.spec = spec; this.module = module; }
  request(request) {
    finiteJSON(request);
    const encoded = encoder.encode(JSON.stringify(request));
    if (encoded.length > MAX_REQUEST_BYTES) throw new ViewError('Request exceeds 262144 bytes');
    let envelope;
    try {
      // State crosses only the explicit JSON ABI. A fresh instance prevents hidden
      // guest globals from making browser and native replay behave differently.
      this.exports = new WebAssembly.Instance(this.module, {}).exports;
      if (this.exports.memory.buffer.byteLength > MAX_MEMORY_BYTES) throw new ViewError('View memory exceeds 16 MiB');
      const pointer = this.exports.input_ptr(), outputPointer = this.exports.output_ptr();
      if (!Number.isInteger(pointer) || pointer < 0 || pointer + MAX_REQUEST_BYTES > this.exports.memory.buffer.byteLength || !Number.isInteger(outputPointer) || outputPointer < 0 || outputPointer + MAX_REQUEST_BYTES > this.exports.memory.buffer.byteLength || !(pointer + MAX_REQUEST_BYTES <= outputPointer || outputPointer + MAX_REQUEST_BYTES <= pointer)) throw new ViewError('View needs bounded, disjoint input and output buffers');
      new Uint8Array(this.exports.memory.buffer, pointer, encoded.length).set(encoded);
      const length = this.exports.execute(encoded.length), output = this.exports.output_ptr();
      if (!Number.isInteger(length) || length < 1 || length > MAX_REQUEST_BYTES || output < 0 || output + length > this.exports.memory.buffer.byteLength) throw new ViewError('Invalid view response length');
      if (this.exports.memory.buffer.byteLength > MAX_MEMORY_BYTES) throw new ViewError('View memory exceeds 16 MiB');
      envelope = JSON.parse(decoder.decode(new Uint8Array(this.exports.memory.buffer, output, length)));
      finiteJSON(envelope);
    } catch (error) {
      throw new ViewError(`View execution failed: ${error.message || error}`);
    }
    if (!envelope || typeof envelope !== 'object' || Array.isArray(envelope) || Object.keys(envelope).length !== 1) throw new ViewError('Invalid view response envelope');
    if (typeof envelope.error === 'string') throw new ViewError(envelope.error);
    if (!Object.hasOwn(envelope, 'ok')) throw new ViewError('Invalid view response envelope');
    return envelope.ok;
  }
  map(documents) { return this.request({op: 'map', documents}); }
  identity() { return this.request({op: 'identity'}); }
  add(state, values) { return this.request({op: 'add', state, values}); }
  finalize(state) { return this.request({op: 'finalize', state}); }
  merge(left, right) { return this.request({op: 'merge', left, right}); }
}

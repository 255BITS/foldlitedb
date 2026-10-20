import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';

const assets = new URL('../../src/foldlitedb/portable_assets/', import.meta.url);
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });

async function load(name = 'metrics') {
  const bytes = await readFile(new URL(`${name}.wasm`, assets));
  const manifest = JSON.parse(await readFile(new URL(`${name}.json`, assets), 'utf8'));
  assert.equal(createHash('sha256').update(bytes).digest('hex'), manifest.sha256);
  assert.equal(manifest.module, `${name}.wasm`);
  assert.equal(manifest.abi, 1);
  const module = await WebAssembly.compile(bytes);
  assert.deepEqual(WebAssembly.Module.imports(module), []);
  const { exports: api } = await WebAssembly.instantiate(module);
  assert.equal(api.abi_version(), 1);
  assert.equal(api.input_capacity(), 262144);
  const raw = (input, overrideLength = input.length) => {
    assert.ok(input.length <= api.input_capacity());
    new Uint8Array(api.memory.buffer).set(input, api.input_ptr());
    const length = api.execute(overrideLength);
    assert.ok(length > 0 && length <= api.input_capacity());
    return JSON.parse(decoder.decode(new Uint8Array(api.memory.buffer, api.output_ptr(), length)));
  };
  const call = request => {
    const result = raw(encoder.encode(JSON.stringify(request)));
    assert.equal(result.error, undefined, result.error);
    assert.ok(Object.hasOwn(result, 'ok'));
    return result.ok;
  };
  assert.deepEqual(call({ op: 'descriptor' }), manifest.descriptor);
  return { api, raw, call, manifest };
}

test('metrics maps, filters, folds, finalizes, and merges grouped documents', async () => {
  const { call } = await load();
  const mapped = call({ op: 'map', documents: [
    { metric: 'latency', value: 2 }, { metric: 'latency', value: 5.5 },
    { metric: 'size', value: -3 }, { other: true }, null, 7,
    { metric: 'invalid', value: '12' },
  ] });
  assert.deepEqual(mapped, [
    [{ key: 'latency', value: 2 }], [{ key: 'latency', value: 5.5 }],
    [{ key: 'size', value: -3 }], [], [], [], [],
  ]);
  const empty = call({ op: 'identity' });
  assert.deepEqual(empty, { count: 0, sum: 0, min: null, max: null });
  assert.deepEqual(call({ op: 'finalize', state: empty }), { ...empty, mean: null });
  const left = call({ op: 'add', state: empty, values: [2, 5.5] });
  const right = call({ op: 'add', state: empty, values: [-3] });
  const merged = call({ op: 'merge', left, right });
  const together = call({ op: 'add', state: empty, values: [2, 5.5, -3] });
  assert.deepEqual(merged, together);
  assert.deepEqual(call({ op: 'finalize', state: merged }), {
    count: 3, sum: 4.5, min: -3, max: 5.5, mean: 1.5,
  });
  assert.deepEqual(call({ op: 'merge', left: empty, right }), right);
  assert.deepEqual(call({ op: 'merge', left, right: empty }), left);
});

test('Unicode, JSON escapes, and zero emissions survive the byte ABI', async () => {
  const { call, raw } = await load();
  const key = '温度 🌡️ / café\n\t\f\b\r\u0000"\\';
  assert.deepEqual(call({ op: 'map', documents: [{ metric: key, value: 1 }] }), [[{ key, value: 1 }]]);
  assert.deepEqual(raw(encoder.encode('{"op":"map","documents":[{"metric":"\\uD83D\\uDE80\\f","value":3}]}')), {
    ok: [[{ key: '🚀\f', value: 3 }]],
  });
  assert.deepEqual(call({ op: 'map', documents: [] }), []);
  assert.deepEqual(call({ op: 'map', documents: [{}, false, [], 'text'] }), [[], [], [], []]);
});

test('malformed syntax and invalid UTF-8 return errors and allow subsequent calls', async () => {
  const { raw, call } = await load();
  for (const text of [
    '', '{', 'null trailing', '{"op":"identity",}', '{"op":"map","documents":[,]}',
    '{"op":"map","documents":[01]}', '{"op":"map","documents":[1.]}',
    '{"op":"map","documents":[1e]}', '{"op":"map","documents":["\\q"]}',
    '{"op":"map","documents":["\u0001"]}', '[[]',
  ]) assert.match(raw(encoder.encode(text)).error, /JSON/);
  for (const bytes of [[0xc0, 0x80], [0xe2, 0x82], [0xed, 0xa0, 0x80], [0xf4, 0x90, 0x80, 0x80], [0xff]]) {
    assert.match(raw(Uint8Array.from(bytes)).error, /UTF-8/);
  }
  assert.match(raw(new Uint8Array(), -1).error, /length/);
  assert.match(raw(new Uint8Array(), 262145).error, /length/);
  assert.deepEqual(call({ op: 'identity' }), { count: 0, sum: 0, min: null, max: null });
});

test('request shapes, nesting, nonfinite numbers, and invalid states are rejected', async () => {
  const { raw, call } = await load();
  for (const request of [
    null, [], {}, { op: 3 }, { op: 'unknown' }, { op: 'map' }, { op: 'map', documents: {} },
    { op: 'add', state: {}, values: [] }, { op: 'add', state: call({ op: 'identity' }), values: ['bad'] },
    { op: 'finalize' }, { op: 'finalize', state: { count: 1, sum: 2, min: null, max: 2 } },
    { op: 'merge', left: {} },
  ]) assert.equal(typeof raw(encoder.encode(JSON.stringify(request))).error, 'string');
  assert.match(raw(encoder.encode('{"op":"map","documents":[{"metric":"x","value":1e400}]}')).error, /finite/);
  const nested = '{"op":"map","documents":' + '['.repeat(70) + '0' + ']'.repeat(70) + '}';
  assert.match(raw(encoder.encode(nested)).error, /nesting/);
  const tooNested = '{"op":"map","documents":' + '['.repeat(300) + '0' + ']'.repeat(300) + '}';
  assert.match(raw(encoder.encode(tooNested)).error, /JSON/);
  assert.match(raw(encoder.encode(JSON.stringify({
    op: 'add', state: { count: 1, sum: 1e308, min: 1e308, max: 1e308 }, values: [1e308],
  }))).error, /overflow/);
});

test('catalogue preserves structured string state and latest-event order', async () => {
  const { call, raw } = await load('catalogue');
  assert.equal(call({ op: 'descriptor' }).mergeable, false);
  const events = [
    { category: 'books', id: '雪"\n', title: 'First 📚' },
    { category: 'books', id: 'two', title: 'Another' },
    { category: 'books', id: '雪"\n', title: 'Updated' },
    { category: 'books', id: 'two', deleted: true },
    { category: 'books', id: 'third', title: 'Third' },
  ];
  const mapped = call({ op: 'map', documents: events });
  assert.equal(mapped.length, events.length);
  const values = mapped.flat().map(item => item.value);
  const empty = call({ op: 'identity' });
  const full = call({ op: 'add', state: empty, values });
  let incremental = empty;
  for (const value of values) incremental = call({ op: 'add', state: incremental, values: [value] });
  assert.deepEqual(incremental, full);
  assert.deepEqual(call({ op: 'finalize', state: full }), {
    count: 2, items: { '雪"\n': 'Updated', third: 'Third' },
  });
  const reversed = call({ op: 'add', state: empty, values: values.toReversed() });
  assert.notDeepEqual(reversed, full);
  assert.match(raw(encoder.encode(JSON.stringify({ op: 'merge', left: empty, right: empty }))).error, /merge/);
  assert.deepEqual(call({ op: 'map', documents: [{}, { category: 'x', id: 'i' }, { category: 'x', id: 3, title: 't' }] }), [[], [], []]);
});

test('reserved object member names, escaped keys, and floating point tokens round trip', async () => {
  const { call } = await load('catalogue');
  const ids = ['__proto__', 'constructor', 'toString', '', '\u0000\n"\\', '\ud800'];
  const docs = ids.map((id, index) => ({ category: 'reserved', id, title: `title-${index}` }));
  const contributions = call({ op: 'map', documents: docs }).flat().map(row => row.value);
  let state = call({ op: 'add', state: call({ op: 'identity' }), values: contributions });
  state = call({ op: 'add', state, values: [] });
  const expected = Object.fromEntries(ids.map((id, index) => [id, `title-${index}`]));
  assert.deepEqual(call({ op: 'finalize', state }), { count: ids.length, items: expected });
  const metrics = await load();
  for (const token of ['-0', '0.1', '1e-9', '1E+12', '-3.125e2', '5e-324', '1.7976931348623157e308']) {
    const result = metrics.raw(encoder.encode(`{"op":"map","documents":[{"metric":"x","value":${token}}]}`));
    // JSON serialization canonicalizes negative zero to zero in both JS and AS.
    assert.equal(result.ok[0][0].value, JSON.parse(JSON.stringify(JSON.parse(token))), token);
  }
});

test('repeated allocations are collected within bounded Wasm memory', async () => {
  const { api, call } = await load();
  const documents = Array.from({ length: 40 }, (_, value) => ({ metric: `long-session-${value % 4}`, value }));
  for (let i = 0; i < 100; i++) call({ op: 'map', documents });
  const warmSize = api.memory.buffer.byteLength;
  for (let i = 0; i < 10000; i++) call({ op: 'map', documents });
  assert.ok(api.memory.buffer.byteLength <= warmSize + 65536 * 2,
    `Memory grew from ${warmSize} to ${api.memory.buffer.byteLength}`);
  assert.ok(api.memory.buffer.byteLength <= 16 * 1024 * 1024);
});

test('10,000 finite IEEE-754 values retain their exact value through map JSON', async () => {
  const { call } = await load();
  let seed = 8091n;
  const bits = new DataView(new ArrayBuffer(8));
  const values = [];
  while (values.length < 10000) {
    seed = BigInt.asUintN(64, seed * 6364136223846793005n + 1442695040888963407n);
    bits.setBigUint64(0, seed);
    const value = bits.getFloat64(0);
    if (Number.isFinite(value)) values.push(value);
  }
  for (let at = 0; at < values.length; at += 100) {
    const batch = values.slice(at, at + 100);
    const mapped = call({ op: 'map', documents: batch.map(value => ({ metric: 'x', value })) });
    assert.deepEqual(mapped.map(row => row[0].value), batch);
  }
});

test('decimal ties, subnormal boundaries, and overflow round correctly', async () => {
  const { raw } = await load();
  const halfMinimum = 5n ** 1075n;
  const oneMidpoint = ((1n << 53n) + 1n) * 5n ** 53n;
  const maximumCutoff = (1n << 1024n) - (1n << 970n);
  for (const token of [
    `${halfMinimum}e-1075`, `${halfMinimum + 1n}e-1075`,
    `${oneMidpoint}e-53`, `${oneMidpoint + 1n}e-53`,
    `${((1n << 52n) - 1n) * 5n ** 1074n}e-1074`,
    `${5n ** 1022n}e-1022`, `${maximumCutoff - 1n}`,
    '1.6636408826621975e40', '3.238006016251917e48', '-5.811827670584866e286',
  ]) {
    const response = raw(encoder.encode(`{"op":"map","documents":[{"metric":"x","value":${token}}]}`));
    assert.equal(response.error, undefined, response.error);
    assert.equal(response.ok[0][0].value, JSON.parse(token), token.slice(0, 40));
  }
  assert.match(raw(encoder.encode(`{"op":"map","documents":[{"metric":"x","value":${maximumCutoff}}]}`)).error, /finite/);
});

test('batched and sequential metrics preserve ordinary IEEE-754 arithmetic', async () => {
  const { call } = await load();
  const values = Array.from({ length: 200 }, (_, i) => Math.sin(i) * 1000);
  let sum = 0;
  for (const value of values) sum += value;
  const empty = call({ op: 'identity' });
  const batched = call({ op: 'add', state: empty, values });
  let sequential = empty;
  for (const value of values) sequential = call({ op: 'add', state: sequential, values: [value] });
  assert.deepEqual(sequential, batched);
  assert.deepEqual(call({ op: 'finalize', state: sequential }), {
    count: values.length, sum, mean: sum / values.length,
    min: Math.min(...values), max: Math.max(...values),
  });
});

test('both modules declare a maximum of 16 MiB', async () => {
  for (const name of ['metrics', 'catalogue']) {
    const { api } = await load(name);
    api.memory.grow(256 - api.memory.buffer.byteLength / 65536);
    assert.equal(api.memory.buffer.byteLength, 16 * 1024 * 1024);
    assert.throws(() => api.memory.grow(1), RangeError);
  }
});

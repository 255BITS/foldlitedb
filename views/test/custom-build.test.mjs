import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { join, relative } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { compilerEnvironment, viewsRoot } from '../scripts/view-build.mjs';

const cli = fileURLToPath(new URL('../scripts/build-view.mjs', import.meta.url));
const run = (args, options = {}) => spawnSync(process.execPath, [cli, ...args], {
  cwd: tmpdir(), encoding: 'utf8', ...options,
});

test('custom build command compiles a view, validates it, and preserves outputs on failure', async (t) => {
  await mkdir(join(viewsRoot, '.build'), { recursive: true });
  const fixture = await mkdtemp(join(viewsRoot, '.build/custom-build-test-'));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const source = join(fixture, 'invoice.totals.ts');
  const output = join(fixture, 'output');
  const sdk = relative(fixture, join(viewsRoot, 'assembly/sdk')).replaceAll('\\', '/');
  const sourceText = `import {JSON, View, Emission, object, numeric, number, dispatch} from ${JSON.stringify(sdk)};
export {abi_version, input_ptr, input_capacity, output_ptr} from ${JSON.stringify(sdk)};
class Totals extends View {
  constructor() { super("invoice-summary", false, "2", "1"); }
  map(value: JSON.Value): Emission[] {
    const doc = object(value); if (doc == null) return [];
    const key = doc.get("category"); const amount = doc.get("amount");
    if (key == null || key.type != JSON.Types.String || !numeric(amount)) return [];
    return [new Emission(key, amount!)];
  }
  identity(): JSON.Value { return JSON.Value.from<f64>(0); }
  add(state: JSON.Value, values: JSON.Arr): JSON.Value {
    let total = number(state);
    for (let i = 0; i < values.length; i++) total += number(values.at(i));
    return JSON.Value.from(total);
  }
  finalize(state: JSON.Value): JSON.Value { return state; }
}
const view = new Totals();
export function execute(length: i32): i32 { return dispatch(view, length); }
`;
  await writeFile(source, sourceText);
  const args = [relative(viewsRoot, source), '--out-dir', relative(viewsRoot, output)];
  const result = spawnSync('npm', ['run', 'build:view', '--prefix', viewsRoot, '--', ...args], {
    cwd: tmpdir(), encoding: 'utf8', env: { ...process.env, JSON_MODE: 'simd', JSON_DEBUG: 'true' },
  });
  assert.equal(result.status, 0, result.stdout + result.stderr);
  const bytes = await readFile(join(output, 'invoice.totals.wasm'));
  const manifestBytes = await readFile(join(output, 'invoice.totals.json'));
  const manifest = JSON.parse(manifestBytes);
  assert.equal(manifest.module, 'invoice.totals.wasm');
  assert.equal(manifest.sha256, createHash('sha256').update(bytes).digest('hex'));
  assert.deepEqual(manifest.descriptor, { abi: 1, name: 'invoice-summary', version: '2', state_version: '1', mergeable: false });
  const { exports: api } = new WebAssembly.Instance(new WebAssembly.Module(bytes));
  const call = request => {
    const input = new TextEncoder().encode(JSON.stringify(request));
    new Uint8Array(api.memory.buffer).set(input, api.input_ptr());
    const length = api.execute(input.length);
    const response = JSON.parse(new TextDecoder().decode(new Uint8Array(api.memory.buffer, api.output_ptr(), length)));
    assert.ok(Object.hasOwn(response, 'ok'), JSON.stringify(response));
    return response.ok;
  };
  assert.deepEqual(call({ op: 'map', documents: [{ category: 'Café', amount: 2.5 }, { category: 'Café', amount: 7.5 }, {}] }), [[{ key: 'Café', value: 2.5 }], [{ key: 'Café', value: 7.5 }], []]);
  assert.equal(call({ op: 'finalize', state: call({ op: 'add', state: call({ op: 'identity' }), values: [2.5, 7.5] }) }), 10);

  const rebuilt = run(args, { env: { ...process.env, JSON_MODE: 'invalid-mode', JSON_STRICT: 'false' } });
  assert.equal(rebuilt.status, 0, rebuilt.stdout + rebuilt.stderr);
  assert.deepEqual(await readFile(join(output, 'invoice.totals.wasm')), bytes, 'JSON_* ambient switches cannot change artifacts');
  assert.deepEqual(await readFile(join(output, 'invoice.totals.json')), manifestBytes);

  await writeFile(source, 'this is not AssemblyScript');
  const broken = run(args);
  assert.notEqual(broken.status, 0);
  assert.match(broken.stderr, /Compilation failed/);
  assert.deepEqual(await readFile(join(output, 'invoice.totals.wasm')), bytes);
  assert.deepEqual(await readFile(join(output, 'invoice.totals.json')), manifestBytes);

  await writeFile(source, `export {input_ptr} from ${JSON.stringify(sdk)}; export function unrelated(): i32 { return 1; }`);
  const invalid = run(args);
  assert.notEqual(invalid.status, 0);
  assert.match(invalid.stderr, /Missing or invalid ABI export: abi_version/);
  assert.deepEqual(await readFile(join(output, 'invoice.totals.wasm')), bytes);
  assert.deepEqual(await readFile(join(output, 'invoice.totals.json')), manifestBytes);

  await writeFile(source, sourceText.replace(
    'export function execute(length: i32): i32 { return dispatch(view, length); }',
    '@external("env", "clock") declare function clock(): i32; export function execute(length: i32): i32 { return dispatch(view, length + clock()); }',
  ));
  const importedHostFunction = run(args);
  assert.notEqual(importedHostFunction.status, 0);
  assert.match(importedHostFunction.stderr, /must not import host functions or memory/);
  assert.deepEqual(await readFile(join(output, 'invoice.totals.wasm')), bytes);

  await writeFile(source, sourceText.replace('super("invoice-summary", false, "2", "1")', 'super("", false, "2", "1")'));
  const badDescriptor = run(args);
  assert.notEqual(badDescriptor.status, 0);
  assert.match(badDescriptor.stderr, /Invalid view descriptor/);
  assert.deepEqual(await readFile(join(output, 'invoice.totals.json')), manifestBytes);
});

test('custom build diagnoses invocation, paths, missing compiler, and protected outputs', async (t) => {
  const fixture = await mkdtemp(join(tmpdir(), 'foldlitedb-cli-test-'));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const source = join(fixture, 'view.ts');
  await writeFile(source, 'export function execute(n: i32): i32 { return n; }');
  const spacedSource = join(fixture, 'view name.ts');
  await writeFile(spacedSource, 'export function execute(n: i32): i32 { return n; }');
  for (const [args, message] of [
    [[], /Provide a source .ts file and --out-dir/],
    [[source], /Provide a source/],
    [[source, '--out-dir'], /requires one output directory/],
    [[source, '--wat'], /Unknown option: --wat/],
    [['missing-custom-view.ts', '--out-dir', '.build/nope'], /Source file not found/],
    [['input.js', '--out-dir', '.build/nope'], /must be an AssemblyScript .ts file/],
    [[spacedSource, '--out-dir', '.build/nope'], /Source filename must use only/],
    [[source, '--out-dir', 'assembly/sdk'], /Output directory is protected/],
    [[source, '--out-dir', '../src/foldlitedb/portable_assets'], /Output directory is protected/],
    [[source, '--out-dir', source], /Cannot (use|create) output directory/],
  ]) {
    const result = run(args);
    assert.notEqual(result.status, 0, args.join(' '));
    assert.match(result.stderr, message);
    assert.doesNotMatch(result.stderr, /\n\s+at /, 'actionable errors do not dump Node stacks');
  }
  assert.equal(run(['--help']).status, 0);
  const sandboxScripts = join(fixture, 'scripts');
  await mkdir(sandboxScripts);
  for (const name of ['build-view.mjs', 'view-build.mjs', 'inspect-view.mjs']) await copyFile(join(viewsRoot, 'scripts', name), join(sandboxScripts, name));
  const missingCompiler = spawnSync(process.execPath, [join(sandboxScripts, 'build-view.mjs'), source, '--out-dir', join(fixture, 'output')], { encoding: 'utf8' });
  assert.notEqual(missingCompiler.status, 0);
  assert.match(missingCompiler.stderr, /Missing compiler dependency.*npm ci --prefix views/);
  assert.doesNotMatch(missingCompiler.stderr, /\n\s+at /);
  assert.deepEqual(compilerEnvironment({ JSON_MODE: 'simd', JSON_DEBUG: 'true', NODE_OPTIONS: 'anything', NODE_PATH: 'anything', PATH: '/example' }), { PATH: '/example', JSON_MODE: 'naive' });
});

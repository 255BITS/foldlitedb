import { spawnSync } from 'node:child_process';
import { access, mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { basename, join, relative, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

export const viewsRoot = fileURLToPath(new URL('..', import.meta.url));

export function compilerEnvironment(environment = process.env) {
  const clean = Object.fromEntries(Object.entries(environment).filter(([key]) =>
    !key.startsWith('JSON_') && key !== 'NODE_OPTIONS' && key !== 'NODE_PATH'));
  return { ...clean, JSON_MODE: 'naive' };
}

export async function compileView(source) {
  for (const dependency of ['assemblyscript/bin/asc.js', 'json-as/transform/lib/index.js']) {
    try { await access(resolve(viewsRoot, 'node_modules', dependency), constants.R_OK); }
    catch { throw new Error(`Missing compiler dependency ${dependency}. Run npm ci --prefix views from the repository root.`); }
  }
  const staging = await mkdtemp(join(tmpdir(), 'foldlitedb-view-'));
  try {
    const output = join(staging, 'view.wasm');
    const result = spawnSync(process.execPath, [
      'node_modules/assemblyscript/bin/asc.js', relative(viewsRoot, source),
      '--outFile', output, '--transform', 'json-as', '--runtime', 'incremental',
      '--maximumMemory', '256', '--initialMemory', '16', '--stackSize', '32768',
      '--use', 'abort=assembly/sdk/index/abort',
      '--optimizeLevel', '3', '--shrinkLevel', '1',
    ], { cwd: viewsRoot, env: compilerEnvironment(), encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 });
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    if (result.error) throw new Error(`Could not run the AssemblyScript compiler: ${result.error.message}`);
    if (result.status !== 0) throw new Error('Compilation failed. Correct the compiler diagnostics above; existing outputs were preserved.');
    const bytes = await readFile(output);
    const moduleName = `${basename(source, '.ts')}.wasm`;
    const inspected = spawnSync(process.execPath, [fileURLToPath(new URL('./inspect-view.mjs', import.meta.url)), output, moduleName], {
      cwd: viewsRoot, env: compilerEnvironment(), encoding: 'utf8', timeout: 5000, maxBuffer: 1024 * 1024,
    });
    if (inspected.error?.code === 'ETIMEDOUT') throw new Error('Module validation exceeded 5 seconds. Ensure module initialization and descriptor execution terminate.');
    if (inspected.error) throw new Error(`Could not validate the compiled module: ${inspected.error.message}`);
    if (inspected.status !== 0) throw new Error(inspected.stderr.trim() || 'Compiled module validation failed.');
    return { bytes, manifest: Buffer.from(JSON.stringify(JSON.parse(inspected.stdout), null, 2) + '\n') };
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
}

export async function requireSource(source) {
  if (!source.endsWith('.ts')) throw new Error(`Source must be an AssemblyScript .ts file: ${source}`);
  let info;
  try { info = await stat(source); }
  catch (error) {
    if (error.code === 'ENOENT') throw new Error(`Source file not found: ${source}. Source paths are relative to views/.`);
    throw new Error(`Cannot read source ${source}: ${error.message}`);
  }
  if (!info.isFile()) throw new Error(`Source is not a file: ${source}`);
  try { await access(source, constants.R_OK); }
  catch { throw new Error(`Source file is not readable: ${source}`); }
}

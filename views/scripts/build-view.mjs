import { lstat, mkdir, mkdtemp, realpath, rename, rm, writeFile } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { compileView, requireSource, viewsRoot } from './view-build.mjs';

const usage = 'Usage: npm run build:view --prefix views -- <source.ts> --out-dir <directory>\nSource and output paths are relative to views/; absolute paths are also accepted.';

function parse(args) {
  if (args.length === 1 && ['--help', '-h'].includes(args[0])) return null;
  let source, outDir;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--out-dir') {
      if (outDir !== undefined || !args[i + 1] || args[i + 1].startsWith('--')) throw new Error('--out-dir requires one output directory.');
      outDir = args[++i];
    } else if (arg.startsWith('-')) throw new Error(`Unknown option: ${arg}`);
    else if (source !== undefined) throw new Error(`Unexpected argument: ${arg}`);
    else source = arg;
  }
  if (!source || !outDir) throw new Error('Provide a source .ts file and --out-dir.');
  return { source: resolve(viewsRoot, source), outDir: resolve(viewsRoot, outDir) };
}

async function canonicalPath(path) {
  try { return await realpath(path); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    return join(await canonicalPath(dirname(path)), basename(path));
  }
}

function within(path, directory) {
  const child = relative(directory, path);
  return child === '' || (!isAbsolute(child) && child !== '..' && !child.startsWith(`..${sep}`));
}

try {
  const args = parse(process.argv.slice(2));
  if (!args) { console.log(usage); }
  else {
    await requireSource(args.source);
    const stem = basename(args.source, '.ts');
    if (!/^[a-zA-Z0-9_.-]+$/.test(stem)) throw new Error('Source filename must use only letters, digits, underscores, dots, or hyphens so its artifacts can load in the browser.');
    let output;
    try { output = await canonicalPath(args.outDir); }
    catch (error) { throw new Error(`Cannot use output directory ${args.outDir}: ${error.message}`); }
    try {
      if (!(await lstat(output)).isDirectory()) throw new Error(`Cannot use output directory ${args.outDir}: path is not a directory.`);
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    for (const protectedPath of ['assembly/sdk', 'node_modules', 'scripts', '../src/foldlitedb/portable_assets']) {
      if (within(output, await canonicalPath(resolve(viewsRoot, protectedPath)))) throw new Error(`Output directory is protected: ${args.outDir}. Choose a separate build directory, such as .build/custom.`);
    }
    for (const suffix of ['wasm', 'json']) {
      const target = join(output, `${stem}.${suffix}`);
      try {
        const info = await lstat(target);
        if (!info.isFile() || info.isSymbolicLink()) throw new Error(`Output must be a regular file, not a directory or symlink: ${target}`);
      } catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
    try { await mkdir(output, { recursive: true }); }
    catch (error) { throw new Error(`Cannot create output directory ${args.outDir}: ${error.message}`); }
    let staging;
    try {
      staging = await mkdtemp(join(output, '.foldlitedb-build-'));
      const { bytes, manifest } = await compileView(args.source);
      await writeFile(join(staging, `${stem}.wasm`), bytes);
      await writeFile(join(staging, `${stem}.json`), manifest);
      for (const suffix of ['wasm', 'json']) await rename(join(staging, `${stem}.${suffix}`), join(output, `${stem}.${suffix}`));
      console.log(`Built ${join(output, `${stem}.wasm`)} (${bytes.length} bytes)`);
      console.log(`Manifest ${join(output, `${stem}.json`)}`);
    } finally {
      if (staging) await rm(staging, { recursive: true, force: true });
    }
  }
} catch (error) {
  console.error(`build:view: ${error.message}\n${usage}`);
  process.exitCode = 1;
}

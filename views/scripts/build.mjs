import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { compileView } from './view-build.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const destination = resolve(root, '../src/foldlitedb/portable_assets');
const check = process.argv.includes('--check');
await mkdir(resolve(root, '.build'), { recursive: true });
await mkdir(destination, { recursive: true });

const noticeFiles = [
  ['AssemblyScript 0.28.20 runtime and standard library', 'assemblyscript/LICENSE'],
  ['AssemblyScript 0.28.20 upstream attribution notice', 'assemblyscript/NOTICE'],
  ['json-as 1.6.2', 'json-as/LICENSE'],
  ['xjb-as 0.1.0 float formatting', 'xjb-as/LICENSE'],
  ['xjb-as 0.1.0 additional Apache-2.0 terms', 'xjb-as/LICENSE.Apache-2.0'],
  ['xjb-as 0.1.0 upstream attribution notice', 'xjb-as/NOTICE'],
];
let notices = `Third-party licenses and notices for the bundled portable Wasm views
===================================================================

The compiled metrics.wasm and catalogue.wasm modules contain AssemblyScript
runtime/standard-library code, json-as JSON code, and xjb-as float-formatting
code. Their upstream license and notice texts follow, with line endings
normalized to LF. Upstream npm source files have not been modified.

The AssemblyScript upstream NOTICE is reproduced in full. Some notices in it
describe compiler-only dependencies. The JavaScript long package, Binaryen,
and the AssemblyScript compiler are build tools and are not bundled as runtimes
in these Wasm modules. This notice does not claim those tools are linked into
the modules.

`;
for (const [label, filename] of noticeFiles) {
  const contents = (await readFile(resolve(root, 'node_modules', filename), 'utf8')).replace(/\r\n/g, '\n');
  notices += `${label}\nSource: npm ${filename}\n${'-'.repeat(72)}\n${contents.trimEnd()}\n\n`;
}
const noticePath = resolve(destination, 'THIRD_PARTY_LICENSES.txt');
if (check) {
  if (await readFile(noticePath, 'utf8') !== notices) throw new Error('Stale third-party license notices. Run npm run build --prefix views.');
} else await writeFile(noticePath, notices);

for (const name of ['metrics', 'catalogue']) {
  const { bytes, manifest } = await compileView(resolve(root, `assembly/examples/${name}.ts`));
  for (const [suffix, contents] of [['wasm', bytes], ['json', manifest]]) {
    const path = resolve(destination, `${name}.${suffix}`);
    if (check) {
      if (!(await readFile(path)).equals(contents)) throw new Error(`Stale artifact: ${path}. Run npm run build --prefix views.`);
    } else await writeFile(path, contents);
  }
  console.log(`${check ? 'Verified' : 'Built'} ${name}: ${bytes.length} bytes`);
}

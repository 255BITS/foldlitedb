import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {compileView} from '../scripts/view-build.mjs';
import {fileURLToPath} from 'node:url';
import {ViewModule, ViewError} from '../../src/foldlitedb/portable_assets/host.js';

const assets = new URL('../../src/foldlitedb/portable_assets/', import.meta.url);
async function view(name = 'metrics') {
  return ViewModule.fromBytes(await readFile(new URL(`${name}.wasm`, assets)), JSON.parse(await readFile(new URL(`${name}.json`, assets), 'utf8')));
}

test('grouped and legacy metrics preserve state, results, explicit null, and bounded chunk order', async () => {
  const grouped = await view(), legacy = await view();
  legacy.descriptor.reduce_groups = false;
  const groups = Array.from({length:70}, (_,i) => ({values:[i,2,-3.5]}));
  const requests = [], original = grouped.request.bind(grouped);
  grouped.request = request => {requests.push(request);return original(request);};
  const actual = grouped.reduceGroups(groups);
  assert.deepEqual(actual, legacy.reduceGroups(groups));
  assert.equal(requests.length, 3);
  assert.ok(requests.every(request => request.op === 'reduce_groups' && request.groups.length <= 32));
  assert.ok(actual.every(row => !Object.hasOwn(row.state, 'mean') && Object.hasOwn(row.result, 'mean')));
  assert.throws(() => grouped.reduceGroups([{state:null,values:[1]}]), /Invalid metrics state/);
});

test('ordered catalogue splits requests and large responses including singleton legacy fallback', async () => {
  const grouped = await view('catalogue'), legacy = await view('catalogue');
  legacy.descriptor.reduce_groups = false;
  const groups = Array.from({length:65}, (_,i) => ({values:[
    {id:'x',title:`first${i}`,deleted:false}, {id:'x',title:'last',deleted:false},
    {id:'y',title:'drop',deleted:false}, {id:'y',title:null,deleted:true},
  ]}));
  assert.deepEqual(grouped.reduceGroups(groups), legacy.reduceGroups(groups));
  const large = Array.from({length:3}, () => ({values:[{id:'x',title:'x'.repeat(140000),deleted:false}]}));
  const requests = [], original = grouped.request.bind(grouped);
  grouped.request = request => {requests.push(request.op);return original(request);};
  assert.deepEqual(grouped.reduceGroups(large), legacy.reduceGroups(large));
  assert.ok(requests.includes('reduce_groups') && requests.includes('add') && requests.includes('finalize'));
});

test('ordinary view errors and malformed split signals never trigger fallback', async () => {
  const grouped = await view();
  for (const output of [{split:true,extra:1},{groups:[]},{groups:[{state:null,result:0,extra:1}]}]) {
    let requests=0;
    grouped.request = () => {requests++;return output;};
    assert.throws(() => grouped.reduceGroups([{values:[1]}]), /reduce_groups|reduced group/);
    assert.equal(requests,1);
  }
  let requests=0;
  grouped.request = () => {requests++;throw new ViewError('Response exceeds ABI byte capacity');};
  assert.throws(() => grouped.reduceGroups([{values:[1]}]), /Response exceeds ABI byte capacity/);
  assert.equal(requests,1);
});

test('SDK returns a structured split for capacity and rejects invalid grouped requests', async () => {
  const grouped = await view('catalogue');
  assert.deepEqual(grouped.request({op:'reduce_groups',groups:[{values:[{id:'x',title:'x'.repeat(140000),deleted:false}]}]}), {split:true});
  for (const groups of [[],Array.from({length:33},()=>({values:[]})),[{}],[{values:[],extra:1}],[{values:{}}]]) {
    assert.throws(() => grouped.request({op:'reduce_groups',groups}), /groups|group|values/);
  }
});

test('group SDK fixture is reproducible and retains intermediate integer spellings and data depth', async () => {
  const fixtureBase = new URL('../../tests/fixtures/', import.meta.url);
  const binary = await readFile(new URL('group_contract.wasm', fixtureBase));
  const manifest = JSON.parse(await readFile(new URL('group_contract.json', fixtureBase), 'utf8'));
  const rebuilt = await compileView(fileURLToPath(new URL('../assembly/test/group_contract.ts', import.meta.url)));
  assert.deepEqual(rebuilt.bytes, binary);
  assert.deepEqual(JSON.parse(rebuilt.manifest), manifest);
  const grouped = await ViewModule.fromBytes(binary, manifest);
  assert.deepEqual(grouped.reduceGroups([{state:null, values:[]}]), [{state:null,result:null}]);
  for (const depth of [63,64]) {
    let nested = null; for (let i=0;i<depth;i++) nested=[nested];
    assert.deepEqual(grouped.reduceGroups([{state:null,values:[nested]}]), [{state:null,result:null}]);
    if (depth === 63) assert.deepEqual(grouped.reduceGroups([{state:nested,values:[]}]), [{state:nested,result:nested}]);
    else assert.throws(() => grouped.reduceGroups([{state:nested,values:[]}]), /invalid|finite|nesting/);
  }
  const api = new WebAssembly.Instance(new WebAssembly.Module(binary)).exports;
  const input = new TextEncoder().encode(JSON.stringify({op:'reduce_groups',groups:[{values:['discard']}]}));
  new Uint8Array(api.memory.buffer).set(input,api.input_ptr());
  const length=api.execute(input.length);
  const response = new TextDecoder().decode(new Uint8Array(api.memory.buffer,api.output_ptr(),length));
  assert.match(response, /"identity":9007199254740993/);
});

test('SDK rejects oversized generated identity and state member names in both paths', async () => {
  const base = new URL('../../tests/fixtures/', import.meta.url);
  for (const name of ['group_contract','group_long_identity']) {
    const binary=await readFile(new URL(`${name}.wasm`,base));
    const manifest=JSON.parse(await readFile(new URL(`${name}.json`,base),'utf8'));
    const rebuilt=await compileView(fileURLToPath(new URL(`../assembly/test/${name}.ts`,import.meta.url)));
    assert.deepEqual(rebuilt.bytes,binary);
    const grouped=await ViewModule.fromBytes(binary,manifest), legacy=await ViewModule.fromBytes(binary,manifest);
    legacy.descriptor.reduce_groups=false;
    const groups=name==='group_contract' ? [{state:null,values:['long-key']}] : [{values:['discard']}];
    for (const engine of [grouped,legacy]) assert.throws(()=>engine.reduceGroups(groups),/65535|unreachable/);
  }
});

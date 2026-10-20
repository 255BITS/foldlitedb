// Isolated from the build process so a broken module cannot hang descriptor inspection.
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';

try {
  const bytes = await readFile(process.argv[2]);
  if (bytes.length < 1 || bytes.length > 16 * 1024 * 1024) throw new Error('Module size must be between 1 byte and 16 MiB');
  const module = new WebAssembly.Module(bytes);
  if (WebAssembly.Module.imports(module).length) throw new Error('Module must not import host functions or memory');
  const api = new WebAssembly.Instance(module).exports;
  for (const [name, arity] of [['abi_version', 0], ['input_ptr', 0], ['input_capacity', 0], ['output_ptr', 0], ['execute', 1]]) {
    if (typeof api[name] !== 'function' || api[name].length !== arity) throw new Error(`Missing or invalid ABI export: ${name}`);
  }
  if (!(api.memory instanceof WebAssembly.Memory) || api.memory.buffer.byteLength > 16 * 1024 * 1024 || api.abi_version() !== 1 || api.input_capacity() !== 262144) throw new Error('Unsupported view ABI: expected ABI 1, exported memory, and 262144-byte input capacity');
  const input = new TextEncoder().encode('{"op":"descriptor"}');
  const inputPtr = api.input_ptr();
  if (!Number.isInteger(inputPtr) || inputPtr < 0 || inputPtr + input.length > api.memory.buffer.byteLength) throw new Error('Invalid ABI input buffer');
  new Uint8Array(api.memory.buffer).set(input, inputPtr);
  const length = api.execute(input.length);
  const outputPtr = api.output_ptr();
  if (!Number.isInteger(length) || length < 1 || length > 262144 || !Number.isInteger(outputPtr) || outputPtr < 0 || outputPtr + length > api.memory.buffer.byteLength) throw new Error('Invalid ABI descriptor response buffer');
  const response = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(new Uint8Array(api.memory.buffer, outputPtr, length)));
  if (!response || !Object.hasOwn(response, 'ok') || Object.hasOwn(response, 'error')) throw new Error(`Descriptor request failed: ${response?.error ?? 'expected an ok envelope'}`);
  const descriptor = response.ok;
  if (!descriptor || descriptor.abi !== 1 || typeof descriptor.name !== 'string' || !descriptor.name || typeof descriptor.version !== 'string' || !descriptor.version || typeof descriptor.state_version !== 'string' || !descriptor.state_version || typeof descriptor.mergeable !== 'boolean') throw new Error('Invalid view descriptor: expected abi, name, version, state_version, and mergeable');
  process.stdout.write(JSON.stringify({ abi: 1, module: process.argv[3], sha256: createHash('sha256').update(bytes).digest('hex'), descriptor }));
} catch (error) {
  process.stderr.write(`Compiled module is not a portable view: ${error.message}\n`);
  process.exitCode = 1;
}

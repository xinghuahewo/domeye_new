import { parentPort, workerData } from 'node:worker_threads';
import { newQuickJSWASMModule } from 'quickjs-emscripten';
import { outlineSchema, selectSchema } from '../schema-tools.mjs';
import { createApiDiscovery } from '../api-operations.mjs';

const { code, spec, sourceSpec, operations, hasRequest, hasReadResult, timeoutMs, memoryBytes, maxRequests, maxRequestBytes, maxResultBytes } = workerData;
const QuickJS = await newQuickJSWASMModule();
const runtime = QuickJS.newRuntime();
runtime.setMemoryLimit(memoryBytes);
runtime.setMaxStackSize(512 * 1024);
const deadline = Date.now() + timeoutMs;
runtime.setInterruptHandler(() => Date.now() > deadline);
// 不设置模块加载器，不提供 Node、文件系统、fetch 或任何环境变量。
const vm = runtime.newContext();
const pending = new Map();
let nextId = 0, ended = false, promiseHandle;
function finish(message) {
  if (ended) return;
  ended = true;
  parentPort.postMessage(message);
  for (const deferred of pending.values()) deferred.dispose();
  pending.clear();
  promiseHandle?.dispose();
  vm.dispose();
  runtime.dispose();
  parentPort.close();
}
function fail(error) {
  finish({ type: 'failure', error: { kind: error.kind ?? 'code', message: error.message ?? String(error) } });
}
function pump() {
  if (ended) return;
  const jobs = runtime.executePendingJobs();
  if (jobs.error) {
    const error = vm.dump(jobs.error);
    jobs.error.dispose();
    fail(error);
  }
}
try {
  if (spec !== undefined) vm.newString(JSON.stringify(spec)).consume(handle => vm.setProp(vm.global, '__specJSON', handle));
  if (sourceSpec !== undefined) vm.newString(JSON.stringify(sourceSpec)).consume(handle => vm.setProp(vm.global, '__sourceSpecJSON', handle));
  if (operations !== undefined) vm.newString(JSON.stringify(operations)).consume(handle => vm.setProp(vm.global, '__operationsJSON', handle));
  for (const [name, type, enabled] of [['__hostRequest', 'request', hasRequest], ['__hostReadResult', 'readResult', hasReadResult],
    ['__hostOperation', 'operation', hasRequest && operations !== undefined]]) {
    if (!enabled) continue;
    vm.newFunction(name, handle => {
      const id = ++nextId;
      const text = vm.getString(handle);
      if (id > maxRequests || Buffer.byteLength(text) > maxRequestBytes) {
        const error = vm.newError('本次执行超过宿主调用次数或参数大小上限。');
        vm.newString('input').consume(kind => vm.setProp(error, 'kind', kind));
        return { error };
      }
      const value = JSON.parse(text);
      const deferred = vm.newPromise();
      pending.set(id, deferred);
      parentPort.postMessage({ type, id, value });
      return deferred.handle;
    }).consume(handle => vm.setProp(vm.global, name, handle));
  }

  parentPort.on('message', message => {
    const deferred = pending.get(message.id);
    if (!deferred || ended) return;
    pending.delete(message.id);
    if (message.error) {
      const error = vm.newError(message.error.message);
      vm.newString(message.error.kind).consume(kind => vm.setProp(error, 'kind', kind));
      deferred.reject(error);
      error.dispose();
    } else vm.newString(JSON.stringify(message.value)).consume(value => deferred.resolve(value));
    deferred.dispose();
    pump();
  });

  const source = `(() => {
    const parse = JSON.parse, stringify = JSON.stringify;
    const rawRequest = globalThis.__hostRequest;
    const rawReadResult = globalThis.__hostReadResult;
    const rawOperation = globalThis.__hostOperation;
    const operations = typeof globalThis.__operationsJSON === 'string' ? parse(globalThis.__operationsJSON) : {};
    const methods = Object.fromEntries(Object.entries(operations).filter(([,operation]) => operation.callable)
      .map(([id]) => [id, async params => parse(await rawOperation(stringify({operationId:id,params})))]));
    if (rawRequest) globalThis.domeye = Object.freeze({
      request: async value => parse(await rawRequest(stringify(value))),
      api: Object.freeze(methods),
      ...(rawReadResult ? {readResult: async id => parse(await rawReadResult(stringify(id)))} : {})
    });
    if (typeof globalThis.__specJSON === 'string') {
      globalThis.spec = parse(globalThis.__specJSON);
      globalThis.schemaTools = Object.freeze({outline: (${outlineSchema.toString()}), select: (${selectSchema.toString()})});
      const sourceSpec = typeof globalThis.__sourceSpecJSON === 'string' ? parse(globalThis.__sourceSpecJSON) : globalThis.spec;
      globalThis.api = (${createApiDiscovery.toString()})(globalThis.spec, operations, globalThis.schemaTools.outline, globalThis.schemaTools.select, sourceSpec);
    }
    delete globalThis.__hostRequest;
    delete globalThis.__hostReadResult;
    delete globalThis.__hostOperation;
    delete globalThis.__specJSON;
    delete globalThis.__sourceSpecJSON;
    delete globalThis.__operationsJSON;
    const isArray = Array.isArray, getPrototypeOf = Object.getPrototypeOf, values = Object.values;
    const plain = Object.prototype, finite = Number.isFinite;
    function assertJSON(value, seen = new Set()) {
      if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
      if (typeof value === 'number' && finite(value)) return;
      if (typeof value !== 'object' || seen.has(value) || (!isArray(value) && getPrototypeOf(value) !== plain)) {
        const error = new Error('返回值不能表示为 JSON；请保留明确数值、字符串、数组、对象或 null。');
        error.kind = 'serialization'; throw error;
      }
      seen.add(value);
      if (isArray(value)) for (let i=0; i<value.length; i++) assertJSON(value[i], seen);
      // JSON 对象可省略未定义的可选字段；数组缺位不能默默变成 null。
      else for (const item of values(value)) if (item !== undefined) assertJSON(item, seen);
      seen.delete(value);
    }
    return (async () => {
      const fn = (${code});
      if (typeof fn !== 'function') throw new Error('code 必须描述一个可调用的异步函数。');
      const value = await fn();
      assertJSON(value);
      return stringify(value);
    })();
  })()`;
  const evaluated = vm.evalCode(source, 'tool-code.js');
  if (evaluated.error) {
    const error = vm.dump(evaluated.error);
    evaluated.error.dispose();
    fail(error);
  } else {
    promiseHandle = evaluated.value;
    const settled = vm.resolvePromise(promiseHandle);
    pump();
    if (!ended) {
      const result = await settled;
      if (result.error) {
        const error = vm.dump(result.error);
        result.error.dispose();
        fail(error);
      } else {
        const text = vm.getString(result.value);
        result.value.dispose();
        if (Buffer.byteLength(text) > maxResultBytes) fail({ kind: 'serialization', message: '结果超过输出大小上限，请在代码中筛选需要的内容。' });
        else finish({ type: 'result', value: JSON.parse(text) });
      }
    }
  }
} catch (error) { fail(error); }

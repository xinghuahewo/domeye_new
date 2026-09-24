import { Worker } from 'node:worker_threads';
import { createPathMatcher } from './paths.mjs';

export class ToolFailure extends Error {
  constructor(kind, message) { super(message); this.kind = kind; }
}

// 执行与模型可见的工具说明共用默认额度，避免说明与实际边界漂移。
export const EXECUTION_LIMITS = Object.freeze({ timeoutMs: 15_000, maxRequests: 16 });
const RESULT_RESPONSE_HEADERS = [
  'x-domeye-result-state', 'x-domeye-result-version', 'x-domeye-result-start',
  'x-domeye-result-end-exclusive', 'x-domeye-result-coverage',
];

// 代码仅在独立线程中的 QuickJS-WASM 执行；请求能力留在宿主。
export function runCode({ code, spec, request, signal, timeoutMs = EXECUTION_LIMITS.timeoutMs, memoryBytes = 64 * 1024 * 1024, maxRequests = EXECUTION_LIMITS.maxRequests, maxRequestBytes = 64 * 1024, maxResultBytes = 4 * 1024 * 1024 }) {
  if (typeof code !== 'string' || !code.trim() || code.length > 100_000) return Promise.reject(new ToolFailure('input', 'code 须为非空且不超过 100000 字符的 JavaScript。'));
  if ((spec === undefined) === (request === undefined)) return Promise.reject(new ToolFailure('input', '执行时须明确选择 spec 或 request 上下文。'));
  if (signal?.aborted) return Promise.reject(new ToolFailure('code', '本次执行已停止。'));
  return new Promise((resolve, reject) => {
    const controller = new AbortController();
    const worker = new Worker(new URL('./sandbox-worker.mjs', import.meta.url), {
      workerData: { code, spec, hasRequest: request !== undefined, timeoutMs, memoryBytes, maxRequests, maxRequestBytes, maxResultBytes },
      env: {}, execArgv: [], resourceLimits: { maxOldGenerationSizeMb: 96, stackSizeMb: 4 }
    });
    let done = false, requestCount = 0;
    const finish = (error, value) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      controller.abort();
      worker.terminate().then(() => error ? reject(error) : resolve(value));
    };
    const abort = () => finish(new ToolFailure('code', '本次执行已停止。'));
    const timer = setTimeout(() => finish(new ToolFailure('code', `执行超过 ${timeoutMs} 毫秒，已终止。`)), timeoutMs);
    signal?.addEventListener('abort', abort, { once: true });
    worker.on('error', error => finish(new ToolFailure('code', error.message)));
    worker.on('exit', code => { if (!done) finish(new ToolFailure('code', `执行线程意外结束（${code}）。`)); });
    worker.on('message', async message => {
      if (done) return;
      if (message.type === 'result') return finish(null, message.value);
      if (message.type === 'failure') return finish(new ToolFailure(message.error.kind, message.error.message));
      if (message.type !== 'request') return;
      try {
        if (!request || ++requestCount > maxRequests) return finish(new ToolFailure('input', '本次执行超过允许的请求次数。'));
        const value = await request(message.value, { signal: controller.signal });
        if (!done) worker.postMessage({ id: message.id, value });
      } catch (error) {
        if (!done) worker.postMessage({ id: message.id, error: { kind: error.kind ?? 'network', message: error.message } });
      }
    });
  });
}

// 宿主仅对显式登记的完整 GET 路径发起请求。代码不能指定目标主机或凭据。
export function createRequest({ baseUrl, paths, onResponse = () => {}, maxResponseBytes = 4 * 1024 * 1024 }) {
  const base = new URL(baseUrl);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.pathname !== '/' || base.search || base.hash) throw new ToolFailure('input', '宿主须配置不含凭据和路径的 API 地址。');
  const matchPath = createPathMatcher(paths);
  return async (input, { signal } = {}) => {
    if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).some(key => !['method', 'path', 'query'].includes(key))) throw new ToolFailure('input', 'request 只接受 method、path 和 query。');
    if (input.method !== 'GET' || !matchPath(input.path)) throw new ToolFailure('input', '只允许已登记的完整 GET 路径；路径参数须逐段编码。');
    const query = input.query ?? {};
    if (!query || typeof query !== 'object' || Array.isArray(query)) throw new ToolFailure('input', 'query 须为参数对象。');
    const url = new URL(input.path, base);
    for (const [key, value] of Object.entries(query)) {
      if (!['string', 'number', 'boolean'].includes(typeof value) || (typeof value === 'number' && !Number.isFinite(value))) throw new ToolFailure('input', '查询参数须为字符串、有限数值或布尔值。');
      url.searchParams.append(key, String(value));
    }
    let response;
    try { response = await fetch(url, { method: 'GET', redirect: 'manual', signal }); }
    catch (error) { throw new ToolFailure('network', signal?.aborted ? '请求已停止。' : `业务请求未取得 HTTP 响应：${error.message}`); }
    let length = 0;
    const chunks = [];
    try {
      if (response.body) for await (const chunk of response.body) {
        length += chunk.length;
        if (length > maxResponseBytes) throw new ToolFailure('network', `HTTP ${response.status} 正文超过读取上限，未返回截断结果。`);
        chunks.push(chunk);
      }
    } catch (error) { throw error instanceof ToolFailure ? error : new ToolFailure('network', `HTTP ${response.status} 正文未读完：${error.message}`); }
    const text = Buffer.concat(chunks).toString('utf8');
    let body = text || null;
    if (text) { try { body = JSON.parse(text); } catch { /* 非 JSON 正文按原文返回。 */ } }
    // 只保留服务端的交付元数据；Cookie 等无关响应头不进入沙箱或会话记录。
    const headers = {};
    for (const name of RESULT_RESPONSE_HEADERS) {
      const value = response.headers.get(name);
      if (value !== null) headers[name] = value;
    }
    const result = { status: response.status, body, ...(Object.keys(headers).length ? { headers } : {}) };
    onResponse({ request: structuredClone(input), response: structuredClone(result) });
    return result;
  };
}

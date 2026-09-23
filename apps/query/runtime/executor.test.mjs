import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { runCode, createRequest } from './executor.mjs';

const simpleSpec = { paths: { '/api/v1/data-availability': { get: { description: '可用范围', responses: { 200: { type: 'object' } } } } } };
test('search 读取说明并保留结构，execute 返回原始 HTTP 正文', async () => {
  const result = await runCode({ code: 'async () => Object.entries(spec.paths)', spec: simpleSpec });
  assert.deepEqual(result, Object.entries(simpleSpec.paths));
  const response = { status: 503, body: { state: 'unavailable', message: '读取失败，不能解释为零' } };
  assert.deepEqual(await runCode({ code: 'async () => await domeye.request({method:"GET",path:"/api/v1/data-availability"})', request: async () => response }), response);
});

test('未注入宿主能力，动态导入不能读取文件', async () => {
  const result = await runCode({ code: 'async () => [typeof process,typeof require,typeof fetch,typeof __hostRequest,typeof domeye]', spec: simpleSpec });
  assert.deepEqual(result, Array(5).fill('undefined'));
  await assert.rejects(runCode({ code: 'async () => await import("node:fs")', spec: simpleSpec }), error => error.kind === 'code');
  await assert.rejects(runCode({ code: 'async () => globalThis.constructor.constructor("return process")()', spec: simpleSpec }), error => error.kind === 'code');
});

test('每次运行重新隔离上下文', async () => {
  assert.equal(await runCode({ code: 'async () => {globalThis.previous=42;return previous}', spec: simpleSpec }), 42);
  assert.equal(await runCode({ code: 'async () => typeof previous', spec: simpleSpec }), 'undefined');
});

test('循环和永不结束的 Promise 均被墙钟终止', async () => {
  for (const code of ['async () => {while(true){}}', 'async () => await new Promise(()=>{})']) {
    const started = Date.now();
    await assert.rejects(runCode({ code, spec: simpleSpec, timeoutMs: 400 }), error => error.kind === 'code');
    assert.ok(Date.now() - started < 3000);
  }
});

test('超过内存预算返回失败', async () => {
  await assert.rejects(runCode({ code: 'async () => {let a=[];while(true)a.push(new Array(4096).fill(42))}', spec: simpleSpec, memoryBytes: 4 * 1024 * 1024, timeoutMs: 3000 }), error => error.kind === 'code' && /memory|alloc/i.test(error.message));
});

test('代码错误与不可序列化结果明确报告', async () => {
  await assert.rejects(runCode({ code: 'async () => {', spec: simpleSpec }), error => error.kind === 'code');
  for (const expression of ['undefined', '[undefined]', 'NaN', 'Infinity', '({count:NaN})', '1n', 'new Array(2)', '(()=>{const a={};a.a=a;return a})()']) {
    await assert.rejects(runCode({ code: `async () => ${expression}`, spec: simpleSpec }), error => error.kind === 'serialization');
  }
});

test('省略对象中未定义的可选字段，保留明确的 null、零和 false', async () => {
  const result = await runCode({
    code: 'async () => {const op=spec.paths["/api/v1/data-availability"].get;return {summary:op.summary,description:op.description,nested:{missing:undefined,unknown:null,count:0,supported:false},rows:[{optional:undefined,count:0}]}}',
    spec:simpleSpec,
  });
  assert.deepEqual(result,{description:'可用范围',nested:{unknown:null,count:0,supported:false},rows:[{count:0}]});
  assert.equal(Object.hasOwn(result,'summary'),false);
});

test('宿主 GET 白名单、HTTP 错误正文与禁止跟随重定向', async () => {
  let hits = 0;
  const server = createServer((req, res) => {
    hits++;
    if (req.url === '/redirect') { res.writeHead(302, { Location: '/forbidden' }); res.end('跳转正文'); }
    else { res.writeHead(409, { 'content-type': 'application/json' }); res.end(JSON.stringify({ state: 'unavailable', message: '版本变化' })); }
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const request = createRequest({ baseUrl: `http://127.0.0.1:${server.address().port}`, paths: ['/query', '/redirect'] });
  try {
    const response = await runCode({ code: 'async () => await domeye.request({method:"GET",path:"/query"})', request });
    assert.deepEqual(response, { status: 409, body: { state: 'unavailable', message: '版本变化' } });
    for (const input of [{ method:'POST',path:'/query' }, { method:'GET',path:'http://example.com/query' }, { method:'GET',path:'/forbidden' }, { method:'GET',path:'/query',headers:{} }]) {
      await assert.rejects(runCode({ code: `async () => await domeye.request(${JSON.stringify(input)})`, request }), error => error.kind === 'input');
    }
    assert.equal(hits, 1);
    assert.deepEqual(await request({ method: 'GET', path: '/redirect' }), { status: 302, body: '跳转正文' });
    assert.equal(hits, 2);
  } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});

test('停止取消在途读取，且不再启动后续请求', async () => {
  let hits = 0, notifyStarted;
  const started = new Promise(resolve => { notifyStarted = resolve; });
  const server = createServer((req, res) => { hits++; notifyStarted(); }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const controller = new AbortController();
  const request = createRequest({ baseUrl: `http://127.0.0.1:${server.address().port}`, paths: ['/slow'] });
  try {
    const running = runCode({ code: 'async () => { await domeye.request({method:"GET",path:"/slow"});return await domeye.request({method:"GET",path:"/slow"})}', request, signal: controller.signal });
    await started;
    controller.abort();
    await assert.rejects(running, error => error.kind === 'code' && /停止/.test(error.message));
    assert.equal(hits, 1);
  } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});

test('未得到 HTTP 响应与请求次数超限明确失败', async () => {
  const request = createRequest({ baseUrl: 'http://127.0.0.1:1', paths: ['/query'] });
  await assert.rejects(runCode({ code: 'async () => await domeye.request({method:"GET",path:"/query"})', request }), error => error.kind === 'network');
  let calls = 0;
  await assert.rejects(runCode({ code: 'async () => {for(let i=0;i<3;i++)await domeye.request({method:"GET",path:"/query"});return 0}', request: async () => { calls++; return { status: 200, body: null }; }, maxRequests: 2 }), error => error.kind === 'input');
  assert.equal(calls, 2);
});

test('大参数和大量并行调用在发送给宿主前受限，大结果不送出线程', async () => {
  let calls = 0;
  const request = async () => { calls++; return { status: 200, body: null }; };
  await assert.rejects(runCode({ code: 'async () => await domeye.request({method:"GET",path:"/query",query:{large:"x".repeat(1000)}})', request, maxRequestBytes: 200 }), error => error.kind === 'input');
  assert.equal(calls, 0);
  await assert.rejects(runCode({ code: 'async () => await Promise.all(Array.from({length:100},()=>domeye.request({method:"GET",path:"/query"})))', request, maxRequests: 2 }), error => error.kind === 'input');
  assert.ok(calls <= 2);
  await assert.rejects(runCode({ code: 'async () => "x".repeat(1000)', spec: simpleSpec, maxResultBytes: 200 }), error => error.kind === 'serialization');
});

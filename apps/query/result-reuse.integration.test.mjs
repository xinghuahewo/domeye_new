import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';

test('截断的完整返回可在本题继续计算，保留末行和空值且不再次请求 HTTP', async () => {
  let hits = 0;
  const body = {state: 'available', version: 'synthetic-v1', padding: '长'.repeat(25000),
    data: [{at: '2000-01-01T00:00:00Z', value: 5}, {at: '2000-01-01T00:05:00Z', value: null},
      {at: '2000-01-01T00:10:00Z', value: 0}, {at: '2000-01-01T00:15:00Z', value: 8}]};
  const server = createServer((_req, res) => {
    hits++; res.writeHead(200, {'content-type': 'application/json'}); res.end(JSON.stringify(body));
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href,
    {namedExports: {createDocs: async () => async () => ({results: []})}});
  let registered;
  const records = [];
  try {
    const {createTools} = await import('./tools.mjs?result-reuse');
    registered = await createTools({apiBaseUrl: `http://127.0.0.1:${server.address().port}`,
      onEvidence: (type, value) => records.push({type, value})});
    registered.beginTurn();
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const first = await execute.execute('wide', {code: `async () => domeye.request({
      method: 'GET', path: '/api/v1/events/statistics', query: {version: 'synthetic-v1'}})`});
    assert.match(first.content[0].text, /TRUNCATED/);
    assert.equal(first.content[0].text.includes('00:15:00Z'), false);
    const narrow = await execute.execute('calculate', {code: `async () => {
      const {body} = await domeye.readResult('wide');
      const rows = body.data;
      return {version: body.version, first: rows[0], last: rows.at(-1),
        delta: rows.at(-1).value - rows[0].value, unknown: rows.filter(row => row.value === null).length,
        zero: rows.filter(row => row.value === 0).length};
    }`});
    assert.deepEqual(narrow.details, {version: body.version, first: body.data[0], last: body.data.at(-1),
      delta: 3, unknown: 1, zero: 1});
    assert.equal(hits, 1, '仅使用已取得的结果，不以重复 HTTP 补齐模型上下文');
    const control = JSON.parse(narrow.content[1].text).requestControl;
    assert.deepEqual(control.responses, [], '复用不能冒充新的 HTTP');
    assert.deepEqual(control.reusedResults, [{toolCallId: 'wide', responseIds: [1]}]);
    assert.equal(JSON.parse(first.content[1].text).resultReference.toolCallId, 'wide');
    assert.match(first.content[0].text, /domeye\.readResult\("wide"\)/);
    assert.deepEqual(records.find(row => row.type === 'tool_result' && row.value.id === 'wide').value.value.body, body);
  } finally {
    await registered?.close(); docsMock.restore();
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  }
});

test('结果引用按问题和会话隔离，复用中的修改不污染原值，失败不保存引用', async () => {
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href,
    {namedExports: {createDocs: async () => async () => ({results: []})}});
  let first, second;
  try {
    const {createTools} = await import('./tools.mjs?result-lifecycle');
    first = await createTools({apiBaseUrl: 'http://127.0.0.1:1'});
    second = await createTools({apiBaseUrl: 'http://127.0.0.1:1'});
    const execute = first.tools.find(tool => tool.name === 'execute');
    const run = (id, code) => execute.execute(id, {code});
    first.beginTurn(); second.beginTurn();
    const result = await run('original', 'async () => ({value:0,unknown:null,nested:{value:4}})');
    result.details.nested.value = 99;
    await run('mutate', 'async () => {const v=await domeye.readResult("original");v.nested.value=33;return v;}');
    assert.deepEqual((await run('unchanged', 'async () => domeye.readResult("original")')).details,
      {value:0,unknown:null,nested:{value:4}});
    await assert.rejects(second.tools.find(tool => tool.name === 'execute').execute('foreign',
      {code:'async () => domeye.readResult("original")'}), /本题没有/);
    await assert.rejects(run('failed', 'async () => {throw new Error("计算失败")}'), /计算失败/);
    for (const ref of ['failed', 'toString', '__proto__', {}, 0]) {
      await assert.rejects(run('invalid', `async () => domeye.readResult(${JSON.stringify(ref)})`), /本题没有/);
    }
    first.beginTurn();
    await assert.rejects(run('expired', 'async () => domeye.readResult("original")'), /本题没有/);
    await first.close();
    await assert.rejects(run('closed', 'async () => 1'), /会话已关闭/);
  } finally {await first?.close(); await second?.close(); docsMock.restore();}
});

test('版本冲突和重发现后旧结果及派生结果仍失效，复用不清除整题重取要求', async () => {
  let status = 200, version = 'synthetic-v1', hits = 0;
  const server = createServer((_req, res) => {
    hits++; res.writeHead(status, {'content-type':'application/json'});
    res.end(JSON.stringify({state: status === 200 ? 'available' : 'version_conflict', version, count: 5}));
  }).listen(0, '127.0.0.1'); await once(server, 'listening');
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href,
    {namedExports: {createDocs: async () => async () => ({results: []})}});
  let registered;
  try {
    const {createTools} = await import('./tools.mjs?result-versions');
    registered = await createTools({apiBaseUrl:`http://127.0.0.1:${server.address().port}`});
    registered.beginTurn();
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const run = (id, code) => execute.execute(id, {code});
    const read = (id, ref) => run(id, `async () => domeye.readResult(${JSON.stringify(ref)})`);
    const request = () => `async () => domeye.request({method:'GET',path:'/api/v1/events/statistics',query:{version:${JSON.stringify(version)}}})`;
    await run('original', request());
    await run('derived', 'async () => ({twice:(await domeye.readResult("original")).body.count*2})');
    status = 409;
    await run('conflict', request());
    for (const ref of ['original','derived']) await assert.rejects(read('blocked', ref), /原结果的请求回执已失效/);
    assert.equal(hits,2);
    status = 200; version = 'synthetic-v2';
    const discovery = await run('rediscover', 'async () => domeye.request({method:"GET",path:"/api/v1/data-availability"})');
    const pending = JSON.parse(discovery.content[1].text).requestControl.pendingReplay;
    assert.equal(pending.length,1);
    await assert.rejects(read('still-invalid', 'derived'), error => {
      const control = JSON.parse(error.message).requestControl;
      assert.deepEqual(control.pendingReplay, pending); assert.equal(control.restartRequired,true);
      assert.deepEqual(control.responses, []); assert.deepEqual(control.reusedResults, []); return true;
    });
    await run('fresh', request());
    const fresh = await read('fresh-reuse', 'fresh');
    assert.equal(fresh.details.body.version, version);
    assert.equal(JSON.parse(fresh.content[1].text).requestControl.restartRequired, false);
    await assert.rejects(read('old-after-refresh', 'original'), /原结果的请求回执已失效/);
    assert.equal(hits,4);
  } finally {
    await registered?.close(); docsMock.restore();
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  }
});

test('读取失败不能通过旧结果复用绕过，原样重试成功后才恢复复用', async () => {
  let status = 200, hits = 0;
  const server = createServer((_req, res) => {
    hits++; res.writeHead(status, {'content-type':'application/json'});
    res.end(JSON.stringify({state:status === 200 ? 'available' : 'unavailable',version:'synthetic-v1',count:status === 200 ? 5 : null}));
  }).listen(0, '127.0.0.1'); await once(server, 'listening');
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href,
    {namedExports: {createDocs: async () => async () => ({results: []})}});
  let registered;
  try {
    const {createTools} = await import('./tools.mjs?result-read-failure');
    registered = await createTools({apiBaseUrl:`http://127.0.0.1:${server.address().port}`});
    registered.beginTurn();
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const run = (id, code) => execute.execute(id, {code});
    const code = 'async () => domeye.request({method:"GET",path:"/api/v1/events/statistics",query:{version:"synthetic-v1"}})';
    await run('first', code); status = 503;
    assert.equal((await run('failure',code)).details.status,503);
    await assert.rejects(run('old', 'async () => domeye.readResult("first")'), /仍有读取故障/);
    assert.equal(hits,2); status = 200;
    await run('retry', code);
    assert.equal((await run('reuse', 'async () => domeye.readResult("retry")')).details.body.count,5);
    assert.equal(hits,3);
  } finally {
    await registered?.close(); docsMock.restore();
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  }
});

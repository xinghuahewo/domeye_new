import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';

test('实际 execute 保留同次交付响应头、原文和失败，响应头不冒充正文版本', async () => {
  const deliveryHeaders = {
    'x-domeye-result-state': 'available',
    'x-domeye-result-version': 'synthetic-delivery-v1',
    'x-domeye-result-start': '2000-01-01T00:00:00Z',
    'x-domeye-result-end-exclusive': '2000-01-01T00:05:00Z',
    'x-domeye-result-coverage': 'partial_window',
  };
  const rows = [{ t: '2000-01-01 08:00:00', announce: 0, withdraw: null }];
  let unavailable = false;
  const evidence = [];
  const server = createServer((req, res) => {
    res.writeHead(unavailable ? 503 : 200, {
      ...deliveryHeaders,
      'x-domeye-result-state': unavailable ? 'unavailable' : 'available',
      'content-type': 'application/json',
      'set-cookie': 'synthetic-cookie=not-for-tools',
      'x-domeye-result-debug': 'synthetic-internal-detail',
    });
    res.end(JSON.stringify(unavailable ? { state: 'unavailable', message: '合成读取失败' } : rows));
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href, {
    namedExports: { createDocs: async () => async () => ({ matches: [] }) },
  });
  let registered;
  try {
    const { createTools } = await import('./tools.mjs?response-headers');
    registered = await createTools({ apiBaseUrl: `http://127.0.0.1:${server.address().port}`,
      onEvidence: (type, value) => evidence.push({ type, value }) });
    registered.beginTurn();
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const requestCode = 'await domeye.request({method:"GET",path:"/api/v1/features/top"})';
    const raw = await execute.execute('headers-raw', { code: `async () => ${requestCode}` });
    assert.deepEqual(raw.details, { status: 200, body: rows, headers: deliveryHeaders });
    assert.deepEqual(JSON.parse(raw.content[0].text), raw.details);
    assert.deepEqual(evidence.find(item => item.type === 'http').value.response, raw.details);

    const projected = await execute.execute('headers-projected', {
      code: `async () => {const response = ${requestCode}; return {samples: response.body.length};}`,
    });
    assert.deepEqual(projected.details, { samples: 1 });
    const receipt = JSON.parse(projected.content[1].text).requestControl.responses[0];
    assert.deepEqual(receipt.headers, deliveryHeaders);
    assert.deepEqual(receipt.scope, {});
    assert.equal(receipt.source.responseVersion, null);
    assert.equal(receipt.source.versionAssurance, 'unverified');
    assert.equal(receipt.source.family, 'compatibility');

    unavailable = true;
    const failed = await execute.execute('headers-failure', { code: `async () => ${requestCode}` });
    assert.equal(failed.details.status, 503);
    assert.deepEqual(failed.details.body, { state: 'unavailable', message: '合成读取失败' });
    assert.deepEqual(failed.details.headers, { ...deliveryHeaders, 'x-domeye-result-state': 'unavailable' });
    assert.deepEqual(JSON.parse(failed.content[1].text).requestControl.responses[0].headers, failed.details.headers);
    assert.equal(JSON.stringify([raw, projected, failed, evidence]).includes('synthetic-cookie'), false);
    assert.equal(JSON.stringify([raw, projected, failed, evidence]).includes('synthetic-internal-detail'), false);
  } finally {
    await registered?.close(); docsMock.restore(); server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});

test('实际 execute 链路阻止已绑定交付的无版本跨入口读取，并从目录恢复整题', async () => {
  let version = 'synthetic-v1';
  const binding = { source_run: 'synthetic-run', collector: 'synthetic-collector', schema_version: 'completed-file-delivery/v1' };
  const reads = [];
  const server = createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    reads.push({ path: url.pathname, version: url.searchParams.get('version') });
    const conflict = url.searchParams.has('version') && url.searchParams.get('version') !== version;
    const body = conflict ? { state: 'unavailable', message: '人工版本冲突，不是零活动' }
      : { state: 'available', version, ...(url.pathname === '/api/v1/core-overview' ? {
        metadata: { interpretation_version: 'completed-file-results/v1', result_delivery: { state: 'available', version, binding } },
      } : {}) };
    res.writeHead(conflict ? 409 : 200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href, { namedExports: { createDocs: async () => async () => ({ matches: [] }) } });
  let registered;
  try {
    const { createTools } = await import('./tools.mjs?shared-delivery-version');
    registered = await createTools({ apiBaseUrl: `http://127.0.0.1:${server.address().port}` });
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const call = (id, path, query = {}) => execute.execute(id, { code: `async () => await domeye.request(${JSON.stringify({ method: 'GET', path, query })})` });
    const control = result => JSON.parse(result.content[1].text).requestControl;
    const window = { scope: 'collector', start_time: '2000-01-01 00:00:00', end_time: '2000-01-01 01:00:00' };
    registered.beginTurn();
    await call('core-first', '/api/v1/core-overview', { date: '2000-01-01' });
    version = 'synthetic-v2';
    await assert.rejects(call('unversioned-feature', '/api/v1/features/summary', window), error => {
      const result = JSON.parse(error.message);
      assert.equal(result.error.kind, 'policy');
      assert.equal(result.requestControl.events.at(-1).reason, 'version_required');
      assert.deepEqual(result.requestControl.responses, []);
      return true;
    });
    assert.equal(reads.length, 1);
    const conflict = await call('feature-old', '/api/v1/features/summary', { ...window, version: 'synthetic-v1' });
    assert.equal(conflict.details.status, 409);
    assert.equal(conflict.details.body.message, '人工版本冲突，不是零活动');
    const recovered = control(await call('discover', '/api/v1/data-availability'));
    assert.deepEqual(recovered.unresolvedFailures, []);
    assert.equal(recovered.restartRequired, true);
    assert.deepEqual(recovered.events.filter(event => event.type === 'version_changed').map(event => event.family).sort(), ['core', 'delivery']);
    const replayed = control(await call('core-replay', '/api/v1/core-overview', { date: '2000-01-01', version }));
    assert.equal(replayed.restartRequired, false);
    const feature = await call('feature-new', '/api/v1/features/summary', { ...window, version });
    assert.equal(feature.details.body.version, version);
    assert.equal(control(feature).responses[0].source.versionAssurance, 'matched');
    assert.deepEqual(reads.map(row => row.version), [null, 'synthetic-v1', null, 'synthetic-v2', 'synthetic-v2']);
  } finally {
    await registered?.close(); docsMock.restore(); server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});

test('三日规范的已交付 Core 通过健康发现恢复，工具保留 409 原文与整题重取回执', async () => {
  let version = 'synthetic-v1';
  const binding = { source_run: 'synthetic-three-day', collector: 'rrc25', schema_version: 'completed-file-delivery/v1' };
  const delivery = () => ({ state: 'available', version, binding });
  const reads = [];
  const server = createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1'); reads.push(url.pathname);
    const health = url.pathname === '/api/v1/healthz';
    const conflict = !health && url.searchParams.has('version') && url.searchParams.get('version') !== version;
    res.writeHead(conflict ? 409 : 200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(conflict ? { state: 'unavailable', message: '人工版本冲突' }
      : health ? { status: 'ok', result_delivery: delivery() }
      : { state: 'available', version, metadata: { interpretation_version: 'completed-file-results/v1', result_delivery: delivery() }, events: { total: 7 } }));
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href, { namedExports: { createDocs: async () => async () => ({ matches: [] }) } });
  let registered;
  try {
    const { createTools } = await import('./tools.mjs?three-day-core-recovery');
    registered = await createTools({ apiBaseUrl: `http://127.0.0.1:${server.address().port}`, specFile: 'openapi-three-day.json' });
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const call = (id, path, query = {}) => execute.execute(id, { code: `async () => await domeye.request(${JSON.stringify({ method: 'GET', path, query })})` });
    const control = result => JSON.parse(result.content[1].text).requestControl;
    registered.beginTurn();
    await call('first', '/api/v1/core-overview', { date: '2026-02-27' });
    version = 'synthetic-v2';
    const conflict = await call('conflict', '/api/v1/core-overview', { date: '2026-02-28', version: 'synthetic-v1' });
    assert.equal(conflict.details.status, 409);
    assert.equal(control(conflict).responses[0].status, 409);
    const discovery = await call('rediscover', '/api/v1/healthz');
    assert.equal(discovery.details.body.version, undefined);
    assert.equal(control(discovery).responses[0].source.responseVersion, null);
    assert.equal(control(discovery).restartRequired, true);
    assert.deepEqual(control(discovery).unresolvedFailures, []);
    assert.ok(control(discovery).events.some(event => event.family === 'core' && event.type === 'version_changed' && event.version === version));
    await assert.rejects(call('old', '/api/v1/core-overview', { date: '2026-02-27', version: 'synthetic-v1' }), /显式/);
    const replay = await call('replay', '/api/v1/core-overview', { date: '2026-02-27', version });
    assert.equal(control(replay).restartRequired, false);
    assert.equal(control(replay).responses[0].source.versionAssurance, 'matched');
    assert.equal(reads.length, 4, '被拦截的旧版请求不发送 HTTP');
  } finally {
    await registered?.close(); docsMock.restore(); server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});

test('版本发现失败即时说明该族本轮恢复结束，原文、其他族和下一轮恢复保持', async () => {
  const conflict={state:'unavailable',message:'人工版本变化，请重新发现数据后再查询'};
  const reads=[];let recover=false;
  const server=createServer((req,res)=>{
    const path=new URL(req.url,'http://127.0.0.1').pathname;reads.push(path);
    const initial=reads.length===1;
    const ok=initial || recover || path==='/api/v1/healthz';
    res.writeHead(ok?200:409,{'content-type':'application/json'});
    res.end(JSON.stringify(ok?{state:'available',version:recover?'synthetic-v2':'synthetic-v1'}:conflict));
  }).listen(0,'127.0.0.1');await once(server,'listening');
  const docsMock=mock.module(new URL('./docs.mjs',import.meta.url).href,{namedExports:{createDocs:async()=>async()=>({matches:[]})}});
  let registered;
  try {
    const {createTools}=await import('./tools.mjs?recovery-stopped-integration');
    registered=await createTools({apiBaseUrl:`http://127.0.0.1:${server.address().port}`});
    const execute=registered.tools.find(t=>t.name==='execute');
    const call=(id,path,query={})=>execute.execute(id,{code:`async () => await domeye.request(${JSON.stringify({method:'GET',path,query})})`});
    const control=r=>JSON.parse(r.content[1].text).requestControl;
    registered.beginTurn();
    await call('initial','/api/v1/data-availability');
    const first=await call('conflict','/api/v1/events/statistics',{version:'synthetic-v1'});
    const last=await call('refresh','/api/v1/data-availability');
    assert.deepEqual(JSON.parse(first.content[0].text),{status:409,body:conflict});
    assert.deepEqual(JSON.parse(last.content[0].text),{status:409,body:conflict});
    assert.deepEqual(control(last).recoveryStopped?.map(x=>x.family),['delivery']);
    assert.deepEqual(control(first).recoveryStopped,[],'第一次冲突仍可发现，不提前宣称结束');
    const health=await call('independent-health','/api/v1/healthz');
    assert.equal(health.details.status,200,'其他独立结果族仍能查询');
    assert.deepEqual(control(health).recoveryStopped.map(x=>x.family),['delivery']);
    await assert.rejects(call('repeat-refresh','/api/v1/data-availability'),/已尝试/);
    assert.equal(reads.length,4,'结束后的重复发现未发HTTP');
    registered.beginTurn();
    await assert.rejects(call('old-version','/api/v1/events/statistics',{version:'synthetic-v1'}),/版本已冲突/);
    recover=true;
    const fresh=await call('next-turn-refresh','/api/v1/data-availability');
    assert.equal(fresh.details.body.version,'synthetic-v2');
    assert.deepEqual(control(fresh).recoveryStopped,[],'新用户轮次成功发现可恢复，不永久关闭该族');
    recover=false;
    const conflictAgain=await call('conflict-after-successful-discovery','/api/v1/events/statistics',{version:'synthetic-v2'});
    assert.equal(conflictAgain.details.status,409);
    const stopped=control(conflictAgain).recoveryStopped[0];
    assert.equal(stopped.reason,'recovery_exhausted','发现曾成功，不能将随后再冲突称为发现失败');
    assert.match(stopped.note,/发现机会已使用/);
  } finally {
    await registered?.close();docsMock.restore();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
  }
});

// 固定集成校验：模型和 docs 用人工替身，规范、请求策略、执行器与本机 HTTP 真实连接。
// 不访问 QMD、模型、业务库、共享服务或凭据。
test('工具注册实际接通策略：HTTP 原文与 requestControl 分离，每轮重试重置但版本保留', async () => {
  const reads = [], evidence = [];
  const raw = { status: 503, body: { state: 'unavailable', message: '人工读取失败，不是零', error: '保留的业务字段', count: null } };
  const server = createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1'); reads.push(url.pathname);
    const discovery = url.pathname === '/api/v1/data-availability';
    res.writeHead(discovery ? 200 : 503, { 'content-type': 'application/json' });
    res.end(JSON.stringify(discovery ? { state: 'available', version: 'synthetic-integration-v1' } : raw.body));
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  let closed = false;
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href, { namedExports: { createDocs: async () => Object.assign(async () => ({ matches: [] }), { close: async () => { closed = true; } }) } });
  let registered;
  try {
    const { createTools } = await import('./tools.mjs?request-policy-integration');
    registered = await createTools({ apiBaseUrl: `http://127.0.0.1:${server.address().port}`, onEvidence: (type, value) => evidence.push({ type, value }) });
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const call = (id, path, query = {}) => execute.execute(id, { code: `async () => await domeye.request(${JSON.stringify({ method: 'GET', path, query })})` });
    registered.beginTurn();
    const discovery = await call('discover', '/api/v1/data-availability');
    assert.deepEqual(discovery.details, { status: 200, body: { state: 'available', version: 'synthetic-integration-v1' } });
    assert.equal(discovery.content.length, 2);
    const query = { version: 'synthetic-integration-v1', bucket: 'day' };
    for (const id of ['first', 'retry']) {
      const result = await call(id, '/api/v1/events/statistics', query);
      assert.deepEqual(result.details, raw);
      assert.deepEqual(JSON.parse(result.content[0].text), raw);
      assert.equal(Object.hasOwn(result.details, 'requestControl'), false);
      const control = JSON.parse(result.content[1].text).requestControl;
      assert.deepEqual(control.unresolvedFailures, ['delivery']);
      assert.equal(control.events.length, 1);
      assert.equal(control.events[0].status, 503);
      assert.equal(control.responses[0].status,503);
      assert.equal(control.responses[0].scope.state,'unavailable');
      assert.equal(Object.hasOwn(control.responses[0].scope,'coverage'),false);
      assert.deepEqual(control.invalidatedEvidence,[]);
    }
    await assert.rejects(call('blocked', '/api/v1/events/statistics', query), error => JSON.parse(error.message).error.kind === 'policy');
    await assert.rejects(call('core-fallback', '/api/v1/core-overview'), error => JSON.parse(error.message).error.kind === 'policy');
    assert.deepEqual(reads, ['/api/v1/data-availability', '/api/v1/events/statistics', '/api/v1/events/statistics']);
    registered.beginTurn();
    await assert.rejects(call('missing-version', '/api/v1/events/statistics'), error => JSON.parse(error.message).error.kind === 'policy');
    const nextTurn = await call('new-turn', '/api/v1/events/statistics', query);
    assert.deepEqual(nextTurn.details, raw);
    const nextControl = JSON.parse(nextTurn.content[1].text).requestControl;
    assert.equal(nextControl.events[0].turn, 2);
    assert.equal(reads.length, 4);
    assert.equal(evidence.filter(item => item.type === 'http').length, 4);
    assert.equal(evidence.find(item => item.type === 'tool_error' && item.value.id === 'blocked').value.error.kind, 'policy');
    assert.deepEqual(evidence.find(item => item.type === 'tool_result' && item.value.id === 'first').value.value, raw);
    await registered.close(); registered = null;
    assert.equal(closed, true);
  } finally {
    await registered?.close(); docsMock.restore();
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  }
});

test('并行工具的范围回执只绑定各自 HTTP；合并与纯计算不伪造字段血缘', async () => {
  const rawBodies = {
    '/api/v1/features/summary': { state:'available', version:'synthetic-v1', query:{subject:'example-a'}, coverage:{state:'partial',seconds:7200}, count:0 },
    '/api/v1/features/ases/overview': { start_time:'synthetic-start', end_time:'synthetic-end', timezone:'Asia/Shanghai', count:0 }
  };
  const evidence=[];
  const server=createServer((req,res)=>{
    const body=rawBodies[new URL(req.url,'http://127.0.0.1').pathname];
    res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify(body));
  }).listen(0,'127.0.0.1');await once(server,'listening');
  const docsMock=mock.module(new URL('./docs.mjs',import.meta.url).href,{namedExports:{createDocs:async()=>async()=>({matches:[]})}});
  let registered;
  try {
    const {createTools}=await import('./tools.mjs?source-receipt-integration');
    registered=await createTools({apiBaseUrl:`http://127.0.0.1:${server.address().port}`,onEvidence:(type,value)=>evidence.push({type,value})});
    registered.beginTurn();
    const execute=registered.tools.find(t=>t.name==='execute');
    const invoke=(id,path)=>execute.execute(id,{code:`async () => { const r=await domeye.request(${JSON.stringify({method:'GET',path})}); return {status:r.status,count:r.body.count}; }`});
    const results=await Promise.all([invoke('delivery-call','/api/v1/features/summary'),invoke('compatibility-call','/api/v1/features/ases/overview')]);
    for(const [i,result] of results.entries()) {
      assert.deepEqual(result.details,{status:200,count:0});
      assert.deepEqual(JSON.parse(result.content[0].text),result.details);
      const control=JSON.parse(result.content[1].text).requestControl;
      assert.equal(control.events.length,1,'并行调用的事件不能混进同一工具结果');
      assert.equal(control.responses.length,1);
      const receipt=control.responses[0];
      assert.equal(receipt.toolCallId,i===0?'delivery-call':'compatibility-call');
      assert.equal(receipt.request.path,i===0?'/api/v1/features/summary':'/api/v1/features/ases/overview');
      assert.equal(receipt.source.family,i===0?'delivery':'compatibility');
      assert.equal(receipt.source.versionAssurance,i===0?'confirmed':'unverified');
      assert.equal(Object.hasOwn(receipt.scope,'coverage'),i===0,'不能给相同零值的兼容结果借用交付覆盖');
      assert.equal(Object.hasOwn(receipt.scope,'count'),false,'回执不把未返回的业务数值增添为计算结果');
    }
    assert.equal(evidence.filter(e=>e.type==='http').length,2);
    const pure=await execute.execute('pure',{code:'async () => ({total:3+4})'});
    assert.deepEqual(pure.details,{total:7});
    const pureControl=JSON.parse(pure.content[1].text).requestControl;
    assert.deepEqual(pureControl.responses,[]);assert.deepEqual(pureControl.events,[]);
    const merged=await execute.execute('merged',{code:`async () => {
      const a=await domeye.request({method:'GET',path:'/api/v1/features/summary',query:{version:'synthetic-v1'}});
      const b=await domeye.request({method:'GET',path:'/api/v1/features/ases/overview'});
      return {a:a.body.count,b:b.body.count};
    }`});
    assert.deepEqual(merged.details,{a:0,b:0});
    const mergedControl=JSON.parse(merged.content[1].text).requestControl;
    assert.deepEqual(mergedControl.responses.map(r=>r.source.family),['delivery','compatibility']);
    assert.equal(new Set(mergedControl.responses.map(r=>r.id)).size,2);
    await assert.rejects(execute.execute('failed-after-read',{code:`async () => {
      await domeye.request({method:'GET',path:'/api/v1/features/ases/overview'});
      throw new Error('人工代码失败');
    }`}),error=>{
      const failed=JSON.parse(error.message);
      assert.equal(failed.error.kind,'code');
      assert.match(failed.error.message,/人工代码失败/);
      assert.equal(failed.requestControl.responses.length,1);
      assert.equal(failed.requestControl.responses[0].toolCallId,'failed-after-read');
      assert.equal(failed.requestControl.responses[0].status,200);
      return true;
    });
  } finally {
    await registered?.close();docsMock.restore();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
  }
});

test('agent 每次 ask 仅重置一次；重置错误记 failed、不调用模型，后续提问可继续', async () => {
  let beginCount = 0, promptCount = 0, emitEvidence;
  class FakeAgent {
    constructor(options) { this.options=options; this.state = { messages: [] }; }
    subscribe() {}
    async prompt(question) {
      promptCount++; const message={ role: 'assistant', stopReason: 'stop', content: [{ type: 'text', text: `${question}：人工完整响应` }] };
      this.state.messages.push(message);await this.options.finishTurn?.({message});
    }
    abort() {}
    async waitForIdle() {}
  }
  const mocks = [
    mock.module('@earendil-works/pi-agent-core', { namedExports: { Agent: FakeAgent } }),
    mock.module('@earendil-works/pi-ai', { namedExports: { createModels: () => ({ setProvider() {}, getModel() { return { id: 'synthetic-model' }; } }) } }),
    mock.module('@earendil-works/pi-ai/providers/deepseek', { namedExports: { deepseekProvider: () => ({}) } }),
    mock.module(new URL('./tools.mjs', import.meta.url).href, { namedExports: { createTools: async ({ onEvidence }) => {
      emitEvidence = onEvidence;
      return { tools: [], close: async () => {}, beginTurn() {
        beginCount++; emitEvidence('request_policy', { type: 'synthetic_begin_turn', turn: beginCount });
        if (beginCount === 2) throw new Error('人工：上轮在途，不能重置');
      } };
    } } }),
  ];
  try {
    const { createDomeyeAgent } = await import('./agent.mjs?request-policy-integration');
    const agent = await createDomeyeAgent({ modelConfig: { model: 'synthetic-model', apiKey: 'synthetic-test-key' } });
    const first = await agent.ask('第一题');
    assert.equal(first.status, 'completed');
    assert.equal(first.events[0].value.turn, 1, '重置时 current 已创建，事件属于当前题');
    const rejected = await agent.ask('第二题');
    assert.equal(rejected.status, 'failed');
    assert.match(rejected.error, /不能重置/);
    assert.equal(agent.running, false);
    assert.equal(promptCount, 1);
    const third = await agent.ask('第三题');
    assert.equal(third.status, 'completed');
    assert.equal(third.events[0].value.turn, 3);
    assert.equal(beginCount, 3); assert.equal(promptCount, 2);
    assert.equal(agent.turns.length, 3);
    await agent.close();
  } finally { for (const entry of mocks.reverse()) entry.restore(); }
});

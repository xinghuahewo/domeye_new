import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile, symlink, readFile } from 'node:fs/promises';
import { runInNewContext } from 'node:vm';
import { randomUUID } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createChatServer } from './server.mjs';

// 固定假 Agent 只验证 HTTP/会话边界；不调用模型、QMD 或业务 API。
const secret = 'synthetic-only-model-key';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function fixture(t, makeAgent, options = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'domeye-chat-test-'));
  const historyDir = join(directory, 'history');
  const service = await createChatServer({ modelConfig: { apiKey: secret }, historyDir, createAgent: makeAgent, ...options });
  const origin = await service.listen(0);
  t.after(async () => { await service.close(); await rm(directory, { recursive: true, force: true }); });
  const post = (path, value, headers = {}) => fetch(origin + path, { method: 'POST', headers: { origin, 'content-type': 'application/json', ...headers }, body: JSON.stringify(value) });
  return { service, origin, post, directory, historyDir };
}
function fakeFactory({ gate, started, stopped } = {}) {
  const agents = [];
  const create = async ({ onEvent }) => {
    const item = { id: randomUUID(), turns: [], running: false, stopped: false, closed: false,
      stop() { this.stopped = true; stopped?.resolve(); },
      async close() { this.closed = true; this.stop(); },
      async ask(question) {
        this.running = true; this.stopped = false;
        onEvent({ type: 'message_update', assistantMessageEvent: { type: 'thinking_delta', delta: '不应发送的思考' } });
        onEvent({ type: 'tool_execution_start', toolName: 'search', args: { code: '不应发送的工具参数' } });
        onEvent({ type: 'message_update', assistantMessageEvent: { type: 'text_delta', delta: '不应发送的草稿：' + secret.slice(0, 10) } });
        onEvent({ type: 'message_update', assistantMessageEvent: { type: 'text_delta', delta: secret.slice(10) } });
        // 模拟正文完成、保存仍待结束；外层不能仅检查 Agent.running 就解锁。
        this.running = false; started?.resolve(); await gate?.promise;
        const turn = { question, answer: this.stopped ? '' : '固定测试回答：' + secret, status: this.stopped ? 'cancelled' : 'completed',
          events: [{ thinking: '不应发送的思考', configuration: secret }], error: '/宿主/敏感路径' };
        if (!this.stopped) onEvent({type:'answer_final',text:turn.answer});
        this.turns.push(turn); return turn;
      },
    }; agents.push(item); return item;
  };
  return { create, agents };
}

test('显式来源支持服务器入口；伪造 Host、Origin 和转发头均不能放行', async t => {
  const factory = fakeFactory();
  const publicOrigin = 'https://query.example.test';
  const { service } = await fixture(t, factory.create, { publicOrigin });
  const request = (headers) => new Promise((done, reject) => {
    const req = httpRequest({ hostname:'127.0.0.1', port:service.server.address().port,
      path:'/api/session', method:'POST', headers:{'content-type':'application/json', ...headers} }, res => {
      res.resume(); res.on('end', () => done(res.statusCode));
    });
    req.on('error', reject); req.end('{}');
  });
  assert.equal(await request({host:'query.example.test',origin:publicOrigin}), 200);
  assert.equal(await request({host:'query.example.test',origin:'https://evil.example'}), 403);
  assert.equal(await request({host:'evil.example',origin:publicOrigin}), 403);
  assert.equal(await request({host:'query.example.test','x-forwarded-host':'query.example.test','x-forwarded-proto':'https'}), 403);
});

test('同源与输入边界；文字流不包含思考、工具参数或配置', async t => {
  const factory = fakeFactory(); const { origin, post } = await fixture(t, factory.create);
  assert.equal((await fetch(origin + '/api/session', { headers: { origin: 'https://example.org' } })).status, 403);
  assert.equal((await post('/api/session', {}, { origin: 'https://example.org' })).status, 403);
  const session = await (await post('/api/session', {})).json();
  assert.equal((await post('/api/chat', { sessionId: session.id, question: 'x'.repeat(4001) })).status, 400);
  assert.equal((await post('/api/chat', { sessionId: session.id, question: '测试', path: '/其他入口' })).status, 400);
  const response = await post('/api/chat', { sessionId: session.id, question: '测试问题' });
  const raw = await response.text(), events = raw.trim().split('\n').map(line => JSON.parse(line));
  assert.equal(events.at(-1).type, 'done'); assert.equal(events.at(-1).turn.status, 'completed');
  assert.ok(events.some(item => item.type === 'tool' && item.label === '查找接口'));
  assert.ok(!raw.includes(secret)); assert.ok(!raw.includes('不应发送')); assert.ok(!raw.includes('敏感路径'));
  const view = await (await fetch(origin + '/api/session')).text(); assert.ok(!view.includes(secret)); assert.ok(!view.includes('configuration')); assert.ok(!view.includes('events'));
  const page = await fetch(origin); assert.equal(page.status, 200); assert.match(page.headers.get('content-security-policy'), /frame-ancestors 'none'/);
});

test('停止不提前解除并发锁：等待回答和保存结束后才能新建会话', async t => {
  const gate = deferred(), started = deferred(), stopped = deferred();
  t.after(() => gate.resolve());
  const factory = fakeFactory({ gate, started, stopped }); const { origin, post } = await fixture(t, factory.create);
  const session = await (await post('/api/session', {})).json();
  const response = await post('/api/chat', { sessionId: session.id, question: '等待保存' }); await started.promise;
  assert.equal(factory.agents[0].running, false);
  assert.equal((await post('/api/chat', { sessionId: session.id, question: '禁止重入' })).status, 409);
  assert.equal((await post('/api/session', {})).status, 409);
  assert.equal((await post('/api/stop', { sessionId: session.id })).status, 200); await stopped.promise;
  assert.equal((await fetch(origin + '/api/session').then(r => r.json())).busy, true);
  assert.equal((await post('/api/session', {})).status, 409);
  gate.resolve();
  const final = (await response.text()).trim().split('\n').map(JSON.parse).at(-1);
  assert.equal(final.turn.status, 'cancelled');
  assert.equal((await post('/api/session', {})).status, 200); assert.equal(factory.agents[0].closed, true);
});

test('浏览器断连会停止在途回答，并等待其退出', async t => {
  const gate = deferred(), started = deferred(), stopped = deferred();
  t.after(() => gate.resolve());
  const factory = fakeFactory({ gate, started, stopped }); const { origin, post } = await fixture(t, factory.create);
  const session = await (await post('/api/session', {})).json();
  const socket = httpRequest(origin + '/api/chat', { method: 'POST', headers: { origin, 'content-type': 'application/json' } });
  const incoming = new Promise((done, reject) => { socket.on('response', done); socket.on('error', reject); });
  socket.end(JSON.stringify({ sessionId: session.id, question: '断连测试' }));
  const response = await incoming; await started.promise; response.destroy(); socket.destroy();
  await stopped.promise; assert.equal(factory.agents[0].stopped, true);
  assert.equal((await post('/api/session', {})).status, 409); gate.resolve();
});

test('历史只读：UUID 文件、拒绝符号链接与路径逃逸，只输出可读字段', async t => {
  const { origin, historyDir, directory } = await fixture(t, fakeFactory().create);
  const valid = randomUUID(), linked = randomUUID();
  const record = { id: valid, agent_instructions: secret, turns: [{ question: '已保存的问题', answer: '<script>原样文字</script>', status: 'completed', events: [{ thinking: secret }] }] };
  await writeFile(join(historyDir, valid + '.json'), JSON.stringify(record));
  const outside = join(directory, 'outside.json'); await writeFile(outside, JSON.stringify({ ...record, id: linked })); await symlink(outside, join(historyDir, linked + '.json'));
  await writeFile(join(historyDir, 'not-a-uuid.json'), JSON.stringify(record));
  const response = await fetch(origin + '/api/history/' + valid); const value = await response.json();
  assert.equal(response.status, 200); assert.equal(value.readOnly, true); assert.equal(value.turns[0].answer, '<script>原样文字</script>');
  assert.ok(!JSON.stringify(value).includes(secret)); assert.ok(!('events' in value.turns[0]));
  assert.equal((await fetch(origin + '/api/history/' + linked)).status, 404);
  assert.equal((await fetch(origin + '/api/history/..%2Foutside')).status, 404);
  const list = await (await fetch(origin + '/api/history')).json(); assert.deepEqual(list.records.map(item => item.id), [valid]);
});

test('会话创建与关闭交错：等待延迟初始化并关闭刚创建的 Agent', async t => {
  const entered = deferred(), gate = deferred(); let closed = 0;
  t.after(() => gate.resolve());
  const { service, post } = await fixture(t, async () => {
    entered.resolve(); await gate.promise;
    return { id: randomUUID(), turns: [], close: async () => { closed++; }, stop() {} };
  });
  const creating = post('/api/session', {}).then(response => response.status, () => 'connection-closed'); await entered.promise;
  let finished = false;
  const closing = service.close().then(() => { finished = true; });
  await new Promise(done => setImmediate(done)); assert.equal(finished, false);
  gate.resolve(); await closing; await creating;
  assert.equal(closed, 1); assert.equal(finished, true); assert.equal(service.server.listening, false);
});

test('页面切换锁与迟到同步：旧响应不能覆盖新会话', async () => {
  const nodes = new Map();
  const element = () => ({ value: '', textContent: '', scrollHeight: 0, scrollTop: 0, clientHeight: 0,
    classList: { add() {}, remove() {}, toggle() {} }, append() {}, replaceChildren() {}, addEventListener() {},
    querySelector: () => element(), querySelectorAll: () => [], focus() {} });
  let current = { id: 'old-session', busy: false, turns: [] }, held, posts = 0;
  const context = {
    document: { getElementById(id) { if (!nodes.has(id)) nodes.set(id, element()); return nodes.get(id); }, createElement: element, createTextNode: element },
    setTimeout, clearTimeout, TextDecoder,
    fetch: async (path, options) => {
      let value;
      if (path === '/api/history') value = { records: [] };
      else if (path === '/api/datasets') value = { datasets: [{id:'completed-files',label:'本项目结果'}] };
      else if (options?.method === 'POST') { posts++; current = { id: 'new-session-' + posts, busy: false, turns: [] }; value = current; }
      else { value = current; if (held) { held.entered.resolve(); await held.gate.promise; } }
      return { ok: true, json: async () => value };
    },
  };
  const source = await readFile(new URL('./web/app.js', import.meta.url), 'utf8');
  runInNewContext(source + '\nglobalThis.check = {backCurrent,newSession,syncCurrent,current:()=>session};', context);
  await new Promise(done => setImmediate(done));
  held = { entered: deferred(), gate: deferred() };
  const returning = context.check.backCurrent(); await held.entered.promise;
  await context.check.newSession(); assert.equal(posts, 0, '返回当前会话期间不应开始切换');
  held.gate.resolve(); await returning; held = undefined;
  await context.check.newSession(); assert.equal(context.check.current().id, 'new-session-1');
  held = { entered: deferred(), gate: deferred() };
  const stale = context.check.syncCurrent(); await held.entered.promise;
  await context.check.newSession(); assert.equal(context.check.current().id, 'new-session-2');
  held.gate.resolve(); await stale; held = undefined;
  assert.equal(context.check.current().id, 'new-session-2', '迟到同步不得覆盖新会话');
});

test('批次绑定在会话创建时，非法批次不关闭会话，历史不借当前来源', async t => {
  const factory = fakeFactory(), options = [];
  const {origin,post,historyDir}=await fixture(t,async input=>{options.push(input);return factory.create(input);});
  const datasets=await fetch(origin+'/api/datasets').then(r=>r.json());
  assert.deepEqual(datasets.datasets.map(d=>d.id),['completed-files','three-day']);
  assert.ok(!JSON.stringify(datasets).includes('127.0.0.1'));
  const first=await post('/api/session',{datasetId:'three-day'}).then(r=>r.json());
  assert.equal(first.dataset.id,'three-day');
  assert.equal(options[0].apiBaseUrl,'http://127.0.0.1:28572');
  assert.equal((await post('/api/session',{datasetId:'legacy-55'})).status,400);
  assert.equal(factory.agents[0].closed,false);
  await writeFile(join(historyDir,first.id+'.json'),JSON.stringify({id:first.id,dataset:first.dataset,turns:[{question:'范围',answer:'部分交付',status:'completed'}]}));
  const second=await post('/api/session',{datasetId:'completed-files'}).then(r=>r.json());
  assert.equal(second.dataset.id,'completed-files');
  const history=await fetch(origin+'/api/history/'+first.id).then(r=>r.json());
  assert.equal(history.dataset.id,'three-day');
  assert.equal((await fetch(origin+'/api/session').then(r=>r.json())).dataset.id,'completed-files');
});

test('模型余额不足可读提示；不向页面暴露原始错误中的配置', async t => {
  const {origin,historyDir}=await fixture(t,fakeFactory().create);
  const id=randomUUID();
  await writeFile(join(historyDir,id+'.json'),JSON.stringify({id,turns:[
    {question:'余额不足',answer:'',status:'failed',error:'402: {"message":"Insufficient Balance","extra":"'+secret+'"}'},
    {question:'其他错误',answer:'',status:'failed',error:'/宿主/路径 '+secret}
  ]}));
  const raw=await fetch(origin+'/api/history/'+id).then(r=>r.text());
  const value=JSON.parse(raw);
  assert.equal(value.turns[0].failureReason,'模型服务余额不足，本次回答未完成。补充 DeepSeek 额度后再试。');
  assert.equal(value.turns[1].failureReason,'');
  assert.ok(!raw.includes(secret));assert.ok(!raw.includes('/宿主/路径'));
});

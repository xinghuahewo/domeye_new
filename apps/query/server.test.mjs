import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile, symlink, readFile } from 'node:fs/promises';
import { runInNewContext } from 'node:vm';
import { randomUUID } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createChatServer } from './server.mjs';
import { Check } from 'typebox/value';

// 固定假 Agent 只验证 HTTP/会话边界；不调用模型、QMD 或业务 API。
const secret = 'synthetic-only-model-key';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function fixture(t, makeAgent, options = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'domeye-chat-test-'));
  const historyDir = join(directory, 'history');
  const service = await createChatServer({ modelConfig: { provider:'deepseek', model:'deepseek-v4-pro', apiKey: secret }, historyDir, createAgent: makeAgent, ...options });
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

test('按会话绑定模型：拒绝未配置选择，保护在途回答，公开与历史合同保留模型身份',async t=>{
  const glmKey='synthetic-glm-key', options=[];
  const gate=deferred(),started=deferred();t.after(()=>gate.resolve());
  const factory=fakeFactory({gate,started});
  const {origin,post,historyDir}=await fixture(t,async input=>{options.push(input);return factory.create(input);},{
    models:{defaultModel:'deepseek-v4-pro',models:[
      {provider:'deepseek',model:'deepseek-v4-pro',apiKey:secret},
      {provider:'deepseek',model:'deepseek-flash',apiKey:secret},
      {provider:'zai',model:'glm-5.3-flashx',apiKey:glmKey},
    ]},
  });
  const contract=JSON.parse(await readFile(new URL('./openapi.json',import.meta.url),'utf8'));
  function expand(value){
    if(Array.isArray(value))return value.map(expand);
    if(!value || typeof value!=='object')return value;
    if(value.$ref)return expand(value.$ref.slice(2).split('/').reduce((node,key)=>node[key],contract));
    return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,expand(item)]));
  }
  const matches=(schema,value)=>assert.ok(Check(expand(contract.components.schemas[schema]),value),schema+' 不符合合同');
  const listed=await fetch(origin+'/api/models').then(r=>r.json());matches('ModelList',listed);
  assert.deepEqual(listed.models.map(item=>item.available),[true,true,true]);
  assert.ok(!JSON.stringify(listed).includes('apiKey'));assert.ok(!JSON.stringify(listed).includes('baseUrl'));
  const first=await post('/api/session',{modelId:'glm-5.3-flashx',datasetId:'three-day'}).then(r=>r.json());
  matches('Session',first);assert.equal(first.model.label,'GLM-5.3-FlashX');
  assert.equal(options[0].modelConfig.apiKey,glmKey);assert.equal(options[0].modelConfig.provider,'zai');
  for(const modelId of ['unknown',null,{},''])assert.equal((await post('/api/session',{modelId})).status,400);
  assert.equal((await post('/api/session',{modelId:'deepseek-flash',apiKey:'client-injected'})).status,400);
  assert.equal(factory.agents[0].closed,false);
  const answering=await post('/api/chat',{sessionId:first.id,question:'绑定模型'});await started.promise;
  assert.equal((await post('/api/session',{modelId:'deepseek-flash'})).status,409);
  gate.resolve();await answering.text();
  assert.equal((await fetch(origin+'/api/session').then(r=>r.json())).model.id,first.model.id);
  await writeFile(join(historyDir,first.id+'.json'),JSON.stringify({id:first.id,provider:'zai',model:'glm-5.3-flashx',model_selection:first.model,
    turns:[{question:'历史模型',answer:secret+glmKey,status:'completed'}]}));
  const second=await post('/api/session',{modelId:'deepseek-flash'}).then(r=>r.json());
  matches('Session',second);assert.notEqual(second.id,first.id);assert.equal(second.model.label,'DeepSeek V4.1 Flash');
  assert.equal(options[1].modelConfig.model,'deepseek-flash');assert.equal(factory.agents[0].closed,true);
  const history=await fetch(origin+'/api/history/'+first.id).then(r=>r.json());matches('HistorySession',history);
  assert.equal(history.model.id,first.model.id);assert.equal(history.turns[0].answer,'[已隐藏凭据][已隐藏凭据]');
  const legacyId=randomUUID();await writeFile(join(historyDir,legacyId+'.json'),JSON.stringify({id:legacyId,provider:'deepseek',model:'deepseek-v4-pro',turns:[{question:'旧模型',status:'completed'}]}));
  const unknownId=randomUUID();await writeFile(join(historyDir,unknownId+'.json'),JSON.stringify({id:unknownId,turns:[{question:'未标注',status:'completed'}]}));
  assert.equal((await fetch(origin+'/api/history/'+legacyId).then(r=>r.json())).model.id,'deepseek-v4-pro');
  assert.equal((await fetch(origin+'/api/history/'+unknownId).then(r=>r.json())).model,null);
  const historyList=await fetch(origin+'/api/history').then(r=>r.json());matches('HistoryList',historyList);
  assert.equal(historyList.records.find(item=>item.id===first.id).model.id,first.model.id);
});

test('未配置模型可见但不可选择，不创建 Agent 或关闭已有会话',async t=>{
  const factory=fakeFactory();const {origin,post}=await fixture(t,factory.create);
  const listed=await fetch(origin+'/api/models').then(r=>r.json());
  assert.deepEqual(listed.models.map(item=>item.available),[true,false,false]);
  const first=await post('/api/session',{}).then(r=>r.json());
  assert.equal((await post('/api/session',{modelId:'glm-5.3-flashx'})).status,400);
  assert.equal(factory.agents.length,1);assert.equal(factory.agents[0].closed,false);
  assert.equal((await fetch(origin+'/api/session').then(r=>r.json())).id,first.id);
});

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

test('流输出积压时先断开再停止，停止事件不会递归发布',async t=>{
  let blocked=false,stops=0;
  const stopped=deferred();
  const {service,post}=await fixture(t,async ({onEvent})=>({
    id:randomUUID(),turns:[],running:false,close:async()=>{},
    stop(){stops++;onEvent({type:'answer_end',messageId:1,stopReason:'aborted'});stopped.resolve();},
    async ask(question){
      onEvent({type:'answer_start',messageId:1});blocked=true;
      onEvent({type:'answer_delta',messageId:1,contentIndex:0,text:'人工积压片段'});
      return {question,answer:'',status:'cancelled'};
    }
  }));
  service.server.on('request',(req,res)=>{
    if(req.url==='/api/chat')Object.defineProperty(res,'writableLength',{get:()=>blocked?1024*1024+1:0});
  });
  const session=await post('/api/session',{}).then(r=>r.json());
  await post('/api/chat',{sessionId:session.id,question:'积压取消'}).then(r=>r.text()).catch(()=>{});
  await stopped.promise;
  assert.ok(stops>=1 && stops<=2,'只允许积压停止和断连通知，不递归重入');
});

test('历史只读：UUID 文件、拒绝符号链接与路径逃逸，只输出可读字段', async t => {
  const { origin, historyDir, directory } = await fixture(t, fakeFactory().create);
  const valid = randomUUID(), linked = randomUUID();
  const record = { id: valid, agent_instructions: secret, turns: [{ question: '已保存的问题', answer: '<script>原样文字</script>', status: 'completed', events: [{ thinking: secret }],reasoning:[{model_round_id:1,state:'recorded',blocks:[{content_index:0,text:'宿主推理内容'}]}] }] };
  await writeFile(join(historyDir, valid + '.json'), JSON.stringify(record));
  const outside = join(directory, 'outside.json'); await writeFile(outside, JSON.stringify({ ...record, id: linked })); await symlink(outside, join(historyDir, linked + '.json'));
  await writeFile(join(historyDir, 'not-a-uuid.json'), JSON.stringify(record));
  const response = await fetch(origin + '/api/history/' + valid); const value = await response.json();
  assert.equal(response.status, 200); assert.equal(value.readOnly, true); assert.equal(value.turns[0].answer, '<script>原样文字</script>');
  assert.ok(!JSON.stringify(value).includes(secret)); assert.ok(!('events' in value.turns[0]));
  assert.ok(!('reasoning' in value.turns[0]));assert.ok(!JSON.stringify(value).includes('宿主推理内容'));
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
      else if (path === '/api/models') value = {defaultModel:'deepseek-v4-pro',models:[{id:'deepseek-v4-pro',label:'DeepSeek V4 Pro',available:true}]};
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
  assert.equal(options[0].apiBaseUrl,'http://127.0.0.1:28473');
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
  assert.equal(value.turns[0].failureReason,'模型服务余额不足，本次回答未完成。请补充所选模型的额度后再试。');
  assert.equal(value.turns[1].failureReason,'');
  assert.ok(!raw.includes(secret));assert.ok(!raw.includes('/宿主/路径'));
});

test('页面消费真实分片：步骤不串文，完成前可见，失败取消及断流清除预览并保留计时',async t=>{
  for(const outcome of ['completed','failed','cancelled','disconnected'])await t.test(outcome,async()=>{
    const elements=new Map();
    const element=(tag='div')=>{
      let value='';
      const node={tag,children:[],dataset:{},value:'',scrollHeight:0,scrollTop:0,clientHeight:0,
        classList:{add(){},remove(){},toggle(){}},addEventListener(){},focus(){},
        append(...items){for(const item of items){item.parent=this;this.children.push(item);}},
        replaceChildren(...items){this.children=[];value='';this.append(...items);},
        replaceWith(other){const index=this.parent.children.indexOf(this);this.parent.children[index]=other;other.parent=this.parent;},
        querySelector(selector){return selector==='span'?element('span'):this.children.find(item=>selector.includes(item.dataset.turnId));},
        querySelectorAll(){return [];},
        get textContent(){return value+this.children.map(item=>item.textContent).join('');},
        set textContent(text){value=text;this.children=[];}
      };return node;
    };
    const id=randomUUID(),turnId=randomUUID();let current={id,busy:false,turns:[]},controller;
    const body=new ReadableStream({start(value){controller=value;}}),encoder=new TextEncoder();
    const emit=value=>controller.enqueue(encoder.encode(JSON.stringify(value)+'\n'));
    const context={
      document:{getElementById(id){if(!elements.has(id))elements.set(id,element());return elements.get(id);},createElement:element,
        createTextNode(value){const node=element('#text');node.textContent=value;return node;}},
      setTimeout,clearTimeout,TextDecoder,performance,requestAnimationFrame:callback=>setImmediate(callback),
      fetch:async path=>{
        if(path==='/api/chat')return {ok:true,body};
        return {ok:true,json:async()=>path==='/api/history'?{records:[]}:path==='/api/datasets'?{datasets:[]}:path==='/api/models'?{defaultModel:'deepseek-v4-pro',models:[]}:current};
      }
    };
    const source=await readFile(new URL('./web/app.js',import.meta.url),'utf8');
    runInNewContext(source+'\nglobalThis.check={submit};',context);
    await new Promise(done=>setImmediate(done));
    elements.get('question').value='固定流式问题';
    const submitted=context.check.submit({preventDefault(){}});
    const article=elements.get('messages').children[0],answer=article.children[2],status=article.children[4];
    emit({type:'start'});emit({type:'answer_start',messageId:1});emit({type:'text',messageId:1,contentIndex:0,text:'中间陈述'});
    await new Promise(done=>setImmediate(done));assert.equal(answer.textContent,'中间陈述');assert.match(status.textContent,/尚未完成/);
    emit({type:'answer_end',messageId:1,state:'discarded'});emit({type:'answer_start',messageId:2});
    emit({type:'text',messageId:1,contentIndex:0,text:'迟到旧片段'});
    emit({type:'text',messageId:2,contentIndex:3,text:'乙'});emit({type:'text',messageId:2,contentIndex:1,text:'甲'});
    await new Promise(done=>setImmediate(done));assert.equal(answer.textContent,'甲\n乙');
    const first=JSON.parse(article.dataset.latency);assert.ok(first.firstTextMs>=0);assert.equal(first.doneMs,null);
    if(outcome==='disconnected'){current={id,busy:false,turns:[]};controller.close();}
    else{
      current={id,busy:false,turns:[{id:turnId,question:'固定流式问题',answer:outcome==='completed'?'甲\n乙':'',status:outcome}]};
      emit({type:'answer_end',messageId:2,state:'generated'});emit({type:'done',turn:current.turns[0]});controller.close();
    }
    await submitted;
    assert.equal(answer.textContent,outcome==='disconnected'?'':'甲\n乙');
    const visible=elements.get('messages').textContent;
    assert.equal(visible.includes('中间陈述'),false);assert.equal(visible.includes('迟到旧片段'),false);
    if(outcome==='completed'){
      assert.ok(visible.includes('甲\n乙'));
      const timing=JSON.parse(elements.get('messages').children[0].dataset.latency);
      assert.ok(timing.finalFirstTextMs>=first.firstTextMs);assert.ok(timing.doneMs>=timing.finalFirstTextMs);
    }else assert.equal(visible.includes('甲\n乙'),false,'未完成结果不可在同步后冒充成功答案');
  });
});

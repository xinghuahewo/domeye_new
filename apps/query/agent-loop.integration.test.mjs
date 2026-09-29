import { test, mock, after } from 'node:test';
import assert from 'node:assert/strict';
import * as files from 'node:fs/promises';
import * as piAi from '@earendil-works/pi-ai';
import { Type } from 'typebox';
import { Check } from 'typebox/value';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const PRIVATE = '禁止公开的人工私有思考标记';
const KEY = 'synthetic-agent-loop-key';
const READ = '固定读取';
let active;
const messageText = message => typeof message.content === 'string' ? message.content :
  (message.content ?? []).filter(part => part.type === 'text').map(part => part.text).join('\n');
const toolCall = (id, code = READ) => ({ type: 'toolCall', id, name: 'execute', arguments: { code } });
const calls = (...items) => ({ content: items, stopReason: 'toolUse' });
const answer = text => ({ content: [{ type: 'thinking', thinking: PRIVATE }, { type: 'text', text }], stopReason: 'stop' });
const model = {
  id: 'synthetic-model', name: '人工测试模型', provider: 'deepseek', api: 'deepseek',
  baseUrl: 'https://api.deepseek.com', reasoning: true, input: ['text'],
  contextWindow: 100_000, maxTokens: 8192,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }
};

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

function controlledStream(selected, context, options) {
  const h = active;
  const index = h.requests.length;
  h.requests.push({ messages: structuredClone(context.messages), maxTokens: options.maxTokens, reasoning: options.reasoning });
  const stream = piAi.createAssistantMessageEventStream();
  const step = h.steps[index];
  queueMicrotask(async () => {
    try {
      assert.ok(step, '应用发出了固定脚本未安排的额外模型请求');
      assert.equal(options.signal?.aborted, false, '已取消仍进入 models.streamSimple');
      step.started?.resolve();
      if (step.wait) await step.wait;
      const result = {
        role: 'assistant', api: selected.api, provider: selected.provider, model: selected.id,
        timestamp: Date.now(), content: step.content ?? [], stopReason: step.stopReason ?? 'stop',
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
        ...(step.errorMessage ? { errorMessage: step.errorMessage } : {})
      };
      stream.push({ type: 'start', partial: { ...result, content: [] } });
      const deltas = step.deltas ?? result.content.flatMap((part,contentIndex)=>
        part.type==='thinking'?[{type:'thinking_delta',contentIndex,delta:part.thinking}]:
          part.type==='text'?[{type:'text_delta',contentIndex,delta:part.text}]:[]);
      for(const part of deltas)stream.push({...part,partial:result});
      if (step.beforeEnd) await step.beforeEnd();
      if (['error', 'aborted'].includes(result.stopReason)) stream.push({ type: 'error', reason: result.stopReason, error: result });
      else stream.push({ type: 'done', reason: result.stopReason, message: result });
    } catch (error) {
      h.fixtureErrors.push(error);
      stream.push({ type: 'error', reason: 'error', error: {
        role: 'assistant', api: model.api, provider: model.provider, model: model.id,
        timestamp: Date.now(), content: [], stopReason: 'error', errorMessage: error.message,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }
      } });
    }
  });
  return stream;
}

const replacements = [
  mock.module('@earendil-works/pi-ai', { namedExports: {
    ...piAi, createModels: () => ({ setProvider() {}, getModel: () => model, streamSimple: controlledStream })
  } }),
  mock.module('@earendil-works/pi-ai/providers/deepseek', { namedExports: { deepseekProvider: () => ({}) } }),
  mock.module('node:fs/promises', { namedExports: {
    ...files, mkdir: async () => {},
    writeFile: async (_file, contents) => {
      const h = active;
      h.saved.push(JSON.parse(contents));
      await h.onWrite?.(h.saved.length);
    }
  } }),
  mock.module(new URL('./tools.mjs', import.meta.url).href, { namedExports: {
    createTools: async ({ onEvidence }) => {
      const h = active;
      return {
        beginTurn() { h.begins++; }, close: async () => {},
        tools: [{
          name: 'execute', label: '固定片段回放', description: '回放记录中的工具返回，不访问业务 API。',
          parameters: Type.Object({ code: Type.String() }, { additionalProperties: false }),
          async execute(id, params, signal) {
            assert.equal(signal?.aborted, false, '已取消仍启动工具');
            assert.ok(h.results.has(params.code), '仅回放测试内固定工具结果，不执行记录中的代码');
            h.toolCalls.push({ id, code: params.code });
            await h.onTool?.(id, params, signal);
            if (signal?.aborted) throw new Error('人工工具已取消');
            const value = structuredClone(h.results.get(params.code));
            onEvidence('tool_result', { id, name: 'execute', params, value });
            return { content: [{ type: 'text', text: JSON.stringify(value) }], details: value };
          }
        }]
      };
    }
  } })
];
after(() => { for (const item of replacements.reverse()) item.restore(); });
// 不替换 Agent，也不覆盖 finishTurn：应用实际使用安装的 Pi 工具循环。
const { createDomeyeAgent } = await import('./agent.mjs?agent-loop-integration');
const { createChatServer } = await import('./server.mjs');

async function setup(t, steps, options = {}) {
  const h = active = {
    steps, requests: [], saved: [], events: [], fixtureErrors: [], toolCalls: [], begins: 0,
    results: new Map([[READ, { count: 17 }],
      [recorded.read.params.code, recorded.read.value],
      [recorded.calculation.params.code, recorded.calculation.value]]), ...options
  };
  h.agent = await createDomeyeAgent({
    modelConfig: { model: model.id, apiKey: KEY, thinkingLevel:options.thinkingLevel }, historyDir: '/synthetic-agent-loop',
    onEvent(event) { h.events.push(structuredClone(event)); h.onEvent?.(event); }
  });
  t.after(async () => { await h.agent.close(); });
  return h;
}

function assertPublicProjection(h) {
  const keys={
    answer_start:['type','messageId'],answer_delta:['type','messageId','contentIndex','text'],
    answer_end:['type','messageId','stopReason'],answer_final:['type','text'],
    tool_execution_start:['type','toolCallId','toolName','isError'],tool_execution_end:['type','toolCallId','toolName','isError']
  };
  for (const event of h.events) {
    assert.ok(keys[event.type],event.type);
    const allowed = keys[event.type];
    assert.ok(Object.keys(event).every(key => allowed.includes(key)));
  }
  assert.equal(JSON.stringify(h.events).includes(PRIVATE), false);
  for (const value of [h.events,h.saved,h.agent.turns]) assert.equal(JSON.stringify(value).includes(KEY),false);
  for (const turn of h.agent.turns) assert.equal(JSON.stringify({...turn,reasoning:[]}).includes(PRIVATE),false);
  assert.deepEqual(h.fixtureErrors, []);
}
function assertToolPairs(messages) {
  const wanted = messages.flatMap(message => message.role === 'assistant' ?
    message.content.filter(part => part.type === 'toolCall').map(part => part.id) : []);
  const actual = messages.filter(message => message.role === 'toolResult').map(part => part.toolCallId);
  assert.deepEqual(actual.sort(), wanted.sort(), '后续追问保留全部工具调用和对应结果');
}
function assertUnpublished(h, turn, status) {
  assert.equal(turn.status, status);
  assert.equal(turn.answer ?? '', '');
  assert.equal(h.events.some(event => event.type === 'answer_final'), false);
  assertPublicProjection(h);
}

test('首字回归：模型未结束、历史尚未保存时已发出公开正文增量', { timeout: 10_000 }, async t => {
  const emitted = deferred(), release = deferred();
  const text = '人工公开正文已经生成，后续生成尚未结束。';
  const h = await setup(t, [{ ...answer(text), async beforeEnd() { emitted.resolve(); await release.promise; } }]);
  const pending = h.agent.ask('首字测试');
  await emitted.promise;
  // 只让已排队的 Pi 事件被消费；不释放模型结束门。
  await new Promise(done => setImmediate(done));
  try {
    assert.equal(h.saved.length, 0);
    assert.equal(h.agent.running, true);
    assert.ok(h.events.some(event => event.type === 'answer_delta' && event.text.includes('人工公开正文')),
      '模型已产出正文，但应用仍等整轮结束才发布');
    assert.equal(h.events.some(event => event.type === 'answer_final'), false);
    assert.equal(JSON.stringify(h.events).includes(PRIVATE), false);
  } finally { release.resolve(); await pending; }
});

test('真实故障片段：自由字段名计算结果正常显示，无额外反馈；工具结果和答案保留给追问', { timeout: 10_000 }, async t => {
  const nextQuestion = '六类里哪类最多？';
  const nextAnswer = '已交付片段内，前缀中断最多，共 2994 次。';
  const h = await setup(t, [
    calls(toolCall('read-record', recorded.read.params.code)),
    calls(toolCall('calculate-record', recorded.calculation.params.code)),
    answer(recorded.answer), answer(nextAnswer)
  ]);
  const turn = await h.agent.ask(recorded.question);
  assert.equal(turn.status, 'completed');
  assert.equal(turn.answer, recorded.answer);
  assert.equal(h.requests.length, 3, '正常 stop 直接完成，不触发补算续跑');
  const computed = JSON.parse(messageText(h.requests[2].messages.find(message => message.role === 'toolResult' && message.toolCallId === 'calculate-record')));
  assert.deepEqual(computed, recorded.calculation.value, '原故障的字段名和数值完整进入模型上下文');
  assert.equal(computed.covered_percent, 14.930555555555555);
  assert.equal(computed.covered_duration_minutes, 215);
  assert.equal(computed.covered_whole_hours, 3);
  assert.equal(computed.covered_remaining_minutes, 35);
  assert.deepEqual(h.saved.at(-1).answer_review, { enabled: false });
  assert.deepEqual(h.saved.at(-1).turns[0].reasoning.at(-1),{
    model_round_id:3,state:'recorded',blocks:[{content_index:0,text:PRIVATE}]
  });
  assert.equal(Object.hasOwn(turn, 'numeric_review'), false);
  assert.equal(Object.hasOwn(turn, 'order_review'), false);
  assert.equal(turn.events.some(event => ['answer_feedback', 'answer_review', 'order_review'].includes(event.type)), false);
  assert.equal(h.requests[0].maxTokens, 32768);
  assert.equal(h.requests[0].reasoning, 'high');
  assert.deepEqual(h.saved.at(-1).model_options, { thinking_level: 'high', max_output_tokens: 32768 });
  const next = await h.agent.ask(nextQuestion);
  assert.equal(next.status, 'completed');
  assert.equal(next.answer, nextAnswer);
  assert.equal(h.requests.length, 4, '排序措辞不触发额外模型调用');
  for (const request of h.requests) assert.deepEqual(request.messages.filter(message => message.role === 'user').map(messageText),
    request === h.requests[3] ? [recorded.question, nextQuestion] : [recorded.question]);
  assert.ok(h.requests[3].messages.some(message => message.role === 'assistant' && messageText(message) === recorded.answer));
  assertToolPairs(h.requests[3].messages);
  assert.equal(h.begins, 2);
  assert.deepEqual(h.events.filter(event => event.type === 'answer_final'), [
    { type: 'answer_final', text: recorded.answer }, { type: 'answer_final', text: nextAnswer }
  ]);
  assertPublicProjection(h);
});

test('明确选择 low 时，Agent 调用和会话记录一致，输出额度不降低',async t=>{
  const h=await setup(t,[answer('人工低档位响应')],{thinkingLevel:'low'});
  assert.equal((await h.agent.ask('人工配置测试')).status,'completed');
  assert.equal(h.requests[0].reasoning,'low');
  assert.deepEqual(h.saved.at(-1).model_options,{thinking_level:'low',max_output_tokens:32768});
});

test('模型 length、error、aborted 均不确认部分预览为最终答案', { timeout: 10_000 }, async t => {
  for (const stopReason of ['length', 'error', 'aborted']) await t.test(stopReason, async t => {
    const h = await setup(t, [{ ...answer('未完整回答'), stopReason, ...(stopReason === 'error' ? { errorMessage: `人工模型失败 ${KEY}` } : {}) }]);
    const turn = await h.agent.ask('说明范围。');
    assert.equal(turn.stopReason, stopReason);
    assert.equal(h.requests.length, 1);
    assert.deepEqual(h.saved.at(-1).turns[0].reasoning,[{
      model_round_id:1,state:'partial',blocks:[{content_index:0,text:PRIVATE}]
    }]);
    assertUnpublished(h, turn, stopReason === 'aborted' ? 'cancelled' : 'failed');
  });
});

test('推理跨分片脱敏，结束消息丢失内容时仍保存已收到的部分',async t=>{
  for(const stopReason of ['error','aborted']) await t.test(stopReason,async t=>{
    const h=await setup(t,[{content:[],stopReason,deltas:[
      {type:'thinking_delta',contentIndex:0,delta:'先检查'+KEY.slice(0,7)},
      {type:'thinking_delta',contentIndex:0,delta:KEY.slice(7)+'再计算'},
    ]}]);
    const turn=await h.agent.ask('固定异常流');
    assert.deepEqual(h.saved.at(-1).turns[0].reasoning,[{
      model_round_id:1,state:'partial',blocks:[{content_index:0,text:'先检查[已隐藏凭据]再计算'}]
    }]);
    assert.equal(h.requests.length,1);
    assertUnpublished(h,turn,stopReason==='aborted'?'cancelled':'failed');
  });
});

test('推理按内容块保存，消息末尾的完整块不重复追加；仅末尾返回也可记录',async t=>{
  const h=await setup(t,[{stopReason:'stop',content:[
    {type:'thinking',thinking:'第一段完整'},
    {type:'thinking',thinking:'第二段'+KEY},
    {type:'text',text:'固定正文'},
  ],deltas:[
    {type:'thinking_delta',contentIndex:0,delta:'第一段'},
    {type:'text_delta',contentIndex:2,delta:'固定正文'},
  ]}]);
  const turn=await h.agent.ask('固定多块流');
  assert.equal(turn.status,'completed');
  assert.deepEqual(h.saved.at(-1).turns[0].reasoning,[{
    model_round_id:1,state:'recorded',blocks:[
      {content_index:0,text:'第一段完整'},{content_index:1,text:'第二段[已隐藏凭据]'}
    ]
  }]);
  assertPublicProjection(h);
});

test('工具预算二十次：第二十一次被阻止，不冒充完成', { timeout: 10_000 }, async t => {
  const h = await setup(t, [calls(...Array.from({ length: 21 }, (_, i) => toolCall(`read-${i}`)))]);
  const turn = await h.agent.ask('读取固定片段。');
  assert.equal(h.toolCalls.length, 20);
  assert.equal(h.begins, 1);
  assert.equal(h.requests.length, 1);
  assert.match(turn.error, /上限/);
  assertUnpublished(h, turn, 'failed');
});

test('工具中停止：剩余工具与后续模型调用不再启动', { timeout: 10_000 }, async t => {
  const started = deferred(), release = deferred();
  const h = await setup(t, [calls(toolCall('wait'), toolCall('blocked'))], {
    async onTool() { started.resolve(); await release.promise; }
  });
  const pending = h.agent.ask('读取固定片段。');
  await started.promise;
  h.agent.stop(); release.resolve();
  const turn = await pending;
  assert.equal(h.requests.length, 1);
  assert.equal(h.toolCalls.length, 1);
  assertUnpublished(h, turn, 'cancelled');
});

test('保存期间允许预览但不确认完成且禁止重入；停止后补存 cancelled', { timeout: 10_000 }, async t => {
  const saving = deferred(), release = deferred();
  const h = await setup(t, [answer('完整正文')], {
    async onWrite(count) { if (count === 1) { saving.resolve(); await release.promise; } }
  });
  const pending = h.agent.ask('说明范围。');
  await saving.promise;
  try {
    assert.equal(h.events.some(event => event.type === 'answer_final'), false);
    assert.ok(h.events.some(event => event.type === 'answer_delta'));
    assert.equal(h.agent.running, true);
    assert.equal(h.agent.turns.at(-1).answer, '');
    await assert.rejects(h.agent.ask('不能重入'), /尚未结束/);
    h.agent.stop();
  } finally { release.resolve(); }
  const turn = await pending;
  assert.equal(h.saved.length, 2);
  assert.equal(h.saved.at(-1).turns.at(-1).status, 'cancelled');
  assert.equal(h.saved.at(-1).turns.at(-1).answer, '');
  assertUnpublished(h, turn, 'cancelled');
});

test('保存失败不确认完成，凭据从错误中脱敏', { timeout: 10_000 }, async t => {
  const h = await setup(t, [answer('完整正文')], { onWrite() { throw new Error(`人工磁盘失败 ${KEY}`); } });
  const turn = await h.agent.ask('说明范围。');
  assert.match(turn.error, /保存失败/);
  assertUnpublished(h, turn, 'failed');
});

test('流式跨片脱敏、内容块与模型轮次分离，计时不把思考当正文', async t => {
  const h=await setup(t,[{
    ...calls({type:'text',text:'先读取数据'},toolCall('read'))
  },{
    ...answer('短句'+KEY+'结束'),
    deltas:[{type:'thinking_delta',contentIndex:0,delta:PRIVATE},
      ...['短句',KEY.slice(0,5),KEY.slice(5),'结束'].map(delta=>({type:'text_delta',contentIndex:1,delta}))]
  }]);
  const turn=await h.agent.ask('脱敏与轮次');
  const chunks=h.events.filter(event=>event.type==='answer_delta');
  assert.equal(chunks.filter(e=>e.messageId===2).map(e=>e.text).join(''),'短句[已隐藏凭据]结束');
  assert.equal(chunks.find(e=>e.messageId===2).text,'短句','普通短句不因凭据长度而滞留');
  assert.equal(h.events.find(e=>e.type==='answer_end' && e.messageId===1).stopReason,'toolUse');
  assert.equal(turn.timings.models.length,2);
  assert.equal(turn.timings.tools.length,1);
  assert.equal(turn.timings.final_message_id,2);
  assert.ok(turn.timings.models[1].first_token_ms<=turn.timings.models[1].first_text_ms);
  assert.ok(turn.timings.final_first_text_ms<=turn.timings.generation_end_ms);
  assert.ok(turn.timings.generation_end_ms<=turn.timings.save_started_ms);
  assert.ok(turn.timings.save_started_ms<=turn.timings.save_finished_ms);
  assert.equal(h.saved.at(-1).turns.at(-1).timings.save_finished_ms,null,'快照不伪造尚未测得的保存完成时间');
  assertPublicProjection(h);
});

test('真实 Pi 到 HTTP：首段先到，生成和保存分别等待，不重复正文', {timeout:10_000}, async t=>{
  const releaseModel=deferred(),saving=deferred(),releaseSave=deferred();
  const h=await setup(t,[{...answer('短正文'),async beforeEnd(){await releaseModel.promise;}}],{
    async onWrite(){saving.resolve();await releaseSave.promise;}
  });
  const directory=await files.mkdtemp(join(tmpdir(),'domeye-stream-'));
  const service=await createChatServer({modelConfig:{apiKey:KEY},historyDir:directory,
    createAgent:async ({onEvent})=>{h.onEvent=onEvent;return h.agent;}});
  const origin=await service.listen(0);
  t.after(async()=>{releaseModel.resolve();releaseSave.resolve();await service.close();await files.rm(directory,{recursive:true,force:true});});
  const post=(path,value)=>fetch(origin+path,{method:'POST',headers:{origin,'content-type':'application/json'},body:JSON.stringify(value)});
  const session=await post('/api/session',{}).then(r=>r.json());
  const response=await post('/api/chat',{sessionId:session.id,question:'流式边界'});
  const contract=JSON.parse(await files.readFile(new URL('./openapi.json',import.meta.url),'utf8'));
  function expand(value){
    if(Array.isArray(value))return value.map(expand);
    if(!value || typeof value!=='object')return value;
    if(value.$ref)return expand(value.$ref.slice(2).split('/').reduce((node,key)=>node[key],contract));
    return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,expand(item)]));
  }
  const eventSchema=expand(contract.components.schemas.ChatStreamEvent);
  assert.equal(Check(eventSchema,{type:'text',text:'缺少消息归属'}),false);
  const reader=response.body.getReader(),decoder=new TextDecoder();let pending='',events=[];
  async function until(predicate){
    while(!predicate()){
      const {done,value}=await reader.read();assert.equal(done,false);
      pending+=decoder.decode(value,{stream:true});let end;
      while((end=pending.indexOf('\n'))>=0){events.push(JSON.parse(pending.slice(0,end)));pending=pending.slice(end+1);}
    }
  }
  try{
    await until(()=>events.some(e=>e.type==='text'));
    assert.equal(h.saved.length,0,'首段经真实 HTTP 到达时模型仍被门阻挡');
    assert.equal(events.filter(e=>e.type==='text').map(e=>e.text).join(''),'短正文');
    assert.equal(events.some(e=>e.type==='done'),false);
    releaseModel.resolve();await saving.promise;
    await until(()=>events.some(e=>e.type==='answer_end'));
    assert.equal((await post('/api/session',{})).status,409,'保存完成前保持忙碌');
    assert.equal(events.some(e=>e.type==='done'),false);
    releaseSave.resolve();await until(()=>events.some(e=>e.type==='done'));
    assert.equal(events.filter(e=>e.type==='text').map(e=>e.text).join(''),'短正文','answer_final 不再次拼入流');
    const done=events.find(e=>e.type==='done');
    assert.equal(done.turn.status,'completed');assert.equal(done.turn.answer,'短正文');
    assert.ok(done.timing.first_text_ms<done.timing.done_ms);
    assert.ok(done.timing.agent.models[0].first_text_ms<done.timing.agent.save_finished_ms);
    for(const event of events)assert.equal(Check(eventSchema,event),true,`实际 HTTP 事件不符合公开合同：${event.type}`);
    assert.equal(JSON.stringify(events).includes(PRIVATE),false);
    assert.equal(JSON.stringify(await fetch(origin+'/api/session').then(r=>r.json())).includes(PRIVATE),false);
    assert.equal(h.requests.length,1);
  }finally{releaseModel.resolve();releaseSave.resolve();await reader.cancel();}
});

// 运行真实 Pi 0.87 Agent/loop；仅模型流、工具数据与文件写入固定化。
// 回归工具循环和发布边界，不检查、评分或证明业务答案正确；不访问真实模型/API。
// 以下片段原样摘自 round-5/runs/048d542e-bb2f-4e2f-b111-219b4c38fe85.json 第二题。
const recorded = {
  "question": "好，就按那天看看，异常一共有多少？",
  "read": {
    "id": "call_00_1zjs94eDvBIdOP4QNx4e1306",
    "name": "execute",
    "params": {
      "code": "async () => {\n  const response = await domeye.request({\n    method: \"GET\",\n    path: \"/api/v1/events/statistics\",\n    query: {\n      start_time: \"2026-02-24 00:00:00\",\n      end_time: \"2026-02-25 00:00:00\",\n      version: \"delivery_7b370f1e5aba00f327483f06e277114872ea8aa1f84755812783df0bd01b937c\",\n      kind: \"all\",\n      family: \"all\",\n      bucket: \"day\"\n    }\n  });\n  if (response.status !== 200) return response;\n  const body = response.body;\n  return {\n    state: body?.state,\n    query: body?.query,\n    version: body?.version,\n    coverage: body?.coverage,\n    total: body?.total,\n    series: Array.isArray(body?.series) ? body.series.map(s => ({kind: s.kind, total: s.total})) : body?.series,\n    metadata: body?.metadata,\n    message: body?.message\n  };\n}"
    },
    "value": {
      "state": "available",
      "query": {
        "start": "2026-02-24T00:00:00+08:00",
        "end_exclusive": "2026-02-25T00:00:00+08:00",
        "timezone": "Asia/Shanghai",
        "window_boundary": "[start,end)",
        "kind": "all",
        "family": "all",
        "bucket": "day",
        "metric": "recorded_event_starts"
      },
      "version": "delivery_7b370f1e5aba00f327483f06e277114872ea8aa1f84755812783df0bd01b937c",
      "coverage": {
        "state": "partial",
        "intervals": [
          {
            "start": "2026-02-24T08:00:00+08:00",
            "end_exclusive": "2026-02-24T11:35:00+08:00"
          }
        ],
        "requested_seconds": 86400,
        "covered_seconds": 12900
      },
      "total": 4169,
      "series": [
        {
          "kind": "prefix_outage",
          "total": 2994
        },
        {
          "kind": "as_outage",
          "total": 218
        },
        {
          "kind": "hijack",
          "total": 70
        },
        {
          "kind": "sub_hijack",
          "total": 74
        },
        {
          "kind": "leak",
          "total": 812
        },
        {
          "kind": "country_outage",
          "total": 1
        }
      ],
      "metadata": {
        "projection_unavailable_records": 0,
        "excluded_unknown_family": 0,
        "limitations": [
          "按事件开始时间统计当前事实，每个事件仅计一次；不是并发中断数或恢复数。",
          "仅统计实际交付片段；覆盖外及片段之间未知，不能由部分覆盖推定整窗总量。",
          "双栈事件同时纳入 IPv4 与 IPv6；两族结果不能相加作为总体。",
          "结果限于当前采集视图，不代表实际用户影响或原因。"
        ]
      }
    }
  },
  "calculation": {
    "id": "call_00_kzEX8fFswpsweYZM6z2M9927",
    "name": "execute",
    "params": {
      "code": "async () => {\n  const requestedSeconds = 86400;\n  const coveredSeconds = 12900;\n  const percent = requestedSeconds > 0 ? 100 * coveredSeconds / requestedSeconds : null;\n  const duration = {\n    seconds: coveredSeconds,\n    minutes: coveredSeconds / 60,\n    wholeHours: Math.floor(coveredSeconds / 3600),\n    remainingMinutes: (coveredSeconds % 3600) / 60\n  };\n  return {\n    requested_seconds: requestedSeconds,\n    covered_seconds: coveredSeconds,\n    covered_percent: percent,\n    covered_duration_minutes: duration.minutes,\n    covered_whole_hours: duration.wholeHours,\n    covered_remaining_minutes: duration.remainingMinutes\n  };\n}"
    },
    "value": {
      "requested_seconds": 86400,
      "covered_seconds": 12900,
      "covered_percent": 14.930555555555555,
      "covered_duration_minutes": 215,
      "covered_whole_hours": 3,
      "covered_remaining_minutes": 35
    }
  },
  "answer": "按你选定的那天（2026-02-24）查，异常开始数一共是 **4169 次**。\n\n范围说明：我按「2026-02-24 00:00:00 至 2026-02-25 00:00:00」（Asia/Shanghai）请求了一整天，但当天只有 **08:00–11:35** 这一个片段有完成文件交付，覆盖 12900 秒（3 小时 35 分钟），约占请求时段的 14.9%。所以 4169 是**已交付片段内**的计数，不是整天的总数。\n\n在 08:00–11:35 片段内，按事件开始时间统计的六类异常：\n\n- 前缀中断：2994\n- AS 中断：218\n- 前缀劫持候选：70\n- 子前缀劫持候选：74\n- 路由泄漏候选：812\n- 国家中断：1\n\n覆盖之外（00:00–08:00 和 11:35–24:00）没有数据，那部分是多少无法判断，不能据此推出整天总量。"
};

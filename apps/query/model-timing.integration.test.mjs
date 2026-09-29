import {test, mock, after} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {Type} from 'typebox';

// 使用真实 Agent、真实 Pi/DeepSeek/OpenAI SDK；只替换网络与业务工具。
const app = process.env.TIMING_TEST_APP ? pathToFileURL(process.env.TIMING_TEST_APP + '/') : new URL('./', import.meta.url);
const PRIVATE = '人工推理仅保存在宿主';
const KEY = 'synthetic-timing-key';
const originalFetch = globalThis.fetch;
const replacement = mock.module(new URL('tools.mjs', app).href, {namedExports:{createTools:async()=>({
  tools:[{name:'execute',label:'回放',description:'固定测试工具',parameters:Type.Object({code:Type.String()}),
    execute:async()=>({content:[{type:'text',text:'{"count":17}'}],details:{count:17}})}],close:async()=>{}
})}});
after(()=>{globalThis.fetch=originalFetch;replacement.restore();});
const {createDomeyeAgent}=await import(new URL('agent.mjs',app));
const usage={prompt_tokens:40,prompt_cache_hit_tokens:24,completion_tokens:12,
  completion_tokens_details:{reasoning_tokens:8},total_tokens:52};
function fixtureResponse(round,signal){
  const deltas=round===1 ? [
    {role:'assistant',reasoning_content:''},{reasoning_content:PRIVATE},{reasoning_content:'。'},
    {tool_calls:[{index:0,id:'fixture-call',type:'function',function:{name:'execute',arguments:'{"code":'}}]},
    {tool_calls:[{index:0,function:{arguments:'"fixture"}'}}]}
  ] : [{role:'assistant'},{reasoning_content:PRIVATE},{content:'公开正文'},{content:'：17。'}];
  const chunks=[': keep-alive\n\n',...deltas.map(delta=>'data: '+JSON.stringify({id:'fixture-response-'+round,
    object:'chat.completion.chunk',model:'deepseek-flash',choices:[{index:0,delta,finish_reason:null}]})+'\n\n'),
    'data: '+JSON.stringify({id:'fixture-response-'+round,choices:[{index:0,delta:{},finish_reason:round===1?'tool_calls':'stop'}],usage})+'\n\n',
    'data: [DONE]\n\n'];
  let i=0;
  return new Response(new ReadableStream({async pull(controller){
    await new Promise(resolve=>setTimeout(resolve,2));
    if(signal?.aborted){controller.error(new DOMException('Aborted','AbortError'));return;}
    if(i<chunks.length)controller.enqueue(new TextEncoder().encode(chunks[i++]));else controller.close();
  }}),{headers:{'content-type':'text/event-stream','x-request-id':'fixture-id'}});
}
test('真实 SDK 流：默认保存各轮推理，保持公开事件和模型输入不变',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'domeye-stream-timing-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const requests=[],events=[];
  globalThis.fetch=async(input,init)=>{
    const body=JSON.parse(init.body);requests.push(body);
    assert.equal(body.model,'deepseek-flash');assert.equal(body.reasoning_effort,'high');
    assert.equal(body.thinking.type,'enabled');assert.equal(body.max_tokens??body.max_completion_tokens,32768);
    return fixtureResponse(requests.length,init.signal);
  };
  const agent=await createDomeyeAgent({modelConfig:{model:'deepseek-flash',thinkingLevel:'high',apiKey:KEY},
    historyDir:dir,onEvent:event=>events.push(event)});t.after(()=>agent.close());
  const turn=await agent.ask('固定问题');
  assert.equal(turn.status,'completed',turn.error);assert.equal(turn.answer,'公开正文：17。');
  assert.equal(turn.timings.models.length,2);assert.equal(requests.length,2);
  const saved=JSON.parse(await readFile(join(dir,agent.id+'.json'),'utf8'));
  for(const value of [turn,events,agent.turns,saved]) assert.equal(JSON.stringify(value).includes(KEY),false);
  assert.equal(JSON.stringify(events).includes(PRIVATE),false);
  if(process.env.TIMING_REQUEST_OUTPUT)await writeFile(process.env.TIMING_REQUEST_OUTPUT,JSON.stringify(requests));
  if(process.env.TIMING_EXPECT_BASELINE==='1')return;
  assert.deepEqual(turn.reasoning,[
    {model_round_id:1,state:'recorded',blocks:[{content_index:0,text:PRIVATE+'。'}]},
    {model_round_id:2,state:'recorded',blocks:[{content_index:0,text:PRIVATE}]},
  ]);
  assert.deepEqual(saved.turns[0].reasoning,turn.reasoning);
  const [first,last]=turn.timings.models;
  assert.ok(first.stream,'缺少分类型流计时，无法区分推理和工具参数阶段');
  assert.deepEqual(first.requests.map(r=>r.status),[200]);assert.equal(last.requests.length,1);
  for(const round of [first,last]){
    assert.equal(round.stream.first_delta_type,'thinking');
    assert.equal(round.first_token_ms,round.stream.kinds.thinking.first_delta_ms);
    assert.ok(round.requests.at(-1).headers_received_ms<=round.first_token_ms);
    assert.ok(round.stream.last_delta_ms<=round.ended_ms);
    assert.equal(round.stream.usage.reasoning,8);assert.equal(round.stream.usage.output,12);
    assert.equal(round.stream.usage.input,16);assert.equal(round.stream.usage.cacheRead,24);
    for(const request of round.requests){
      assert.equal(request.model_round_id,round.id);assert.ok(request.headers_received_ms>=request.started_ms);
      assert.ok(request.request_bytes>0);assert.equal(request.outcome,'response');
      assert.deepEqual(Object.keys(request).sort(),['attempt','headers_received_ms','model_round_id','outcome','request_bytes','started_ms','status'].sort());
    }
  }
  assert.equal(first.stream.kinds.thinking.delta_count,2);
  assert.equal(first.stream.kinds.toolcall.delta_count,2);assert.equal(first.stream.kinds.text.first_delta_ms,null);
  assert.deepEqual(first.stream.phases.map(p=>p.kind),['thinking','toolcall']);
  assert.deepEqual(last.stream.phases.map(p=>p.kind),['thinking','text']);
  assert.equal(last.stream.kinds.text.delta_count,2);assert.ok(last.first_public_text_ms>=last.first_text_ms);
});

test('HTTP 429：沿用 SDK 的终止策略，计时不偷偷增加重试',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'domeye-stream-error-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  let calls=0;globalThis.fetch=async()=>{calls++;return new Response('{"error":{"message":"fixture limit"}}',{
    status:429,headers:{'content-type':'application/json','retry-after':'0'}});};
  const agent=await createDomeyeAgent({modelConfig:{model:'deepseek-flash',thinkingLevel:'high',apiKey:KEY},historyDir:dir});
  t.after(()=>agent.close());const turn=await agent.ask('限流测试');
  assert.equal(turn.status,'failed');assert.equal(calls,1);
  if(process.env.TIMING_EXPECT_BASELINE==='1')return;
  const round=turn.timings.models[0];assert.equal(round.requests.length,1);assert.equal(round.requests[0].status,429);
  assert.equal(round.stream.first_delta_type,null);assert.equal(round.stream.usage,null);
  assert.deepEqual(turn.reasoning,[{model_round_id:1,state:'not_returned',blocks:[]}]);
});

test('取消首个响应前的请求：仍然停止，未知阶段保持 null，HTTP 失败只有分类',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'domeye-stream-cancel-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  let entered;const waiting=new Promise(resolve=>entered=resolve);
  globalThis.fetch=async(_input,init)=>{
    entered();return new Promise((_resolve,reject)=>init.signal.addEventListener('abort',()=>reject(new DOMException('fixture abort','AbortError')),{once:true}));
  };
  const agent=await createDomeyeAgent({modelConfig:{model:'deepseek-flash',thinkingLevel:'high',apiKey:KEY},historyDir:dir});
  t.after(()=>agent.close());const pending=agent.ask('取消测试');await waiting;agent.stop();
  const turn=await pending;assert.equal(turn.status,'cancelled');assert.equal(turn.answer,'');
  if(process.env.TIMING_EXPECT_BASELINE==='1')return;
  const round=turn.timings.models[0];assert.ok(round.stream);assert.equal(round.stream.first_delta_type,null);
  assert.equal(round.stream.kinds.thinking.first_delta_ms,null);assert.equal(round.requests[0].headers_received_ms,null);
  assert.equal(round.requests[0].outcome,'aborted');assert.equal(round.requests[0].status,null);
  assert.equal(round.stream.usage,null);
});

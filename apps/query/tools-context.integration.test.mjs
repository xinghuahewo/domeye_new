import {test,mock} from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {once} from 'node:events';

test('后台预热不阻塞会话和其他工具；关闭会话仍释放检索器',async()=>{
  let warmed=false,closed=false;
  const docs=Object.assign(async()=>({results:[]}),{
    warmup:()=>{warmed=true;return new Promise(()=>{});},
    close:async()=>{closed=true;}
  });
  const docsMock=mock.module(new URL('./docs.mjs',import.meta.url).href,{namedExports:{createDocs:async()=>docs}});
  let registered;
  try{
    const {createTools}=await import('./tools.mjs?background-warmup');
    registered=await createTools({apiBaseUrl:'http://127.0.0.1:1'});
    assert.equal(warmed,true);
    const search=registered.tools.find(tool=>tool.name==='search');
    assert.deepEqual((await search.execute('during-warmup',{code:'async () => ({ready:true})'})).details,{ready:true});
    await registered.close();assert.equal(closed,true);
  }finally{await registered?.close();docsMock.restore();}
});

test('同题文档原文只传一次；来源变化、新问题与原始证据不被去重',async()=>{
  let source='domeye@synthetic-v1:rules.md:L1-L2',body='定义\n适用条件';
  const records=[];
  const docsMock=mock.module(new URL('./docs.mjs',import.meta.url).href,{namedExports:{
    createDocs:async()=>async()=>({results:[{title:'人工规则',source,text:body}]})
  }});
  let registered;
  try{
    const {createTools}=await import('./tools.mjs?docs-context');
    registered=await createTools({apiBaseUrl:'http://127.0.0.1:1',onEvidence:(type,value)=>records.push({type,value})});
    const docs=registered.tools.find(t=>t.name==='docs');registered.beginTurn();
    const first=await docs.execute('first',{query:'定义'});
    const repeated=await docs.execute('second',{query:'适用条件'});
    const firstView=JSON.parse(first.content[0].text),view=JSON.parse(repeated.content[0].text);
    assert.equal(firstView.results[0].text,body);
    assert.equal(view.results[0].text,undefined,'重复原文仍占模型上下文');
    assert.deepEqual(view.results[0].textReference,{toolCallId:'first',source});
    assert.equal(repeated.details.results[0].text,body);
    assert.equal(records.find(r=>r.value.id==='second').value.value.results[0].text,body);
    body='定义\n已变化的适用条件';
    assert.equal(JSON.parse((await docs.execute('changed',{query:'定义'})).content[0].text).results[0].text,body);
    source='domeye@synthetic-v2:rules.md:L1-L2';
    assert.equal(JSON.parse((await docs.execute('revision',{query:'定义'})).content[0].text).results[0].text,body);
    registered.beginTurn();
    assert.equal(JSON.parse((await docs.execute('new-turn',{query:'定义'})).content[0].text).results[0].text,body);
  }finally{await registered?.close();docsMock.restore();}
});

test('search 前置显示已确认版本；未知、冲突和非同族路径不能借版本，省略仍拒绝',async()=>{
  let conflict=false,hits=0;
  const server=createServer((_req,res)=>{hits++;res.writeHead(conflict?409:200,{'content-type':'application/json'});
    res.end(JSON.stringify({state:conflict?'version_conflict':'available',version:conflict?'synthetic-v2':'synthetic-v1'}));
  }).listen(0,'127.0.0.1');await once(server,'listening');
  const docsMock=mock.module(new URL('./docs.mjs',import.meta.url).href,{namedExports:{createDocs:async()=>async()=>({results:[]})}});
  let registered;
  try{
    const {createTools}=await import('./tools.mjs?version-context');
    registered=await createTools({apiBaseUrl:`http://127.0.0.1:${server.address().port}`});
    const search=registered.tools.find(t=>t.name==='search'),execute=registered.tools.find(t=>t.name==='execute');
    const context=async()=>JSON.parse((await search.execute('schema',{code:'async () => ({ready:true})'})).content[1]?.text ?? '{}').requestContext;
    registered.beginTurn();assert.deepEqual((await context())?.requiredVersions,[]);
    await execute.execute('discover',{code:'async () => domeye.request({method:"GET",path:"/api/v1/data-availability"})'});
    registered.beginTurn();
    const hints=(await context()).requiredVersions;
    assert.equal(hints.length,1);assert.equal(hints[0].version,'synthetic-v1');
    assert.ok(hints[0].paths.includes('/api/v1/events/statistics'));
    assert.equal(hints[0].paths.includes('/api/v1/core-overview'),false);
    assert.equal(hints[0].paths.includes('/api/v1/features/top'),false);
    await assert.rejects(execute.execute('missing',{code:'async () => domeye.request({method:"GET",path:"/api/v1/events/statistics"})'}),/显式携带/);
    assert.equal(hits,1);
    conflict=true;
    await execute.execute('conflict',{code:'async () => domeye.request({method:"GET",path:"/api/v1/events/statistics",query:{version:"synthetic-v1"}})'});
    assert.deepEqual((await context()).requiredVersions,[]);
  }finally{await registered?.close();docsMock.restore();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});

test('选定接口的 search 示例同次返回参数和完整小合同；大合同明确要求投影',async()=>{
  const docsMock=mock.module(new URL('./docs.mjs',import.meta.url).href,{namedExports:{createDocs:async()=>async()=>({results:[]})}});
  let registered;
  try{
    const {createTools,SEARCH_PARAMETERS_EXAMPLE}=await import('./tools.mjs?combined-contract');
    registered=await createTools({apiBaseUrl:'http://127.0.0.1:1'});
    const search=registered.tools.find(t=>t.name==='search');
    const run=path=>search.execute('contract',{code:SEARCH_PARAMETERS_EXAMPLE.replace('/从发现结果选定的完整路径',path)});
    const selected=(await run('/api/v1/events/statistics')).details[0];
    assert.ok(selected.parameters.some(p=>p.name==='version'));
    assert.ok(selected.schema.properties.coverage);assert.ok(selected.schema.properties.series);
    const wide=(await run('/api/v1/features/ases/overview')).details[0];
    assert.equal(wide.schema,undefined);assert.equal(wide.requiresProjection,true);
    assert.ok(wide.responseStructure.fields.includes('selected_asn'));
  }finally{await registered?.close();docsMock.restore();}
});

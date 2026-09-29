import {test} from 'node:test';
import assert from 'node:assert/strict';
import {collectOperations, requestForOperation, createApiDiscovery} from './api-operations.mjs';
import {outlineSchema,selectSchema} from './schema-tools.mjs';
import {runCode} from './runtime/executor.mjs';
import {readFile} from 'node:fs/promises';
import {resolveLocalRefs} from './spec.mjs';

const spec = {paths:{'/synthetic/{id}':{parameters:[{name:'id',in:'path',required:true,schema:{type:'string'}}],
  get:{operationId:'getSynthetic',parameters:[{name:'label',in:'query',schema:{type:'string'}},
    {name:'count',in:'query',schema:{type:'integer',minimum:1,maximum:4}},
    {name:'flag',in:'query',schema:{type:'boolean'}},
    {name:'mode',in:'query',schema:{enum:['a','b']}}],responses:{'200':{description:'合成读数，不表示连续状态',content:{'application/json':{
      schema:{oneOf:[{type:'object',required:['value'],properties:{value:{type:'number',nullable:true,description:'未知保持空值'}}},
        {type:'object',properties:{error:{type:'string'}}}]}
    }}}}},post:{operationId:'writeSynthetic'}},'/no-id':{get:{}}}};

test('操作从合同派生，路径参数继承且 GET 以外的方法不注册',()=>{
  const ops=collectOperations(spec);
  assert.deepEqual(Object.keys(ops),['getSynthetic','GET /no-id']);
  assert.equal(ops.getSynthetic.parameters[0].name,'id');
  assert.deepEqual(requestForOperation(ops.getSynthetic,{id:'值 /?#% +',label:'不提前编码 /+%',count:2,flag:false,mode:'b'}),{
    method:'GET',path:'/synthetic/'+encodeURIComponent('值 /?#% +'),query:{label:'不提前编码 /+%',count:2,flag:false,mode:'b'}
  });
  assert.deepEqual(requestForOperation(ops['GET /no-id']),{method:'GET',path:'/no-id',query:{}});
});

test('发现保留参数和原始响应分支，原始子树不被压平或选择',()=>{
  const discovery=createApiDiscovery(spec,collectOperations(spec),outlineSchema,selectSchema);
  const [detail]=discovery.describe('getSynthetic');
  assert.equal(detail.call,'domeye.api["getSynthetic"](params)');
  assert.equal(detail.responseContract.schema.oneOf.length,2);
  assert.deepEqual(discovery.schema('getSynthetic',['oneOf',0,'properties','value']),
    spec.paths['/synthetic/{id}'].get.responses['200'].content['application/json'].schema.oneOf[0].properties.value);
  assert.throws(()=>discovery.describe('invented'),/操作不存在/);
  assert.throws(()=>discovery.schema('getSynthetic',['missing']),/不存在/);
  assert.equal(discovery.list().length,2);
});

test('标量结构在发请求前校验，默认值不擅自注入',()=>{
  const operation=collectOperations(spec).getSynthetic;
  for(const params of [null,[],{id:'a',count:'2'},{id:'a',count:0},{id:'a',flag:0},{id:'a',mode:'c'},
    {id:'a',extra:1},{label:'a'},{id:null},{id:'a',count:NaN}]) assert.throws(()=>requestForOperation(operation,params));
  assert.deepEqual(requestForOperation(operation,{id:'a'}),{method:'GET',path:'/synthetic/a',query:{}});
});

test('重复操作名拒绝，路径参数覆盖遵守 OpenAPI 的同名同位置规则',()=>{
  const copied=structuredClone(spec);
  copied.paths['/duplicate']={get:{operationId:'getSynthetic'}};
  assert.throws(()=>collectOperations(copied),/唯一/);
  delete copied.paths['/duplicate'];
  copied.paths['/synthetic/{id}'].get.parameters.push({name:'id',in:'path',required:true,schema:{type:'string',minLength:2}});
  const operation=collectOperations(copied).getSynthetic;
  assert.equal(operation.parameters.filter(p=>p.name==='id').length,1);
  assert.throws(()=>requestForOperation(operation,{id:'x'}),/id/);
});

test('同名不同位置和宿主不支持的参数位置不暴露可调用方法',()=>{
  const copied=structuredClone(spec);
  copied.paths['/synthetic/{id}'].get.parameters.push({name:'id',in:'query'});
  assert.equal(collectOperations(copied).getSynthetic.callable,false);
  copied.paths['/synthetic/{id}'].get.parameters.pop();
  copied.paths['/synthetic/{id}'].get.parameters.push({name:'token',in:'header'});
  assert.equal(collectOperations(copied).getSynthetic.callable,false);
});

test('实际 QuickJS 操作调用保留失败响应并共用原宿主调用额度',async()=>{
  const operations=collectOperations(spec), reads=[];
  const response={status:503,body:{state:'unavailable',value:null},headers:{'x-domeye-result-state':'unavailable'}};
  const request=async value=>{reads.push(value);return response;};
  const result=await runCode({operations,request,code:'async()=>domeye.api.getSynthetic({id:"a/b"})'});
  assert.deepEqual(result,response);
  assert.equal(reads[0].path,'/synthetic/a%2Fb');
  await assert.rejects(runCode({operations,request,maxRequests:1,code:`async()=>{
    await domeye.api.getSynthetic({id:'a'}); return await domeye.api.getSynthetic({id:'b'});
  }`}),/次数/);
  assert.equal(reads.length,2,'额度外请求没有传至宿主');
  await assert.rejects(runCode({operations,request,maxRequests:1,code:`async()=>{
    try {await domeye.api.getSynthetic({id:'a',unknown:true});} catch {}
    return await domeye.api.getSynthetic({id:'a'});
  }`}),/次数/);
  assert.equal(reads.length,2,'校验失败的操作也消耗额度，不能绕过原限制');
});

test('同次发现交付完整小合同及引用定义，大合同保留目录；完整合同与展开结果等价',async()=>{
  const raw=JSON.parse(await readFile(new URL('./data/openapi-three-day.json',import.meta.url),'utf8'));
  const expanded=resolveLocalRefs(raw), operations=collectOperations(expanded);
  const discovery=createApiDiscovery(expanded,operations,outlineSchema,selectSchema,raw);
  const items=discovery.describe(['compareCountryWindows','getCoreOverview']);
  const comparison=items[0], overview=items[1];
  assert.equal(comparison.responseState,'complete');
  assert.equal(overview.responseState,'outline');
  assert.ok(JSON.stringify(items).length<=24000);
  const contract=comparison.responseContract;
  const restored={root:contract.schema};
  for(const [ref,value] of Object.entries(contract.references)){
    const path=decodeURIComponent(ref.slice(2)).split('/').map(part=>part.replace(/~1/g,'/').replace(/~0/g,'~'));
    let at=restored;
    for(const part of path.slice(0,-1))at=at[part]??={};
    at[path.at(-1)]=value;
  }
  assert.deepEqual(resolveLocalRefs(restored).root,
    expanded.paths['/api/v1/features/countries/comparison'].get.responses['200'].content['application/json'].schema);
  assert.ok(JSON.stringify(contract).length<JSON.stringify(resolveLocalRefs(restored).root).length/2,
    '重复引用只保存一次，不靠删除字段、约束或业务说明缩短合同');
  assert.ok(overview.responseStructure.fields.includes('metadata'));
});

test('完整合同保留转义引用、旁侧约束与复合分支，规范未声明的响应不补造结构',()=>{
  const raw={paths:{'/ref':{get:{operationId:'getRef',responses:{200:{content:{'application/json':{
    schema:{$ref:'#/definitions/a~1b~0c',maxProperties:2,description:'旁侧说明'}
  }}}}}},'/undeclared':{get:{operationId:'getUndeclared'}}},
    definitions:{'a/b~c':{oneOf:[{type:'null'},{type:'object',required:['value'],properties:{value:{type:'number'}}}]}}};
  const spec=resolveLocalRefs(raw);
  const [result,undeclared]=createApiDiscovery(spec,collectOperations(spec),outlineSchema,selectSchema,raw)
    .describe(['getRef','getUndeclared']);
  assert.equal(result.responseState,'complete');
  assert.deepEqual(result.responseContract.schema,raw.paths['/ref'].get.responses[200].content['application/json'].schema);
  assert.deepEqual(result.responseContract.references,{'#/definitions/a~1b~0c':raw.definitions['a/b~c']});
  assert.equal(undeclared.responseState,'not_declared');
  assert.equal(undeclared.responseContract,undefined);
});

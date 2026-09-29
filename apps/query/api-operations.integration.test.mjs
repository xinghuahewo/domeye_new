import {test, mock} from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {loadSearchSpec} from './spec.mjs';

test('同次 Code Mode 按发现的操作读取目录并传递真实国家和版本，保留比较资格', async () => {
  const reads = [], evidence = [];
  let country = '合成甲国', version = 'fixture-v1';
  const binding = {source_run:'fixture-run', collector:'fixture-collector', schema_version:'completed-file-delivery/v1'};
  const comparison = () => ({query:{country}, metadata:{version, schema_version:'country-window-comparison/v1',
    collector_id:binding.collector, recovery_assessment:'not_assessed'}, metrics:{
    activity:{unit:'次', reference:{value:null,known_window_sum:12},current:{value:null,known_window_sum:36},
      comparison:{state:'not_comparable',delta:null,percent:null}},
    resources:{unit:'合成资源',reference:{value:40,at:'2000-01-01T00:00:00Z'},
      current:{value:39,at:'2000-01-01T01:00:00Z'},comparison:{state:'comparable',delta:-1,percent:-2.5}}
  }});
  const server = createServer((req,res) => {
    const url = new URL(req.url,'http://127.0.0.1');
    reads.push({path:url.pathname,query:Object.fromEntries(url.searchParams)});
    let status = 200, body;
    if (url.pathname === '/api/v1/core-overview') body = {state:'available',version,metadata:{countries:[country],
      interpretation_version:'completed-file-results/v2',result_delivery:{state:'available',version,binding}}};
    else if (url.pathname === '/api/v1/features/countries/comparison') {
      if (url.searchParams.get('country') !== country || url.searchParams.get('version') !== version) {
        status = 400; body = {message:'须使用刚返回的国家和版本'};
      } else body = comparison();
    } else {status=404;body={message:'未登记的合成请求'};}
    res.writeHead(status,{'content-type':'application/json','x-domeye-result-version':version});
    res.end(JSON.stringify(body));
  }).listen(0,'127.0.0.1');
  await once(server,'listening');
  const docsMock = mock.module(new URL('./docs.mjs',import.meta.url).href,{namedExports:{createDocs:async()=>async()=>({results:[]})}});
  let registered;
  try {
    const {createTools} = await import('./tools.mjs?operation-chain');
    registered = await createTools({apiBaseUrl:`http://127.0.0.1:${server.address().port}`,specFile:'openapi-three-day.json',
      onEvidence:(type,value)=>evidence.push({type,value})});
    const search = registered.tools.find(tool=>tool.name==='search');
    const execute = registered.tools.find(tool=>tool.name==='execute');
    const discovery = await search.execute('discover',{code:`async()=>({
      contracts:api.describe(['getCoreOverview','compareCountryWindows']),
      countries:api.schema('getCoreOverview',['properties','metadata','properties','countries']),
      version:api.schema('getCoreOverview',['properties','version'])
    })`});
    assert.equal(reads.length,0,'发现只读取合同');
    assert.deepEqual(discovery.details.contracts.map(item=>item.operationId),['getCoreOverview','compareCountryWindows']);
    const schema = (await loadSearchSpec('openapi-three-day.json')).paths['/api/v1/core-overview'].get.responses['200'].content['application/json'].schema;
    assert.deepEqual(discovery.details.countries,schema.properties.metadata.properties.countries);
    assert.deepEqual(discovery.details.version,schema.properties.version);
    let knownVersion;
    for (const [label,span] of [['合成甲国',60],['合成乙国',30]]) {
      country=label;
      registered.beginTurn();
      const start = '2000-01-01 08:00:00', end = span === 60 ? '2000-01-01 09:00:00' : '2000-01-01 08:30:00';
      const result = await execute.execute(`chain-${span}`,{code:`async()=>{
        const window=${JSON.stringify({start_time:start,end_time:end,...(knownVersion?{version:knownVersion}:{})})};
        const directory=await domeye.api.getCoreOverview(window);
        if(directory.status!==200 || directory.body.state!=='available') return directory;
        const country=directory.body.metadata.countries.find(value=>value===${JSON.stringify(label)});
        if(country===undefined) return {state:'unavailable',reason:'未找到请求对象'};
        return await domeye.api.compareCountryWindows({...window,country,version:directory.body.version,
          reference_start_time:'2000-01-01 07:00:00',reference_end_time:'2000-01-01 08:00:00'});
      }`});
      assert.equal(result.details.status,200);
      assert.deepEqual(result.details.body,comparison(),'SDK 不重算不可比较片段，也不丢失原值和单位');
      const receipts = JSON.parse(result.content[1].text).requestControl.responses;
      assert.equal(receipts.length,2);
      assert.equal(receipts[1].request.query.version,version);
      assert.equal(receipts[1].request.query.country,label);
      assert.equal(receipts[1].request.query.end_time,end);
      assert.equal(result.details.headers['x-domeye-result-version'],version);
      knownVersion=result.details.body.metadata.version;
    }
    assert.equal(reads.length,4,'每个窗口一次目录读取和一次比较，没有纠错请求');
    assert.equal(evidence.filter(item=>item.type==='http').length,4);
    const before=reads.length;
    await assert.rejects(execute.execute('missing-version',{code:`async()=>domeye.api.compareCountryWindows({
      country:'合成乙国',start_time:'2000-01-01 08:00:00',end_time:'2000-01-01 09:00:00',
      reference_start_time:'2000-01-01 07:00:00',reference_end_time:'2000-01-01 08:00:00'})`}),/version/);
    assert.equal(reads.length,before,'操作调用继续经过既有版本策略，不能自动补版本');
    await assert.rejects(execute.execute('wrong-type',{code:'async()=>domeye.api.getCoreOverview({page:"1"})'}),/page/);
    await assert.rejects(execute.execute('unknown-param',{code:'async()=>domeye.api.getCoreOverview({unexpected:true})'}),/unexpected/);
    await assert.rejects(execute.execute('missing-param',{code:'async()=>domeye.api.compareCountryWindows({country:"合成乙国"})'}),/start_time/);
    await assert.rejects(execute.execute('raw-shape',{code:'async()=>domeye.request({operationId:"getCoreOverview",params:{}})'}),/method.*path.*query/);
    assert.equal(reads.length,before,'参数结构错误在发 HTTP 前报告');
  } finally {
    await registered?.close();docsMock.restore();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
  }
});

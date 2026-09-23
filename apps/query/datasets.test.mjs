import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { selectDataset } from './datasets.mjs';
import { loadSearchSpec } from './spec.mjs';
import { createRequest } from './runtime/executor.mjs';
import { createRequestPolicy } from './runtime/request-policy.mjs';

test('每个批次只发现它实际部署的接口与参数合同', async () => {
  const completed = selectDataset(), advancing = selectDataset('three-day');
  assert.notEqual(completed.apiBaseUrl, advancing.apiBaseUrl);
  const [first, second] = await Promise.all([loadSearchSpec(completed.specFile),loadSearchSpec(advancing.specFile)]);
  assert.ok(first.paths['/api/v1/data-availability']);
  assert.equal(second.paths['/api/v1/data-availability'], undefined);
  assert.ok(second.paths['/api/v1/result-rollups']);
  assert.equal(second.paths['/result-rollups'], undefined);
  const versions = spec => spec.paths['/api/v1/resources'].get.parameters.some(p=>p.name==='version');
  assert.equal(versions(first), true); assert.equal(versions(second), false);
  assert.equal(advancing.apiSourceCommit, null);
  assert.throws(()=>selectDataset('legacy-55'), /本项目/);
});

test('路径参数经过宿主与版本策略，完整 HTTP 正文保留且不能逃离路由', async () => {
  const urls = [];
  const server = createServer((req,res)=>{
    urls.push(req.url); res.setHeader('content-type','application/json');
    res.end(JSON.stringify({state:'available',version:'publication-1',value:null}));
  }).listen(0,'127.0.0.1');
  await once(server,'listening');
  const path='/api/v2/country-outages/{incident_id}/overview';
  const spec={paths:{[path]:{get:{parameters:[{in:'query',name:'version'}]}}}};
  const events=[];
  const policy=createRequestPolicy({spec,request:createRequest({baseUrl:`http://127.0.0.1:${server.address().port}`,paths:[path]}),onEvidence:event=>events.push(event)});
  const actual='/api/v2/country-outages/'+encodeURIComponent('SB 2026-02-24T09:29:42')+'/overview';
  try {
    policy.beginTurn();
    assert.deepEqual(await policy.request({method:'GET',path:actual}),{status:200,body:{state:'available',version:'publication-1',value:null}});
    await assert.rejects(policy.request({method:'GET',path:actual}),error=>error.kind==='policy');
    await policy.request({method:'GET',path:actual,query:{version:'publication-1'}});
    assert.equal(events.filter(e=>e.type==='response').length,2);
    for(const invalid of ['//example.org/a','/api/v2/country-outages/../overview','/api/v2/country-outages/%2e%2e/overview','/api/v2/country-outages/a%2fb/overview','/api/v2/country-outages/%252e%252e/overview','/api/v2/country-outages/{incident_id}/overview',actual+'?x=1',actual+'#x',actual+'/extra']) {
      await assert.rejects(policy.request({method:'GET',path:invalid}),error=>error.kind==='input');
    }
    assert.equal(urls.length,2);
    assert.ok(urls[0].includes('%20'));
  } finally {server.closeAllConnections();await new Promise(done=>server.close(done));}
});

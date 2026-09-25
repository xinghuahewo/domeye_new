import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { truncateResponse } from './runtime/truncate.mjs';

test('Cloudflare 字符边界：短文本原样，超长正文前缀后明确提示收窄', () => {
  for (const size of [23999, 24000]) {
    const text = '中'.repeat(size);
    assert.equal(truncateResponse(text), text);
  }
  const long = '中'.repeat(24001);
  const visible = truncateResponse(long);
  assert.equal(visible.slice(0, 24000), long.slice(0, 24000));
  assert.match(visible.slice(24000), /TRUNCATED/);
  assert.match(visible.slice(24000), /更具体的查询/);
  assert.equal(truncateResponse(null), 'null');
  const value = { description: '保留原文', count: null, observed: 0, applicable: false };
  assert.equal(truncateResponse(value), JSON.stringify(value, null, 2));
});

test('实际 search 先保留能容纳的完整结构，仍过大的内容明确截断，原始结构留档', async () => {
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href, {
    namedExports: { createDocs: async () => async () => ({ matches: [] }) }
  });
  let registered;
  const evidence = [];
  try {
    const { createTools } = await import('./tools.mjs?search-truncation');
    registered = await createTools({ apiBaseUrl: 'http://127.0.0.1:1',
      onEvidence: (type, value) => evidence.push({ type, value }) });
    assert.deepEqual(registered.tools.map(tool => tool.name), ['docs', 'search', 'execute']);
    const search = registered.tools.find(tool => tool.name === 'search');
    const schema = 'spec.paths["/api/v1/features/ases/overview"].get.responses["200"].content["application/json"].schema';
    const wide = await search.execute('wide', { code: `async () => ${schema}` });
    const raw = JSON.stringify(wide.details, null, 2);
    assert.ok(raw.length > 24000, '固定版本的真实 ASN schema 确实越过上游边界');
    assert.ok(JSON.stringify(wide.details).length > 24000, '固定合同的紧凑表示仍然过大');
    assert.equal(wide.content[0].text.slice(0, 24000), raw.slice(0, 24000));
    assert.match(wide.content[0].text.slice(24000), /TRUNCATED/);
    assert.deepEqual(evidence.find(item => item.value.id === 'wide').value.value, wide.details);
    const narrow = await search.execute('narrow', {
      code: `async () => ${schema}.properties.selected_asn.anyOf[0].properties.sample_count`
    });
    assert.deepEqual(JSON.parse(narrow.content[0].text),
      wide.details.properties.selected_asn.anyOf[0].properties.sample_count);
    assert.ok(!narrow.content[0].text.includes('TRUNCATED'));
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const executed = await execute.execute('execute-wide',{code:'async () => ({known:0,unknown:null,rows:"中".repeat(25000)})'});
    assert.match(executed.content[0].text, /TRUNCATED/);
    assert.equal(executed.details.rows.length,25000);
    assert.equal(executed.details.unknown,null);
    assert.deepEqual(evidence.find(item=>item.value.id==='execute-wide').value.value,executed.details);
    assert.deepEqual(JSON.parse(executed.content[1].text).requestControl.responses,[]);
    await assert.rejects(search.execute('bad-code', {code:'async () => { throw new Error("人工代码失败"); }'}), /人工代码失败/);
    assert.equal(evidence.some(item => item.type === 'http'), false, 'search 只读规范，不访问业务 API');
  } finally {
    await registered?.close();
    docsMock.restore();
  }
});

test('实际 execute 的完整嵌套时序不因缩进丢失时间与空值，正文和留档一致', async () => {
  const docsMock = mock.module(new URL('./docs.mjs', import.meta.url).href, {
    namedExports: { createDocs: async () => async () => ({ results: [] }) }
  });
  let registered;
  const records = [];
  try {
    const { createTools } = await import('./tools.mjs?lossless-structure');
    registered = await createTools({ apiBaseUrl: 'http://127.0.0.1:1',
      onEvidence: (type,value) => records.push({type,value}) });
    const execute = registered.tools.find(tool => tool.name === 'execute');
    const result = await execute.execute('observations', {code: `async () => ({
      scope: {version:'synthetic-v1', unit:'synthetic-unit', note:'原文  保留\\n换行'},
      data: Array.from({length:72}, (_,i) => ({
        label: new Date(i*300000).toISOString(), value:i % 2 ? 0 : null,
        resource_state_at:new Date((i+1)*300000).toISOString(),
        source_window:{source_id:'synthetic-'+i,start:new Date(i*300000).toISOString(),
          end_exclusive:new Date((i+1)*300000).toISOString()},
        evidence:{applicable:false, reference:null}
      }))
    })`});
    assert.ok(JSON.stringify(result.details,null,2).length > 24000);
    assert.ok(JSON.stringify(result.details).length <= 24000);
    assert.deepEqual(JSON.parse(result.content[0].text),result.details);
    assert.deepEqual(records.find(r=>r.value.id==='observations').value.value,result.details);
    assert.deepEqual(JSON.parse(result.content[1].text).requestControl.responses,[]);
  } finally {
    await registered?.close(); docsMock.restore();
  }
});

test('紧凑 JSON 的边界不放宽原额度，字符串内部空白不被删改', () => {
  const value={point:{value:null,at:'2000-01-01T00:00:00Z',note:'字段内  空格\n换行'},padding:''};
  value.padding='x'.repeat(24000-JSON.stringify(value).length);
  assert.equal(JSON.stringify(value).length,24000);
  assert.deepEqual(JSON.parse(truncateResponse(value)),value);
  value.padding+='x';
  assert.match(truncateResponse(value),/TRUNCATED/);
});

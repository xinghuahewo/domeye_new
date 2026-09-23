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

test('实际 search 超长结构给模型截断提示，原始结构留档，收窄后可取得原子树', async () => {
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

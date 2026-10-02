import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolveLocalRefs, loadSearchSpec } from './spec.mjs';
import { runCode } from './runtime/executor.mjs';

test('实际同版规范展开参数和响应，保留时间说明、弃用标记及源文件', async () => {
  const raw = JSON.parse(await readFile(new URL('./data/openapi.json', import.meta.url), 'utf8'));
  const before = JSON.stringify(raw), spec = resolveLocalRefs(raw);
  assert.equal(JSON.stringify(raw), before);
  assert.deepEqual(Object.keys(spec.paths), Object.keys(raw.paths));
  assert.ok(!JSON.stringify(spec).includes('"$ref":'));
  const operation = spec.paths['/api/v1/features/ases/overview'].get;
  const start = operation.parameters.find(item => item.name === 'start_time');
  assert.equal(start.description, raw.components.parameters.StartTime.description);
  const overview = operation.responses['200'].content['application/json'].schema;
  assert.equal(overview.properties.selected_asn.anyOf[0].properties.sample_count.type, 'integer');
  assert.equal(spec.paths['/api/v1/features/outages/as-prefix'].get.deprecated, true);
  const diagnostic = spec.components.schemas.CoreOverviewDiagnosticWindow;
  assert.ok(diagnostic && Object.keys(diagnostic).length);
  const result = await runCode({ spec, code: 'async () => { const op = spec.paths["/api/v1/features/ases/overview"].get; return { names: op.parameters.map(p => p.name), schema: op.responses["200"].content["application/json"].schema }; }' });
  assert.ok(result.names.includes('start_time'));
  assert.equal(result.schema.properties.selected_asn.anyOf[0].properties.sample_count.type, 'integer');
});

test('引用错误明确失败，不把缺失或循环引用省略成空结构', () => {
  assert.throws(() => resolveLocalRefs({ value: { $ref: 'https://example.org/spec.json' } }), /文件内/);
  assert.throws(() => resolveLocalRefs({ value: { $ref: '#/missing' } }), /不存在/);
  assert.throws(() => resolveLocalRefs({ value: { $ref: '#/value' } }), /循环引用/);
});

test('支持转义指针，保留引用旁的说明和同时适用的结构约束', async () => {
  const spec = resolveLocalRefs({ definitions: { 'a/b~c': { type: 'number', minimum: 0, description: '原说明' } },
    value: { $ref: '#/definitions/a~1b~0c', maximum: 10, description: '本处说明' } });
  assert.deepEqual(spec.value, { allOf: [{ type: 'number', minimum: 0, description: '原说明' }, { maximum: 10 }], description: '本处说明' });
  assert.equal(Object.keys((await loadSearchSpec()).paths).length, 35);
});

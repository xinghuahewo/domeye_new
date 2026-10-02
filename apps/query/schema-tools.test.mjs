import { test } from 'node:test';
import assert from 'node:assert/strict';
import { outlineSchema, selectSchema } from './schema-tools.mjs';
import { runCode } from './runtime/executor.mjs';

test('结构目录保留共同约束和各自分支；选择子树不改写合同', () => {
  const schema = { type: 'object', properties: { shared: { type: 'string' } }, required: ['shared'], allOf: [
    { properties: { count: { type: 'integer', minimum: 0 } }, required: ['count'] },
    { anyOf: [
      { type: 'object', properties: { state: { enum: ['known', 'unknown'], description: '未知不能补零' } } },
      { type: 'null' }
    ] }
  ] };
  const before = structuredClone(schema), result = outlineSchema(schema);
  assert.deepEqual(result.fields, ['shared']);
  assert.deepEqual(result.required, ['shared']);
  assert.deepEqual(result.allOf[0].fields, ['count']);
  assert.deepEqual(result.allOf[0].required, ['count']);
  const branch = result.allOf[1].anyOf[0];
  assert.deepEqual(branch.schemaPath, ['allOf', 1, 'anyOf', 0]);
  assert.deepEqual(selectSchema(schema, [...branch.schemaPath, 'properties', 'state']),
    { enum: ['known', 'unknown'], description: '未知不能补零' });
  assert.equal(selectSchema(schema, ['allOf', 0, 'properties', 'count', 'minimum']), 0);
  assert.deepEqual(schema, before);
});

test('数组、映射、可空、真假 Schema 和未声明彼此区分', () => {
  const schema = { type: ['array', 'null'], nullable: true, const: null, items: {
    type: 'object', additionalProperties: { oneOf: [{ type: 'number', const: 0 }, { enum: [null, false] }] }
  }, prefixItems: [false, true] };
  const result = outlineSchema(schema);
  assert.deepEqual(result.type, ['array', 'null']);
  assert.equal(result.nullable, true);
  assert.equal(result.const, null);
  assert.equal(result.items.additionalProperties.oneOf[0].const, 0);
  assert.deepEqual(result.items.additionalProperties.oneOf[1].enum, [null, false]);
  assert.deepEqual(result.prefixItems[0], { schemaPath: ['prefixItems', 0], booleanSchema: false });
  assert.equal(selectSchema(schema, ['prefixItems', 0]), false);
  assert.deepEqual(outlineSchema(undefined), { schemaPath: [], state: 'not_declared' });
  assert.deepEqual(outlineSchema({ properties: {} }).fields, []);
  assert.throws(() => outlineSchema(null), /不是 Schema/);
});

test('结构路径须明确有效；缺字段不能退回空对象、第一分支或继承属性', () => {
  const schema = { oneOf: [{ type: 'string' }, false] };
  for (const path of [['properties'], ['oneOf', 2], ['constructor'], ['__proto__']]) {
    assert.throws(() => selectSchema(schema, path), /不存在/);
  }
  for (const path of ['oneOf.0', [-1], [1.2], [null]]) {
    assert.throws(() => selectSchema(schema, path), /schemaPath/);
  }
  assert.throws(() => selectSchema(undefined, []), /不存在/);
  assert.equal(selectSchema(schema, []), schema);
});

test('沙箱导航与宿主实现一致，execute 不获得额外规范或宿主能力', async () => {
  const schema = { type: 'array', items: [{ type: 'integer' }, false] };
  const result = await runCode({ spec: { schema }, code: `async () => ({
    outline: schemaTools.outline(spec.schema),
    selected: schemaTools.select(spec.schema, ['items', 1]),
    request: typeof domeye, network: typeof fetch, filesystem: typeof process
  })` });
  assert.deepEqual(result, { outline: outlineSchema(schema), selected: false,
    request: 'undefined', network: 'undefined', filesystem: 'undefined' });
  const execute = await runCode({ request: async () => { throw new Error('不应请求'); },
    code: 'async () => ({spec: typeof spec, schemaTools: typeof schemaTools})' });
  assert.deepEqual(execute, { spec: 'undefined', schemaTools: 'undefined' });
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeSourceReceipt } from './source-receipt.mjs';

// 纯人工 HTTP/事件边界，不调用模型、业务库或共享服务。
const eventOf = (extra = {}) => ({
  type: 'response', id: 7, turn: 3, family: 'delivery', binding: 'delivery',
  request: { method: 'GET', path: '/synthetic/summary', query: { version: 'synthetic-v1', object: 'synthetic-object' } },
  status: 200, expectedVersion: 'synthetic-v1', responseVersion: 'synthetic-v1', versionAssurance: 'matched', epoch: 0,
  ...extra,
});
const receipt = (body, options = {}) => makeSourceReceipt({
  toolCallId: 'synthetic-tool-call', event: eventOf({ responseVersion: body?.version ?? null,
    versionAssurance: body?.version == null ? 'missing' : 'matched' }), response: { status: 200, body }, ...options,
});
const bytes = value => Buffer.byteLength(JSON.stringify(value), 'utf8');
const freeze = value => {
  if (value && typeof value === 'object') { Object.freeze(value); Object.values(value).forEach(freeze); }
  return value;
};

test('同数值的不同来源分别绑定；兼容结果不借其他调用的版本或覆盖', () => {
  const delivered = receipt({ version: 'synthetic-v1', coverage: { state: 'partial' }, count: 0 });
  const compatibleEvent = eventOf({ id: 8, family: 'compatibility', binding: 'compatibility',
    request: { method: 'GET', path: '/synthetic/legacy', query: { object: 'synthetic-object' } },
    expectedVersion: null, responseVersion: null, versionAssurance: 'unverified' });
  const compatible = receipt({ count: 0 }, { toolCallId: 'synthetic-other-call', event: compatibleEvent });
  assert.equal(delivered.source.versionAssurance, 'matched');
  assert.deepEqual(delivered.scope.coverage, { state: 'partial' });
  assert.equal(compatible.toolCallId, 'synthetic-other-call');
  assert.equal(compatible.id, 8);
  assert.deepEqual(compatible.request, compatibleEvent.request);
  assert.deepEqual(compatible.source, { family: 'compatibility', binding: 'compatibility', expectedVersion: null, responseVersion: null, versionAssurance: 'unverified', epoch: 0 });
  assert.deepEqual(compatible.scope, {});
  assert.equal(Object.hasOwn(compatible.source, 'requestVersion'), false);
  assert.equal(Object.hasOwn(delivered.scope, 'count'), false);
  assert.match(compatible.note, /不是任意 JavaScript 返回值的字段血缘/);
  assert.match(delivered.note, /已确认也不等于跨入口语义可比/);
});

test('只摘正文自有白名单，保留 null、0、false；缺失不填，不摘计数', () => {
  const body = Object.assign(Object.create({ scope: '继承范围不能摘录', start_time: '继承时间不能摘录' }), {
    version: null, state: false, query: { offset: 0, enabled: false, optional: null }, coverage: null,
    scope_kind: 0, scope_note: '', timezone: null, window_boundary: false,
    count: 0, total: 17, metadata: { scope: '嵌套范围不能上提' }, activity_samples: { sample_count: 0 },
  });
  const got = receipt(body);
  assert.deepEqual(got.scope, { version: null, state: false, query: { offset: 0, enabled: false, optional: null }, coverage: null,
    scope_kind: 0, scope_note: '', timezone: null, window_boundary: false });
  assert.equal(Object.hasOwn(got.scope, 'scope'), false);
  assert.equal(Object.hasOwn(got.scope, 'end_time'), false);
  assert.equal(Object.hasOwn(got.scope, 'activity_samples'), false);
  assert.deepEqual(got.scopeInfo.omittedFields, []);
  // 不能拿策略中的期望版本填补本次正文的空版本。
  assert.equal(got.source.expectedVersion, 'synthetic-v1');
  assert.equal(got.source.responseVersion, null);
  assert.equal(got.scope.version, null);
});

test('HTTP 409 原状态、旧请求版本与响应版本各自保留，不变成成功', () => {
  const event = eventOf({ status: 409, versionAssurance: 'missing', responseVersion: 'synthetic-v2', epoch: 2 });
  const response = { status: 409, body: { state: 'unavailable', version: 'synthetic-v2', query: null, coverage: false, total: null, message: '人工版本冲突' } };
  const got = makeSourceReceipt({ toolCallId: 'synthetic-conflict', event, response });
  assert.equal(got.status, 409);
  assert.equal(got.source.requestVersion, 'synthetic-v1');
  assert.equal(got.source.expectedVersion, 'synthetic-v1');
  assert.equal(got.source.responseVersion, 'synthetic-v2');
  assert.equal(got.source.versionAssurance, 'missing');
  assert.equal(got.source.epoch, 2);
  assert.deepEqual(got.scope, { version: 'synthetic-v2', state: 'unavailable', query: null, coverage: false });
  assert.equal(response.body.message, '人工版本冲突');
});

test('文本 HTTP 错误及其他非对象正文显式无范围；不把文本解析成另一个响应', () => {
  for (const body of ['{"version":"看似 JSON 的原始文本"}', null, false, 0, [], undefined]) {
    const got = receipt(body, { event: eventOf({ status: 503, responseVersion: null, versionAssurance: 'missing' }), response: { status: 503, body } });
    assert.equal(got.status, 503);
    assert.deepEqual(got.scope, {});
    assert.deepEqual(got.scopeInfo.omittedFields, []);
    assert.match(got.scopeInfo.note, /正文不是对象/);
  }
  assert.equal(receipt([]).scopeInfo.bodyType, 'array');
  assert.equal(receipt(null).scopeInfo.bodyType, 'null');
  assert.equal(receipt('错误').scopeInfo.bodyType, 'string');
});

test('UTF-8 总预算按整字段处理，不截字符串或 JSON，后续较小字段仍可保留', () => {
  const body = { version: '版甲', state: 'available', query: { note: '大范围'.repeat(100) }, coverage: { state: 'partial' }, timezone: 'UTC', total: 999 };
  const expected = { version: '版甲', state: 'available', coverage: { state: 'partial' }, timezone: 'UTC' };
  const got = receipt(body, { maxScopeBytes: bytes(expected) });
  assert.deepEqual(got.scope, expected);
  assert.deepEqual(got.scopeInfo.omittedFields, ['query']);
  assert.equal(got.scopeInfo.scopeBytes, bytes(expected));
  assert.equal(got.scopeInfo.maxScopeBytes, bytes(expected));
  const escaped = '中文"\\\n';
  const exact = bytes({ scope_note: escaped });
  assert.deepEqual(receipt({ scope_note: escaped }, { maxScopeBytes: exact }).scope, { scope_note: escaped });
  const below = receipt({ scope_note: escaped }, { maxScopeBytes: exact - 1 });
  assert.deepEqual(below.scope, {});
  assert.deepEqual(below.scopeInfo.omittedFields, ['scope_note']);
  assert.equal(JSON.stringify(below.scope), '{}');
});

test('最小预算完整列出省略字段；范围预算不截断完整 request', () => {
  const event = eventOf({ responseVersion: null, versionAssurance: 'missing', request: { method: 'GET', path: '/synthetic/summary', query: { version: 'synthetic-v1', filter: '完整查询'.repeat(100) } } });
  const got = receipt({ version: null, state: false, query: {}, scope: 0, end_time: 'synthetic-end' }, { event, maxScopeBytes: 2 });
  assert.deepEqual(got.scope, {});
  assert.deepEqual(got.scopeInfo.omittedFields, ['version', 'state', 'query', 'scope', 'end_time']);
  assert.deepEqual(got.request, event.request);
  assert.equal(got.scopeInfo.scopeBytes, 2);
  for (const budget of [0, -1, 1.5, Infinity, NaN]) assert.throws(() => receipt({}, { maxScopeBytes: budget }), RangeError);
});

test('输入及返回彼此无共享可变对象，不改写原始 HTTP 正文或事件', () => {
  const event = eventOf();
  const response = { status: 200, body: { version: 'synthetic-v1', query: { flags: [false, null, 0] }, coverage: { intervals: [{ start: '人工开始', end_exclusive: '人工结束' }] }, total: 0 } };
  const before = structuredClone({ event, response });
  freeze(event); freeze(response);
  const got = makeSourceReceipt({ toolCallId: 'synthetic-immutable', event, response });
  got.request.query.version = 'changed-only-copy';
  got.scope.query.flags.push(1);
  got.scope.coverage.intervals[0].start = 'changed-only-copy';
  assert.deepEqual({ event, response }, before);
  assert.equal(response.body.total, 0);
});

test('拒绝无 HTTP 的策略或网络事件，以及不能匹配状态的绑定', () => {
  for (const type of ['policy_block', 'network_failure', 'version_changed']) {
    assert.throws(() => receipt({}, { event: eventOf({ type }) }), /实际 response 事件/);
  }
  assert.throws(() => receipt({}, { response: { status: 503, body: {} } }), /状态一致/);
  assert.throws(() => receipt({}, { toolCallId: '' }), /工具调用标识/);
});

test('拒绝将另一响应的已核对版本贴过来；相同期望版本不代替正文版本', () => {
  assert.throws(() => receipt({}, { event: eventOf() }), /原始 HTTP 正文一致/);
  assert.throws(() => receipt({ version: 'synthetic-v2' }, { event: eventOf() }), /原始 HTTP 正文一致/);
  const ownScope = receipt({ resource_points: [{ version: 'synthetic-v1' }], total: 0 });
  assert.equal(ownScope.source.expectedVersion, 'synthetic-v1');
  assert.equal(ownScope.source.responseVersion, null);
  assert.equal(Object.hasOwn(ownScope.scope, 'version'), false);
});

test('不能完整序列化的范围明确失败，不静默改为空范围', () => {
  assert.throws(() => receipt({ scope_note: undefined }), /完整 JSON 值/);
  const cycle = {}; cycle.self = cycle;
  assert.throws(() => receipt({ query: cycle }), TypeError);
});

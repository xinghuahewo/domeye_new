import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequestPolicy } from './request-policy.mjs';
import { makeSourceReceipt } from './source-receipt.mjs';

// 人工合同边界，不使用真实数据库或模型。覆盖 Core → 时序 → 详情的实际调用顺序。
const core = '/api/v1/core-overview', series = '/api/v1/features/outages/country-as';
const features = '/api/v1/features/countries/series', resolve = '/api/v2/events/resolve';
const health = '/api/v1/healthz';
const spec = { paths: Object.fromEntries([core, series, features, resolve, health].map(path =>
  [path, { get: { parameters: path === health ? [] : [{ in: 'query', name: 'version' }] } }])) };
const delivered = version => ({ state: 'available', version, binding: {
  schema_version: 'completed-file-delivery/v1', source_run: 'synthetic-run', collector: 'synthetic-collector',
} });
const detail = version => ({ state: 'available', version, metadata: {
  interpretation_version: 'completed-file-results/v2', result_delivery: delivered(version),
}, item: { lifecycle: { state: 'ongoing', observed_at: '2026-02-28T09:58:12Z', data_end_exclusive: '2026-02-28T10:00:00Z' } } });
const samples = (version, feature = false) => ({ query: { country: '人工国家' }, metadata: {
  version, interpretation_version: feature ? 'country-feature-series/v1' : 'outage-series/v2',
  collector_id: 'synthetic-collector', sample_seconds: feature ? 300 : 180,
  ...(feature ? { units: { announce: 'accepted_route_element' } } : { unit: 'asn' }),
  coverage: { state: 'complete', intervals: [] },
}, data: [] });
const input = (path, version) => ({ method: 'GET', path, query: version === undefined ? {} : { version } });
function harness(respond) {
  const calls = [], events = [];
  const policy = createRequestPolicy({ spec, request: async request => {
    calls.push(structuredClone(request)); return { status: 200, body: await respond(request) };
  }, onEvidence: event => events.push(event) });
  policy.beginTurn();
  return { ...policy, calls, events };
}

test('嵌套版本和范围进入同一次 HTTP 回执，缺省的后续版本不能绕过校验', async () => {
  const h = harness(() => samples('synthetic-v1'));
  const response = await h.request(input(series));
  const event = h.events.at(-1);
  assert.equal(event.responseVersion, 'synthetic-v1');
  assert.equal(event.versionAssurance, 'confirmed');
  const receipt = makeSourceReceipt({ toolCallId: 'test-series', event, response });
  assert.deepEqual(receipt.scope.metadata, response.body.metadata);
  assert.doesNotMatch(receipt.note, /本次正文未返回整体版本/);
  await assert.rejects(h.request(input(series)), error => error.kind === 'policy');
  assert.equal(h.calls.length, 1);
  await h.request(input(series, 'synthetic-v1'));
  assert.equal(h.events.at(-1).versionAssurance, 'matched');
});

test('Core v2、两种时序和完成文件详情共享版本；独立原文不被改写', async () => {
  const h = harness(request => request.path === core ? detail('synthetic-v1') : request.path === resolve
    ? { schema_version: 'country-outage-delivery/v1', event: detail('synthetic-v1') }
    : samples('synthetic-v1', request.path === features));
  await h.request(input(core));
  assert.equal(h.snapshot().families.core.binding, 'delivery');
  await assert.rejects(h.request(input(series)), error => error.kind === 'policy');
  for (const path of [series, features, resolve]) {
    const response = await h.request(input(path, 'synthetic-v1'));
    const event = h.events.at(-1);
    assert.equal(event.versionAssurance, 'matched');
    assert.equal(event.responseVersion, 'synthetic-v1');
    if (path === resolve) {
      const receipt = makeSourceReceipt({ toolCallId: 'test-resolve', event, response });
      assert.deepEqual(receipt.scope['event.item.lifecycle'], response.body.event.item.lifecycle);
      assert.deepEqual(receipt.scope['event.metadata'], response.body.event.metadata);
    }
  }
  assert.equal(h.snapshot().families['country-publication'].binding, 'delivery');
});

test('嵌套返回版本不匹配保留原文并使共享读取失效，同源健康发现后须重取', async () => {
  const h = harness(request => request.path === health ? { result_delivery: delivered('synthetic-v2') }
    : request.path === core ? detail(request.query.version ?? 'synthetic-v1') : samples('synthetic-v2'));
  await h.request(input(core));
  const response = await h.request(input(series, 'synthetic-v1'));
  assert.equal(response.body.metadata.version, 'synthetic-v2');
  assert.equal(h.events.at(-1).versionAssurance, 'mismatch');
  await assert.rejects(h.request(input(core, 'synthetic-v1')), error => error.kind === 'policy');
  await h.request(input(health));
  assert.equal(h.snapshot().families.delivery.version, 'synthetic-v2');
  assert.equal(h.snapshot().restartRequired, true);
  await h.request(input(core, 'synthetic-v2'));
  await h.request(input(series, 'synthetic-v2'));
  assert.equal(h.snapshot().restartRequired, false);
});

test('完成文件解析先读也能建立共享绑定；历史发布和未知元数据不冒充交付', async () => {
  const h = harness(request => request.path === resolve
    ? { schema_version: 'country-outage-delivery/v1', event: detail('synthetic-v1') }
    : samples('synthetic-v1'));
  await h.request(input(resolve));
  await assert.rejects(h.request(input(series)), error => error.kind === 'policy');
  await h.request(input(series, 'synthetic-v1'));
  assert.equal(h.events.at(-1).versionAssurance, 'matched');

  const unknown = harness(() => ({ metadata: { version: 'synthetic-v1', interpretation_version: 'unknown/v1' }, data: [] }));
  await unknown.request(input(series));
  assert.equal(unknown.events.at(-1).versionAssurance, 'missing');
  assert.equal(unknown.events.at(-1).responseVersion, null);
  const historical = harness(() => ({ schema_version: 'historical/v1', event: { version: 'synthetic-v1' }, publication_id: 'synthetic-p1' }));
  await historical.request(input(resolve));
  assert.equal(historical.events.at(-1).responseVersion, null);
  assert.notEqual(historical.snapshot().families['country-publication'].binding, 'delivery');
});

test('同名版本不能掩盖另一个来源或 collector；冲突正文保留而不更新已有绑定', async () => {
  for (const changed of ['source_run', 'collector']) {
    const h = harness(request => {
      if (request.path === core) return detail('synthetic-v1');
      if (changed === 'collector') {
        const body = samples('synthetic-v1'); body.metadata.collector_id = 'other-collector'; return body;
      }
      const event = detail('synthetic-v1'); event.metadata.result_delivery.binding.source_run = 'other-run';
      return { schema_version: 'country-outage-delivery/v1', event };
    });
    await h.request(input(core));
    const oldIdentity = h.snapshot().families.core.deliveryIdentity;
    await h.request(input(changed === 'collector' ? series : resolve, 'synthetic-v1'));
    assert.equal(h.events.at(-1).versionAssurance, 'mismatch');
    assert.equal(h.snapshot().families.core.deliveryIdentity, oldIdentity);
    assert.equal(h.snapshot().families.delivery.conflict, true);
  }
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { loadSearchSpec } from '../spec.mjs';
import { createRequest, runCode, ToolFailure } from './executor.mjs';
import { createRequestPolicy } from './request-policy.mjs';

// 全部数字、版本、响应都是人工边界样本，不调用模型、业务库或共享服务。
const spec = await loadSearchSpec();
const paths = { discover: '/api/v1/data-availability', statistics: '/api/v1/events/statistics', feature: '/api/v1/features/summary', resource: '/api/v1/resources', core: '/api/v1/core-overview', detail: '/api/v1/core-overview/record', legacy: '/api/v1/events', legacyTop: '/api/v1/events/top', health: '/api/v1/healthz' };
const input = (name, query = {}) => ({ method: 'GET', path: paths[name], query });
const available = (version = 'synthetic-v1', extra = {}) => ({ status: 200, body: { state: 'available', version, ...extra } });
const failed = status => ({ status, body: { state: 'unavailable', message: `人工 HTTP ${status}；不是零或无数据`, count: null } });
function harness(respond) {
  const calls = [], events = [];
  const policy = createRequestPolicy({ spec, request: async (request, options) => {
    calls.push(structuredClone(request));
    return respond(request, calls.length, options);
  }, onEvidence: value => events.push(value) });
  policy.beginTurn();
  return { ...policy, calls, events };
}
const blocked = (promise, reason) => assert.rejects(promise, error => error.kind === 'policy' && (!reason || error.message.includes(reason)));

test('正常发现、显式同版跨入口与兼容入口均能读取；不静默注入版本', async () => {
  const h = harness(request => request.path === paths.legacy ? { status: 200, body: { total: 9 } } : available());
  await h.request(input('discover'));
  await blocked(h.request(input('statistics')), '显式');
  await blocked(h.request(input('feature', { version: 'synthetic-other' })), '显式');
  await h.request(input('statistics', { version: 'synthetic-v1', bucket: 'hour' }));
  await h.request(input('feature', { version: 'synthetic-v1', scope: 'asn', subject: '48715' }));
  await h.request(input('resource', { version: 'synthetic-v1' }));
  assert.deepEqual(await h.request(input('legacy')), { status: 200, body: { total: 9 } });
  assert.equal(h.calls[0].query.version, undefined);
  assert.equal(h.snapshot().families.delivery.version, 'synthetic-v1');
  assert.equal(h.events.at(-1).versionAssurance, 'unverified');
  assert.equal(h.calls.length, 5);
});

test('409 原文保留；删版本、换兼容入口、未经发现使用新版本均不能发出 HTTP', async () => {
  const raw = failed(409);
  const h = harness(request => request.path === paths.discover ? available() : raw);
  await h.request(input('discover'));
  assert.equal(await h.request(input('statistics', { version: 'synthetic-v1' })), raw);
  await blocked(h.request(input('statistics')));
  await blocked(h.request(input('statistics', { version: 'synthetic-v2' })));
  await blocked(h.request(input('legacy')));
  assert.equal(h.calls.length, 2);
  assert.deepEqual(h.snapshot().unresolvedFailures, ['delivery']);
  h.beginTurn();
  await blocked(h.request(input('statistics', { version: 'synthetic-v1' })));
  assert.equal(h.snapshot().families.delivery.version, 'synthetic-v1');
  assert.equal(h.snapshot().families.delivery.conflict, true);
});

test('成功刷新进入新版本；此前已取证的完整查询须重取，失败刷新不改变确认版本', async () => {
  let discovery = 0, conflict = true;
  const h = harness(request => {
    if (request.path === paths.discover) return available(++discovery === 1 ? 'synthetic-v1' : 'synthetic-v2');
    if (request.path === paths.feature && conflict) { conflict = false; return failed(409); }
    return available(request.query.version, { total: 6 });
  });
  await h.request(input('discover'));
  await h.request(input('statistics', { version: 'synthetic-v1', bucket: 'day' }));
  await h.request(input('feature', { version: 'synthetic-v1' }));
  assert.deepEqual(await h.request(input('discover')), available('synthetic-v2'));
  assert.equal(h.snapshot().restartRequired, true);
  assert.equal(h.snapshot().invalidatedEvidence.length, 1);
  await blocked(h.request(input('statistics', { version: 'synthetic-v1', bucket: 'day' })));
  await h.request(input('statistics', { version: 'synthetic-v2', bucket: 'hour' }));
  assert.equal(h.snapshot().restartRequired, true, '换分桶不等于已重取原查询');
  await h.request(input('statistics', { bucket: 'day', version: 'synthetic-v2' }));
  assert.equal(h.snapshot().restartRequired, false);
  await h.request(input('feature', { version: 'synthetic-v2' }));
  assert.equal(h.events.find(event => event.type === 'version_changed').wholeQuestionMustBeRequeried, true);

  const f = harness((request, number) => number === 1 ? available() : failed(409));
  await f.request(input('discover'));
  await f.request(input('statistics', { version: 'synthetic-v1' }));
  assert.deepEqual(await f.request(input('discover')), failed(409));
  await blocked(f.request(input('discover')), '已尝试');
  await blocked(f.request(input('statistics')));
  assert.equal(f.snapshot().families.delivery.version, 'synthetic-v1');
  assert.equal(f.calls.length, 3);
});

test('Core 不冒充共享交付版本，冲突后不能把无版本列表当发现；其他族继续', async () => {
  const h = harness((request, number) => request.path === paths.core ? available('synthetic-core') : request.path === paths.detail ? failed(409) : available());
  await h.request(input('core', { date: '2026-02-24' }));
  await h.request(input('detail', { ref: 'synthetic-ref', version: 'synthetic-core' }));
  await blocked(h.request(input('core', { date: '2026-02-24' })), '没有独立');
  await h.request(input('discover'));
  await h.request(input('statistics', { version: 'synthetic-v1' }));
  assert.equal(h.snapshot().families.core.version, 'synthetic-core');
  assert.equal(h.snapshot().families.core.conflict, true);
  assert.equal(h.snapshot().families.delivery.conflict, false);
});

test('503 只准原样重试一次；不同参数、同族换入口及无版回退被限，独立族仍可查', async () => {
  const h = harness(request => request.path === paths.discover ? available() : request.path === paths.core ? available('synthetic-core') : request.path === paths.health ? { status: 200, body: { ok: true } } : failed(503));
  await h.request(input('discover'));
  const query = input('statistics', { version: 'synthetic-v1', bucket: 'day' });
  const first = await h.request(query);
  assert.deepEqual(first, failed(503));
  await blocked(h.request(input('statistics', { version: 'synthetic-v1', bucket: 'hour' })));
  await blocked(h.request(input('feature', { version: 'synthetic-v1' })));
  await blocked(h.request(input('legacy')));
  assert.deepEqual(await h.request(input('statistics', { bucket: 'day', version: 'synthetic-v1' })), first);
  await blocked(h.request(query));
  await blocked(h.request(input('core', { date: '2026-02-24' })), '独立');
  await h.request(input('health'));
  assert.equal(h.calls.filter(request => request.path === paths.statistics).length, 2);
  assert.equal(h.snapshot().families.delivery.failure.attempts, 2);
});

test('交付冲突后未知 Core 不可穿过；已证实独立留存、独立 RIB 与国家发布仍可读', async () => {
  const retained = available('synthetic-core', { metadata: { interpretation_version: 'recorded-anomaly-overview/v3', source: { instance: 'synthetic-retained', collector_id: 'rrc25' } } });
  const h = harness(request => request.path === paths.core ? retained : request.path === paths.discover ? available() : request.path === paths.statistics ? failed(409) : { status: 200, body: { state: 'not_configured' } });
  await h.request(input('discover'));
  await h.request(input('statistics', { version: 'synthetic-v1' }));
  await blocked(h.request(input('core', { date: '2026-02-24' })), '独立');
  await h.request({ method: 'GET', path: '/api/v1/rib-snapshots', query: {} });
  await h.request({ method: 'GET', path: '/api/v2/events/resolve', query: { ref: 'synthetic-country-ref' } });
  assert.equal(h.calls.some(request => request.path === paths.core), false);

  const independent = harness(request => request.path === paths.core ? retained : request.path === paths.discover ? available() : failed(409));
  await independent.request(input('core', { date: '2026-02-24' }));
  await independent.request(input('discover'));
  await independent.request(input('statistics', { version: 'synthetic-v1' }));
  await independent.request(input('core', { date: '2026-02-24', version: 'synthetic-core' }));
  assert.equal(independent.snapshot().families.core.binding, 'retained');
  assert.equal(independent.snapshot().families.delivery.conflict, true);
});

test('同值字符串不证明 Core 独立；明确交付绑定将故障关联，原请求恢复后可继续', async () => {
  const unknown = harness(request => request.path === paths.statistics ? failed(409) : available());
  await unknown.request(input('core'));
  await unknown.request(input('discover'));
  await unknown.request(input('statistics', { version: 'synthetic-v1' }));
  await blocked(unknown.request(input('core', { version: 'synthetic-v1' })), '独立');
  assert.equal(unknown.snapshot().families.core.binding, 'unknown');

  let coreReads = 0;
  const linked = harness(request => request.path === paths.core && ++coreReads === 2 ? failed(503) : available('synthetic-v1', { metadata: { interpretation_version: 'completed-file-results/v1', result_delivery: { binding: { source_run: 'synthetic-run', collector: 'rrc25' } } } }));
  await linked.request(input('core'));
  await linked.request(input('discover'));
  const query = input('core', { version: 'synthetic-v1' });
  await linked.request(query);
  await blocked(linked.request(input('statistics', { version: 'synthetic-v1' })));
  await linked.request(query);
  await linked.request(input('statistics', { version: 'synthetic-v1' }));
  assert.deepEqual(linked.snapshot().unresolvedFailures, []);
});

const delivered = (version, extra = {}) => ({ state: 'available', version, binding: {
  source_run: 'synthetic-run', collector: 'rrc25', schema_version: 'completed-file-delivery/v1', ...extra,
} });
const deliveredCore = version => available(version, { metadata: {
  interpretation_version: 'completed-file-results/v1', result_delivery: delivered(version),
} });

test('已确认同一交付来源的 Core 与汇总共享版本，变化后原查询全部重取', async () => {
  let version = 'synthetic-v1';
  const h = harness(request => request.query.version && request.query.version !== version ? failed(409)
    : request.path === paths.core ? deliveredCore(version) : available(version));
  const coreQuery = { date: '2000-01-01' };
  await h.request(input('core', coreQuery));
  version = 'synthetic-v2';
  await blocked(h.request(input('feature', { scope: 'collector' })), '显式');
  assert.equal(h.calls.length, 1, '确认来源后，不发出无版本的第二入口请求');
  assert.equal(h.snapshot().families.delivery.version, 'synthetic-v1');
  assert.deepEqual(await h.request(input('feature', { scope: 'collector', version: 'synthetic-v1' })), failed(409));
  await h.request(input('discover'));
  assert.equal(h.snapshot().families.core.version, version);
  assert.deepEqual(h.snapshot().unresolvedFailures, []);
  assert.equal(h.snapshot().restartRequired, true);
  await blocked(h.request(input('core', { ...coreQuery, version: 'synthetic-v1' })), '显式');
  await h.request(input('core', { ...coreQuery, version }));
  await h.request(input('feature', { scope: 'collector', version }));
  assert.equal(h.snapshot().restartRequired, false);
});

test('首次识别 Core 交付来源时，不能接受与已读汇总不同的版本', async () => {
  let version = 'synthetic-v1';
  const h = harness(request => request.path === paths.health
    ? { status: 200, body: { result_delivery: delivered(version) } }
    : request.path === paths.core ? deliveredCore(version) : available(version));
  const featureQuery = { scope: 'collector' };
  await h.request(input('feature', featureQuery));
  version = 'synthetic-v2';
  assert.deepEqual(await h.request(input('core')), deliveredCore(version), '冲突正文原样保留');
  assert.equal(h.events.at(-1).versionAssurance, 'mismatch');
  assert.equal(h.events.at(-1).expectedVersion, 'synthetic-v1');
  assert.deepEqual(h.snapshot().unresolvedFailures.sort(), ['core', 'delivery']);
  await h.request(input('health'));
  assert.equal(h.snapshot().families.core.version, version);
  assert.equal(h.snapshot().families.delivery.version, version);
  assert.equal(h.snapshot().restartRequired, true);
  await h.request(input('feature', { ...featureQuery, version }));
  assert.equal(h.snapshot().restartRequired, false);
});

test('交付目录恢复同时解除已绑定 Core 冲突，不遗留无法重取的旧版本', async () => {
  let version = 'synthetic-v1';
  const h = harness(request => request.query.version && request.query.version !== version ? failed(409)
    : request.path === paths.core ? deliveredCore(version) : available(version));
  await h.request(input('core'));
  await h.request(input('statistics', { version, bucket: 'hour' }));
  version = 'synthetic-v2';
  await h.request(input('core', { version: 'synthetic-v1' }));
  await h.request(input('discover'));
  assert.deepEqual(h.snapshot().unresolvedFailures, []);
  assert.equal(h.snapshot().families.core.version, version);
  assert.equal(h.snapshot().pendingReplay.length, 2);
  await h.request(input('core', { version }));
  await h.request(input('statistics', { version, bucket: 'hour' }));
  assert.equal(h.snapshot().restartRequired, false);
});

test('同一交付的两个发现入口共享一次恢复额度，不能换入口再次恢复', async () => {
  for (const firstDiscovery of ['health', 'discover']) {
    let version = 'synthetic-v1', failDiscovery = true;
    const h = harness(request => {
      if (request.path === paths.health) return { status: 200, body: { result_delivery: failDiscovery ? undefined : delivered(version) } };
      if (request.path === paths.discover) return failDiscovery ? failed(503) : available(version);
      return request.query.version && request.query.version !== version ? failed(409) : deliveredCore(version);
    });
    await h.request(input('core'));
    version = 'synthetic-v2';
    await h.request(input('core', { version: 'synthetic-v1' }));
    await h.request(input(firstDiscovery));
    failDiscovery = false;
    if (firstDiscovery === 'health') await blocked(h.request(input('discover')), '已尝试');
    else await h.request(input('health'));
    assert.equal(h.snapshot().families.core.conflict, true);
    assert.equal(h.snapshot().families.delivery.conflict, true);
    assert.equal(h.snapshot().families.core.version, 'synthetic-v1');
    assert.equal(h.snapshot().families.core.refreshUsed, true);
    assert.equal(h.snapshot().families.delivery.refreshUsed, true);
  }
});

test('已确认交付 Core 冲突后可从健康入口同一来源重新发现；旧版受限且整题重取', async () => {
  let version = 'synthetic-v1';
  const h = harness(request => request.path === paths.health
    ? { status: 200, body: { status: 'ok', result_delivery: delivered(version) } }
    : request.query.version && request.query.version !== version ? failed(409) : deliveredCore(version));
  await h.request(input('core', { date: '2026-02-27' }));
  await h.request(input('core', { date: '2026-02-28', version }));
  version = 'synthetic-v2';
  const query = input('core', { date: '2026-02-27', version: 'synthetic-v1' });
  assert.deepEqual(await h.request(query), failed(409));
  await blocked(h.request(input('core', { date: '2026-02-27' })));
  await blocked(h.request(input('core', { date: '2026-02-27', version })));
  const raw = await h.request(input('health'));
  assert.equal(raw.body.result_delivery.version, version);
  assert.equal(raw.body.version, undefined, '不把嵌套发现版本改写为健康正文顶层版本');
  assert.equal(h.snapshot().families.core.version, version);
  assert.equal(h.snapshot().families.delivery.version, version);
  assert.deepEqual(h.snapshot().unresolvedFailures, []);
  assert.equal(h.snapshot().pendingReplay.length, 2);
  await blocked(h.request(query), '显式');
  await h.request(input('core', { date: '2026-02-27', version }));
  assert.equal(h.snapshot().restartRequired, true);
  await h.request(input('core', { date: '2026-02-28', version }));
  assert.equal(h.snapshot().restartRequired, false);
  version = 'synthetic-v3';
  await h.request(input('core', { date: '2026-02-27', version: 'synthetic-v2' }));
  await h.request(input('health'));
  assert.equal(h.snapshot().families.core.version, 'synthetic-v2', '同轮第二次健康读取不再恢复');
  await blocked(h.request(input('core', { date: '2026-02-27', version })));
  h.beginTurn();
  await h.request(input('health'));
  await h.request(input('core', { date: '2026-02-27', version }));
});

test('健康读取缺少或更换交付绑定、发现不可用均不解除 Core 冲突', async () => {
  for (const discovery of [undefined, delivered('synthetic-v2', { source_run: 'another-run' }),
    delivered('synthetic-v2', { collector: 'rrc00' }), delivered('synthetic-v2', { schema_version: 'other' }),
    { ...delivered('synthetic-v2'), state: 'unavailable' }]) {
    let first = true;
    const h = harness(request => request.path === paths.health
      ? { status: 200, body: { status: 'ok', result_delivery: discovery } }
      : first ? (first = false, deliveredCore('synthetic-v1')) : failed(409));
    await h.request(input('core'));
    await h.request(input('core', { version: 'synthetic-v1' }));
    await h.request(input('health'));
    assert.equal(h.snapshot().families.core.version, 'synthetic-v1');
    assert.equal(h.snapshot().families.core.conflict, true);
    assert.equal(h.snapshot().families.core.refreshUsed, true);
    await blocked(h.request(input('core', { version: 'synthetic-v2' })));
  }
});

test('不匹配正文不能改写已确认来源，再用同一新来源的健康响应恢复', async () => {
  for (const changedBinding of [{ source_run: 'another-run' }, { collector: 'rrc00' }]) {
    let first = true;
    const changedDelivery = delivered('synthetic-v2', changedBinding);
    const mismatch = available('synthetic-v2', { metadata: {
      interpretation_version: 'completed-file-results/v1', result_delivery: changedDelivery,
    } });
    const h = harness(request => request.path === paths.health
      ? { status: 200, body: { status: 'ok', result_delivery: changedDelivery } }
      : first ? (first = false, deliveredCore('synthetic-v1')) : mismatch);
    await h.request(input('core'));
    assert.deepEqual(await h.request(input('core', { version: 'synthetic-v1' })), mismatch);
    await h.request(input('health'));
    assert.equal(h.snapshot().families.core.version, 'synthetic-v1');
    assert.equal(h.snapshot().families.core.conflict, true);
    await blocked(h.request(input('core', { version: 'synthetic-v2' })));
    assert.equal(h.calls.length, 3, '来源变化不能获准发出新版本业务请求');
  }
});

test('相同版本不能代替交付来源证据；健康入口不恢复未知或独立 Core', async () => {
  for (const metadata of [undefined, { interpretation_version: 'recorded-anomaly-overview/v3',
    source: { instance: 'synthetic-retained', collector_id: 'rrc25' } }]) {
    let first = true;
    const h = harness(request => request.path === paths.health
      ? { status: 200, body: { result_delivery: delivered('synthetic-v2') } }
      : first ? (first = false, available('synthetic-v1', { metadata })) : failed(409));
    await h.request(input('core'));
    await h.request(input('core', { version: 'synthetic-v1' }));
    await h.request(input('health'));
    assert.equal(h.snapshot().families.core.conflict, true);
    assert.equal(h.snapshot().families.core.refreshUsed, false);
    await blocked(h.request(input('core', { version: 'synthetic-v2' })));
  }
});

test('503 同请求重试恢复后合法同版读取继续；下一用户 turn 重置重试而保留版本', async () => {
  let failCount = 0;
  const h = harness(request => request.path === paths.statistics && ++failCount === 1 ? failed(503) : available());
  await h.request(input('discover'));
  const query = input('statistics', { version: 'synthetic-v1' });
  await h.request(query); await h.request(query);
  await h.request(input('feature', { version: 'synthetic-v1' }));
  assert.deepEqual(h.snapshot().unresolvedFailures, []);
  const f = harness(request => request.path === paths.discover ? available() : failed(503));
  await f.request(input('discover'));
  await f.request(query); await f.request(query);
  await blocked(f.request(query));
  f.beginTurn();
  await blocked(f.request(input('statistics')), '显式');
  await f.request(query); await f.request(query);
  assert.equal(f.calls.filter(request => request.path === paths.statistics).length, 4);
  assert.equal(f.snapshot().families.delivery.version, 'synthetic-v1');
});

test('网络错误只保留原错误并限制重试；取消不被误判成服务失败', async () => {
  const error = new ToolFailure('network', '人工网络中断：未取得 HTTP 响应');
  const h = harness(() => { throw error; });
  await assert.rejects(h.request(input('legacy')), value => value === error);
  await blocked(h.request(input('legacyTop')));
  await assert.rejects(h.request(input('legacy')), value => value === error);
  await blocked(h.request(input('legacy')));
  assert.equal(h.calls.length, 2);
  assert.equal(h.events.some(event => event.type === 'response'), false);
  const controller = new AbortController(); controller.abort();
  const c = harness(() => available());
  await assert.rejects(c.request(input('discover'), { signal: controller.signal }), value => value.kind === 'code');
  assert.equal(c.calls.length, 0);
  assert.deepEqual(c.snapshot().unresolvedFailures, []);
});

test('400、404、观察到的零、no_data/null 和缺失字段不改写，也不全部封锁', async () => {
  const responses = [{ status: 400, body: '人工参数错误' }, { status: 404, body: null },
    available('synthetic-v1', { total: 0, series: [] }),
    available('synthetic-v1', { state: 'no_data', total: null, coverage: { state: 'none' } }),
    { status: 200, body: { state: 'not_configured', version: null } },
    { status: 200, body: { error: '合法业务字段', total: 4 } }];
  const h = harness(() => responses.shift());
  for (let i = 0; i < 6; i++) {
    const expected = responses[0];
    assert.equal(await h.request(input('statistics', i >= 3 ? { version: 'synthetic-v1' } : {})), expected);
  }
  assert.deepEqual(h.snapshot().unresolvedFailures, []);
  assert.equal(h.snapshot().families.delivery.version, 'synthetic-v1');
  assert.equal(h.events.at(-1).versionAssurance, 'missing');
});

test('HTTP 200 正文版本不匹配时原文仍返回，但不确认为同版且后续受限', async () => {
  const wrong = available('synthetic-v2');
  const h = harness((request, number) => number === 1 ? available() : wrong);
  await h.request(input('discover'));
  assert.equal(await h.request(input('statistics', { version: 'synthetic-v1' })), wrong);
  assert.equal(h.events.at(-1).versionAssurance, 'mismatch');
  assert.equal(h.snapshot().families.delivery.version, 'synthetic-v1');
  await blocked(h.request(input('feature', { version: 'synthetic-v1' })));
});

test('并行请求不能在首个失败返回前整批穿过边界；在途不可切 turn', async () => {
  let release, started;
  const ready = new Promise(resolve => { started = resolve; });
  const h = harness(async () => { started(); await new Promise(resolve => { release = resolve; }); return failed(503); });
  const first = h.request(input('legacy'));
  const others = Promise.allSettled([h.request(input('legacyTop')), h.request(input('legacyTop'))]);
  await ready;
  assert.throws(() => h.beginTurn(), error => error.kind === 'policy');
  release();
  await first;
  assert.ok((await others).every(result => result.status === 'rejected' && result.reason.kind === 'policy'));
  assert.equal(h.calls.length, 1);
  h.beginTurn();
});

test('QuickJS 与真实 loopback HTTP 接通时仍保留人工 HTTP 正文，第三次请求不发出', async () => {
  let hits = 0;
  const server = createServer((req, res) => {
    hits++; res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify(failed(503).body));
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = createRequest({ baseUrl: `http://127.0.0.1:${server.address().port}`, paths: [paths.legacy] });
  const policy = createRequestPolicy({ spec, request: base }); policy.beginTurn();
  try {
    const code = `async () => await domeye.request(${JSON.stringify(input('legacy'))})`;
    assert.deepEqual(await runCode({ code, request: policy.request }), failed(503));
    assert.deepEqual(await runCode({ code, request: policy.request }), failed(503));
    await assert.rejects(runCode({ code, request: policy.request }), error => error.kind === 'policy');
    assert.equal(hits, 2);
  } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});

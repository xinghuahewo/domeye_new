import { ToolFailure } from './executor.mjs';
import { createPathMatcher } from './paths.mjs';

// 93f8d58 的 load_delivery 共享版本；Core 的旧留存模式另有版本，不能凭同名字段合并。
const DELIVERY = new Set(['/api/v1/data-availability', '/api/v1/events/statistics', '/api/v1/features/summary', '/api/v1/resources']);
const CORE = new Set(['/api/v1/core-overview', '/api/v1/core-overview/record']);
// Core 没有独立发现入口：冲突后不能把删 version 的列表读取冒称为发现。
const DISCOVERY = new Set(['/api/v1/data-availability']);
const keyOf = (input, withoutVersion = false) => JSON.stringify([input.method, input.path,
  Object.entries(input.query ?? {}).filter(([key]) => !withoutVersion || key !== 'version').map(([key, value]) => [key, String(value)]).sort(([a], [b]) => a.localeCompare(b))]);
const hasVersion = value => typeof value === 'string' && value.trim().length > 0;
const successful = response => response.status >= 200 && response.status < 300;
const RETAINED_INTERPRETATIONS = new Set(['recorded-anomaly-overview/v1', 'recorded-anomaly-overview/v2', 'recorded-anomaly-overview/v3']);
function coreBinding(body) {
  const meta = body?.metadata;
  const binding = meta?.result_delivery?.binding;
  if (meta?.interpretation_version === 'completed-file-results/v1' && hasVersion(binding?.source_run) && hasVersion(binding?.collector)) return 'delivery';
  if (RETAINED_INTERPRETATIONS.has(meta?.interpretation_version) && hasVersion(meta.source?.instance) && hasVersion(meta.source?.collector_id) && !Object.hasOwn(meta, 'result_delivery')) return 'retained';
  return 'unknown';
}

/**
 * 仅包装宿主请求，不修改实际 HTTP 的 {status,body}，也不代模型添加 version。
 * spec 使用 loadSearchSpec() 已展开的规范；每个会话创建一次，每个用户 turn 调 beginTurn()。
 * snapshot 是执行证据，不是答案正确性或业务验收结论。
 */
export function createRequestPolicy({ spec, request, onEvidence = () => {} }) {
  if (typeof request !== 'function') throw new TypeError('须提供实际宿主 request。');
  const routes = new Map(Object.entries(spec.paths).filter(([, item]) => item.get).map(([path, item]) => {
    const versioned = [...(item.parameters ?? []), ...(item.get.parameters ?? [])].some(parameter => parameter.in === 'query' && parameter.name === 'version');
    const family = DELIVERY.has(path) ? 'delivery' : CORE.has(path) ? 'core' : path === '/api/v1/healthz' ? 'health' : path === '/api/v1/rib-snapshots' ? 'rib-discovery' : path === '/api/v2/events/resolve' ? 'country-publication' : versioned ? path : 'compatibility';
    return [path, { family, versioned, discovery: DISCOVERY.has(path) }];
  }));
  const matchPath = createPathMatcher([...routes.keys()]);
  const families = new Map();
  let turn = 0, pending = 0, tail = Promise.resolve();
  let sequence = 0, evidence = [], successes = new Map(), replay = new Map(), invalidated = [];
  const familyState = name => {
    if (!families.has(name)) families.set(name, { version: null, epoch: 0, conflict: false, failure: null, refreshUsed: false, binding: name === 'core' ? 'unknown' : name });
    return families.get(name);
  };
  const emit = (value, observe) => { evidence.push(value); onEvidence(structuredClone(value)); observe?.(structuredClone(value)); };
  const block = (input, reason, message, observe) => {
    emit({ type: 'policy_block', turn, request: input, reason, httpRequested: false }, observe);
    throw new ToolFailure('policy', `${message} 本次未发送 HTTP 请求；此前收到的 HTTP 状态和正文仍有效。`);
  };
  const beginTurn = () => {
    if (pending) throw new ToolFailure('policy', '上轮仍有在途请求，不能重置请求状态。');
    turn++;
    evidence = []; successes = new Map(); replay = new Map(); invalidated = [];
    for (const state of families.values()) {
      state.failure = null; state.refreshUsed = false;
      // 已观察到的版本冲突跨 turn 保留，不能靠新提问复活失效版本。
    }
    return snapshot();
  };
  const snapshot = () => structuredClone({ turn,
    families: Object.fromEntries(families), evidence,
    restartRequired: replay.size > 0,
    pendingReplay: [...replay.values()], invalidatedEvidence: invalidated,
    unresolvedFailures: [...families].filter(([, state]) => state.failure || state.conflict).map(([family]) => family),
    unverifiedResponses: evidence.filter(item => item.type === 'response' && !['matched', 'confirmed'].includes(item.versionAssurance)).map(item => item.id),
  });
  async function perform(input, options, queuedTurn) {
    // 观察器属于本次工具调用；并行工具不能借用别人的事件或响应。
    const report = value => emit(value, options.onPolicyEvent);
    const deny = (input, reason, message) => block(input, reason, message, options.onPolicyEvent);
    if (options.signal?.aborted) throw new ToolFailure('code', '本次请求已停止。');
    if (queuedTurn !== turn) return deny(input, 'stale_turn', '请求所属用户轮次已结束。');
    const route = routes.get(matchPath(input?.path));
    // 结构与完整路径仍由底层 createRequest 校验，策略不扩大它的权限。
    if (!route || input.method !== 'GET') return request(input, options);
    const state = familyState(route.family), version = input.query?.version;
    const discovery = route.discovery && version === undefined;
    const key = keyOf(input), semanticKey = keyOf(input, true);
    const activeFailure = [...families.values()].some(value => value.failure || value.conflict);
    const delivery = families.get('delivery');
    if (route.family === 'core' && state.binding !== 'retained' && (delivery?.conflict || (delivery?.failure && delivery.failure.key !== key))) {
      return deny(input, 'unconfirmed_binding_fallback', '完成交付读取失败或版本冲突；Core 尚未证实为独立留存绑定，不能以另一个入口替代。请先恢复完成交付读取或重新发现版本。');
    }
    if (route.family === 'compatibility' && activeFailure && state.failure?.key !== key) {
      return deny(input, 'unverified_fallback', '已有读取失败或版本冲突，不能改用无版本兼容入口充当同版替代结果。');
    }
    if (state.conflict && !discovery) return deny(input, 'version_conflict', route.family === 'core'
      ? 'Core 版本已冲突；规范没有独立的 Core 发现入口，本会话不能通过删 version 自动恢复，其他结果族仍可查询。'
      : '当前结果族版本已冲突；须先从该族发现入口取得当前版本，再整题重取。');
    if (route.versioned && state.version && !discovery && version !== state.version) {
      return deny(input, 'version_required', `该结果族已确认版本 ${state.version}；后续结果读取须显式携带此版本。`);
    }
    if (discovery && (state.conflict || state.failure)) {
      if (state.refreshUsed) return deny(input, 'refresh_exhausted', '本轮故障后的版本发现已尝试；请保留失败依据，停止重复发现。');
      state.refreshUsed = true;
    } else if (state.failure) {
      if (state.failure.key !== key || state.failure.attempts >= 2) return deny(input, 'retry_exhausted', '本轮该结果族读取失败；只允许将失败请求原样重试一次，不能换参数或入口绕过。');
      state.failure.attempts++;
    }
    let response;
    try { response = await request(input, options); }
    catch (error) {
      if (error.kind === 'network' && !options.signal?.aborted) {
        state.failure ??= { key, request: input, attempts: 1, kind: 'network' };
        if (route.family === 'core' && state.binding === 'delivery') familyState('delivery').failure ??= state.failure;
        report({ type: 'network_failure', turn, family: route.family, request: input, message: error.message });
      }
      throw error;
    }
    const id = ++sequence;
    let assurance = route.versioned ? 'missing' : 'unverified';
    if (response.status === 409 && route.versioned) {
      state.conflict = true;
      if (route.family === 'core' && state.binding === 'delivery') familyState('delivery').conflict = true;
    } else if (response.status === 503) {
      state.failure ??= { key, request: input, attempts: 1, kind: 'http_503' };
      if (route.family === 'core' && state.binding === 'delivery') familyState('delivery').failure ??= state.failure;
    } else if (successful(response)) {
      if (route.family === 'core') {
        const binding = coreBinding(response.body);
        if (binding !== 'unknown') state.binding = binding;
      }
      const returned = response.body?.version;
      if (route.versioned && hasVersion(returned) && response.body?.state !== 'unavailable') {
        if (version !== undefined && returned !== version) {
          assurance = 'mismatch'; state.conflict = true;
        } else {
          assurance = version === returned ? 'matched' : 'confirmed';
          const changed = state.version !== null && state.version !== returned;
          if (changed || state.conflict) {
            state.epoch++;
            for (const [priorKey, prior] of successes) {
              replay.set(priorKey, { family: prior.family, request: prior.request });
              if (!invalidated.includes(prior.id)) invalidated.push(prior.id);
            }
            report({ type: 'version_changed', turn, family: route.family, previousVersion: state.version, version: returned, epoch: state.epoch, invalidatedEvidence: [...invalidated], wholeQuestionMustBeRequeried: true });
            state.failure = null;
          }
          state.version = returned; state.conflict = false;
          if (!discovery) {
            successes.set(semanticKey, { id, family: route.family, request: input });
            replay.delete(semanticKey);
          }
        }
      }
      for (const sibling of families.values()) if (sibling.failure?.key === key) sibling.failure = null;
    }
    const receipt = { type: 'response', id, turn, family: route.family, request: input, status: response.status,
      versionAssurance: assurance, expectedVersion: state.version, responseVersion: response.body?.version ?? null, epoch: state.epoch, binding: state.binding };
    report(receipt);
    options.onResponse?.(structuredClone(receipt), structuredClone(response));
    return response;
  }
  function controlled(input, options = {}) {
    // 串行检查避免 Promise.all 在首个失败到达前先发出整批旁路请求。
    const captured = structuredClone(input), queuedTurn = turn;
    pending++;
    const result = tail.then(() => perform(captured, options, queuedTurn));
    tail = result.catch(() => {});
    return result.finally(() => { pending--; });
  }
  return { request: controlled, beginTurn, snapshot };
}

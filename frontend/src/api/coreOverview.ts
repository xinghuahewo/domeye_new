import { apiGet } from './client'
import type { components, operations } from '@/types/openapi.generated'
import { toBusinessTime } from '@/utils/businessTime'

export type CoreOverview = components['schemas']['CoreOverviewPayload']
export type CoreOverviewItem = components['schemas']['CoreOverviewItem']
export type CoreOverviewDetail = components['schemas']['CoreOverviewDetail']
export type CoreOverviewQuery = NonNullable<operations['getCoreOverview']['parameters']['query']>

function validateLevelEvidence(item: CoreOverviewItem | undefined) {
  if (item?.level_conflict && item.level !== null) throw new Error('等级冲突不能同时提供确定等级')
  const identity = item?.object_identity
  if (identity || (item?.kind === 'as_outage' && item.object?.startsWith('{'))) {
    if (!item || !identity || identity.state !== 'unresolved' || identity.reason !== 'as_set_in_asn_field'
      || identity.label !== '对象待核实' || item.kind !== 'as_outage' || typeof item.object !== 'string'
      || item.object.length > 4096 || !/^\{[0-9]{1,10}(?:, ?[0-9]{1,10})*\}$/.test(item.object)
      || !Array.isArray(item.asns) || item.asns.length) throw new Error('对象待核实不能解释为单一ASN')
  }
}

function validateScale(payload: CoreOverview) {
  const scale = payload.metadata?.scale
  const value = payload.overview?.visible_prefixes
  const originValue = payload.overview?.visible_origin_ases
  if (!scale) {
    if (value != null || originValue != null) throw new Error('数值规模缺少快照依据')
    return
  }
  if (payload.state !== 'available') throw new Error('规模不能绕过不可用日期')
  if (scale.state === 'unavailable') {
    if (value !== null || originValue !== null || typeof scale.message !== 'string') throw new Error('不可用规模不能填值')
    return
  }
  const stamp = new Date(scale.observed_at)
  if (!['available', 'date_not_retained', 'family_not_supported'].includes(scale.state)
    || !/^rib_scale_v1_[0-9a-f]{64}$/.test(scale.version) || !Number.isFinite(stamp.getTime())
    || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(scale.observed_at)
    || scale.source?.collector_id !== 'rrc25' || scale.source.coverage !== 'unknown'
    || !/^[0-9a-f]{64}$/.test(scale.source.sha256)
    || scale.interpretation_version !== 'rib-prefix-union/v1' || scale.family !== payload.query?.family
    || !Array.isArray(scale.available_dates) || scale.available_dates.length !== 1
    || scale.available_dates[0] !== toBusinessTime(stamp).slice(0, 10)) throw new Error('规模来源或范围不一致')
  const expected = payload.query.date !== scale.available_dates[0] ? 'date_not_retained'
    : scale.family === 'unknown' ? 'family_not_supported' : 'available'
  if (scale.state !== expected || (scale.state === 'available'
    ? !Number.isSafeInteger(value) || (value ?? -1) < 0 || !['all', 'ipv4', 'ipv6'].includes(scale.family)
    : value !== null)) throw new Error('规模数值与快照日期或地址族不一致')
  if (['pending_definition', 'unavailable'].includes(scale.origin_metric_state)) {
    if (originValue !== null || scale.origin) throw new Error('未准入的起源规模不能填值')
  } else {
    const origin = scale.origin
    if (scale.origin_metric_state !== expected || !origin
      || !/^rib_origin_v1_[0-9a-f]{64}$/.test(origin.version)
      || origin.interpretation_version !== 'rib-attributed-origin/private-skip-v1'
      || (expected === 'available'
        ? !Number.isSafeInteger(originValue) || (originValue ?? -1) < 0
          || !Number.isSafeInteger(origin.unattributed_entries) || (origin.unattributed_entries ?? -1) < 0
        : originValue !== null || origin.unattributed_entries !== null)) throw new Error('起源规模与来源或适用范围不一致')
  }
}

function validatePathComparison(payload: CoreOverview) {
  const value = payload.metadata?.path_comparison
  if (!value) return
  const fail = () => { throw new Error('路径对照的来源、范围或统计不一致') }
  if (payload.state !== 'available' || !Array.isArray(value.examples)) fail()
  if (value.state === 'unavailable') {
    if (value.metrics !== null || value.examples.length || typeof value.message !== 'string') fail()
    return
  }
  if (!['available', 'date_not_retained', 'family_not_supported'].includes(value.state)
    || !/^rib_path_consumption_v1_[0-9a-f]{64}$/.test(value.version)
    || !/^rib_path_comparison_v1_[0-9a-f]{64}$/.test(value.comparison_version)
    || value.interpretation_version !== 'rrc25-raw-peer-rib-endpoint/as-sequence-v1'
    || value.collector_id !== 'rrc25' || value.coverage !== 'unknown' || value.session_continuity !== 'unknown'
    || value.interval_change_count !== null || value.unit !== 'raw_peer_afi_safi_prefix'
    || !value.left || !value.right || value.family !== payload.query?.family
    || !Array.isArray(value.available_dates) || value.available_dates.length !== 1) fail()
  for (const source of [value.left, value.right]) {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(source.observed_at)
      || !Number.isFinite(new Date(source.observed_at).getTime()) || !/^[0-9a-f]{64}$/.test(source.sha256)
      || toBusinessTime(new Date(source.observed_at)).slice(0, 10) !== value.available_dates[0]) fail()
  }
  if (value.left.observed_at >= value.right.observed_at) fail()
  const expected = payload.query.date !== value.available_dates[0] ? 'date_not_retained'
    : value.family === 'unknown' ? 'family_not_supported' : 'available'
  if (value.state !== expected || value.examples.length > 10) fail()
  if (expected !== 'available') {
    if (value.metrics !== null || value.examples.length) fail()
    return
  }
  const metric = value.metrics
  if (!metric) return fail()
  const natural = (n: unknown, max = 160_000_000) => Number.isSafeInteger(n) && Number(n) >= 0 && Number(n) <= max
  if (![metric.same, metric.different, metric.left_only, metric.right_only, metric.not_comparable, metric.comparable_pairs].every(n => natural(n))
    || metric.comparable_pairs !== metric.same + metric.different
    || (metric.comparable_pairs ? metric.different_fraction !== metric.different / metric.comparable_pairs : metric.different_fraction !== null)) fail()
  for (const example of value.examples) {
    if (!['ipv4', 'ipv6'].includes(example?.family) || (value.family !== 'all' && example.family !== value.family)
      || typeof example.prefix !== 'string' || !example.prefix.includes('/') || !example.peer
      || !natural(example.peer.asn, 4294967295) || typeof example.peer.bgp_id !== 'string' || typeof example.peer.ip !== 'string') fail()
    for (const path of [example.left_path, example.right_path]) {
      if (!Array.isArray(path) || !path.length || path.length > 256 || !path.every(asn => natural(asn, 4294967295))) fail()
    }
    if (JSON.stringify(example.left_path) === JSON.stringify(example.right_path)) fail()
    for (const ref of [example.left_reference, example.right_reference]) {
      if (!ref || ![ref.record, ref.offset, ref.entry, ref.peer_index].every(n => natural(n, 12 * 1024 ** 3))) fail()
    }
  }
  if (value.examples.length > metric.different || ['ipv4', 'ipv6'].some(family => value.examples.filter(example => example.family === family).length > 5)) fail()
}

export async function getCoreOverview(params: CoreOverviewQuery = {}, signal?: AbortSignal): Promise<CoreOverview> {
  let payload: CoreOverview
  try {
    payload = await apiGet<CoreOverview>('core-overview', { params, signal })
  } catch (cause) {
    const response = (cause as { response?: { status?: number; data?: CoreOverview } })?.response
    const unavailable = response?.data
    if (response?.status !== 503 || unavailable?.state !== 'unavailable'
      || !Array.isArray(unavailable.metadata?.available_dates) || !unavailable.query
      || unavailable.overview !== null || unavailable.trend !== null || unavailable.events !== null) throw cause
    payload = unavailable
  }
  if (!payload || !['available', 'window_not_retained', 'unavailable'].includes(payload.state) || typeof payload.version !== 'string') {
    throw new Error('首页数据响应不可用')
  }
  if (params.version && payload.version !== params.version) throw new Error('首页数据版本不一致')
  if (payload.diagnostic && (payload.state !== 'unavailable' || payload.overview !== null
    || payload.trend !== null || payload.events !== null
    || !payload.metadata?.diagnostic_dates?.includes(payload.query?.date))) {
    throw new Error('首页诊断不能与可用数据混用')
  }
  const diagnostic = payload.diagnostic
  if (diagnostic?.schema_version === 'core-overview-diagnostic/v2') {
    const reading = diagnostic.stage === 'source_read'
    if (!['source_read', 'source_field_validation'].includes(diagnostic.stage)
      || !Array.isArray(diagnostic.reasons) || !diagnostic.reasons.length
      || diagnostic.reasons.length > (reading ? 1 : 6)
      || diagnostic.reasons.some(reason => reading
        ? reason.code !== 'source_read_timeout' || reason.kind !== 'all' || reason.count !== null
          || reason.evidence?.format !== 'failed-day-read/v1' || reason.evidence.finished_at !== null
          || reason.evidence.receipt_sha256 !== null || reason.evidence.audit_sha256 !== null
        : reason.code === 'source_read_timeout' || !Number.isInteger(reason.count) || (reason.count ?? 0) < 1
          || reason.evidence?.format !== 'complete-day-audit/v1' || !reason.evidence.finished_at)) {
      throw new Error('首页诊断的读取或校验阶段不一致')
    }
  }
  payload.events?.items?.forEach(validateLevelEvidence)
  validateScale(payload)
  validatePathComparison(payload)
  return payload
}

export async function getCoreOverviewRecord(ref: string, version: string, signal?: AbortSignal): Promise<CoreOverviewDetail> {
  const payload = await apiGet<CoreOverviewDetail>('core-overview/record', { params: { ref, version }, signal })
  if (!payload || payload.state !== 'available') throw new Error('异常详情响应不可用')
  if (payload.version !== version) throw new Error('异常详情版本不一致')
  validateLevelEvidence(payload.item)
  return payload
}

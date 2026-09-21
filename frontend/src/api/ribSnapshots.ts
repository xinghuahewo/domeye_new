import { apiGet } from './client'
import type { components } from '@/types/openapi.generated'
import { toBusinessTime } from '@/utils/businessTime'

export type RibSnapshot = components['schemas']['RibSnapshotSummary']
export type RibSnapshotAsn = components['schemas']['RibSnapshotAsn']
type Discovery = components['schemas']['RibSnapshotDiscovery'] | components['schemas']['RibSnapshotNotConfigured']
const validVersion = (value: string) => /^rib_snapshot_v1_[0-9a-f]{64}$/.test(value)
export class RibSnapshotNotConfigured extends Error {}
type SnapshotDay = components['schemas']['RibSnapshotDay']
const dayLabels = { available: '已发布', not_calculated: '尚未计算', missing_input: '缺少输入', validation_failed: '输入校验失败' }

export function ribDayMessage(day: SnapshotDay | undefined, fallback: string): string {
  return day ? `${dayLabels[day.state]}；数量为 Unknown，不跳到其他日期` : fallback
}

export function ribBatchNote(day: SnapshotDay | undefined): string {
  return day?.state === 'available' && day.last_batch && day.last_batch.state !== 'available'
    ? `最近批次：${dayLabels[day.last_batch.state]}；当前显示此前已发布快照` : ''
}

export function ribErrorMessage(cause: unknown, fallback: string): string {
  const value = cause as { response?: { data?: { state?: string } } }
  if (value?.response?.data?.state === 'unknown_version') return '未知快照版本；数量为 Unknown'
  if (value?.response?.data?.state === 'validation_failed') return '已选快照校验失败；数量为 Unknown，不回退其他版本'
  return cause instanceof Error && !('response' in cause) ? cause.message : fallback
}

export async function discoverRibSnapshots(date?: string, signal?: AbortSignal): Promise<Discovery> {
  const data = await apiGet<Discovery>('rib-snapshots', { params: date === undefined ? { latest: 'true' } : { date }, signal })
  if (data?.state === 'not_configured') return data
  if (data?.state !== 'available' || !Array.isArray(data.snapshots)
    || data.snapshots.length > 1 || data.snapshots.some(item => (date !== undefined && item.date !== date) || !validVersion(item.version)
      || toBusinessTime(new Date(item.observed_at)).slice(0, 10) !== item.date)) throw new Error('共享快照目录与所选日期不一致')
  if (data.days !== undefined && (!Array.isArray(data.days) || data.days.length > 1
    || data.days.some(day => !(day.state in dayLabels) || (date !== undefined && day.date !== date)
      || (day.state === 'available' && (!day.version || !data.snapshots.some(item => item.version === day.version && item.date === day.date)))
      || (day.last_batch && (!/^rib_batch_v1_[0-9a-f]{64}$/.test(day.last_batch.selection_id)
        || !['available', 'missing_input', 'validation_failed'].includes(day.last_batch.state)))))) throw new Error('快照逐日状态无效')
  return data
}

function validateSnapshotIdentity(data: RibSnapshot | RibSnapshotAsn, version: string, family: RibSnapshot['family']) {
  if (data?.state !== 'available' || data.version !== version || !validVersion(version) || data.family !== family
    || data.source?.collector_id !== 'rrc25' || data.source.coverage !== 'unknown'
    || !/^[0-9a-f]{64}$/.test(data.source.sha256) || data.origin_rule !== 'rib-attributed-origin/private-skip-v1'
    || !Array.isArray(data.limitations) || data.date !== toBusinessTime(new Date(data.observed_at)).slice(0, 10)) {
    throw new Error('共享快照响应、版本或来源不一致')
  }
}

export async function getRibSnapshot(version: string, family: RibSnapshot['family'], signal?: AbortSignal): Promise<RibSnapshot> {
  const data = await apiGet<RibSnapshot | components['schemas']['RibSnapshotNotConfigured']>(`rib-snapshots/${encodeURIComponent(version)}`, { params: { family }, signal })
  if (data?.state === 'not_configured') throw new RibSnapshotNotConfigured(data.message)
  validateSnapshotIdentity(data, version, family)
  if (data.unit !== 'distinct_prefix_and_origin_asn' || !data.metrics) throw new Error('共享快照统计口径不一致')
  const m = data.metrics
  if (![m.visible_prefixes, m.visible_origin_ases, m.attributed_prefixes, m.unattributed_prefixes, m.rib_entries, m.unattributed_entries]
    .every(value => Number.isSafeInteger(value) && value >= 0)
    || m.attributed_prefixes + m.unattributed_prefixes !== m.visible_prefixes
    || m.unattributed_entries > m.rib_entries) throw new Error('共享快照统计对账失败')
  return data
}

export async function getRibSnapshotAsn(version: string, asn: string, family: RibSnapshot['family'], signal?: AbortSignal): Promise<RibSnapshotAsn> {
  const data = await apiGet<RibSnapshotAsn | components['schemas']['RibSnapshotNotConfigured']>(`rib-snapshots/${encodeURIComponent(version)}/asns/${encodeURIComponent(asn)}`, { params: { family }, signal })
  if (data?.state === 'not_configured') throw new RibSnapshotNotConfigured(data.message)
  validateSnapshotIdentity(data, version, family)
  if (data.asn !== Number(asn)
    || data.unit !== 'distinct_origin_prefix' || !Number.isSafeInteger(data.prefix_count) || data.prefix_count < 0
    || data.sample_limit !== 20 || typeof data.sample_truncated !== 'boolean' || !Array.isArray(data.items)
    || data.items.length > 20 || (data.sample_truncated && data.items.length !== 20)
    || (data.prefix_count === 0 && (data.items.length !== 0 || data.sample_truncated))
    || data.items.some(item => item.attributed_origin_asn !== Number(asn)
      || !['ipv4', 'ipv6'].includes(item.family) || (family !== 'all' && item.family !== family))) {
    throw new Error('ASN快照响应、版本或来源不一致')
  }
  return data
}

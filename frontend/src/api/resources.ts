import { apiGet } from './client'
import type { components } from '@/types/openapi.generated'

export type ResourcePoint = components['schemas']['ResourceStatisticsPoint']
export type ResourceStatistics = components['schemas']['ResourceStatisticsPayload']
const units = {
  ipv4_prefix_count: 'distinct_ipv4_prefix', ipv6_prefix_count: 'covered_ipv6_48_block',
  ipv6_48_count: 'covered_ipv6_48_block', ipv4_address_count: 'covered_ipv4_24_block_times_256',
  vp_count: 'distinct_first_path_asn', private_as_count: 'distinct_legacy_private_tail_asn',
  public_as_count: 'distinct_legacy_public_tail_asn', path_count: 'distinct_rendered_path',
} as const

export async function getResources(range: { start_time: string; end_time: string }, signal?: AbortSignal) {
  const payload = await apiGet<ResourceStatistics>('resources', { params: range, signal })
  if (!payload || !['available', 'not_calculated', 'not_configured'].includes(payload.state)
    || payload.query?.window_boundary !== '[start,end)' || payload.query.timezone !== 'Asia/Shanghai'
    || payload.query.scope !== 'global' || !Array.isArray(payload.points)
    || (payload.state === 'available') !== (payload.points.length > 0)) throw new Error('资源统计响应不可用')
  const start = Date.parse(payload.query.start), end = Date.parse(payload.query.end_exclusive)
  const inputTime = (value: string) => Date.parse(/(?:Z|[+-]\d{2}:\d{2})$/.test(value) ? value : `${value.replace(' ', 'T')}+08:00`)
  if (start !== inputTime(range.start_time) || end !== inputTime(range.end_time) || !(start < end)) throw new Error('资源统计窗口不一致')
  let previous = -Infinity
  const ids = new Set<string>()
  for (const point of payload.points) {
    const time = Date.parse(point.observed_at)
    if (!/^rib_statistics_v1_[0-9a-f]{64}$/.test(point.snapshot_id) || ids.has(point.snapshot_id)
      || !(start <= time && time < end && previous <= time) || !/^[0-9a-f]{64}$/.test(point.metadata?.source_sha256)
      || point.metadata.rule !== 'resource-39578fe-v1') throw new Error('资源统计时点或来源不一致')
    for (const name of Object.keys(units) as Array<keyof typeof units>) {
      const cell = point.metrics?.[name]
      if (!cell || cell.unit !== units[name]
        || !['qualified', 'unknown', 'not_applicable'].includes(cell.qualification)
        || (cell.qualification === 'qualified'
          ? !Number.isSafeInteger(cell.main) || (cell.main ?? -1) < 0 : cell.main !== null)) throw new Error('资源统计单位或主值不一致')
    }
    previous = time; ids.add(point.snapshot_id)
  }
  return payload
}

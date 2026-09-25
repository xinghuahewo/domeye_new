import { apiGetWithResultMetadata } from './client'
import type { CountryOutageRecord } from './countryOutageRecord'
import type { FeatureRange } from './features'
import type { components } from '@/types/openapi.generated'
import { businessTimeToIso, toBusinessTime } from '@/utils/businessTime'
import { isRecord, normalizeOutagePoints } from '@/utils/normalize'

export type CountryChartKind = 'features' | 'as' | 'prefix'
export type ChartPoints = Array<[string, number | null]>
export interface CountryChartData {
  series: Record<string, ChartPoints>
  version: string
}

const hour = 3_600_000
const rangeTime = (value: string) => Date.parse(businessTimeToIso(value))

/** 默认从事件前一个整点开始，最多一天；实际覆盖终点不是事件结束时间。 */
export function countryEventWindow(record: CountryOutageRecord): FeatureRange {
  const eventStart = Date.parse(record.item.start_time)
  if (!Number.isFinite(eventStart)) throw new Error('事件缺少有效开始时间')
  const start = Math.max(Math.floor(eventStart / hour) * hour - hour,
    Date.parse(record.delivery.start))
  const eventEnd = record.item.end_time.value ? Date.parse(record.item.end_time.value) + hour : Infinity
  const end = Math.min(start + 24 * hour, eventEnd,
    Date.parse(record.delivery.endExclusive))
  if (start >= end) throw new Error('事件附近尚无可查询的已覆盖时段')
  return { start_time: toBusinessTime(new Date(start)), end_time: toBusinessTime(new Date(end)) }
}

export function validateCountryEventRange(range: FeatureRange, record: CountryOutageRecord): void {
  const start = rangeTime(range.start_time)
  const end = rangeTime(range.end_time)
  if (!Number.isFinite(start) || !Number.isFinite(end) || start >= end) throw new Error('请选择有效的起止时间')
  if (end - start > 24 * hour) throw new Error('单次最多查询 24 小时，可调整时间查看其他时段')
  if (start < Date.parse(record.delivery.start)
    || end > Date.parse(record.delivery.endExclusive)) {
    throw new Error('所选时间超出当前数据覆盖范围，请调整起止时间')
  }
}

export async function getCountryEventSeries(kind: CountryChartKind, country: string, range: FeatureRange, eventVersion: string): Promise<CountryChartData> {
  const endpoint = kind === 'features' ? 'features/countries/series' : `features/outages/country-${kind}`
  const { data, result } = await apiGetWithResultMetadata<unknown>(endpoint, { params: { country, ...range, version: eventVersion } })
  if (!eventVersion || result.version !== eventVersion) throw new Error('统计与事件版本不一致，请刷新事件后重试')
  if (result.state !== 'available' || !Number.isFinite(Date.parse(result.start || ''))
    || !Number.isFinite(Date.parse(result['end-exclusive'] || ''))
    || Date.parse(result.start!) >= Date.parse(result['end-exclusive']!)) throw new Error('统计数据的版本或覆盖时间不完整')
  const start = rangeTime(range.start_time)
  const end = rangeTime(range.end_time)
  if (!isRecord(data) || !isRecord(data.metadata) || !isRecord(data.query)) {
    throw new Error('统计缺少查询与覆盖元数据，请使用匹配的 API')
  }
  if (data.metadata.version !== result.version || data.query.country !== country
    || Date.parse(String(data.query.start)) !== start || Date.parse(String(data.query.end_exclusive)) !== end) {
    throw new Error('统计的版本、对象或窗口与请求不一致')
  }
  const series: Record<string, ChartPoints> = {}
  if (kind === 'features') {
    const payload = data as unknown as components['schemas']['CountryFeatureSeriesPayload']
    const { metadata } = payload
    if (metadata.interpretation_version !== 'country-feature-series/v2' || metadata.sample_seconds !== 300
      || metadata.time_basis?.time !== 'source_file_label' || metadata.time_basis?.resource !== 'resource_state_at'
      || !Array.isArray(payload.data) || !Array.isArray(metadata.coverage?.intervals)) throw new Error('国家时序响应结构不完整')
    const fields = [
      ['announce', 'announce', 'accepted_route_element'], ['withdraw', 'withdraw', 'accepted_route_element'],
      ['ipv4Addresses', 'ipv4_addresses', 'ipv4_address'],
      ['ipv4Prefixes', 'ipv4_prefixes', 'ipv4_24_covered_block'],
      ['ipv6Prefixes', 'ipv6_prefixes', 'ipv6_48_covered_block'],
    ] as const
    const intervals = metadata.coverage.intervals.map((part) => [Date.parse(part.start), Date.parse(part.end_exclusive)] as const)
    if (intervals.some(([left, right]) => !Number.isFinite(left) || !Number.isFinite(right) || left >= right || left < start || right > end)) {
      throw new Error('国家时序覆盖范围无效')
    }
    const times = payload.data.map((point) => Date.parse(point.time))
    if (new Set(times).size !== times.length || times.some((time) => !intervals.some(([left, right]) => left <= time && time < right))) {
      throw new Error('国家时序时点重复或位于处理覆盖之外')
    }
    const resourceTimes = payload.data.map((point, i) => {
      const left = Date.parse(point.source_window?.start)
      const right = Date.parse(point.source_window?.end_exclusive)
      const stateAt = Date.parse(point.resource_state_at)
      if (!point.source_window?.source_id || !Number.isFinite(stateAt) || stateAt !== right
        || !(left <= times[i]! && times[i]! < right)) throw new Error('国家时序来源窗口或资源时点无效')
      return stateAt
    })
    for (const [chartKey, field, unit] of fields) {
      if (metadata.units?.[field] !== unit) throw new Error('国家时序单位与图表不一致')
      const resource = field !== 'announce' && field !== 'withdraw'
      const values = new Map<number, number | null>()
      for (let time = Math.ceil(start / 300_000) * 300_000; time < end; time += 300_000) values.set(time + (resource ? 300_000 : 0), null)
      payload.data.forEach((point, i) => {
        const value = point[field]
        if (value !== null && (!Number.isSafeInteger(value) || value < 0)) throw new Error('国家时序存在无效数值')
        values.set((resource ? resourceTimes : times)[i]!, value)
      })
      // 小于五分钟的处理缺口也须断线；覆盖终点不能画成恢复或零。
      intervals.forEach(([, right], i) => {
        if (right < end) values.set(resource ? (right + (intervals[i + 1]?.[0] ?? end)) / 2 : right, null)
      })
      series[chartKey] = [...values].sort((a, b) => a[0] - b[0]).map(([time, value]) => [new Date(time).toISOString(), value])
    }
  } else {
    if (data.metadata.unit !== (kind === 'as' ? 'asn' : 'prefix')) throw new Error('中断统计单位与请求不一致')
    const points = normalizeOutagePoints(data)
    series.count = points.map((point) => [new Date(point.time).toISOString(), point.count])
  }
  return { series, version: result.version }
}

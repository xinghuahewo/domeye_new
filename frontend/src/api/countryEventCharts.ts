import { apiGetWithResultMetadata } from './client'
import type { CountryOutageRecord } from './countryOutageRecord'
import type { FeatureRange } from './features'
import { businessTimeToIso, toBusinessTime } from '@/utils/businessTime'
import { normalizeCountryOverview, normalizeOutagePoints } from '@/utils/normalize'
import dataProfile from '../../../config/data-profile.json'

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
  const eventStart = Date.parse(record.bundle.event.eventTimeUtc || '')
  if (!Number.isFinite(eventStart)) throw new Error('事件缺少有效开始时间')
  const start = Math.max(Math.floor(eventStart / hour) * hour - hour,
    Date.parse(record.delivery?.start || dataProfile.window_start))
  const eventEnd = record.bundle.event.endTimeUtc ? Date.parse(record.bundle.event.endTimeUtc) + hour : Infinity
  const end = Math.min(start + 24 * hour, eventEnd,
    Date.parse(record.delivery?.endExclusive || dataProfile.window_end_exclusive))
  if (start >= end) throw new Error('事件附近尚无可查询的已覆盖时段')
  return { start_time: toBusinessTime(new Date(start)), end_time: toBusinessTime(new Date(end)) }
}

export function validateCountryEventRange(range: FeatureRange, record: CountryOutageRecord): void {
  const start = rangeTime(range.start_time)
  const end = rangeTime(range.end_time)
  if (!Number.isFinite(start) || !Number.isFinite(end) || start >= end) throw new Error('请选择有效的起止时间')
  if (end - start > 24 * hour) throw new Error('单次最多查询 24 小时，可调整时间查看其他时段')
  if (start < Date.parse(record.delivery?.start || dataProfile.window_start)
    || end > Date.parse(record.delivery?.endExclusive || dataProfile.window_end_exclusive)) {
    throw new Error('所选时间超出当前数据覆盖范围，请调整起止时间')
  }
}

export async function getCountryEventSeries(kind: CountryChartKind, country: string, range: FeatureRange): Promise<CountryChartData> {
  const endpoint = kind === 'features' ? 'features/countries/overview' : `features/outages/country-${kind}`
  const { data, result } = await apiGetWithResultMetadata<unknown>(endpoint, { params: { country, ...range } })
  // 使用每次查询自己的覆盖范围，避免接口补槽的零延伸到尚未处理的时间。
  if (Object.keys(result).length && (result.state !== 'available' || !result.version
    || !Number.isFinite(Date.parse(result.start || '')) || !Number.isFinite(Date.parse(result['end-exclusive'] || ''))
    || Date.parse(result.start!) >= Date.parse(result['end-exclusive']!))) throw new Error('统计数据的版本或覆盖时间不完整')
  const start = rangeTime(range.start_time)
  const end = rangeTime(range.end_time)
  const coveredStart = result.start ? Date.parse(result.start) : start
  const coveredEnd = result['end-exclusive'] ? Date.parse(result['end-exclusive']) : end
  const pad = (rows: Array<{ time: string }>, value: (row: number) => number | null, minutes: number): ChartPoints => {
    const values = new Map(rows.map((row, i) => [rangeTime(row.time), value(i)]))
    const interval = minutes * 60_000
    // 特征是整五分钟桶；中断接口从请求起点按三分钟采样。
    const first = kind === 'features' ? Math.ceil(start / interval) * interval : start
    const points: ChartPoints = []
    for (let time = first; time < end; time += interval) {
      points.push([new Date(time).toISOString(), time >= coveredStart && time < coveredEnd ? values.get(time) ?? null : null])
    }
    return points
  }
  const series: Record<string, ChartPoints> = {}
  if (kind === 'features') {
    const profile = normalizeCountryOverview(data).selectedCountry
    if (profile && profile.country !== country) throw new Error('统计响应与请求国家不一致')
    const points = profile?.series ?? []
    for (const key of ['announce', 'withdraw', 'ipv4Addresses', 'ipv4Prefixes', 'ipv6Prefixes'] as const) {
      series[key] = pad(points, (i) => points[i]?.[key] ?? null, 5)
    }
  } else {
    const points = normalizeOutagePoints(data)
    series.count = pad(points, (i) => points[i]?.count ?? null, 3)
  }
  return { series, version: result.version || '' }
}

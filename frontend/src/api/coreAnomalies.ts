import { getCoreOverview, type CoreOverview, type CoreOverviewItem } from './coreOverview'
import { toBusinessTime } from '@/utils/businessTime'

export const anomalyKinds = [
  ['country_outage', '国家中断'], ['as_outage', 'AS 中断'], ['prefix_outage', '前缀中断'],
  ['leak', '路由泄漏'], ['hijack', '前缀劫持'], ['sub_hijack', '子前缀劫持'],
] as const
export type AnomalyKind = typeof anomalyKinds[number][0]
export interface AnomalySummary {
  count: number | null
  hours: number[] | null
  buckets?: { start: string; end_exclusive: string; value: number }[]
  note: string
}

// 兼容现有只读接口；只在日期、地址族或版本变化时读取。
// 小类别复用完整记录；大类别读取 24 个小时的精确计数，不下载整日明细。
const PAGE_SIZE = 100
const MAX_CHART_RECORDS = 1000

export async function getAnomalySummary(base: CoreOverview, kind: AnomalyKind, signal?: AbortSignal): Promise<AnomalySummary> {
  if (base.state !== 'available') return { count: null, hours: null, note: '区间异常数据不可用' }
  if (!base.metadata.kinds.includes(kind)) return { count: null, hours: null, note: '此版本尚未接入' }
  const trends = base.event_trends
  if (trends) {
    if (trends.state !== 'available') return { count: null, hours: null, note: '本时段异常统计不可用' }
    const matches = trends.series.filter(series => series.kind === kind)
    if (trends.metric !== 'recorded_event_starts' || trends.filter_scope !== (base.query.window_mode || base.query.country ? 'window_country_and_family' : 'date_and_family') || matches.length !== 1) throw new Error('异常趋势定义不一致')
    const value = matches[0]!
    let lastEnd = 0
    for (const bucket of value.buckets) {
      const start = Date.parse(bucket.start), end = Date.parse(bucket.end_exclusive)
      if (!Number.isSafeInteger(bucket.value) || bucket.value < 0 || !Number.isFinite(start) || !Number.isFinite(end)
        || start >= end || start < lastEnd || start < Date.parse(base.query.start) || end > Date.parse(base.query.end_exclusive)) throw new Error('异常趋势超出日期或覆盖区间无效')
      lastEnd = end
    }
    if (!Number.isSafeInteger(value.total) || value.total < 0 || value.buckets.reduce((sum, b) => sum + b.value, 0) !== value.total) throw new Error('异常分桶与总数不一致')
    return { count: value.total, hours: value.buckets.map(bucket => bucket.value), buckets: value.buckets,
      note: `${trends.bucket_seconds === 86400 ? '每天' : trends.bucket_seconds === 21600 ? '每 6 小时' : '每小时'}新增 · 仅已覆盖时段` }
  }
  const params = { date: base.query.date, family: base.query.family as 'all' | 'ipv4' | 'ipv6' | 'unknown',
    version: base.version, kind, level: 'all' as const, sort: 'time' as const, page_size: PAGE_SIZE }
  async function read(page: number, hour?: number) {
    signal?.throwIfAborted()
    const result = await getCoreOverview({ ...params, page, hour, page_size: hour === undefined ? PAGE_SIZE : 1 }, signal)
    const events = result.events
    if (result.state !== 'available' || result.query.date !== params.date || result.query.family !== params.family
      || result.query.kind !== kind || result.query.hour !== (hour ?? null) || result.query.level !== 'all' || result.query.q !== ''
      || result.version !== base.version || !events || !Number.isSafeInteger(events.total) || events.total < 0 || events.page !== page) {
      throw new Error('异常概况的日期、版本或记录数不一致')
    }
    return events
  }
  let total: number | undefined
  const records: CoreOverviewItem[] = []
  for (let page = 1; ; page++) {
    const events = await read(page)
    if (total !== undefined && total !== events.total) throw new Error('异常记录数在分页期间发生变化')
    total = events.total
    if (base.metadata.result_delivery?.coverage === 'partial_window') {
      return { count: total, hours: null, note: '仅已交付时段；全天其余时段未知' }
    }
    if (total > MAX_CHART_RECORDS) {
      const hours = Array<number>(24).fill(0)
      let nextHour = 0
      async function worker() {
        while (nextHour < 24) {
          const hour = nextHour++
          hours[hour] = (await read(1, hour)).total
        }
      }
      await Promise.all([worker(), worker()])
      if (hours.reduce((sum, value) => sum + value, 0) !== total) throw new Error('小时记录数与当日总数不一致')
      return { count: total, hours, note: '每小时新增 · 条' }
    }
    records.push(...events.items)
    if (records.length >= total) break
    if (events.items.length !== PAGE_SIZE) throw new Error('异常记录未完整读取')
  }
  if (records.length !== total || new Set(records.map(item => item.reference)).size !== total) throw new Error('异常记录分页不完整或重复')
  const hours = Array<number>(24).fill(0)
  for (const record of records) {
    const time = toBusinessTime(new Date(record.start_time))
    if (record.kind !== kind || time.slice(0, 10) !== params.date) throw new Error('异常记录超出所选类型或日期')
    const hour = Number(time.slice(11, 13))
    hours[hour] = (hours[hour] ?? 0) + 1
  }
  return { count: total, hours, note: '每小时新增 · 条' }
}

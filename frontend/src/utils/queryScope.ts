import type { LocationQuery } from 'vue-router'
import type { HealthPayload } from '@/types/api'
import { businessTimeToIso, toBusinessTime } from './businessTime'
import { parseInputTime } from './time'

type Delivery = HealthPayload['result_delivery']
const text = (value: unknown) => typeof value === 'string' ? value : undefined

/** 跨页面保留业务时间；明确选择的窗口外日期也保留，不偷偷跳回有数据的日子。 */
export function scopeQuery(target: string, query: LocationQuery, delivery?: Delivery): Record<string, string> {
  let start = text(query.start), end = text(query.end)
  const date = text(query.date)
  if (!start || !end || !parseInputTime(start) || !parseInputTime(end) || start >= end) {
    start = undefined; end = undefined
    if (date && /^\d{4}-\d{2}-\d{2}$/.test(date) && parseInputTime(`${date}T00:00:00`)) {
      start = `${date}T00:00:00`
      end = toBusinessTime(new Date(new Date(businessTimeToIso(start)).getTime() + 86400000)).replace(' ', 'T')
      if (delivery?.state === 'available' && delivery.start && delivery.end_exclusive) {
        const a = toBusinessTime(new Date(delivery.start)).replace(' ', 'T')
        const b = toBusinessTime(new Date(delivery.end_exclusive)).replace(' ', 'T')
        if (a < end && b > start) { start = a > start ? a : start; end = b < end ? b : end }
      }
    } else if (delivery?.state === 'available' && delivery.start && delivery.end_exclusive) {
      start = toBusinessTime(new Date(delivery.start)).replace(' ', 'T')
      end = toBusinessTime(new Date(delivery.end_exclusive)).replace(' ', 'T')
      // 普通档案查询最多一天；更长批次默认从首个实际时点查看一天。
      const limit = new Date(businessTimeToIso(start)).getTime() + 86400000
      if (new Date(businessTimeToIso(end)).getTime() > limit) end = toBusinessTime(new Date(limit)).replace(' ', 'T')
    }
  }
  if (!start || !end) return {}
  if (target === 'home' || target === '/') return { date: date || start.slice(0, 10) }
  if (target === 'events' || target === '/events') return { date: start.slice(0, 10) }
  return { start, end }
}

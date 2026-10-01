import type { LocationQuery } from 'vue-router'
import type { HealthPayload } from '@/types/api'
import { businessTimeToIso, toBusinessTime } from './businessTime'
import { validScopeTime } from './coreScope'

type Delivery = HealthPayload['result_delivery']
const text = (value: unknown) => typeof value === 'string' ? value : undefined
const inputTime = (value: unknown) => {
  const input = text(value)
  return input?.length === 16 ? `${input}:00` : input
}

/** 跨页面保留业务时间；明确选择的窗口外日期也保留，不偷偷跳回有数据的日子。 */
export function scopeQuery(target: string, query: LocationQuery, delivery?: Delivery): Record<string, string> {
  let start = inputTime(query.start), end = inputTime(query.end)
  const date = text(query.date)
  if (!start || !end || !validScopeTime(start) || !validScopeTime(end) || start >= end) {
    start = undefined; end = undefined
    if (date && /^\d{4}-\d{2}-\d{2}$/.test(date) && validScopeTime(`${date}T00:00:00`)) {
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
    }
  }
  // 事件页的 country 是国内/国外筛选，国家名称使用 attacked_country。
  const country = text(query.attacked_country)?.trim()
    || (text(query.country) && !['all', 'domestic', 'foreign'].includes(text(query.country)!)
      ? text(query.country)!.trim() : '')
  const result: Record<string, string> = start && end ? { start, end } : {}
  if (country) result[target === 'events' || target === '/events' ? 'attacked_country' : 'country'] = country
  return result
}

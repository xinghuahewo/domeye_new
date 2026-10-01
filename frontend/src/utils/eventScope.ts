import { localTime, scopeError, scopeFromQuery, scopeMillis, validScopeTime } from './coreScope'
import type { DataWindow } from './time'

export type EventRange = Pick<DataWindow, 'start' | 'end'>

export function normalizeEventRange(range: EventRange): EventRange {
  const normalize = (time: string) => time.length === 16 ? `${time}:00` : time
  return { start: normalize(range.start), end: normalize(range.end) }
}

/** 构建配置的 end 是最后一秒快照，页面统一转换为右端不含的区间。 */
export function eventDataRange(window: DataWindow | null): EventRange | null {
  if (!window || !validScopeTime(window.start) || !validScopeTime(window.end)) return null
  return { start: window.start, end: localTime(new Date(scopeMillis(window.end) + 1000).toISOString()) }
}

export function eventRangeFromQuery(query: Record<string, unknown>, fallback: EventRange): EventRange {
  const { start, end } = scopeFromQuery(query, { ...fallback, country: '' })
  return { start, end }
}

export function eventRangeError(range: EventRange): string {
  return scopeError({ ...normalizeEventRange(range), country: '' })
}

export function eventPresetRange(days: number, window: EventRange): EventRange {
  if (!validScopeTime(window.start) || !validScopeTime(window.end)) return window
  const start = localTime(new Date(scopeMillis(window.end) - days * 86400000).toISOString())
  return { start: start < window.start ? window.start : start, end: window.end }
}

/** 旧事件接口按秒含右端点；只在请求边界减一秒，URL 和页面仍保留 [start,end)。 */
export function eventDateParameter(range: EventRange): string {
  range = normalizeEventRange(range)
  const error = eventRangeError(range)
  if (error) throw new Error(error)
  const inclusiveEnd = localTime(new Date(scopeMillis(range.end) - 1000).toISOString())
  return `${range.start.replace('T', ' ')}_${inclusiveEnd.replace('T', ' ')}`
}

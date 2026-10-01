import profile from '../../../config/data-profile.json'
import { toBusinessTime } from './businessTime'

export interface CoreScope { start: string; end: string; country: string }
export const localTime = (value: string) => toBusinessTime(new Date(value)).replace(' ', 'T')
export const scopeMillis = (value: string) => Date.parse(`${value.replace(' ', 'T')}+08:00`)
export const scopeMinimum = profile.window_start.slice(0, 19)
export const scopeMaximum = profile.window_end_exclusive.slice(0, 19)
export const validScopeTime = (value: string) => /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(value)
  && Number.isFinite(scopeMillis(value)) && localTime(new Date(scopeMillis(value)).toISOString()) === value

export function scopeError(scope: CoreScope): string {
  if (!validScopeTime(scope.start) || !validScopeTime(scope.end)) return '请填写完整有效的起止时间'
  if (scope.start >= scope.end) return '结束时间须晚于开始时间'
  if (scope.start < scopeMinimum || scope.end > scopeMaximum) return '请选择项目数据范围内的时间'
  return ''
}

export function scopeFromQuery(query: Record<string, unknown>, fallback: CoreScope): CoreScope {
  const country = typeof query.country === 'string' ? query.country : ''
  if (query.start !== undefined || query.end !== undefined) {
    const normalize = (value: unknown) => typeof value === 'string' ? value.length === 16 ? `${value}:00` : value : ''
    return { start: normalize(query.start), end: normalize(query.end), country }
  }
  if (typeof query.date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(query.date)) {
    const start = `${query.date}T00:00:00`
    if (Number.isFinite(scopeMillis(start))) return { start,
      end: localTime(new Date(scopeMillis(start) + 86400000).toISOString()), country }
  }
  return { ...fallback, country }
}

export function presetScope(scope: CoreScope, hours: number, end: string): CoreScope {
  const start = localTime(new Date(scopeMillis(end) - hours * 3600000).toISOString())
  return { ...scope, start: start < scopeMinimum ? scopeMinimum : start, end }
}

export const scopeLabel = (scope: Pick<CoreScope, 'start' | 'end'>) => {
  const length = [scope.start, scope.end].some(value => value.slice(17, 19) !== '00') ? 19 : 16
  return `${scope.start.replace('T', ' ').slice(0, length)} — ${scope.end.replace('T', ' ').slice(0, length)}`
}

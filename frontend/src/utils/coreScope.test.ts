import { expect, it } from 'vitest'
import { presetScope, scopeError, scopeFromQuery, type CoreScope } from './coreScope'

const scope: CoreScope = { start: '2026-02-24T08:00:00', end: '2026-02-24T11:35:00', country: '伊朗' }
it('支持跨日和分钟输入，保留地区与显式无数据窗口', () => {
  const query = { start: '2026-02-25T08:00', end: '2026-02-27T11:35', country: '中国' }
  expect(scopeFromQuery(query, scope)).toEqual({ start: '2026-02-25T08:00:00', end: '2026-02-27T11:35:00', country: '中国' })
  expect(scopeError(scopeFromQuery(query, scope))).toBe('')
  expect(scopeError(scopeFromQuery({ start: scope.start }, scope))).not.toBe('')
  expect(scopeFromQuery({ date: '2026-02-28' }, scope).end).toBe('2026-03-01T00:00:00')
})
it('拒绝无效日、反向、零长度及数据档外窗口', () => {
  for (const invalid of [
    { ...scope, start: '2026-02-30T08:00:00' }, { ...scope, start: scope.end },
    { ...scope, end: scope.start }, { ...scope, start: '2026-01-31T00:00:00' },
    { ...scope, end: '2026-04-01T00:00:01' },
  ]) expect(scopeError(invalid)).not.toBe('')
})
it('快捷区间锚定数据末端，跨月时受历史数据档约束', () => {
  expect(presetScope(scope, 24, scope.end)).toEqual({ ...scope, start: '2026-02-23T11:35:00' })
  expect(presetScope(scope, 720, scope.end)).toEqual({ ...scope, start: '2026-02-01T00:00:00' })
})

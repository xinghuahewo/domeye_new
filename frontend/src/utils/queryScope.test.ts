import { expect, it } from 'vitest'
import { scopeQuery } from './queryScope'

const delivery = { state: 'available' as const, files: 44, start: '2026-02-24T00:00:00Z', end_exclusive: '2026-02-24T03:35:00Z' }

it('首页进入国家和 AS 时只带实际时窗，返回首页或事件页保留日期', () => {
  const window = { start: '2026-02-24T08:00:00', end: '2026-02-24T11:35:00' }
  expect(scopeQuery('/countries', { date: '2026-02-24', kind: 'leak' }, delivery)).toEqual(window)
  expect(scopeQuery('/ases', window, delivery)).toEqual(window)
  expect(scopeQuery('/', window, delivery)).toEqual({ date: '2026-02-24' })
  expect(scopeQuery('/events', window, delivery)).toEqual({ date: '2026-02-24' })
})

it('缺省使用本批时间，但不覆盖用户明确选择的窗口外日期', () => {
  expect(scopeQuery('home', {}, delivery)).toEqual({ date: '2026-02-24' })
  expect(scopeQuery('/countries', { date: '2026-02-25' }, delivery)).toEqual({ start: '2026-02-25T00:00:00', end: '2026-02-26T00:00:00' })
  expect(scopeQuery('home', { date: '2026-02-25' }, delivery)).toEqual({ date: '2026-02-25' })
  expect(scopeQuery('home', {})).toEqual({})
})

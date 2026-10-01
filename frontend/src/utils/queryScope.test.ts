import { expect, it } from 'vitest'
import { scopeQuery } from './queryScope'

const delivery = { state: 'available' as const, files: 44, start: '2026-02-24T00:00:00Z', end_exclusive: '2026-02-24T03:35:00Z' }

it('首页、AS 和事件之间保留相同的完整时窗', () => {
  const window = { start: '2026-02-24T08:00:00', end: '2026-02-24T11:35:00' }
  expect(scopeQuery('/countries', { date: '2026-02-24', kind: 'leak' }, delivery)).toEqual(window)
  expect(scopeQuery('/ases', window, delivery)).toEqual(window)
  expect(scopeQuery('/', window, delivery)).toEqual(window)
  expect(scopeQuery('/events', window, delivery)).toEqual(window)
})

it('缺省使用本批时间，但不覆盖用户明确选择的窗口外日期', () => {
  expect(scopeQuery('home', {}, delivery)).toEqual({ start: '2026-02-24T08:00:00', end: '2026-02-24T11:35:00' })
  expect(scopeQuery('/countries', { date: '2026-02-25' }, delivery)).toEqual({ start: '2026-02-25T00:00:00', end: '2026-02-26T00:00:00' })
  expect(scopeQuery('home', { date: '2026-02-25' }, delivery)).toEqual({ start: '2026-02-25T00:00:00', end: '2026-02-26T00:00:00' })
  expect(scopeQuery('home', {})).toEqual({})
})


it('跨日与地区参数在返回首页时保留，不缩为单日', () => {
  const window = { start: '2026-02-20T08:00:00', end: '2026-02-24T11:35:00', country: '伊朗' }
  expect(scopeQuery('home', window, delivery)).toEqual(window)
  expect(scopeQuery('/countries', window, delivery)).toEqual(window)
  expect(scopeQuery('/ases', window, delivery)).toEqual(window)
})

it('跨页面按北京时间校验，不受浏览器夏令时缺失小时影响', () => {
  const previous = process.env.TZ
  process.env.TZ = 'America/New_York'
  try {
    const window = { start: '2026-03-08T02:30:00', end: '2026-03-08T06:30:00', country: '伊朗' }
    expect(scopeQuery('home', window, delivery)).toEqual(window)
    expect(scopeQuery('/countries', window, delivery)).toEqual(window)
    expect(scopeQuery('home', { ...window, start: '2026-03-08T02:30' }, delivery)).toEqual(window)
  } finally {
    if (previous === undefined) delete process.env.TZ
    else process.env.TZ = previous
  }
})

it('分钟与秒格式表示同一时刻时不保留零长度窗口', () => {
  expect(scopeQuery('home', { start: '2026-03-08T02:30', end: '2026-03-08T02:30:00' })).toEqual({})
})


it('事件使用受影响国家，国内国外标记不能误作国家名', () => {
  const window = { start: '2026-02-20T08:00:00', end: '2026-02-24T11:35:00', country: '伊朗' }
  const eventsQuery = { start: window.start, end: window.end, attacked_country: '伊朗' }
  expect(scopeQuery('/events', window, delivery)).toEqual(eventsQuery)
  expect(scopeQuery('home', { ...eventsQuery, country: 'foreign' }, delivery)).toEqual(window)
  expect(scopeQuery('ases', eventsQuery, delivery)).toEqual(window)
  expect(scopeQuery('home', { ...eventsQuery, attacked_country: '', country: 'all' })).toEqual({ start: window.start, end: window.end })
  expect(scopeQuery('/events', { country: '中国' })).toEqual({ attacked_country: '中国' })
  const batch = { ...delivery, end_exclusive: '2026-02-27T00:00:00Z' }
  expect(scopeQuery('ases', {}, batch)).toEqual({ start: '2026-02-24T08:00:00', end: '2026-02-27T08:00:00' })
})

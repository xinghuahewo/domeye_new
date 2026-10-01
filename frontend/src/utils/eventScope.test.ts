import { describe, expect, it } from 'vitest'
import { eventDataRange, eventDateParameter, eventPresetRange, eventRangeError, eventRangeFromQuery } from './eventScope'

const window = { start: '2026-02-01T00:00:00', end: '2026-04-01T00:00:00' }

describe('事件区间与旧接口的秒级适配', () => {
  it('跨月精确区间保留，右端事件被排除，前一秒仍纳入', () => {
    const range = { start: '2026-02-27T08:00:12', end: '2026-03-02T08:00:34' }
    expect(eventRangeFromQuery(range, window)).toEqual(range)
    const date = eventDateParameter(range)
    expect(date).toBe('2026-02-27 08:00:12_2026-03-02 08:00:33')
    const [start = '', end = ''] = date.split('_')
    const recordedTimes = ['2026-02-27 08:00:11', '2026-02-27 08:00:12', '2026-03-02 08:00:33', '2026-03-02 08:00:34']
    expect(recordedTimes.filter(time => time >= start && time <= end)).toEqual(recordedTimes.slice(1, 3))
  })

  it('旧 date 兼容为整天，并支持分钟输入与最短一秒区间', () => {
    const range = eventRangeFromQuery({ date: '2026-02-28' }, window)
    expect(range).toEqual({ start: '2026-02-28T00:00:00', end: '2026-03-01T00:00:00' })
    expect(eventDateParameter(range)).toBe('2026-02-28 00:00:00_2026-02-28 23:59:59')
    expect(eventDateParameter({ start: '2026-02-28T08:00', end: '2026-02-28T08:00:01' })).toBe('2026-02-28 08:00:00_2026-02-28 08:00:00')
  })

  it('显式无效区间不回退默认值，不请求 API', () => {
    for (const query of [
      { start: '2026-02-30T08:00:00', end: '2026-03-01T08:00:00' },
      { start: '2026-03-01T08:00:00', end: '2026-03-01T08:00:00' },
      { start: '2026-03-01T08:00:00' },
      { start: '2026-01-31T23:59:59', end: '2026-02-01T08:00:00' },
    ]) {
      const range = eventRangeFromQuery(query, window)
      expect(eventRangeError(range)).not.toBe('')
      expect(() => eventDateParameter(range)).toThrow()
    }
  })

  it('快捷范围锚定数据末端，完整窗口可含最后一秒', () => {
    expect(eventDataRange({ start: window.start, end: '2026-03-31T23:59:59' })).toEqual(window)
    expect(eventDateParameter(window)).toBe('2026-02-01 00:00:00_2026-03-31 23:59:59')
    expect(eventPresetRange(7, window)).toEqual({ start: '2026-03-25T00:00:00', end: window.end })
    expect(eventPresetRange(30, { start: '2026-02-27T08:00:00', end: '2026-03-02T08:00:00' })).toEqual({ start: '2026-02-27T08:00:00', end: '2026-03-02T08:00:00' })
    expect(eventDataRange(null)).toBeNull()
    expect(eventPresetRange(7, { start: '', end: '' })).toEqual({ start: '', end: '' })
  })

  it('浏览器处于夏令时切换区时仍按北京时间解释', () => {
    const previous = process.env.TZ
    process.env.TZ = 'America/New_York'
    try {
      const range = { start: '2026-03-08T02:30:00', end: '2026-03-09T02:30:00' }
      expect(eventRangeError(range)).toBe('')
      expect(eventDateParameter(range)).toBe('2026-03-08 02:30:00_2026-03-09 02:29:59')
      expect(eventPresetRange(1, { start: window.start, end: range.end })).toEqual(range)
    } finally {
      if (previous === undefined) delete process.env.TZ
      else process.env.TZ = previous
    }
  })
})

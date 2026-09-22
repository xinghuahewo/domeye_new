import { describe, expect, it } from 'vitest'

import {
  rangeFromQuery,
  eventDateTimeRange,
  parseInputTime,
  recentDateRange,
  recentRange,
  resolveDataWindow,
  toBackendTime,
} from './time'

const developmentWindow = {
  VITE_DATA_WINDOW_START: '2026-02-01T00:00:00',
  VITE_DATA_WINDOW_END: '2026-03-31T23:59:59',
}

describe('开发数据时间窗口', () => {
  it('事件日期范围转换为数据库接受的秒级边界', () => {
    expect(eventDateTimeRange('2026-03-25', '2026-03-31')).toBe(
      '2026-03-25 00:00:00_2026-03-31 23:59:59',
    )
  })

  it('解析合法的固定开发窗口', () => {
    expect(resolveDataWindow(developmentWindow)).toEqual({
      start: '2026-02-01T00:00:00',
      end: '2026-03-31T23:59:59',
    })
  })

  it('拒绝缺失、逆序或格式错误的窗口', () => {
    expect(resolveDataWindow({})).toBeNull()
    expect(resolveDataWindow({
      VITE_DATA_WINDOW_START: '2026-04-01T00:00:00',
      VITE_DATA_WINDOW_END: '2026-03-31T23:59:59',
    })).toBeNull()
    expect(resolveDataWindow({
      VITE_DATA_WINDOW_START: 'not-a-time',
      VITE_DATA_WINDOW_END: '2026-03-31T23:59:59',
    })).toBeNull()
    expect(resolveDataWindow({
      VITE_DATA_WINDOW_START: '2026-02-30T00:00:00',
      VITE_DATA_WINDOW_END: '2026-03-31T23:59:59',
    })).toBeNull()
    expect(resolveDataWindow({
      VITE_DATA_WINDOW_START: '2026-02-01T00:00:00',
      VITE_DATA_WINDOW_END: '2026-03-31T24:00:00',
    })).toBeNull()
    expect(resolveDataWindow({
      VITE_DATA_WINDOW_START: '2026-02-01T00:00',
      VITE_DATA_WINDOW_END: '2026-03-31T23:59',
    })).toBeNull()
  })

  it('严格解析日历时间并保留后端秒精度', () => {
    expect(parseInputTime('2026-02-29T00:00:00')).toBeNull()
    expect(parseInputTime('2028-02-29T00:00:00')).not.toBeNull()
    expect(parseInputTime('2026-03-31T23:59')).not.toBeNull()
    expect(toBackendTime('2026-03-31T23:59:59')).toBe('2026-03-31 23:59:59')
    expect(toBackendTime('2026-03-31T23:59')).toBe('2026-03-31 23:59:00')
  })

  it('以开发窗口终点生成 24 小时和 7 日默认范围', () => {
    expect(recentRange(24, developmentWindow)).toEqual({
      start: '2026-03-30T23:59:59',
      end: '2026-03-31T23:59:59',
    })
    expect(recentDateRange(7, developmentWindow)).toEqual({
      start: '2026-03-24',
      end: '2026-03-31',
    })
  })

  it('超过窗口长度时将起点截断到二月一日且不丢失终点秒数', () => {
    expect(recentRange(24 * 90, developmentWindow)).toEqual({
      start: '2026-02-01T00:00:00',
      end: '2026-03-31T23:59:59',
    })
  })

  it('近 30 天入口以固定窗口终点计算，不读取浏览器当前日期', () => {
    expect(recentDateRange(30, developmentWindow)).toEqual({
      start: '2026-03-01',
      end: '2026-03-31',
    })
  })
})


it('结果链接使用明确时间窗，无效链接保留默认范围', () => {
  const fallback = { start: '2026-03-30T00:00:00', end: '2026-03-31T00:00:00' }
  expect(rangeFromQuery('2026-02-24T08:00:00', '2026-02-24T11:35:00', fallback)).toEqual({ start: '2026-02-24T08:00:00', end: '2026-02-24T11:35:00' })
  expect(rangeFromQuery('bad', '2026-02-24T11:35:00', fallback)).toEqual(fallback)
})

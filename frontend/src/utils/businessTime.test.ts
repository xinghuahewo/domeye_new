import { describe, expect, it } from 'vitest'
import { toBusinessTime, businessTimeToIso, formatBusinessEndTime } from './businessTime'

describe('事件窗口的业务时间', () => {
  it('UTC 事件边界按数据档转换，保留跨日、跨月和秒精度', () => {
    expect(toBusinessTime(new Date('2026-02-28T16:00:01Z'))).toBe('2026-03-01 00:00:01')
    expect(toBusinessTime(new Date('2026-03-31T15:59:59Z'))).toBe('2026-03-31 23:59:59')
    expect(businessTimeToIso('2026-03-01 00:00:01')).toBe('2026-02-28T16:00:01.000Z')
  })
  it('结束时间跨业务日时带日期，同日可简写，详情始终显示日期', () => {
    const start = '2026-02-27T15:55:00Z'
    expect(formatBusinessEndTime('2026-02-27T15:59:00Z', start)).toBe('23:59:00')
    expect(formatBusinessEndTime('2026-02-27T16:05:00Z', start)).toBe('2026-02-28 00:05:00')
    expect(formatBusinessEndTime('2026-02-28T16:05:00Z', start)).toBe('2026-03-01 00:05:00')
    expect(formatBusinessEndTime('2026-02-27T15:59:00Z', start, true)).toBe('2026-02-27 23:59:00')
    // UTC 跨日但业务日期相同，不应误判为跨业务日。
    expect(formatBusinessEndTime('2026-02-28T00:05:00Z', '2026-02-27T23:55:00Z')).toBe('08:05:00')
  })
})

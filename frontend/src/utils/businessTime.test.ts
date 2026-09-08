import { describe, expect, it } from 'vitest'
import { toBusinessTime, businessTimeToIso } from './businessTime'

describe('事件窗口的业务时间', () => {
  it('UTC 事件边界按数据档转换，保留跨日、跨月和秒精度', () => {
    expect(toBusinessTime(new Date('2026-02-28T16:00:01Z'))).toBe('2026-03-01 00:00:01')
    expect(toBusinessTime(new Date('2026-03-31T15:59:59Z'))).toBe('2026-03-31 23:59:59')
    expect(businessTimeToIso('2026-03-01 00:00:01')).toBe('2026-02-28T16:00:01.000Z')
  })
})

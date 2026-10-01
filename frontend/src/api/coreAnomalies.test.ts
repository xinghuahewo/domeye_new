import { beforeEach, expect, it, vi } from 'vitest'
import { getAnomalySummary } from './coreAnomalies'
import type { CoreOverview, CoreOverviewItem } from './coreOverview'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('./coreOverview', () => ({ getCoreOverview: get }))
const base = { state: 'available', version: 'same-version', metadata: { kinds: ['prefix_outage', 'leak'] },
  query: { date: '2026-03-31', start: '2026-03-31T00:00:00+08:00', end_exclusive: '2026-04-01T00:00:00+08:00', family: 'all', kind: 'prefix_outage', hour: null, level: 'all', q: '' } } as CoreOverview
const item = (id: number, time = '2026-03-30T16:12:00Z') => ({ reference: `ref-${id}`, kind: 'prefix_outage',
  object: '192.0.2.0/24', start_time: time }) as CoreOverviewItem
const page = (items: CoreOverviewItem[], total = items.length, number = 1) => ({ ...base,
  events: { items, total, page: number, page_size: 100 } })
beforeEach(() => { get.mockReset() })

it('同一前缀的重复异常计作记录条数，并按业务时区分桶', async () => {
  get.mockResolvedValue(page([item(1), item(2), item(3, '2026-03-31T15:59:00Z')]))
  const result = await getAnomalySummary(base, 'prefix_outage')
  expect(result.count).toBe(3)
  expect(result.hours?.[0]).toBe(2)
  expect(result.hours?.[23]).toBe(1)
  expect(result.hours?.reduce((a, b) => a + b, 0)).toBe(3)
  expect(get).toHaveBeenCalledWith(expect.objectContaining({ version: 'same-version', date: '2026-03-31',
    family: 'all', kind: 'prefix_outage', level: 'all', sort: 'time', page: 1 }), undefined)
})

it('零条与未接入、日数据不可用分别处理', async () => {
  get.mockResolvedValue(page([]))
  expect(await getAnomalySummary(base, 'prefix_outage')).toMatchObject({ count: 0, hours: Array(24).fill(0) })
  expect(await getAnomalySummary(base, 'hijack')).toMatchObject({ count: null, hours: null, note: '此版本尚未接入' })
  expect(await getAnomalySummary({ ...base, state: 'unavailable' }, 'prefix_outage')).toMatchObject({ count: null, hours: null })
  expect(get).toHaveBeenCalledTimes(1)
})

it('完整合并分页，大类别查询各小时计数，不下载整日明细', async () => {
  get.mockResolvedValueOnce(page(Array.from({ length: 100 }, (_, i) => item(i)), 101))
    .mockResolvedValueOnce(page([item(100)], 101, 2))
  expect((await getAnomalySummary(base, 'prefix_outage')).hours?.[0]).toBe(101)
  get.mockReset().mockImplementation(params => {
    if (params.hour === undefined) return page([item(1)], 1001)
    return { ...page([], params.hour === 9 ? 1001 : 0), query: { ...base.query, hour: params.hour } }
  })
  const large = await getAnomalySummary(base, 'prefix_outage')
  expect(large.count).toBe(1001)
  expect(large.hours?.[9]).toBe(1001)
  expect(get).toHaveBeenCalledTimes(25)
  expect(get.mock.calls.slice(1).every(([params]) => params.page_size === 1)).toBe(true)
})

it('日期、版本、重复记录或分页缺项不生成看似完整的趋势', async () => {
  const failures = [
    { ...page([item(1)]), version: 'changed-version' },
    page([item(1, '2026-03-31T16:00:00Z')]),
    page([item(1), item(1)]),
    page([item(1)], 2),
  ]
  for (const result of failures) {
    get.mockResolvedValue(result)
    await expect(getAnomalySummary(base, 'prefix_outage')).rejects.toThrow()
  }
})

it('切换日期取消后停止后续分页', async () => {
  const controller = new AbortController()
  get.mockImplementationOnce(() => {
    controller.abort()
    return page(Array.from({ length: 100 }, (_, i) => item(i)), 101)
  })
  await expect(getAnomalySummary(base, 'prefix_outage', controller.signal)).rejects.toThrow()
  expect(get).toHaveBeenCalledTimes(1)
})

it('部分交付窗口只给本批计数，不给窗口外补零小时', async () => {
  const partial = { ...base, metadata: { ...base.metadata, result_delivery: { state: 'available', coverage: 'partial_window' } } } as CoreOverview
  get.mockResolvedValue(page([item(1)], 30))
  expect(await getAnomalySummary(partial, 'prefix_outage')).toEqual({ count: 30, hours: null, note: '仅已交付时段；全天其余时段未知' })
  expect(get).toHaveBeenCalledTimes(1)
})

it('新 API 的部分时段直接使用已覆盖分桶，不额外分页或补全天零值', async () => {
  const buckets = [{ start: '2026-03-31T00:00:00Z', end_exclusive: '2026-03-31T01:00:00Z', value: 2 },
    { start: '2026-03-31T01:00:00Z', end_exclusive: '2026-03-31T01:35:00Z', value: 0 }]
  const partial = { ...base, metadata: { ...base.metadata, result_delivery: { coverage: 'partial_window' } },
    event_trends: { state: 'available', metric: 'recorded_event_starts', filter_scope: 'date_and_family',
      bucket_seconds: 3600, series: [{ kind: 'prefix_outage', total: 2, buckets }] } } as CoreOverview
  expect(await getAnomalySummary(partial, 'prefix_outage')).toMatchObject({ count: 2, hours: [2, 0], buckets })
  expect(get).not.toHaveBeenCalled()
  partial.event_trends!.series[0]!.total = 3
  await expect(getAnomalySummary(partial, 'prefix_outage')).rejects.toThrow('总数')
})

it('国家跨日趋势只取同范围分桶，保留缺口而不退回全球单日查询', async () => {
  const buckets = [{ start: '2026-02-20T08:00:00+08:00', end_exclusive: '2026-02-20T12:00:00+08:00', value: 2 },
    { start: '2026-02-24T08:00:00+08:00', end_exclusive: '2026-02-24T11:35:00+08:00', value: 0 }]
  const ranged = { ...base,
    query: { ...base.query, date: '2026-02-20', start: '2026-02-20T00:00:00+08:00', end_exclusive: '2026-02-27T00:00:00+08:00', window_mode: 'range', country: '伊朗' },
    event_trends: { state: 'available', metric: 'recorded_event_starts', filter_scope: 'window_country_and_family',
      bucket_seconds: 21600, series: [{ kind: 'prefix_outage', total: 2, buckets }] } } as CoreOverview
  expect(await getAnomalySummary(ranged, 'prefix_outage')).toMatchObject({ count: 2, hours: [2, 0], buckets, note: '每 6 小时新增 · 仅已覆盖时段' })
  expect(get).not.toHaveBeenCalled()
  ranged.event_trends!.filter_scope = 'date_and_family'
  await expect(getAnomalySummary(ranged, 'prefix_outage')).rejects.toThrow('定义不一致')
  ranged.event_trends!.filter_scope = 'window_country_and_family'
  buckets[1]!.end_exclusive = '2026-02-27T00:00:01+08:00'
  await expect(getAnomalySummary(ranged, 'prefix_outage')).rejects.toThrow('超出日期')
})

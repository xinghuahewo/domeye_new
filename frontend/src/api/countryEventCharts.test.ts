import { beforeEach, describe, expect, it, vi } from 'vitest'
import { getCountryEventSeries } from './countryEventCharts'
import { apiGetWithResultMetadata } from './client'
import { businessTimeToIso } from '@/utils/businessTime'
import { normalizeOutagePoints } from '@/utils/normalize'

vi.mock('./client', () => ({ apiGetWithResultMetadata: vi.fn() }))

const range = { start_time: '2026-03-01 19:15:00', end_time: '2026-03-01 19:27:00' }
const reply = () => ({
  result: { state: 'available', version: 'delivery_test', start: '2026-02-27T00:00:00Z', 'end-exclusive': '2026-03-01T11:20:00Z' },
  data: {
    query: { start: '2026-03-01T19:15:00+08:00', end_exclusive: '2026-03-01T19:27:00+08:00', country: '测试地区' },
    metadata: { version: 'delivery_test', unit: 'asn', coverage: { state: 'partial', intervals: [
      { start: '2026-03-01T19:15:00+08:00', end_exclusive: '2026-03-01T19:20:00+08:00' },
    ] } },
    data: [
      { time_slot: '2026-03-01T19:15:00+08:00', outage_count: 1, observation_state: 'observed' },
      { time_slot: '2026-03-01T19:18:00+08:00', outage_count: 0, observation_state: 'observed' },
      { time_slot: '2026-03-01T19:21:00+08:00', outage_count: null, observation_state: 'not_observed' },
      { time_slot: '2026-03-01T19:24:00+08:00', outage_count: null, observation_state: 'not_observed' },
    ],
  },
})

beforeEach(() => vi.clearAllMocks())

describe('中断响应到详情图表', () => {
  it('拒绝旧裸数组，避免把未升级 API 当作已核对覆盖的曲线', async () => {
    const response = reply()
    vi.mocked(apiGetWithResultMetadata).mockResolvedValue({ result: response.result, data: response.data.data })
    await expect(getCountryEventSeries('as', '测试地区', range, 'delivery_test')).rejects.toThrow('匹配的 API')
  })

  it('保留真实零、未处理点和排他截止的断线边界', async () => {
    vi.mocked(apiGetWithResultMetadata).mockResolvedValue(reply())
    const result = await getCountryEventSeries('as', '测试地区', range, 'delivery_test')
    expect(result.series.count).toEqual([
      ['2026-03-01T11:15:00.000Z', 1], ['2026-03-01T11:18:00.000Z', 0],
      ['2026-03-01T11:20:00.000Z', null], ['2026-03-01T11:21:00.000Z', null],
      ['2026-03-01T11:24:00.000Z', null],
    ])
  })

  it.each(['features', 'as', 'prefix'] as const)('拒绝把更新版本的 %s 图叠到旧版事件上', async (kind) => {
    const response = reply()
    if (kind === 'prefix') response.data.metadata.unit = 'prefix'
    vi.mocked(apiGetWithResultMetadata).mockResolvedValue(response)
    await expect(getCountryEventSeries(kind, '测试地区', range, 'older_event')).rejects.toThrow('事件版本')
  })

  it('不足一个采样间隔的内部缺口也不能连线', () => {
    const payload = reply().data
    payload.metadata.coverage.intervals = [
      { start: '2026-03-01T19:15:00+08:00', end_exclusive: '2026-03-01T19:16:00+08:00' },
      { start: '2026-03-01T19:17:00+08:00', end_exclusive: '2026-03-01T19:27:00+08:00' },
    ]
    const points = normalizeOutagePoints(payload)
    expect(points.slice(0, 3).map((point) => point.count)).toEqual([1, null, 0])
    expect(points[1]?.time).toBe('2026-03-01T19:16:00+08:00')
  })

  it.each(['version', 'unit', 'country', 'window'])('拒绝 %s 不一致的响应', async (field) => {
    const response = reply()
    if (field === 'version') response.data.metadata.version = 'another'
    if (field === 'unit') response.data.metadata.unit = 'prefix'
    if (field === 'country') response.data.query.country = '另一地区'
    if (field === 'window') response.data.query.start = '2026-03-01T19:12:00+08:00'
    vi.mocked(apiGetWithResultMetadata).mockResolvedValue(response)
    await expect(getCountryEventSeries('as', '测试地区', range, 'delivery_test')).rejects.toThrow('不一致')
  })

  it('兼容已有时区和旧本地时间，不重复追加偏移', () => {
    for (const time of ['2026-03-01T19:15:00+08:00', '2026-03-01 19:15:00', '2026-03-01T11:15:00Z']) {
      expect(businessTimeToIso(time)).toBe('2026-03-01T11:15:00.000Z')
    }
  })
})

it('单国 Feature 曲线直读时序，保留零、未知字段与处理缺口', async () => {
  const response = reply()
  const data = {
    query: response.data.query,
    metadata: { ...response.data.metadata, interpretation_version: 'country-feature-series/v2', sample_seconds: 300,
      time_basis: { time: 'source_file_label', activity: 'source_window', resource: 'resource_state_at' },
      units: { announce: 'accepted_route_element', withdraw: 'accepted_route_element', ipv4_addresses: 'ipv4_address', ipv4_prefixes: 'ipv4_24_covered_block', ipv6_prefixes: 'ipv6_48_covered_block' },
      coverage: { state: 'partial', intervals: [
        { start: '2026-03-01T19:15:00+08:00', end_exclusive: '2026-03-01T19:16:00+08:00' },
        { start: '2026-03-01T19:17:00+08:00', end_exclusive: '2026-03-01T19:20:00+08:00' },
      ] },
    },
    data: [{ time: '2026-03-01T19:15:00+08:00', announce: 0, withdraw: null, ipv4_addresses: 2048, ipv4_prefixes: 8, ipv6_prefixes: 2,
      source_window: { source_id: '测试文件', start: '2026-03-01T19:15:00+08:00', end_exclusive: '2026-03-01T19:16:00+08:00' },
      resource_state_at: '2026-03-01T19:16:00+08:00' }],
  }
  vi.mocked(apiGetWithResultMetadata).mockResolvedValue({ result: response.result, data })
  const result = await getCountryEventSeries('features', '测试地区', range, 'delivery_test')
  expect(apiGetWithResultMetadata).toHaveBeenCalledWith('features/countries/series', { params: { country: '测试地区', ...range, version: 'delivery_test' } })
  expect(result.series.announce).toEqual([
    ['2026-03-01T11:15:00.000Z', 0], ['2026-03-01T11:16:00.000Z', null],
    ['2026-03-01T11:20:00.000Z', null], ['2026-03-01T11:25:00.000Z', null],
  ])
  expect(result.series.withdraw?.[0]?.[1]).toBeNull()
  expect(result.series.ipv4Addresses?.[0]?.[1]).toBe(2048)
  expect(result.series.ipv4Addresses?.[0]?.[0]).toBe('2026-03-01T11:16:00.000Z')
  expect(result.series.ipv4Addresses?.[1]).toEqual(['2026-03-01T11:16:30.000Z', null])
  expect(result.series.ipv4Prefixes?.[0]?.[1]).toBe(8)
  data.metadata.units.announce = 'message'
  await expect(getCountryEventSeries('features', '测试地区', range, 'delivery_test')).rejects.toThrow('单位')
  data.metadata.units.announce = 'accepted_route_element'
  data.metadata.units.ipv4_prefixes = 'prefix'
  await expect(getCountryEventSeries('features', '测试地区', range, 'delivery_test')).rejects.toThrow('单位')
})

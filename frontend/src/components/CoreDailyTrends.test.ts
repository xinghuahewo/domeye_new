import { beforeEach, expect, it, vi } from 'vitest'
import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import CoreDailyTrends from './CoreDailyTrends.vue'

const { get, resource, chart } = vi.hoisted(() => ({ get: vi.fn(), resource: vi.fn(), chart: vi.fn() }))
vi.mock('@/api/features', () => ({ getTopFeatures: get }))
vi.mock('@/api/resources', () => ({ getResources: resource }))
vi.mock('./LineChart.vue', () => ({ default: { props: ['series', 'timeBounds'], setup(props: unknown) { chart(props); return () => null } } }))
beforeEach(() => { get.mockReset(); chart.mockReset(); resource.mockReset().mockResolvedValue({ state: 'not_calculated', points: [], message: '此日没有 RIB 资源统计' }) })
const render = () => renderToString(h(CoreDailyTrends, { date: '2026-03-31', refreshKey: 0 }))

it('只读所选业务日，Feature 单位分开，Resource 未计算保持缺失', async () => {
  get.mockResolvedValue([{ time: '2026-03-31T15:55:00Z', announce: 9, withdraw: 2, ipv4Prefixes: 5, ipv6Prefixes: 0, ipv4Addresses: 1280 }])
  const html = await render()
  expect(get).toHaveBeenCalledWith('collector', { start_time: '2026-03-31 00:00:00', end_time: '2026-03-31 23:59:59' }, expect.any(AbortSignal))
  expect(html).toContain('末值 5')
  expect(html).toContain('末值 0')
  expect(html).toContain('/24 等价量')
  expect(html).toContain('/48 等价量')
  expect(html).toContain('此日没有 RIB 资源统计')
  expect(resource).toHaveBeenCalledWith({ start_time: '2026-03-31 00:00:00', end_time: '2026-04-01 00:00:00' }, expect.any(AbortSignal))
  expect(html).toContain('末次采样 23:55:00')
})

it('资源独立时点只使用主值，真实零、未限定主值与读取失败分别显示', async () => {
  get.mockResolvedValue([])
  resource.mockResolvedValue({ state: 'available', points: [{ observed_at: '2026-03-31T00:00:00Z',
    metrics: { ipv4_prefix_count: { main: 12 }, ipv6_48_count: { main: 0 }, public_as_count: { main: null, raw: 99 } } }] })
  const html = await render()
  expect(html).toContain('1 个 RIB 时点 · 末次 08:00:00 · 时点之间未知')
  expect(html).toContain('末值 12'); expect(html).toContain('末值 0')
  expect(html).toContain('该指标主值不可用'); expect(html).not.toContain('末值 99')
  resource.mockRejectedValue(new Error('network'))
  expect(await render()).toContain('此日资源统计读取失败')
})

it('末值缺失不能拿此前资源值充作当前值', async () => {
  get.mockResolvedValue([
    { time: '2026-03-31T00:00:00Z', announce: 0, withdraw: 0, ipv4Prefixes: 9, ipv6Prefixes: null, ipv4Addresses: 0 },
    { time: '2026-03-31T00:05:00Z', announce: null, withdraw: null, ipv4Prefixes: null, ipv6Prefixes: null, ipv4Addresses: null },
  ])
  const html = await render()
  expect(html).toContain('末值 —')
  expect(html).not.toContain('末值 9')
  expect(html).toContain('此日未返回该指标')
})

it('空日和响应失败不填零，跨日数据不混入曲线', async () => {
  get.mockResolvedValue([])
  expect(await render()).toContain('此日没有可用特征采样')
  get.mockRejectedValue(new Error('network'))
  expect(await render()).toContain('此日特征读取失败')
  get.mockResolvedValue([{ time: '2026-04-01T00:00:00Z', announce: 99 }])
  expect(await render()).toContain('此日特征读取失败')
})

it('跨日地区查询使用同一半开窗口，不借用全球 Resource', async () => {
  get.mockResolvedValue([{ time: '2026-02-24T03:30:00Z', announce: 2, withdraw: 0, ipv4Prefixes: 10, ipv6Prefixes: null }])
  const html = await renderToString(h(CoreDailyTrends, { date: '2026-02-23', start: '2026-02-23T11:35:00', end: '2026-02-24T11:35:00', country: '伊朗', refreshKey: 0 }))
  expect(get).toHaveBeenCalledWith('伊朗', { start_time: '2026-02-23 11:35:00', end_time: '2026-02-24 11:34:59' }, expect.any(AbortSignal))
  expect(resource).not.toHaveBeenCalled()
  expect(html).toContain('末值 10')
  expect(html).toContain('地区 RIB 统计尚未生成')
  expect(html).toContain('伊朗')
})

it('地区区间查询拒绝右端点与窗口外采样', async () => {
  get.mockResolvedValue([{ time: '2026-02-24T03:35:00Z', announce: 99 }])
  const html = await renderToString(h(CoreDailyTrends, { date: '2026-02-23', start: '2026-02-23T11:35:00', end: '2026-02-24T11:35:00', country: '中国', refreshKey: 0 }))
  expect(html).toContain('此区间特征读取失败')
  expect(html).not.toContain('末值 99')
})

it('多日曲线保留所选完整横轴，采样缺口断线且真实零仍可见', async () => {
  get.mockResolvedValue([
    { time: '2026-02-24T00:00:00Z', announce: 3, withdraw: 0, ipv4Prefixes: 10, ipv6Prefixes: null },
    { time: '2026-02-24T00:15:00Z', announce: 0, withdraw: 1, ipv4Prefixes: 0, ipv6Prefixes: null },
  ])
  await renderToString(h(CoreDailyTrends, { date: '2026-02-20', start: '2026-02-20T00:00:00', end: '2026-02-27T00:00:00', country: '伊朗', refreshKey: 0 }))
  expect(get).toHaveBeenCalledWith('伊朗', { start_time: '2026-02-20 00:00:00', end_time: '2026-02-26 23:59:59' }, expect.any(AbortSignal))
  expect(chart.mock.calls[0]![0]).toMatchObject({
    timeBounds: ['2026-02-20T00:00:00+08:00', '2026-02-27T00:00:00+08:00'],
    series: [{ name: '宣告', data: [['2026-02-24T00:00:00Z', 3], ['2026-02-24T00:05:00.000Z', null], ['2026-02-24T00:15:00Z', 0]] },
      { name: '撤回', data: [['2026-02-24T00:00:00Z', 0], ['2026-02-24T00:05:00.000Z', null], ['2026-02-24T00:15:00Z', 1]] }],
  })
})

it('非法或倒置范围不发起图表请求', async () => {
  const html = await renderToString(h(CoreDailyTrends, { date: '2026-02-24', start: '2026-02-24T11:35:00', end: '2026-02-24T11:35:00', refreshKey: 0 }))
  expect(get).not.toHaveBeenCalled()
  expect(resource).not.toHaveBeenCalled()
  expect(html).toContain('结束时间须晚于开始时间')
})

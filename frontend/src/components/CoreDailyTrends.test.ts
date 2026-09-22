import { beforeEach, expect, it, vi } from 'vitest'
import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import CoreDailyTrends from './CoreDailyTrends.vue'

const { get, resource } = vi.hoisted(() => ({ get: vi.fn(), resource: vi.fn() }))
vi.mock('@/api/features', () => ({ getTopFeatures: get }))
vi.mock('@/api/resources', () => ({ getResources: resource }))
beforeEach(() => { get.mockReset(); resource.mockReset().mockResolvedValue({ state: 'not_calculated', points: [], message: '此日没有 RIB 资源统计' }) })
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

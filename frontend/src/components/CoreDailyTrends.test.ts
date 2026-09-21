import { beforeEach, expect, it, vi } from 'vitest'
import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import CoreDailyTrends from './CoreDailyTrends.vue'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('@/api/features', () => ({ getTopFeatures: get }))
beforeEach(() => { get.mockReset() })
const render = () => renderToString(h(CoreDailyTrends, { date: '2026-03-31', refreshKey: 0 }))

it('只读所选业务日，Feature 单位分开，Resource 明确待接入', async () => {
  get.mockResolvedValue([{ time: '2026-03-31T15:55:00Z', announce: 9, withdraw: 2, ipv4Prefixes: 5, ipv6Prefixes: 0, ipv4Addresses: 1280 }])
  const html = await render()
  expect(get).toHaveBeenCalledWith('collector', { start_time: '2026-03-31 00:00:00', end_time: '2026-03-31 23:59:59' }, expect.any(AbortSignal))
  expect(html).toContain('末值 5')
  expect(html).toContain('末值 0')
  expect(html).toContain('/24 等价量')
  expect(html).toContain('/48 等价量')
  expect(html).toContain('Resource 时序待接入')
  expect(html).toContain('末次采样 23:55:00')
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

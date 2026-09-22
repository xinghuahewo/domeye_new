import { beforeEach, expect, it, vi } from 'vitest'
import { getResources } from './resources'
const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('./client', () => ({ apiGet: get }))
const units = ['distinct_ipv4_prefix', 'covered_ipv6_48_block', 'covered_ipv6_48_block', 'covered_ipv4_24_block_times_256',
  'distinct_first_path_asn', 'distinct_legacy_private_tail_asn', 'distinct_legacy_public_tail_asn', 'distinct_rendered_path']
const keys = ['ipv4_prefix_count', 'ipv6_prefix_count', 'ipv6_48_count', 'ipv4_address_count', 'vp_count', 'private_as_count', 'public_as_count', 'path_count']
const range = { start_time: '2026-02-24 08:00:00', end_time: '2026-02-24 11:35:00' }
const point = () => ({ snapshot_id: `rib_statistics_v1_${'a'.repeat(64)}`, observed_at: '2026-02-24T00:00:00Z',
  metrics: Object.fromEntries(keys.map((key, i) => [key, { main: 0, qualification: 'qualified', unit: units[i], reason: 'verified_independent_rib' }])),
  metadata: { source_sha256: 'b'.repeat(64), rule: 'resource-39578fe-v1' } })
const response = () => ({ state: 'available', points: [point()], query: { start: '2026-02-24T08:00:00+08:00',
  end_exclusive: '2026-02-24T11:35:00+08:00', scope: 'global', timezone: 'Asia/Shanghai', window_boundary: '[start,end)' } })
beforeEach(() => { get.mockReset() })

it('独立时点允许真实零，不接受右端点、错单位或未限定的主值', async () => {
  get.mockResolvedValue(response()); expect((await getResources(range)).points).toHaveLength(1)
  for (const mutate of [
    (p: ReturnType<typeof point>) => { p.observed_at = '2026-02-24T03:35:00Z' },
    (p: ReturnType<typeof point>) => { p.metrics.ipv4_prefix_count!.unit = 'covered_ipv6_48_block' },
    (p: ReturnType<typeof point>) => { p.metrics.public_as_count!.qualification = 'unknown' },
  ]) {
    const data = response(); mutate(data.points[0]!); get.mockResolvedValue(data)
    await expect(getResources(range)).rejects.toThrow('资源统计')
  }
})

it('空序列必须说明未计算，不能把 available 的空数据或读取错误当零', async () => {
  get.mockResolvedValue({ ...response(), state: 'not_calculated', points: [] })
  expect((await getResources(range)).state).toBe('not_calculated')
  get.mockResolvedValue({ ...response(), points: [] }); await expect(getResources(range)).rejects.toThrow()
  get.mockRejectedValue(new Error('503')); await expect(getResources(range)).rejects.toThrow('503')
})

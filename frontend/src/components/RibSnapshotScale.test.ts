import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { beforeEach, expect, it, vi } from 'vitest'
import RibSnapshotScale from './RibSnapshotScale.vue'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('axios', () => ({ default: { create: () => ({ get, interceptors: { response: { use: vi.fn() } } }) } }))

const version = `rib_snapshot_v1_${'a'.repeat(64)}`
const summary = {
  state: 'available', version, date: '2026-02-27', observed_at: '2026-02-27T00:00:00Z', family: 'all',
  source: { collector_id: 'rrc25', sha256: 'b'.repeat(64), coverage: 'unknown' },
  origin_rule: 'rib-attributed-origin/private-skip-v1', unit: 'distinct_prefix_and_origin_asn',
  metrics: { visible_prefixes: 5, visible_origin_ases: 2, attributed_prefixes: 3, unattributed_prefixes: 2,
    rib_entries: 9, unattributed_entries: 4 }, limitations: ['单RIB时点，不是连续RouteState。'],
}
beforeEach(() => get.mockReset())

it('实际规模组件显示共享版本的值、时点、归属对账和Unknown', async () => {
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [{ date: summary.date, observed_at: summary.observed_at, version }] } })
    .mockResolvedValueOnce({ data: summary })
  const html = await renderToString(h(RibSnapshotScale, { date: '2026-02-27', family: 'all' }))
  expect(html).toMatch(/data-testid="core-prefix-count"[^>]*>5 /)
  expect(html).toMatch(/data-testid="core-origin-count"[^>]*>2 /)
  expect(html).toContain('2026-02-27 08:00:00')
  expect(html).toContain(version)
  expect(html).toContain('至少一个明确起源的前缀：3')
  expect(html).toContain('完全无明确起源的前缀：2')
  expect(html).toContain('不可归属观察：4')
  expect(html).toContain('Unknown')
  expect(get).toHaveBeenLastCalledWith(`rib-snapshots/${version}`, expect.objectContaining({ params: { family: 'all' } }))
})

it('仅明确未配置时沿用旧规模；已配置缺日或损坏不能回退旧数值', async () => {
  const render = () => renderToString(h(RibSnapshotScale, { date: '2026-02-27', family: 'all' },
    { default: () => h('span', '旧规模 999') }))
  get.mockResolvedValueOnce({ data: { state: 'not_configured', message: '未配置' } })
  expect(await render()).toContain('旧规模 999')
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [] } })
  const missing = await render()
  expect(missing).not.toContain('旧规模 999')
  expect(missing).toContain('此日无已登记的 RIB 快照')
  get.mockRejectedValueOnce({ response: { status: 503, data: { state: 'unavailable' } } })
  const broken = await render()
  expect(broken).not.toContain('旧规模 999')
  expect(broken).toContain('共享快照校验或读取失败')
})

it('发现后只读同一版本；返回其他版本或不闭合计数时不展示数值', async () => {
  for (const value of [{ ...summary, version: `rib_snapshot_v1_${'c'.repeat(64)}` },
    { ...summary, metrics: { ...summary.metrics, attributed_prefixes: 4 } }]) {
    get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [{ date: summary.date, observed_at: summary.observed_at, version }] } })
      .mockResolvedValueOnce({ data: value })
    const html = await renderToString(h(RibSnapshotScale, { date: '2026-02-27', family: 'all' }))
    expect(html).not.toMatch(/data-testid="core-prefix-count"[^>]*>5 /)
    expect(html).toContain('共享快照不可用')
  }
})

it('首页ASN下钻链接携带实际RIB版本、业务日和地址族', async () => {
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [{ date: summary.date, observed_at: summary.observed_at, version }] } })
    .mockResolvedValueOnce({ data: { ...summary, family: 'ipv6' } })
  const html = await renderToString(h(RibSnapshotScale, { date: '2026-02-27', family: 'ipv6' }))
  expect(html).toContain(`/ases?snapshot_version=${version}&amp;snapshot_date=2026-02-27&amp;snapshot_family=ipv6`)
  expect(html).toContain('查看同一快照的 ASN')
})

it.each([['not_calculated', '尚未计算'], ['missing_input', '缺少输入'], ['validation_failed', '输入校验失败']])('首页缺日保留%s语义', async (state, message) => {
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [], days: [{ date: '2026-02-27', state }] } })
  const html = await renderToString(h(RibSnapshotScale, { date: '2026-02-27', family: 'all' }))
  expect(html).toContain(message)
  expect(html).toContain('Unknown')
  expect(html).not.toMatch(/data-testid="core-prefix-count"[^>]*>0 /)
})

it('首页保留已发布数值并单独提示最近批次缺口', async () => {
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [{ date: summary.date, observed_at: summary.observed_at, version }],
    days: [{ date: summary.date, state: 'available', version, last_batch: { selection_id: `rib_batch_v1_${'b'.repeat(64)}`, state: 'missing_input' } }] } })
    .mockResolvedValueOnce({ data: summary })
  const html = await renderToString(h(RibSnapshotScale, { date: '2026-02-27', family: 'all' }))
  expect(html).toMatch(/data-testid="core-prefix-count"[^>]*>5 /)
  expect(html).toContain('最近批次：缺少输入；当前显示此前已发布快照')
})

it('首页旧URL指定版本时直接读取该版本，不重新发现默认', async () => {
  get.mockResolvedValueOnce({ data: summary })
  const html = await renderToString(h(RibSnapshotScale, { date: '2026-02-27', family: 'all', version }))
  expect(html).toMatch(/data-testid="core-prefix-count"[^>]*>5 /)
  expect(get).toHaveBeenCalledTimes(1)
  expect(get).toHaveBeenCalledWith(`rib-snapshots/${version}`, expect.anything())
  expect(html).toContain('重新选择此日已发布快照')
})

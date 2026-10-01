import { createSSRApp, h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, expect, it, vi } from 'vitest'
import AsnRibSnapshot from './AsnRibSnapshot.vue'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('axios', () => ({ default: { create: () => ({ get, interceptors: { response: { use: vi.fn() } } }) } }))
const version = `rib_snapshot_v1_${'a'.repeat(64)}`
const data = {
  state: 'available', version, date: '2026-02-27', observed_at: '2026-02-27T00:00:00Z', family: 'ipv4', asn: 13335,
  source: { collector_id: 'rrc25', sha256: 'b'.repeat(64), coverage: 'unknown' },
  origin_rule: 'rib-attributed-origin/private-skip-v1', unit: 'distinct_origin_prefix', prefix_count: 2,
  sample_limit: 20, sample_truncated: false, items: [], limitations: ['单RIB时点，不是连续RouteState。'],
}
beforeEach(() => get.mockReset())
async function render(query: string, asn = '13335') {
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/ases/:asn?', component: AsnRibSnapshot }] })
  await router.push(`/ases/${asn}?${query}`)
  const app = createSSRApp({ render: () => h(AsnRibSnapshot, { asn }) })
  app.use(router)
  return { html: await renderToString(app), router }
}

it('实际ASN快照组件按显式版本读取，日期冲突仍显示版本实际时点', async () => {
  get.mockResolvedValue({ data })
  const { html } = await render(`snapshot_version=${version}&snapshot_date=2026-03-01&snapshot_family=ipv4`)
  expect(html).toContain('RIB 时点快照')
  expect(html).toContain('明确起源前缀')
  expect(html).toMatch(/data-testid="asn-rib-prefix-count"[^>]*>2</)
  expect(html).toContain('2026-02-27 08:00:00')
  expect(html).toContain(version)
  expect(html).toContain('与五分钟资源／报文特征独立')
  expect(html).toContain('此处单独选择快照日期，不随上方时间区间改变')
  expect(html).not.toMatch(/<details[^>]*\sopen(?:\s|>)/)
  expect(get).toHaveBeenCalledWith(`rib-snapshots/${version}/asns/13335`, expect.objectContaining({ params: { family: 'ipv4' } }))
  expect(get.mock.calls.every(([url]) => url.includes(version))).toBe(true)
})

it('无快照选择使用latest发现，与旧特征日期无关，并固定URL版本', async () => {
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [{ version, date: data.date, observed_at: data.observed_at }] } })
    .mockResolvedValueOnce({ data })
  const { html, router } = await render('event_start=2026-03-01T00:00:00Z&event_end=2026-03-03T00:00:00Z&snapshot_family=ipv4')
  expect(html).toMatch(/data-testid="asn-rib-prefix-count"[^>]*>2</)
  expect(router.currentRoute.value.query.snapshot_version).toBe(version)
  expect(router.currentRoute.value.query.snapshot_date).toBe('2026-02-27')
  expect(router.currentRoute.value.query.event_start).toBe('2026-03-01T00:00:00Z')
  expect(get).toHaveBeenNthCalledWith(1, 'rib-snapshots', expect.objectContaining({ params: { latest: 'true' } }))
})

it('显式缺日说明原因不跳最新；未配置隐藏新区域；坏版本没有零值', async () => {
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [] } })
  const missing = await render('snapshot_date=2026-03-01')
  expect(missing.html).toContain('此日无已发布的 RIB 快照')
  expect(missing.html).not.toContain('asn-rib-prefix-count')
  expect(get).toHaveBeenCalledWith('rib-snapshots', expect.objectContaining({ params: { date: '2026-03-01' } }))
  get.mockResolvedValueOnce({ data: { state: 'not_configured', message: '未配置' } })
  expect((await render('')).html).not.toContain('RIB 时点快照')
  get.mockRejectedValueOnce({ response: { status: 404, data: { state: 'unknown_version', message: '未知快照版本' } } })
  const bad = (await render(`snapshot_version=${version}`)).html
  expect(bad).not.toContain('asn-rib-prefix-count')
  expect(bad).toContain('Unknown')
})

it('ASN总览保持快照，提供独立日期和地址族选择再检索ASN', async () => {
  const { asn, prefix_count, sample_limit, sample_truncated, items, ...metadata } = data
  get.mockResolvedValueOnce({ data: { ...metadata, unit: 'distinct_prefix_and_origin_asn',
    metrics: { visible_prefixes: 5, visible_origin_ases: 2, attributed_prefixes: 3, unattributed_prefixes: 2, rib_entries: 9, unattributed_entries: 4 } } })
  const { html } = await render(`snapshot_version=${version}&snapshot_family=ipv4`, '')
  expect(html).toContain('选择 ASN 查看同一快照的明确起源前缀')
  expect(html).toContain('快照日期')
  expect(html).toContain('快照地址族')
  expect(html).toContain('使用最新已发布快照')
  expect(html).not.toContain('asn-rib-prefix-count')
  expect(get).toHaveBeenCalledWith(`rib-snapshots/${version}`, expect.objectContaining({ params: { family: 'ipv4' } }))
})

it('显式版本在完全未配置时也保持旧页面行为', async () => {
  get.mockResolvedValueOnce({ data: { state: 'not_configured', message: '未配置' } })
  expect((await render(`snapshot_version=${version}`)).html).not.toContain('RIB 时点快照')
})

it.each([['not_calculated', '尚未计算'], ['missing_input', '缺少输入'], ['validation_failed', '输入校验失败']])('ASN缺日保留%s语义', async (state, message) => {
  get.mockResolvedValueOnce({ data: { state: 'available', snapshots: [], days: [{ date: '2026-02-27', state }] } })
  const { html } = await render('snapshot_date=2026-02-27')
  expect(html).toContain(message)
  expect(html).not.toContain('asn-rib-prefix-count')
})

it.each([['validation_failed', '已选快照校验失败'], ['unknown_version', '未知快照版本']])('ASN版本错误保留%s语义', async (state, message) => {
  get.mockRejectedValueOnce({ response: { status: 503, data: { state } } })
  const { html } = await render(`snapshot_version=${version}`)
  expect(html).toContain(message)
  expect(html).toContain('Unknown')
  expect(html).not.toContain('asn-rib-prefix-count')
})

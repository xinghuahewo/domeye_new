import { createSSRApp, h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, expect, it, vi } from 'vitest'
import AsnPage from './AsnPage.vue'
import type { AsCandidatePage, AsOverview } from '@/types/api'

const { get, getOverview, getRecent, getOutage, getCandidates, chart } = vi.hoisted(() => ({
  get: vi.fn(), getOverview: vi.fn(), getRecent: vi.fn(), getOutage: vi.fn(), getCandidates: vi.fn(), chart: vi.fn(),
}))
vi.mock('axios', () => ({ isAxiosError: () => false, default: { create: () => ({ get, interceptors: { response: { use: vi.fn() } } }) } }))
vi.mock('@/api/features', () => ({ getAsOverview: getOverview, getAsRecentEvents: getRecent, getASPrefixOutages: getOutage, getAsCandidates: getCandidates }))
vi.mock('@/api/health', async () => ({ resultDelivery: (await import('vue')).ref({ state: 'available' }) }))
vi.mock('@/components/LineChart.vue', () => ({ default: { props: { series: Array, timeBounds: Array, showDataZoom: Boolean, showPoints: Boolean, height: Number, unit: String }, setup(props: unknown) { chart(props); return () => null } } }))

const start = '2026-02-24T08:00:00', end = '2026-02-27T08:00:00'
const windowQuery = { start, end }
const overview = {
  startTime: start.replace('T', ' '), endTime: end.replace('T', ' '), timezone: 'Asia/Shanghai', windowBoundary: '[start,end)',
  scopeSize: 2, candidatePoolSize: 100, selectedAsn: { asn: '13335', asName: '测试网络', orgName: '测试组织', country: '美国', asType: '内容网络',
    announce: 9, withdraw: 1, updateTotal: 10, withdrawRate: 10, sampleCount: 2, anomalyCount: 3, highRiskCount: 0,
    latestObservation: '2026-02-24 08:15:00', globalRank: null, countryRank: null, important: false,
    series: [{ time: '2026-02-24 08:00:00', announce: 9, withdraw: 1, ipv4Prefixes: 20, ipv6Prefixes: 3, ipv4Addresses: 5120 },
      { time: '2026-02-24 08:15:00', announce: 0, withdraw: 0, ipv4Prefixes: 21, ipv6Prefixes: 4, ipv4Addresses: 5376 }],
  },
} as AsOverview
const candidates: AsCandidatePage = { state: 'available', version: 'delivery-1', collectorId: 'rrc25', scopeKind: 'delivered_asn_feature_samples',
  countryBasis: 'result_delivery.features.country', coverage: { state: 'partial', intervals: [] }, limitations: [], start, end, country: '伊朗',
  total: 21, page: 1, pageCount: 2, items: [{ asn: '13335', country: '伊朗', countries: ['伊朗'], asName: null, orgName: null,
    sampleCount: 1, latestObservation: '2026-02-24T00:00:00+00:00', announce: 9, withdraw: 1, updateTotal: 10, withdrawRate: 10, anomalyCount: null }] }
beforeEach(() => {
  get.mockReset().mockResolvedValue({ data: { state: 'not_configured' } })
  getOverview.mockReset().mockResolvedValue(overview)
  getRecent.mockReset().mockResolvedValue({ data: [] })
  getOutage.mockReset().mockResolvedValue([{ time: '2026-02-24 08:00:00', count: 0 }])
  getCandidates.mockReset().mockResolvedValue(candidates)
  chart.mockReset()
})
async function render(asn = '', query: Record<string, string> = windowQuery) {
  const router = createRouter({ history: createMemoryHistory(), routes: [
    { path: '/ases/:asn', name: 'asn-detail', component: AsnPage }, { path: '/ases', name: 'ases', component: AsnPage },
    { path: '/events', name: 'events', component: { render: () => null } }, { path: '/events/detail', name: 'event-detail', component: { render: () => null } },
  ] })
  await router.push(asn ? { name: 'asn-detail', params: { asn }, query } : { name: 'ases', query })
  const html = await renderToString(createSSRApp({ render: () => h(AsnPage) }).use(router))
  return { html, router }
}

it('入口只读取候选列表，保留国家、ASN前缀、排序与分页，不加载旧排行', async () => {
  const { html } = await render('', { ...windowQuery, country: '伊朗', q: 'AS13', sort: 'asn', order: 'asc', page: '2' })
  expect(getCandidates).toHaveBeenCalledWith({ start_time: '2026-02-24 08:00:00', end_time: '2026-02-27 08:00:00' },
    expect.objectContaining({ country: '伊朗', q: 'AS13', sort: 'asn', order: 'asc', page: 2, page_size: 20 }), expect.any(AbortSignal))
  expect(getOverview).not.toHaveBeenCalled(); expect(getRecent).not.toHaveBeenCalled(); expect(getOutage).not.toHaveBeenCalled()
  expect(html).toContain('有特征记录的网络'); expect(html).toContain('名称未知'); expect(html).toContain('仅部分时段有交付数据')
  expect(html).toContain('02-24 08:00'); expect(html).not.toContain('<th>异常</th>')
  expect(html).not.toContain('更新量最高'); expect(html).not.toContain('ASN 候选集排行')
})

it('单AS显示身份摘要与三张缩放图，资源仅一个单位，事件链接保留完整范围', async () => {
  const { html, router } = await render('13335', { ...windowQuery, country: '伊朗' })
  expect(getCandidates).not.toHaveBeenCalled()
  expect(html).toContain('测试网络'); expect(html).toContain('测试组织'); expect(html).toContain('撤回占比')
  expect(html).not.toContain('更新量最高'); expect(html).not.toContain('有特征记录的网络')
  expect(chart).toHaveBeenCalledTimes(3)
  for (const [props] of chart.mock.calls) expect(props).toMatchObject({ showDataZoom: true, timeBounds: ['2026-02-24T00:00:00.000Z', '2026-02-27T00:00:00.000Z'] })
  expect(chart.mock.calls[2]![0]).toMatchObject({ unit: '/24 覆盖块', series: [{ name: 'IPv4 资源' }] })
  expect(chart.mock.calls[2]![0].series).toHaveLength(1)
  expect(chart.mock.calls[0]![0].series[0].data).toContainEqual(['2026-02-24T00:05:00.000Z', null])
  const href = router.resolve({ name: 'events', query: { attacked_as: '13335', attacked_country: '伊朗', ...windowQuery } }).href.replaceAll('&', '&amp;')
  expect(html).toContain(`href="${href}"`)
  expect(html).toContain('AS 开始时间'); expect(html).toContain('AS 结束时间')
})

it('详情与时序并行读取，共用同一个可取消请求信号', async () => {
  let finish!: (value: AsOverview) => void
  getOverview.mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
  const rendering = render('13335')
  await vi.waitFor(() => expect(getRecent).toHaveBeenCalledTimes(1))
  expect(getOutage).toHaveBeenCalledTimes(1)
  const signal = getOverview.mock.calls[0]![5]
  expect(signal).toBeInstanceOf(AbortSignal)
  expect(getRecent.mock.calls[0]![5]).toBe(signal)
  expect(getOutage.mock.calls[0]![2]).toBe(signal)
  finish(overview)
  await rendering
})

it('国家事件模式锁定原窗口并保留返回上下文', async () => {
  const query = { event_start: '2026-02-24T00:00:00Z', event_end: '2026-02-24T00:15:00Z', event_ref: 'country-event-fixture',
    return_anchor: 'affected-as', as_page: '3', as_query: '13335', as_classification: 'affected' }
  getOverview.mockResolvedValueOnce({ ...overview, startTime: '2026-02-24 08:00:00', endTime: '2026-02-24 08:15:00' })
  const { html, router } = await render('13335', query)
  expect(getOverview).toHaveBeenCalledWith({ start_time: '2026-02-24 08:00:00', end_time: '2026-02-24 08:15:00' }, '13335', 6, true, 'country-event-fixture', expect.any(AbortSignal))
  expect(html).toContain('报文与资源 · 时段核对')
  expect(html).not.toContain('AS 开始时间'); expect(html).not.toContain('有特征记录的网络')
  const href = router.resolve({ name: 'event-detail', query: { ref: query.event_ref, focus: query.return_anchor, as_page: '3', as_query: '13335', as_classification: 'affected' } }).href.replaceAll('&', '&amp;')
  expect(html).toContain(`href="${href}"`)
})

it.each([
  [{ start: '2026-02-24T08:00:00', end: '2026-02-24T08:00:00' }, '结束时间须晚于开始时间'],
  [{ start: '2026-02-01T00:00:00', end: '2026-04-01T00:00:00' }, '单次最多查看 45 天'],
  [{ event_ref: 'incomplete' }, '事件上下文不完整'],
])('无效或超长窗口不发起业务查询', async (query, message) => {
  const { html } = await render('13335', query)
  expect(html).toContain(message)
  expect(html).not.toContain('当前窗口没有可展示的异常事件')
  expect(html).toContain('ASN 事件不可用')
  expect(html).toContain('前缀中断时序不可用')
  if ('event_ref' in query) expect(html).not.toContain('AS 开始时间')
  expect(getOverview).not.toHaveBeenCalled(); expect(getCandidates).not.toHaveBeenCalled(); expect(getRecent).not.toHaveBeenCalled(); expect(getOutage).not.toHaveBeenCalled()
})

it('恰好45天可查询，非法ASN保留查询失败而非成功空事件', async () => {
  await render('13335', { start: '2026-02-01T00:00:00', end: '2026-03-18T00:00:00' })
  expect(getOverview).toHaveBeenCalledTimes(1)
  getOverview.mockClear(); getRecent.mockClear(); getOutage.mockClear()
  const { html } = await render('4294967296')
  expect(html).toContain('ASN 无效')
  expect(html).not.toContain('当前窗口没有可展示的异常事件')
  expect(getOverview).not.toHaveBeenCalled(); expect(getRecent).not.toHaveBeenCalled(); expect(getOutage).not.toHaveBeenCalled()
})

it('特征失败不阻止独立RIB与中断查询，快照放在事件之后并保留返回选择', async () => {
  const version = `rib_snapshot_v1_${'a'.repeat(64)}`
  const query = { snapshot_version: version, snapshot_date: '2026-02-27', snapshot_family: 'ipv4', ...windowQuery }
  getOverview.mockRejectedValueOnce(new Error('特征fixture不可用'))
  get.mockResolvedValue({ data: { state: 'available', version, date: '2026-02-27', observed_at: '2026-02-27T00:00:00Z',
    family: 'ipv4', asn: 13335, prefix_count: 0, unit: 'distinct_origin_prefix', source: { collector_id: 'rrc25', sha256: 'b'.repeat(64), coverage: 'unknown' },
    origin_rule: 'rib-attributed-origin/private-skip-v1', limitations: [], sample_limit: 20, sample_truncated: false, items: [] } })
  const { html, router } = await render('13335', query)
  expect(html).toContain('ASN 态势不可用')
  expect(html).toMatch(/data-testid="asn-rib-prefix-count"[^>]*>0</)
  expect(html.indexOf('RIB 时点快照')).toBeGreaterThan(html.indexOf('区间异常事件'))
  expect(html).toContain('此处单独选择快照日期')
  const returnHref = router.resolve({ name: 'ases', query }).href.replaceAll('&', '&amp;')
  expect(html).toContain(`href="${returnHref}"`)
  expect(chart).toHaveBeenCalledTimes(1)
})

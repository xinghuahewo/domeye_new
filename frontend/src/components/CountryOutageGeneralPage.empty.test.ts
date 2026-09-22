import { createSSRApp, h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import CountryOutageGeneralPage from './CountryOutageGeneralPage.vue'
import { getCountryOutageGeneralAffectedAs, getCountryOutageGeneralPathDownstreams } from '@/api/events'
import type { CountryOutageGeneralPageModel, CountryOutageGeneralTrackKey } from '@/types/api'

vi.mock('@/api/events', () => ({
  getCountryOutageGeneralAffectedAs: vi.fn(async () => ({ items: [], page_count: 0 })),
  getCountryOutageGeneralPathDownstreams: vi.fn(async () => ({ items: [], page_count: 0 })),
}))

const headings = ['路由不可见的集中变化', '路由变化 · 时段核对', '前缀中断数量变化',
  'AS 分类数量变化', 'IP 地址变化趋势', '事件窗口中的相关 AS', '实际路径中关联了哪些网络']

async function render(props: InstanceType<typeof CountryOutageGeneralPage>['$props']) {
  const router = createRouter({ history: createMemoryHistory(), routes: [
    { path: '/events', name: 'events', component: { template: '<div />' } },
    { path: '/events/detail', name: 'event-detail', component: { template: '<div />' } },
    { path: '/ases/:asn', name: 'asn-detail', component: { template: '<div />' } },
  ] })
  await router.push({ name: 'event-detail', query: { ref: props.reference } })
  return renderToString(createSSRApp({ render: () => h(CountryOutageGeneralPage, props) }).use(router))
}

function page(): CountryOutageGeneralPageModel {
  const metadata = {
    revision: 1, publication_id: 'fixture-publication', publication_state: 'published',
    observation_state: 'evidence_complete', data_mode: 'replay', data_through: '2026-02-27T01:20:00Z',
    is_final_in_data_range: false, lifecycle_state: 'active', quality_state: 'complete',
    missing_slot_count: 0, collector_id: 'rrc25', incident_id: 'fixture-incident', cohort_id: 'fixture-cohort',
    window_start_utc: '2026-02-27T01:10:00Z', window_end_utc: '2026-02-27T01:20:00Z',
  } as const
  const capabilities = { overview: 'available', event_series: 'available', affected_as: 'available',
    path_downstreams: 'available', full_path_evidence: 'audit_only' } as const
  const keys: CountryOutageGeneralTrackKey[] = ['interrupted_prefix_count', 'completely_interrupted_prefix_count',
    'invisible_direction_count', 'affected_asn_count', 'route_interrupted_asn_count', 'fixed_visible_ipv4_address_count',
    'fixed_visible_ipv6_slash48_count', 'new_visible_ipv4_prefix_count', 'new_visible_ipv6_prefix_count',
    'new_visible_ipv4_address_count', 'new_visible_ipv6_slash48_count', 'new_cumulative_ipv4_prefix_count',
    'new_cumulative_ipv6_prefix_count', 'new_cumulative_ipv4_address_count', 'new_cumulative_ipv6_slash48_count']
  const reference = 'country_outage/2026-02-27 09:12:32/IR/1/r'
  return {
    resolution: { ...metadata, schema_version: 'country_outage_general_resolution_v1', legacy_reference: reference,
      event_type: 'country_outage', country_code: 'IR', latest_revision: 1, capabilities },
    overview: { ...metadata, schema_version: 'country_outage_general_overview_v1', capabilities,
      event: { legacy_reference: reference, country_code: 'IR', detected_at_utc: '2026-02-27T01:12:32Z',
        event_end_at_utc: null, event_duration_seconds: null }, interval_seconds: 300, state_point_count: 2,
      cohort: { cohort_id: metadata.cohort_id, fixed_prefix_count: 12, fixed_asn_count: 3,
        independent_direction_relation_count: 24, new_prefix_count: 0 },
      current: Object.fromEntries(keys.map(key => [key, 0])) as Record<CountryOutageGeneralTrackKey, number>,
      peaks: { interrupted_prefix_count: { value: 0, state_point_utc: metadata.window_start_utc } },
      affected_as_count: 0, route_interrupted_as_count: 0, path_downstream_relation_count: 0,
      concurrent_path_downstream_relation_count: 0, semantic_boundary: 'rrc25_control_plane_observation_not_user_impact_or_cause' },
    series: { ...metadata, schema_version: 'country_outage_general_series_v1', interval_seconds: 300, point_count: 2,
      timestamps: [metadata.window_start_utc, '2026-02-27T01:15:00Z'],
      tracks: Object.fromEntries(keys.map(key => [key, [0, 0]])) as Record<CountryOutageGeneralTrackKey, number[]>,
      track_definitions: Object.fromEntries(keys.map(key => [key, { label: key, unit: 'fixture', definition: '测试数据' }])) as CountryOutageGeneralPageModel['series']['track_definitions'] },
  }
}

beforeEach(() => vi.clearAllMocks())
describe('国家中断统一模板与缺失数据', () => {
  it.each([['SB', '所罗门群岛'], ['IR', '伊朗']])('%s 无观测数据仍保留全部区域', async (code, name) => {
    const html = await render({ reference: `country_outage/2026-02-24 09:29:42/${code}/1/r` })
    expect(html).toContain(`${name}网络中断事件`)
    for (const heading of headings) expect(html).toContain(heading)
    expect(html).toContain('暂无前缀中断时序')
    expect(html).toContain('暂无事件窗口相关 AS 数据')
    expect(html).toContain('暂无实际路径关联数据')
    expect(html).toContain('缺失不表示数量为零')
    expect(html).toMatch(/固定前缀<\/dt><dd[^>]*>—<\/dd>/)
    expect(html).not.toContain('当前条件下没有相关 AS')
    expect(html).not.toContain('该事件在当前数据范围内已结束')
    expect(getCountryOutageGeneralAffectedAs).not.toHaveBeenCalled()
    expect(getCountryOutageGeneralPathDownstreams).not.toHaveBeenCalled()
  })

  it('读取失败保留同一布局和重试，不伪装成无数据成功', async () => {
    const html = await render({ reference: 'country_outage/2026-02-27 09:12:32/IR/1/r', error: 'fixture：来源校验失败' })
    for (const heading of headings) expect(html).toContain(heading)
    expect(html).toContain('data-kind="error"')
    expect(html).toContain('fixture：来源校验失败')
    expect(html).toContain('重新读取')
    expect(html).toContain('不能判断此项是否有数据')
    expect(html).not.toContain('事件记录已保留')
    expect(getCountryOutageGeneralAffectedAs).not.toHaveBeenCalled()
  })

  it('加载期间不将请求中的事件当作已读事实', async () => {
    const html = await render({ reference: 'country_outage/2026-02-24 09:29:42/SB/1/r', loading: true })
    expect(html).toContain('data-kind="loading"')
    expect(html).toContain('正在读取当前事件的数据')
    expect(html).not.toContain('事件记录已保留')
  })

  it('完整伊朗数据继续显示原指标与实际零，并使用同一发布下钻', async () => {
    const model = page()
    const html = await render({ reference: model.resolution.legacy_reference, page: model })
    for (const heading of headings) expect(html).toContain(heading)
    expect(html).toMatch(/固定前缀<\/dt><dd[^>]*>12<\/dd>/)
    expect(html).toMatch(/窗口相关 AS<\/dt><dd[^>]*>0<\/dd>/)
    expect(html).toContain('最高 0 个前缀')
    expect(html).not.toContain('暂无前缀中断时序')
    expect(getCountryOutageGeneralAffectedAs).toHaveBeenCalledWith(model.resolution, expect.objectContaining({ page_size: 20 }))
    expect(getCountryOutageGeneralPathDownstreams).toHaveBeenCalledWith(model.resolution, expect.objectContaining({ page_size: 15 }))
  })
})

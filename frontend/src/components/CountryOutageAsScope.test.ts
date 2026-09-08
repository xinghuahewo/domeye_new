import { createSSRApp, h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { createMemoryHistory, createRouter } from 'vue-router'
import { describe, expect, it } from 'vitest'
import type { CountryOutageGeneralPageModel, CountryOutageGeneralTrackKey } from '@/types/api'
import CountryOutageGeneralPage from './CountryOutageGeneralPage.vue'

function fixture() {
  const keys = ['interrupted_prefix_count', 'completely_interrupted_prefix_count', 'invisible_direction_count',
    'affected_asn_count', 'route_interrupted_asn_count', 'fixed_visible_ipv4_address_count',
    'new_visible_ipv4_address_count', 'fixed_visible_ipv6_slash48_count', 'new_visible_ipv6_slash48_count'] as CountryOutageGeneralTrackKey[]
  const tracks = Object.fromEntries(keys.map(key => [key, [0, 0, 0]]))
  tracks.affected_asn_count = [0, 4, 1]
  tracks.route_interrupted_asn_count = [0, 1, 2]
  return {
    resolution: { country_code: 'IR', is_final_in_data_range: false, window_start_utc: '2026-02-27T00:10:00Z', window_end_utc: '2026-02-27T00:20:00Z' },
    overview: { event: { detected_at_utc: '2026-02-27T00:12:00Z', event_end_at_utc: null },
      cohort: { fixed_prefix_count: 10, fixed_asn_count: 6, independent_direction_relation_count: 20 },
      current: { affected_asn_count: 1, route_interrupted_asn_count: 2 },
      peaks: { affected_asn_count: { value: 4, state_point_utc: '2026-02-27T00:15:00Z' },
        route_interrupted_asn_count: { value: 2, state_point_utc: '2026-02-27T00:20:00Z' } },
      affected_as_count: 5, route_interrupted_as_count: 2, path_downstream_relation_count: 1 },
    series: { interval_seconds: 300, timestamps: ['2026-02-27T00:10:00Z', '2026-02-27T00:15:00Z', '2026-02-27T00:20:00Z'], tracks,
      track_definitions: { affected_asn_count: { definition: '来源的受影响类说明' }, route_interrupted_asn_count: { definition: '来源的路由中断类说明' } } },
  } as CountryOutageGeneralPageModel
}
async function render(page = fixture()) {
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/events/detail', component: { render: () => null } }, { path: '/events', component: { render: () => null } }] })
  await router.push('/events/detail')
  const app = createSSRApp({ render: () => h(CountryOutageGeneralPage, { page, reference: 'country_outage/2026-02-27 09:12:32/IR/1/r' }) })
  app.use(router)
  return renderToString(app)
}

describe('国家页 ASN 分类与时间口径', () => {
  it('将时点曲线和窗口名单分别命名，分类生产规则未核实则保持 Unknown', async () => {
    const html = await render()
    expect(html).toContain('AS 分类数量变化')
    expect(html).toContain('事件窗口中的相关 AS')
    expect(html).toContain('窗口相关 AS')
    expect(html).toContain('受影响类 AS')
    expect(html).toContain('路由中断类 AS')
    expect(html).toContain('两类是否互斥、分类优先级及未知前缀处理仍为 Unknown')
    expect(html).toContain('窗口分类和峰值不能定位单个 ASN 的状态转换时刻')
    expect(html).not.toContain('部分固定前缀不可见')
    expect(html).not.toContain('不可见程度')
  })
  it('两个分类各自显示峰值时点和窗口末点，不将错时峰值当作同一时刻', async () => {
    const html = await render()
    const cards = html.match(/<article class="as-count-card"[\s\S]*?<\/article>/g) ?? []
    expect(cards).toHaveLength(2)
    expect(cards[0]).toContain('受影响类 AS')
    expect(cards[0]).toContain('窗口峰值：4')
    expect(cards[0]).toContain('2026/02/27 08:15')
    expect(cards[0]).toContain('窗口末点：1')
    expect(cards[1]).toContain('路由中断类 AS')
    expect(cards[1]).toContain('窗口峰值：2')
    expect(cards[1]).toContain('2026/02/27 08:20')
    expect(cards[1]).toContain('窗口末点：2')
    expect(html).toContain('来源的受影响类说明')
    expect(html).toContain('来源的路由中断类说明')
  })

  it('缺少分类说明或峰值时保留 Unknown 和未知时点，不补造转换时间', async () => {
    const page = fixture()
    page.series.track_definitions = {} as typeof page.series.track_definitions
    page.overview.peaks = {}
    const html = await render(page)
    expect(html).toContain('Unknown：当前发布未提供说明。')
    expect(html).toContain('窗口峰值：— 个 AS · 未知')
    expect(html).toContain('窗口末点：1 个 AS')
    expect(html).toContain('窗口分类和峰值不能定位单个 ASN 的状态转换时刻')
  })

})

import { renderToString } from 'vue/server-renderer'
import { h } from 'vue'
import { describe, expect, it } from 'vitest'
import { normalizeAsOverview } from '@/utils/normalize'
import AsnEventTimeline from './AsnEventTimeline.vue'

const profile = (series: unknown[], extra = {}) => normalizeAsOverview({ selected_asn: {
  asn: '48715', sample_count: 2, announce: 7, withdraw: 2, update_total: 9,
  series, ...extra,
} }).selectedAsn!
const points = [
  { time: '2026-02-28 11:10:00', announce: 0, withdraw: 2, ipv4_prefixes: 64, ipv6_prefixes: null, ipv4_addresses: 16384 },
  { time: '2026-02-28 11:20:00', announce: 7, withdraw: 0, ipv4_prefixes: 0, ipv6_prefixes: 0, ipv4_addresses: 0 },
]
const render = (selected = profile(points)) => renderToString(h(AsnEventTimeline, {
  profile: selected, startTime: '2026-02-28 11:10:00', endTime: '2026-02-28 11:20:00',
}))

describe('事件窗口 ASN 报文与资源核对', () => {
  it('将无记录与字段缺失分别说明，真实零仍作为数值', async () => {
    const html = await render()
    expect(html).toContain('无匹配记录 1 个时点')
    expect(html).toContain('字段缺失')
    expect(html).toContain('>0</td>')
    expect(html).toContain('报文汇总与已返回时序一致')
  })
  it('同时展示选中时段的增减和各指标实际极值时点', async () => {
    const html = await render()
    expect(html).toContain('核对起点')
    expect(html).toContain('核对终点')
    expect(html).toContain('>+7</td>')
    expect(html).toContain('>-64</td>')
    expect(html).toContain('核对首条记录至IPv4 资源字段极值')
    expect(html).toContain('2026-02-28 11:20:00')
    expect(html).toContain('已有记录合计')
    expect(html).toContain('最后有效记录')
  })

  it('无聚合且没有记录时不把默认零写为观测或制造极值', async () => {
    const html = await render(profile([], { sample_count: 0, announce: 0, withdraw: 0, update_total: 0 }))
    expect(html).toContain('暂无报文汇总')
    expect(html).toContain('无匹配记录 3 个时点')
    expect(html).not.toContain('>0</td>')
    expect(html).not.toContain('核对首条记录至ANNOUNCE极值')
    expect(html).toContain('当前范围没有可绘制数据')
  })

  it('有记录但数值全 null 时保留字段缺失及图表空态', async () => {
    const html = await render(profile([{ time: '2026-02-28 11:10:00', announce: null, withdraw: null }]))
    expect(html).toContain('字段缺失')
    expect(html).toContain('报文字段存在缺失')
    expect(html).not.toContain('>0</td>')
    expect(html).toContain('当前范围没有可绘制数据')
  })

  it('真实零保留为可绘制数值，来源与资源单位的未知项仍可见', async () => {
    const zero = { time: '2026-02-28 11:10:00', announce: 0, withdraw: 0, ipv4_prefixes: 0, ipv6_prefixes: 0, ipv4_addresses: 0 }
    const html = await render(profile([zero], { sample_count: 1, announce: 0, withdraw: 0, update_total: 0 }))
    expect(html).toContain('>0</td>')
    expect(html).not.toContain('当前范围没有可绘制数据')
    expect(html).toContain('单位 Unknown')
    expect(html).toContain('接口未提供 collector、publication 或 cohort')
  })

  it('汇总与记录不同或缺少字段时不确认一致', async () => {
    expect(await render(profile(points, { announce: 99 }))).toContain('报文汇总与已返回时序不一致')
  })

})

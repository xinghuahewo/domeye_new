import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { describe, expect, it } from 'vitest'

import CountryOutageTimeline from './CountryOutageTimeline.vue'
import type { CountryOutageGeneralOverview, CountryOutageGeneralSeries } from '@/types/api'

function fixture() {
  const series = {
    timestamps: ['2026-02-27T00:10:00Z', '2026-02-27T00:15:00Z', '2026-02-27T00:20:00Z'],
    tracks: {
      interrupted_prefix_count: [2, 9, 4], completely_interrupted_prefix_count: [1, 3, 4],
      invisible_direction_count: [3, 10, 20], fixed_visible_ipv4_address_count: [100, 70, 90],
      fixed_visible_ipv6_slash48_count: [20, 15, 10], new_visible_ipv4_address_count: [0, 0, 8],
      new_visible_ipv6_slash48_count: [0, 4, 4],
    },
    track_definitions: {}, interval_seconds: 300, collector_id: 'rrc25', cohort_id: 'cohort-test',
    publication_id: 'publication-test', data_through: '2026-02-27T00:25:00Z',
    is_final_in_data_range: false, window_end_utc: '2026-02-27T00:20:00Z',
  } as CountryOutageGeneralSeries
  const overview = {
    cohort: { fixed_prefix_count: 12, independent_direction_relation_count: 30 },
    event: { event_end_at_utc: null },
  } as CountryOutageGeneralOverview
  return { series, overview }
}

function row(html: string, label: string) {
  const rows = html.match(/<tr[\s\S]*?<\/tr>/g) ?? []
  const found = rows.find(value => value.includes(label))
  expect(found, `missing row: ${label}`).toBeTruthy()
  return found!
}

describe('国家路由变化核对', () => {
  it('分别显示各指标极值的实际时点，不把不同峰值拼成同一时刻', async () => {
    const html = await renderToString(h(CountryOutageTimeline, fixture()))
    expect(row(html, '出现不可见的固定前缀')).toContain('2026-02-27 08:15')
    expect(row(html, '所有观察方向均不可见')).toContain('2026-02-27 08:20')
    expect(row(html, '不可见独立观察方向')).toContain('2026-02-27 08:20')
  })
  it('固定可见资源显示最低值，新出现资源单列最高值，均保留单位', async () => {
    const html = await renderToString(h(CountryOutageTimeline, fixture()))
    expect(row(html, '固定前缀可见 IPv4 地址')).toContain('最低 70 个地址')
    expect(row(html, '固定前缀可见 IPv4 地址')).toContain('2026-02-27 08:15')
    expect(row(html, '固定前缀可见 IPv6 /48')).toContain('最低 10 个 /48 等价块')
    expect(row(html, '新出现前缀可见 IPv4 地址')).toContain('最高 8 个地址')
  })

  it('默认核对首末两个状态点，区分净变化、窗口末点与数据覆盖末端', async () => {
    const html = await renderToString(h(CountryOutageTimeline, fixture()))
    expect(html).toContain('value="2026-02-27T08:10"')
    expect(html).toContain('value="2026-02-27T08:20"')
    expect(row(html, '出现不可见的固定前缀')).toContain('+2')
    expect(row(html, '固定前缀可见 IPv4 地址')).toContain('-10')
    expect(html).toContain('数据覆盖至：2026-02-27 08:25')
    expect(html).toContain('事件结束时间：未知')
    expect(html).toContain('当前数据范围内尚不能确认事件结束')
    expect(html).toContain('极值相同时显示首次达到的时点')
  })

  it('真实零仍显示数值，重复极值使用首次时点，定义缺少时标为 Unknown', async () => {
    const props = fixture()
    props.series.tracks.interrupted_prefix_count = [0, 0, 0]
    const html = await renderToString(h(CountryOutageTimeline, props))
    expect(row(html, '出现不可见的固定前缀')).toContain('最高 0 个前缀')
    expect(row(html, '出现不可见的固定前缀')).toContain('2026-02-27 08:10')
    expect(html).toContain('Unknown：当前发布未提供该指标定义。')
  })

  it('没有状态点时不生成极值时点或把未知数值显示成零', async () => {
    const props = fixture()
    props.series.timestamps = []
    for (const key of Object.keys(props.series.tracks)) {
      props.series.tracks[key as keyof typeof props.series.tracks] = []
    }
    const html = await renderToString(h(CountryOutageTimeline, props))
    expect(html).toContain('当前窗口没有可核对的状态点。')
    expect(row(html, '出现不可见的固定前缀')).toContain('最高 — 个前缀')
    expect(row(html, '出现不可见的固定前缀')).not.toContain('extreme-time')
  })

})

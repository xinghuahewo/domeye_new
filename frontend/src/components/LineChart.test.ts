import { describe, expect, it } from 'vitest'
import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'

import LineChart from './LineChart.vue'

import { formatChartTime } from '@/utils/chartTime'

describe('LineChart 时间显示', () => {
  it('固定使用 Asia/Shanghai，而不是浏览器本地时区', () => {
    const utc = '2026-01-31T16:00:00Z'
    expect(formatChartTime(utc, 'Asia/Shanghai', true)).toBe('2026-02-01 00:00')
    expect(formatChartTime(utc, 'Asia/Shanghai')).toBe('02-01 00:00')
  })
})

describe('LineChart 无可用数值时的展示', () => {
  const cases: Array<{ name: string; values: Array<number | null>; empty: boolean }> = [
    { name: '空序列', values: [], empty: true },
    { name: '所有值缺失', values: [null, null], empty: true },
    { name: '真实零', values: [0, 0], empty: false },
    { name: '缺失与有效值混合', values: [null, 7, null], empty: false },
    { name: '非有限数值', values: [NaN, Infinity], empty: true },
  ]

  it.each(cases)('$name', async ({ values, empty }) => {
    const html = await renderToString(h(LineChart, {
      series: [{
        name: 'ANNOUNCE',
        color: '#0b57b7',
        data: values.map<[string, number | null]>((value, index) => [`2026-02-27T00:${10 + index * 5}:00Z`, value]),
      }],
    }))
    expect(html.includes('当前范围没有可绘制数据')).toBe(empty)
  })
})

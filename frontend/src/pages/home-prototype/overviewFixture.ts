import { dateLabel, timezone, type Detail, type Family } from './fixture'

// C 专用模拟记录：图、卡片和列表来自同一份内存数据，绝不读取真实数据。
export const overviewVersion = 'home-overview-demo-v2'
export const anomalyTypes = ['前缀中断', '前缀劫持', '子前缀劫持', '路由泄漏', 'AS中断', '国家中断'] as const
export type Severity = '高' | '中' | '低'
export type OverviewEvent = {
  id: string; family: Family; type: typeof anomalyTypes[number]; severity: Severity
  target: string; minute: number; endMinute?: number
}
export const severityOrder: Record<Severity, number> = { 高: 0, 中: 1, 低: 2 }
export const clockLabel = (minute: number) => `${String(Math.floor(minute / 60)).padStart(2, '0')}:${String(minute % 60).padStart(2, '0')}`
export const hourLabel = (hour: number) => `${clockLabel(hour * 60)}–${clockLabel((hour + 1) * 60)}`
export const isGapHour = (hour: number) => hour >= 9 && hour < 13
const hourlyCounts = [1, 0, 1, 1, 0, 1, 2, 1, 2, 1, 0, 2, 1, 2, 3, 6, 8, 5, 3, 2, 1, 2, 1, 0]
const prefixLabel = (index: number, family: Family) => family === 'IPv4'
  ? `${['192.0.2', '198.51.100', '203.0.113'][Math.floor(index / 16) % 3]}.${(index % 16) * 16}/28`
  : `2001:db8:${(index + 1).toString(16)}::/48`

export const overviewEvents: OverviewEvent[] = (['IPv4', 'IPv6'] as const).flatMap(family => {
  let serial = 0
  const code = family === 'IPv4' ? '4' : '6'
  const outages: OverviewEvent[] = hourlyCounts.flatMap((count, hour) => Array.from({ length: family === 'IPv4' ? count : Math.floor(count / 2) }, (_, index) => {
    const id = serial++
    const minute = hour * 60 + 4 + index * 5
    return {
      id: `DEMO-${code}-P${String(id + 1).padStart(3, '0')}`, family, type: '前缀中断',
      severity: id % 7 === 0 ? '高' : id % 3 === 0 ? '中' : '低',
      target: prefixLabel(id % 32, family), minute,
      endMinute: id % 3 === 0 ? undefined : Math.min(minute + 48, 1439),
    }
  }))
  const other: OverviewEvent[] = [
    { id: `DEMO-${code}-H01`, family, type: '前缀劫持', severity: '高', target: prefixLabel(35, family), minute: 1210 },
    { id: `DEMO-${code}-S01`, family, type: '子前缀劫持', severity: '中', target: prefixLabel(36, family), minute: 1035, endMinute: 1165 },
    { id: `DEMO-${code}-L01`, family, type: '路由泄漏', severity: '高', target: prefixLabel(37, family), minute: 985 },
    { id: `DEMO-${code}-A01`, family, type: 'AS中断', severity: '中', target: 'AS64496', minute: 940 },
    { id: `DEMO-${code}-C01`, family, type: '国家中断', severity: '低', target: '示例国家', minute: 560 },
  ]
  return [...outages, ...other]
})

export function overviewEventDetail(event: OverviewEvent): Detail {
  return {
    title: `${event.type} · ${event.target}`, subtitle: `${event.id} / 模拟事件，非真实检测结果`,
    rows: [
      ['发生时间', `${dateLabel} ${clockLabel(event.minute)} · ${timezone}`], ['涉及对象', event.target],
      ['观察范围', `${event.family} · 全部示例观察点`], ['危险等级', `${event.severity}（人为设置的演示标签）`],
      ['结束信息', event.endMinute === undefined ? '未记录；不能据此判为持续中' : `${clockLabel(event.endMinute)}（模拟记录终点）`],
      ['判定依据', '此处为详情入口占位，未接入检测规则及原始证据'], ['数据版本', overviewVersion],
    ],
    note: '等级不等于判定可信度。记录终点不表示全部观察点恢复可见；路由观测不能直接推出实际断网、用户影响、原因或责任。',
  }
}

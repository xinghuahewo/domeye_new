import profile from '../../../../config/data-profile.json'

// 以下数值、曲线、事件、身份全部为设计占位；不来自业务数据或检测器。
// 时间与时区沿用项目数据档，绝不暗示实时采集。
export type Family = 'IPv4' | 'IPv6'
export type Scenario = 'ready' | 'gap' | 'unavailable'
export type Variant = 'A' | 'B' | 'C'
export type Detail = { title: string; subtitle: string; rows: [string, string][]; note: string }
export const timezone = profile.timezone
export const snapshot = profile.snapshot_time
export const dateLabel = snapshot.slice(0, 10).replaceAll('-', '.')
export const timeLabel = snapshot.slice(11, 19)
export const windowLabel = `${dateLabel} 00:00 — ${timeLabel}`
export const variants = [
  { key: 'A' as const, name: '观测总览', idea: '先看规模，再看变化；事件常驻右侧。' },
  { key: 'B' as const, name: '态势简报', idea: '用一条变化主线串起规模、事件与关注对象。' },
  { key: 'C' as const, name: '路由态势', idea: '整体概况、变化趋势与异常列表；不以 ASN 为首页主线。' },
]
export const scenarioLabels: Record<Scenario, string> = {
  ready: '完整示例', gap: '观测有缺口', unavailable: '数据不可用',
}
export const scale = {
  IPv4: [
    { name: '可见前缀', value: '984,216', delta: '−0.18%', unit: '个', detail: '指定时点，纳入视角中可靠可见前缀的去重并集；聚合与更具体前缀分别计数。' },
    { name: 'Origin ASN', value: '76,842', delta: '+0.04%', unit: '个', detail: '可见路由中能够明确识别的起源 ASN 去重计数；不计仅在路径中经过的 ASN，歧义身份保留未知。' },
    { name: '可见地址空间', value: '3.12', delta: '−0.06%', unit: '十亿地址', detail: '对可见前缀覆盖的地址范围取并集，去除重叠；不等于活跃主机数、用户数或实际连通性。' },
  ],
  IPv6: [
    { name: '可见前缀', value: '218,450', delta: '−0.18%', unit: '个', detail: '与 IPv4 分开统计的 IPv6 可见前缀去重并集。' },
    { name: 'Origin ASN', value: '33,184', delta: '+0.04%', unit: '个', detail: 'IPv6 可见路由中可明确识别的起源 ASN，并非与 IPv4 的简单相加关系。' },
    { name: '可见地址空间', value: '—', delta: '单位待定', unit: '', detail: 'IPv6 地址空间的展示单位尚未确定。本原型保留占位，不把前缀数量当成地址空间。' },
  ],
}
// 相对窗口起点的模拟可见前缀指数；100 为起点，不是健康评分。
export const prefixIndex = [100,100.01,100,100.02,100.01,100,100.03,100.02,100.04,100.02,100.01,100.02,100.04,100.03,100.02,99.93,99.73,99.72,99.74,99.77,99.79,99.78,99.81,99.82]
export const activities = [16,19,13,15,22,20,19,26,21,19,23,24,20,22,27,62,86,51,37,32,29,24,28,23]
export const events = [
  { id: 'DEMO-001', time: '15:40', name: '前缀可见性集中下降', type: '可见性异常', asn: 'AS64496', object: '示例骨干网络', prefixes: 128, support: '多视角支持', color: 'teal', summary: '部分前缀在模拟观察窗口内失去可见性。这里只表达路由观测，不作实际断网判断。' },
  { id: 'DEMO-002', time: '17:15', name: '起源 ASN 出现变化', type: '起源变化', asn: 'AS64497', object: '示例区域网络', prefixes: 24, support: '证据有限', color: 'amber', summary: '模拟路由记录出现不同的 Origin ASN。变化本身不构成劫持结论。' },
  { id: 'DEMO-003', time: '20:10', name: '路径变化短时集中', type: '路径异常', asn: 'AS64498', object: '示例接入网络', prefixes: 42, support: '待核对', color: 'muted', summary: '多条模拟路径在相近时刻变化；原因、责任与用户影响均未知。' },
]
export const objects = [
  { asn: 'AS64496', name: '示例骨干网络', before: 840, after: 712, delta: '−15.24%', trend: [100,100,100,99,100,83,85,85] },
  { asn: 'AS64497', name: '示例区域网络', before: 326, after: 302, delta: '−7.36%', trend: [100,101,100,98,93,93,92,93] },
  { asn: 'AS64498', name: '示例接入网络', before: 510, after: 496, delta: '−2.75%', trend: [100,101,100,98,99,98,97,97] },
  { asn: 'AS64499', name: '示例内容网络', before: 204, after: 222, delta: '+8.82%', trend: [100,100,102,102,105,105,108,109] },
]
export function eventDetail(index: number): Detail {
  const item = events[index]!
  return {
    title: item.name, subtitle: `${item.id} / 模拟事件 · 非真实检测结果`,
    rows: [['时间', `${dateLabel} ${item.time} · ${timezone}`], ['涉及对象', `${item.asn} · ${item.object}`], ['风险等级', '高风险（人为设置的演示标签）'], ['证据状态', item.support], ['涉及前缀', `${item.prefixes} 个（示例）`], ['数据版本', 'home-demo-v1 · 模拟输入'], ['原因 / 用户影响', '未知，不能从路由观测直接推断']],
    note: item.summary,
  }
}
export function objectDetail(index: number): Detail {
  const item = objects[index]!
  return {
    title: `${item.asn} · ${item.name}`, subtitle: '对象深入查看 / 交互占位',
    rows: [['窗口', `${windowLabel} · ${timezone}`], ['可见 IPv4 前缀', `${item.before} → ${item.after} 个`], ['变化', item.delta], ['统计口径', '相同模拟观察范围，起点 / 终点快照比较'], ['身份', '文档示例 ASN；不代表真实网络'], ['后续入口', '对象详情 / 前缀变化 / 相关事件（尚未实现）']],
    note: '此处只验证“从首页找到对象”的路径。模拟排行不是完整总体，也不能由首页并集规模反推这些对象的实际影响。',
  }
}
export function spark(values: number[]): string {
  const low = Math.min(...values) - 1
  const high = Math.max(...values) + 1
  return values.map((value, i) => `${i * 84 / (values.length - 1)},${28 - (value - low) / (high - low) * 24}`).join(' ')
}

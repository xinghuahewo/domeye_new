<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import LineChart, { type ChartSeries } from './LineChart.vue'
import type { AsnProfile, FeaturePoint } from '@/types/api'
import { businessTimezone, businessTimeToIso, toBusinessTime } from '@/utils/businessTime'
import { toBackendTime } from '@/utils/time'

const props = defineProps<{ profile: AsnProfile; startTime: string; endTime: string }>()
type Field = Exclude<keyof FeaturePoint, 'time'>
const metrics: Array<{ key: Field; label: string; unit: string; message: boolean }> = [
  { key: 'announce', label: 'ANNOUNCE', unit: '条', message: true },
  { key: 'withdraw', label: 'WITHDRAW', unit: '条', message: true },
  { key: 'ipv4Prefixes', label: 'IPv4 资源字段', unit: '单位 Unknown', message: false },
  { key: 'ipv6Prefixes', label: 'IPv6 资源字段', unit: '单位 Unknown', message: false },
  { key: 'ipv4Addresses', label: 'IPv4 地址字段', unit: '单位 Unknown', message: false },
]
const records = computed(() => props.profile.series)
const byTime = computed(() => new Map(records.value.map(point => [point.time, point])))
const slots = computed(() => {
  const result: string[] = []
  const end = Date.parse(businessTimeToIso(props.endTime))
  for (let time = Date.parse(businessTimeToIso(props.startTime)); time <= end; time += 300_000) {
    result.push(toBusinessTime(new Date(time)))
  }
  return result
})
const missing = computed(() => slots.value.filter(time => !byTime.value.has(time)))
const start = ref('')
const end = ref('')
const inputTime = (value: string) => value.replace(' ', 'T')
function resetRange() {
  start.value = inputTime(records.value[0]?.time ?? props.startTime)
  end.value = inputTime(records.value.at(-1)?.time ?? props.endTime)
}
watch(() => props.profile, resetRange, { immediate: true })
const startTime = computed(() => toBackendTime(start.value))
const endTime = computed(() => toBackendTime(end.value))
const rangeError = computed(() => {
  if (!slots.value.includes(startTime.value) || !slots.value.includes(endTime.value)) return '请选择窗口内已有的五分钟展示时点。'
  if (startTime.value > endTime.value) return '核对起点不能晚于终点。'
  return ''
})
const first = computed(() => rangeError.value ? undefined : byTime.value.get(startTime.value))
const last = computed(() => rangeError.value ? undefined : byTime.value.get(endTime.value))
const selectedMissing = computed(() => missing.value.filter(time => time >= startTime.value && time <= endTime.value).length)
function compareTo(time: string) {
  start.value = inputTime(records.value[0]?.time ?? props.startTime)
  end.value = inputTime(time)
}
const markers = computed(() => rangeError.value ? [] : [
  { time: businessTimeToIso(startTime.value), label: '核对起点', color: '#176d8f' },
  { time: businessTimeToIso(endTime.value), label: '核对终点', color: '#d96c0b' },
])
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value)
const formatNumber = (value: number | undefined) => finite(value) ? value.toLocaleString('zh-CN') : '—'
function valueAt(point: FeaturePoint | undefined, key: Field) {
  if (!point) return '无匹配记录'
  return finite(point[key]) ? formatNumber(point[key]) : '字段缺失'
}
const messageSummary = computed(() => {
  if (props.profile.sampleCount === 0) return '暂无报文汇总；不将默认值当作真实零。'
  if (!records.value.length || records.value.some(point => !finite(point.announce) || !finite(point.withdraw))) return '报文字段存在缺失，汇总暂不能与时序完整核对。'
  const announce = records.value.reduce((sum, point) => sum + point.announce!, 0)
  const withdraw = records.value.reduce((sum, point) => sum + point.withdraw!, 0)
  return announce === props.profile.announce && withdraw === props.profile.withdraw && announce + withdraw === props.profile.updateTotal
    ? '报文汇总与已返回时序一致；无记录时段未计入，不代表窗口完整。'
    : '报文汇总与已返回时序不一致，暂不能合并解释。'
})

function difference(key: Field) {
  const left = first.value?.[key]
  const right = last.value?.[key]
  if (rangeError.value || !finite(left) || !finite(right)) return '—'
  const delta = right - left
  return `${delta > 0 ? '+' : ''}${formatNumber(delta)}`
}
const rows = computed(() => metrics.map(metric => {
  const valid = records.value.filter(point => finite(point[metric.key]))
  const extreme = valid.reduce<FeaturePoint | undefined>((best, point) => {
    if (!best) return point
    return (metric.message ? point[metric.key]! > best[metric.key]! : point[metric.key]! < best[metric.key]!) ? point : best
  }, undefined)
  return { ...metric, valid, extreme, final: valid.at(-1), total: valid.length ? valid.reduce((sum, point) => sum + point[metric.key]!, 0) : undefined }
}))
function chartSeries(keys: Field[]): ChartSeries[] {
  return keys.map((key, index) => ({
    name: metrics.find(metric => metric.key === key)!.label,
    color: index === 0 ? '#3e6f89' : '#788f58',
    data: slots.value.map(time => [businessTimeToIso(time), byTime.value.get(time)?.[key] ?? null]),
  }))
}
const messageSeries = computed(() => chartSeries(['announce', 'withdraw']))
const resourceSeries = computed(() => chartSeries(['ipv4Prefixes', 'ipv6Prefixes']))
</script>

<template>
  <section class="asn-timeline" aria-label="ASN 报文与资源时段核对">
    <header><div><p class="eyebrow">同一事件窗口 · 单 ASN 特征</p><h3>报文与资源 · 时段核对</h3></div><span>{{ businessTimezone }} · 五分钟展示时点</span></header>
    <div class="range-controls">
      <label>核对起点<input v-model="start" type="datetime-local" step="300" :min="inputTime(props.startTime)" :max="inputTime(props.endTime)" /></label>
      <label>核对终点<input v-model="end" type="datetime-local" step="300" :min="inputTime(props.startTime)" :max="inputTime(props.endTime)" /></label>
      <button type="button" @click="resetRange">核对首末记录</button>
    </div>
    <p class="range-error" role="status">{{ rangeError }}</p>
    <p class="record-note">返回 {{ records.length }} 条记录 · 无匹配记录 {{ missing.length }} 个时点<span v-if="!rangeError"> · 所选时段内 {{ selectedMissing }} 个时点无匹配记录</span></p>
    <p>{{ messageSummary }}</p>
    <div class="table-scroll" tabindex="0" role="region" aria-label="ASN 特征核对表，可横向滚动">
      <table><caption>起止值与两点差值；报文取已有记录合计，资源取最后有效记录</caption>
        <thead><tr><th scope="col">指标 / 单位</th><th scope="col">起点值</th><th scope="col">终点值</th><th scope="col">两点差值</th><th scope="col">全窗口参照</th><th scope="col">全窗口极值 / 首次时点</th></tr></thead>
        <tbody><tr v-for="row in rows" :key="row.key">
          <th scope="row">{{ row.label }}<small>{{ row.unit }}</small></th>
          <td>{{ rangeError ? '—' : valueAt(first, row.key) }}</td><td>{{ rangeError ? '—' : valueAt(last, row.key) }}</td><td>{{ difference(row.key) }}</td>
          <td><template v-if="row.message">{{ formatNumber(row.total) }}<small>已有记录合计 · {{ row.valid.length }} 个有效值</small></template><template v-else>{{ row.final ? valueAt(row.final, row.key) : '—' }}<small>最后有效记录<br />{{ row.final?.time || '未知' }}</small></template></td>
          <td>{{ row.message ? '最高' : '最低' }} {{ row.extreme ? valueAt(row.extreme, row.key) : '—' }}<button v-if="row.extreme" type="button" class="extreme-time" :aria-label="`核对首条记录至${row.label}极值`" @click="compareTo(row.extreme.time)">{{ row.extreme.time }} ↗</button><small v-else>没有有效值</small></td>
        </tr></tbody>
      </table>
    </div>
    <p>两点差值只比较这两个记录值，不代表时段内持续增减。报文合计排除缺失值；资源值不跨空槽延续，不相加为窗口资源总量。并列极值显示首次时点。</p>
    <p>图中的点表示已有数值；断线处可能无匹配记录或字段缺失，不表示资源归零。</p>
    <div class="charts">
      <section><h4>报文活动</h4><LineChart :series="messageSeries" :markers="markers" :timezone="businessTimezone" unit="条" :height="290" show-data-zoom show-points /></section>
      <section><h4>IPv4 / IPv6 资源字段</h4><LineChart :series="resourceSeries" :markers="markers" :timezone="businessTimezone" unit="原始数值" :height="290" show-data-zoom show-points /></section>
    </div>
    <details class="source-note" open>
      <summary>数据来源与可解释范围</summary>
      <p>来源为既有 ASN 特征库，按 ASN 与事件时间范围独立查询。接口未提供 collector、publication 或 cohort 标识，均为 Unknown；共享窗口不能证明与国家制品属于同一观测集合。</p>
      <p>ANNOUNCE / WITHDRAW 是记录中的报文计数。资源保留返回字段的原始值；旧界面称 IPv4 /24、IPv6 /48 等效段，但生产换算依据尚未核实，单位为 Unknown，不能与国家页的地址并集混算。</p>
      <p>有记录且数值为 0、无匹配记录、记录中字段缺失分别显示。无记录原因仍为 Unknown，不补零、不推定状态延续；这些特征不能单独说明 ASN 可见性转换、对国家变化的贡献或原因。</p>
    </details>
  </section>
</template>

<style scoped>
.asn-timeline { min-width: 0; padding: 22px; border: 1px solid #d8e1e5; border-top: 3px solid #176d8f; background: #fff; color: #28424f; }
header { display: flex; justify-content: space-between; align-items: end; gap: 14px; margin-bottom: 18px; }
header span { font-size: 10px; color: #687c85; }
h3 { margin: 5px 0 0; font-size: 21px; color: #153646; }
p { font-size: 11px; line-height: 1.8; color: #627680; }
.eyebrow { margin: 0; color: #176d8f; font-weight: 700; }
.range-controls { display: flex; align-items: end; flex-wrap: wrap; gap: 14px; padding: 15px; background: #f6f2ea; border: 1px solid #e4dccf; }
label { display: grid; gap: 7px; font-size: 11px; font-weight: 700; }
input { height: 36px; padding: 0 9px; border: 1px solid #aebdc7; background: #fff; color: #203e4d; font: 12px var(--mono); }
button { min-height: 36px; padding: 0 14px; border: 1px solid #173f51; border-radius: 3px; background: #173f51; color: #fff; font-size: 11px; cursor: pointer; }
button:focus-visible, input:focus-visible, .table-scroll:focus-visible, summary:focus-visible { outline: 2px solid #e27839; outline-offset: 3px; }
.range-error { color: #a13a31; }
.range-error:empty { display: none; }
.record-note { color: #315b70; }
.table-scroll { max-width: 100%; overflow-x: auto; border: 1px solid #dbe2e6; }
table { width: 100%; min-width: 940px; border-collapse: collapse; text-align: right; }
caption { text-align: left; padding: 10px 13px; font-size: 11px; background: #f6f8fa; }
th, td { padding: 12px; border-bottom: 1px solid #e1e7eb; }
th:first-child { text-align: left; }
thead th { background: #edf2f5; font-size: 10px; white-space: nowrap; }
tbody th { min-width: 125px; font-size: 11px; }
td { font: 650 12px var(--mono); white-space: nowrap; }
tbody tr:nth-child(even) { background: #fafbfc; }
small { display: block; margin-top: 5px; color: #72828a; font: 10px/1.6 var(--sans); }
.extreme-time { display: block; margin-left: auto; min-height: 26px; padding: 3px 0 0; border: 0; background: none; color: #176d8f; font: 10px var(--mono); text-decoration: underline; }
.charts { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 20px; }
.charts section { min-width: 0; }
h4 { font-size: 12px; }
.source-note { padding: 14px 16px; background: #f2f6f8; border-left: 3px solid #7f9ca9; }
summary { font-size: 12px; color: #28546a; cursor: pointer; }
.source-note p:last-child { margin-bottom: 0; }
@media (max-width: 700px) { .asn-timeline { padding: 14px; } header { align-items: start; flex-direction: column; } .range-controls { display: grid; grid-template-columns: minmax(0, 1fr); } input { min-width: 0; width: 100%; } .charts { grid-template-columns: minmax(0, 1fr); } }
</style>

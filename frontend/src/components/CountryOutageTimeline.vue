<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { ObservationChartMarker } from './ObservationChart.vue'
import type { CountryOutageGeneralOverview, CountryOutageGeneralSeries, CountryOutageGeneralTrackKey } from '@/types/api'

const props = defineProps<{ series: CountryOutageGeneralSeries; overview: CountryOutageGeneralOverview }>()
const metrics: Array<{ key: CountryOutageGeneralTrackKey; label: string; unit: string; extreme: 'max' | 'min' }> = [
  { key: 'interrupted_prefix_count', label: '出现不可见的固定前缀', unit: '个前缀', extreme: 'max' },
  { key: 'completely_interrupted_prefix_count', label: '所有观察方向均不可见', unit: '个前缀', extreme: 'max' },
  { key: 'invisible_direction_count', label: '不可见独立观察方向', unit: '个方向', extreme: 'max' },
  { key: 'fixed_visible_ipv4_address_count', label: '固定前缀可见 IPv4 地址', unit: '个地址', extreme: 'min' },
  { key: 'fixed_visible_ipv6_slash48_count', label: '固定前缀可见 IPv6 /48', unit: '个 /48 等价块', extreme: 'min' },
  { key: 'new_visible_ipv4_address_count', label: '新出现前缀可见 IPv4 地址', unit: '个地址', extreme: 'max' },
  { key: 'new_visible_ipv6_slash48_count', label: '新出现前缀可见 IPv6 /48', unit: '个 /48 等价块', extreme: 'max' },
]
function formatTime(value: string | undefined) {
  if (!value || !Number.isFinite(Date.parse(value))) return '未知'
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).format(new Date(value))
}
function formatNumber(value: number | null | undefined) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString('zh-CN') : '—'
}
const emit = defineEmits<{ (event: 'range-change', markers: ObservationChartMarker[]): void }>()
const start = ref('')
const end = ref('')
const inputs = computed(() => props.series.timestamps.map(time => formatTime(time).replace(' ', 'T')))
const lastIndex = computed(() => props.series.timestamps.length - 1)
function resetRange() {
  start.value = inputs.value[0] ?? ''
  end.value = inputs.value[lastIndex.value] ?? ''
}
watch(() => props.series, resetRange, { immediate: true })
const startIndex = computed(() => inputs.value.indexOf(start.value))
const endIndex = computed(() => inputs.value.indexOf(end.value))
const rangeError = computed(() => {
  if (!inputs.value.length) return '当前窗口没有可核对的状态点。'
  if (startIndex.value < 0 || endIndex.value < 0) return '请选择窗口内已有的状态点，不对缺少的时点插值。'
  if (startIndex.value > endIndex.value) return '核对起点不能晚于终点。'
  return ''
})
watch([startIndex, endIndex, rangeError], () => {
  emit('range-change', rangeError.value ? [] : [
    { time: props.series.timestamps[startIndex.value]!, label: '核对起点', color: '#176d8f' },
    { time: props.series.timestamps[endIndex.value]!, label: '核对终点', color: '#d96c0b' },
  ])
}, { immediate: true })
function compareTo(index: number) {
  start.value = inputs.value[0] ?? ''
  end.value = inputs.value[index] ?? ''
}
function difference(values: number[]) {
  const left = values[startIndex.value]
  const right = values[endIndex.value]
  if (rangeError.value || typeof left !== 'number' || typeof right !== 'number' || !Number.isFinite(left) || !Number.isFinite(right)) return '—'
  const change = right - left
  return `${change > 0 ? '+' : ''}${formatNumber(change)}`
}
const rows = computed(() => metrics.map(metric => {
  const values = props.series.tracks[metric.key] ?? []
  let extremeIndex = -1
  values.forEach((value, index) => {
    if (!Number.isFinite(value) || typeof value !== 'number') return
    if (extremeIndex < 0 || (metric.extreme === 'max' ? value > values[extremeIndex]! : value < values[extremeIndex]!)) extremeIndex = index
  })
  return { ...metric, values, extremeIndex }
}))
</script>

<template>
  <section class="timeline" aria-labelledby="timeline-title">
    <header>
      <div><p class="eyebrow">沿时间核对观测</p><h2 id="timeline-title">路由变化 · 时段核对</h2></div>
      <p class="timezone">北京时间 · {{ series.interval_seconds / 60 }} 分钟状态点</p>
    </header>
    <div class="range-controls">
      <label>核对起点<input v-model="start" type="datetime-local" :step="series.interval_seconds" :min="inputs[0]" :max="inputs[lastIndex]" aria-describedby="range-note range-error" /></label>
      <span class="range-arrow" aria-hidden="true">→</span>
      <label>核对终点<input v-model="end" type="datetime-local" :step="series.interval_seconds" :min="inputs[0]" :max="inputs[lastIndex]" aria-describedby="range-note range-error" /></label>
      <button type="button" @click="resetRange">核对窗口首末点</button>
    </div>
    <p id="range-error" class="range-error" role="status">{{ rangeError }}</p>
    <p id="range-note" class="note">净变化仅比较两个状态点，不表示期间持续单调变化。所选起止时点同时标在下方曲线上。</p>
    <div class="comparison-scroll" tabindex="0" role="region" aria-label="国家路由时段数值表，可横向滚动">
      <table>
        <caption>所选时点的数值、净变化与全窗口参照</caption>
        <thead><tr><th scope="col">指标 / 单位</th><th scope="col">起点值</th><th scope="col">终点值</th><th scope="col">净变化</th><th scope="col">全窗口极值 / 首次时点</th><th scope="col">窗口末值</th></tr></thead>
        <tbody>
          <tr v-for="row in rows" :key="row.key">
            <th scope="row">{{ row.label }}<small>{{ row.unit }}</small></th>
            <td>{{ rangeError ? '—' : formatNumber(row.values[startIndex]) }}</td>
            <td>{{ rangeError ? '—' : formatNumber(row.values[endIndex]) }}</td>
            <td class="delta">{{ difference(row.values) }}</td>
            <td><span>{{ row.extreme === 'max' ? '最高' : '最低' }} {{ formatNumber(row.values[row.extremeIndex]) }} {{ row.unit }}</span><button v-if="row.extremeIndex >= 0" class="extreme-time" type="button" :aria-label="`核对窗口首点至${row.label}极值`" @click="compareTo(row.extremeIndex)">{{ formatTime(series.timestamps[row.extremeIndex]) }} ↗</button><small v-else>未知</small></td>
            <td>{{ formatNumber(row.values[lastIndex]) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <p class="note">极值相同时显示首次达到的时点；点击极值时间，核对窗口首点至该时点的各项数值。固定可见资源取最低值，新出现资源单列最高值。</p>
    <div class="window-state">
      <strong>{{ series.is_final_in_data_range ? '该事件在当前数据范围内已结束' : '当前数据范围内尚不能确认事件结束' }}</strong>
      <span>窗口末点：{{ formatTime(series.timestamps[lastIndex]) }}</span>
      <span>数据覆盖至：{{ formatTime(series.data_through) }}</span>
      <span>事件结束时间：{{ formatTime(overview.event.event_end_at_utc ?? undefined) }}</span>
      <small>窗口末值描述该时点的观测，不直接表示完全恢复。</small>
    </div>
    <details class="definitions">
      <summary>指标口径与数据来源</summary>
      <p>{{ series.collector_id }} · 固定集合 {{ formatNumber(overview.cohort.fixed_prefix_count) }} 个前缀 · {{ formatNumber(overview.cohort.independent_direction_relation_count) }} 个前缀—观察方向关系</p>
      <p>本表使用数量，不把不同单位相加或换算为用户影响。固定集合与新出现前缀分别统计。</p>
      <dl><div v-for="row in rows" :key="row.key"><dt>{{ row.label }}</dt><dd>{{ series.track_definitions[row.key]?.definition || 'Unknown：当前发布未提供该指标定义。' }}</dd></div></dl>
      <p class="identity">发布：{{ series.publication_id }}<br />固定集合：{{ series.cohort_id }}</p>
    </details>
  </section>
</template>

<style scoped>
.timeline { min-width: 0; padding: 24px; background: #fff; border: 1px solid #d8dfe3; border-top: 3px solid #176d8f; color: #233b49; }
header { display: flex; align-items: end; justify-content: space-between; gap: 18px; margin-bottom: 18px; }
h2 { margin: 5px 0 0; color: #122b3b; font-size: 22px; }
.eyebrow { margin: 0; color: #176d8f; font-size: 10px; font-weight: 750; letter-spacing: .1em; }
.timezone { margin: 0; color: #677783; font-size: 11px; }
.range-controls { display: flex; flex-wrap: wrap; align-items: end; gap: 14px; padding: 16px; background: #f6f2ea; border: 1px solid #e1dbd0; }
label { display: grid; gap: 7px; font-size: 11px; font-weight: 700; }
input { min-width: 210px; height: 38px; padding: 0 10px; border: 1px solid #aebdc7; border-radius: 3px; background: #fff; color: #183545; font: 12px var(--mono); }
.range-arrow { padding-bottom: 10px; color: #7e8f98; }
button { min-height: 38px; padding: 0 14px; border: 1px solid #173f51; border-radius: 3px; background: #173f51; color: #fff; font-size: 11px; cursor: pointer; }
button:focus-visible, input:focus-visible, .comparison-scroll:focus-visible, summary:focus-visible { outline: 2px solid #e27839; outline-offset: 3px; }
.range-error { margin: 8px 0 0; color: #9b3430; font-size: 12px; }
.range-error:empty { display: none; }
.note { margin: 10px 0; color: #62737d; font-size: 11px; line-height: 1.7; }
.comparison-scroll { max-width: 100%; overflow-x: auto; border: 1px solid #dbe2e6; }
table { width: 100%; min-width: 920px; border-collapse: collapse; text-align: right; }
caption { text-align: left; padding: 10px 13px; color: #526875; background: #f8fafb; font-size: 11px; }
thead th { padding: 11px 13px; background: #edf2f5; color: #526875; font-size: 10px; white-space: nowrap; }
tbody th { min-width: 160px; text-align: left; font-size: 11px; font-weight: 650; }
th:first-child { text-align: left; }
td, tbody th { padding: 12px 13px; border-top: 1px solid #e1e7eb; }
td { font: 650 12px var(--mono); white-space: nowrap; }
tbody tr:nth-child(even) { background: #fafbfc; }
td:nth-child(5) { background: #f5f8fa; font-size: 10px; }
small { display: block; margin-top: 5px; color: #71828c; font: 10px/1.5 var(--sans); }
.delta { color: #176d8f; }
.extreme-time { display: block; min-height: 28px; padding: 4px 0 0; margin-left: auto; border: none; border-radius: 0; background: none; color: #176d8f; font: 10px var(--mono); text-decoration: underline; text-underline-offset: 3px; }
.window-state { display: flex; flex-wrap: wrap; gap: 10px 20px; padding: 14px 16px; margin-top: 16px; background: #f1f5f7; border-left: 3px solid #176d8f; font-size: 11px; }
.window-state strong, .window-state small { flex-basis: 100%; }
.window-state small { margin: 0; }
.definitions { margin-top: 16px; color: #60727d; font-size: 11px; line-height: 1.7; }
summary { cursor: pointer; color: #176d8f; font-weight: 700; }
dl { display: grid; gap: 8px; }
dl div { display: grid; grid-template-columns: 220px 1fr; gap: 12px; }
dt { font-weight: 650; }
dd { margin: 0; }
.identity { overflow-wrap: anywhere; font: 10px/1.8 var(--mono); }
@media (max-width: 680px) { .timeline { padding: 16px; } header { align-items: start; flex-direction: column; gap: 10px; } .range-controls { display: grid; grid-template-columns: minmax(0, 1fr); padding: 12px; } input { min-width: 0; width: 100%; } .range-arrow { display: none; } dl div { grid-template-columns: 1fr; gap: 0; } }
</style>

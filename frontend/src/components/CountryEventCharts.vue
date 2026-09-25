<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { countryEventWindow, getCountryEventSeries, validateCountryEventRange, type CountryChartData, type CountryChartKind } from '@/api/countryEventCharts'
import type { CountryOutageRecord } from '@/api/countryOutageRecord'
import type { FeatureRange } from '@/api/features'
import ObservationChart from '@/components/ObservationChart.vue'
import PageState from '@/components/PageState.vue'
import { businessTimezone } from '@/utils/businessTime'
import { cleanText, errorMessage } from '@/utils/normalize'

const props = defineProps<{ record: CountryOutageRecord }>()
const country = computed(() => cleanText(props.record.item.country_name))
const range = ref<FeatureRange | null>(null)
const startInput = ref('')
const endInput = ref('')
const rangeError = ref('')
let automaticWindow = true
let generation = 0
let pending = 0
interface ChartState { data: CountryChartData | null; loading: boolean; error: string }
const state = reactive<Record<CountryChartKind, ChartState>>({
  features: { data: null, loading: false, error: '' },
  as: { data: null, loading: false, error: '' },
  prefix: { data: null, loading: false, error: '' },
})
const panels = [
  { id: 'messages', kind: 'features' as const, title: 'BGP 报文时序', unit: '条', note: '每 5 分钟的通告与撤销报文数', lines: [{ key: 'announce', name: '通告', color: '#167c80' }, { key: 'withdraw', name: '撤销', color: '#ce683c' }] },
  { id: 'ipv4', kind: 'features' as const, title: 'IPv4 地址数量变化', unit: '个地址', note: '每 5 分钟的路由可见 IPv4 地址量', lines: [{ key: 'ipv4Addresses', name: 'IPv4 地址', color: '#167c80' }] },
  { id: 'v4-prefix', kind: 'features' as const, title: 'IPv4 /24 等价量变化', unit: '/24', note: '地址资源折算值，不是实际前缀条数', lines: [{ key: 'ipv4Prefixes', name: 'IPv4 /24 等价量', color: '#4879b0' }] },
  { id: 'v6-prefix', kind: 'features' as const, title: 'IPv6 /48 等价量变化', unit: '/48', note: '地址资源折算值，与 IPv4 分开计量', lines: [{ key: 'ipv6Prefixes', name: 'IPv6 /48 等价量', color: '#8662ad' }] },
  { id: 'as', kind: 'as' as const, title: 'AS 中断时序', unit: '个 AS', note: '每 3 分钟处于中断区间的去重 AS 数', lines: [{ key: 'count', name: '中断 AS', color: '#ce683c' }] },
  { id: 'prefix', kind: 'prefix' as const, title: '前缀中断时序', unit: '个前缀', note: '每 3 分钟处于中断区间的去重前缀数，沿用既有粗路由筛选', lines: [{ key: 'count', name: '中断前缀', color: '#a16b35' }] },
]
const charts = computed(() => panels.map((panel) => {
  const source = state[panel.kind]
  const series = panel.lines.map((line) => ({ name: line.name, color: line.color, data: source.data?.series[line.key] ?? [] }))
  const values = series.flatMap((line) => line.data.flatMap((point) => point[1] === null ? [] : [point[1]]))
  const resource = panel.kind === 'features' && panel.id !== 'messages'
  const padding = Math.max(1, (Math.max(...values) - Math.min(...values)) * 0.1)
  return { ...panel, ...source, series, hasData: values.length > 0,
    yMin: resource && values.length ? Math.max(0, Math.floor(Math.min(...values) - padding)) : 0,
    yMax: resource && values.length ? Math.ceil(Math.max(...values) + padding) : null }
}))
const markers = computed(() => [{ time: props.record.item.start_time, label: '事件检测' },
  ...(props.record.item.country_incident?.peak_at ? [{ time: props.record.item.country_incident.peak_at, label: 'AS 影响峰值' }] : [])])
const busy = computed(() => Object.values(state).some((item) => item.loading))

async function readCharts(selected: FeatureRange, background = false) {
  if (background && pending) return
  rangeError.value = ''
  try {
    validateCountryEventRange(selected, props.record)
    if (!country.value) throw new Error('事件记录缺少国家名称，无法查询国家统计')
  } catch (cause) { rangeError.value = errorMessage(cause); return }
  const token = ++generation
  const sameWindow = range.value?.start_time === selected.start_time && range.value?.end_time === selected.end_time
  range.value = { ...selected }
  if (!background || automaticWindow) {
    startInput.value = selected.start_time.replace(' ', 'T')
    endInput.value = selected.end_time.replace(' ', 'T')
  }
  const selectedCountry = country.value
  const selectedVersion = props.record.delivery.version
  pending++
  try {
  await Promise.all((['features', 'as', 'prefix'] as const).map(async (kind) => {
    state[kind] = { data: sameWindow && state[kind].data?.version === selectedVersion ? state[kind].data : null, loading: true, error: '' }
    try {
      const data = await getCountryEventSeries(kind, selectedCountry, selected, selectedVersion)
      if (token === generation) state[kind].data = data
    } catch (cause) {
      if (token === generation) state[kind].error = errorMessage(cause)
    } finally { if (token === generation) state[kind].loading = false }
  }))
  } finally { pending-- }
}
function applyRange() {
  automaticWindow = false
  const sqlTime = (value: string) => value.replace('T', ' ') + (value.length === 16 ? ':00' : '')
  void readCharts({ start_time: sqlTime(startInput.value), end_time: sqlTime(endInput.value) })
}
function resetRange() {
  automaticWindow = true
  try { void readCharts(countryEventWindow(props.record)) }
  catch (cause) { rangeError.value = errorMessage(cause) }
}
watch(() => props.record, (record, previous) => {
  if (record.item.reference !== previous?.item.reference) { resetRange(); return }
  try { void readCharts(automaticWindow || !range.value ? countryEventWindow(record) : range.value, record.delivery.version === previous?.delivery.version) }
  catch (cause) { rangeError.value = errorMessage(cause) }
}, { immediate: true })
onBeforeUnmount(() => { generation++ })
</script>

<template>
  <section class="country-charts" aria-labelledby="country-charts-title">
    <header class="charts-header">
      <div><h2 id="country-charts-title">{{ country }} · 同期统计</h2><p>国家特征与中断记录。图表反映当前观察点的 BGP 可见性，不等同于用户断网范围。</p></div>
      <button type="button" :disabled="busy" @click="resetRange">回到事件附近</button>
    </header>
    <form class="chart-range" @submit.prevent="applyRange">
      <label>开始时间<input v-model="startInput" type="datetime-local" step="1" required @input="automaticWindow = false" /></label>
      <label>结束时间<input v-model="endInput" type="datetime-local" step="1" required @input="automaticWindow = false" /></label>
      <button type="submit" :disabled="busy">查询</button><span>北京时间 · 单次最多 24 小时 · 不含终点</span>
    </form>
    <p v-if="rangeError" class="chart-error" role="alert">{{ rangeError }}</p>
    <p v-if="range" class="chart-window">当前图表：{{ range.start_time }} — {{ range.end_time }}。统计全国家同期记录，不限于上方事件名单；缺失时点保留断线。</p>
    <div class="chart-grid">
      <section v-for="chart in charts" :key="chart.id" class="chart-panel">
        <header><h3>{{ chart.title }}</h3><p>{{ chart.note }}</p></header>
        <PageState v-if="chart.loading && !chart.hasData" kind="loading" title="正在读取统计" />
        <PageState v-else-if="chart.error" kind="error" title="此项统计暂不可用" :detail="chart.error" @retry="range && readCharts(range)" />
        <ObservationChart v-else-if="chart.hasData" :series="chart.series" :markers="markers" :unit="chart.unit" :y-min="chart.yMin" :y-max="chart.yMax" :timezone="businessTimezone" :height="270" />
        <PageState v-else kind="empty" title="当前窗口没有可绘制数据" detail="没有返回有效观测值，不能解释为零。" />
      </section>
    </div>
    <details v-if="Object.values(state).some((item) => item.data)" class="chart-source"><summary>统计来源</summary>
      <p>直接查询已有国家特征、AS 中断与前缀中断接口；三类查询各自读取当前数据，不合算为本事件的峰值或恢复结论。</p>
      <p v-for="(item, key) in state" :key="key">{{ key === 'features' ? '国家特征' : key === 'as' ? 'AS 中断' : '前缀中断' }}：{{ item.data?.version || '未提供数据版本' }}</p>
    </details>
  </section>
</template>

<style scoped>
.country-charts { margin-top: 24px; }
.charts-header { display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }
h2 { margin: 0; font-size: 20px; } h3 { margin: 0; font-size: 16px; }
.charts-header p, .chart-window, .chart-panel header p, .chart-range span { color: var(--muted, #63707c); font-size: 12px; line-height: 1.65; }
.chart-range { display: flex; align-items: end; gap: 14px; flex-wrap: wrap; padding: 16px 0; }
.chart-range label { display: grid; gap: 6px; font-size: 12px; }
input, button { border: 1px solid var(--line); border-radius: 4px; background: var(--surface, #fff); padding: 8px 10px; color: inherit; }
button { cursor: pointer; } button:disabled { opacity: .5; cursor: default; }
.chart-error { color: #a23529; font-size: 13px; }
.chart-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
.chart-panel { min-width: 0; padding: 18px; background: var(--surface, #fff); border: 1px solid var(--line); border-radius: 5px; }
.chart-panel header p { margin: 8px 0; }
.chart-source { margin-top: 18px; font-size: 12px; color: var(--muted, #63707c); overflow-wrap: anywhere; }
.chart-source summary { cursor: pointer; }
@media (max-width: 850px) { .chart-grid { grid-template-columns: 1fr; } .chart-panel { padding: 12px; } }
</style>

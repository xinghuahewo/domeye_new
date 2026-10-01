<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, onServerPrefetch, ref, watch } from 'vue'
import { getTopFeatures } from '@/api/features'
import type { FeaturePoint } from '@/types/api'
import { toBusinessTime } from '@/utils/businessTime'
import { scopeMillis, scopeError, scopeLabel, scopeMinimum, scopeMaximum } from '@/utils/coreScope'
import { useCoreTrendScope } from '@/utils/coreTrendScope'
import LineChart, { type ChartSeries } from './LineChart.vue'

const props = defineProps<{ date: string; start?: string; end?: string; country?: string; refreshKey: number }>()
const homeWindow = computed(() => ({
  start: props.start || `${props.date}T00:00:00`,
  end: props.end || toBusinessTime(new Date(Date.parse(`${props.date}T00:00:00+08:00`) + 86400000)).replace(' ', 'T'),
  country: props.country || '',
}))
const { selectedWindow, draft, rangeError, localRange, applyRange, resetRange } = useCoreTrendScope(homeWindow)
const timeBounds = computed<[string, string]>(() => [selectedWindow.value.start + '+08:00', selectedWindow.value.end + '+08:00'])
const period = computed(() => props.start || localRange.value ? '此区间' : '此日')
const points = ref<FeaturePoint[]>([])
const loading = ref(false)
const error = ref('')
let controller: AbortController | undefined
let requestNumber = 0
const metrics = computed(() => [
  { title: '宣告与撤回', unit: '元素 / 文件', note: '已接受的宣告与撤回元素', series: [series('宣告', 'announce'), series('撤回', 'withdraw', '#788f58')] },
  { title: 'IPv4 资源量', unit: '/24 等价量', note: 'Feature 文件末状态 · 非去重前缀数', series: [series('IPv4 资源', 'ipv4Prefixes')] },
  { title: 'IPv6 资源量', unit: '/48 等价量', note: 'Feature 文件末状态', series: [series('IPv6 资源', 'ipv6Prefixes')] },
])
function series(name: string, key: keyof Omit<FeaturePoint, 'time'>, color = '#3e6f89'): ChartSeries {
  const data: ChartSeries['data'] = []
  points.value.forEach((point, index) => {
    const prior = points.value[index - 1]
    if (prior && Date.parse(point.time) - Date.parse(prior.time) > 300000) {
      data.push([new Date(Date.parse(prior.time) + 300000).toISOString(), null])
    }
    data.push([point.time, point[key]])
  })
  return { name, color, data }
}
const hasValues = (items: ChartSeries[]) => items.some(item => item.data.some(([, value]) => value !== null))
function lastValue(items: ChartSeries[]) {
  if (items.length !== 1) return ''
  const last = items[0]?.data.at(-1)
  return last?.[1] == null ? '末值 —' : `末值 ${last[1].toLocaleString('zh-CN')}`
}
const observedAt = computed(() => points.value.length
  ? toBusinessTime(new Date(points.value.at(-1)!.time)).slice(scopeMillis(selectedWindow.value.end) - scopeMillis(selectedWindow.value.start) > 86400000 ? 5 : 11)
  : '')
async function load() {
  const current = ++requestNumber
  controller?.abort()
  points.value = []; error.value = ''; loading.value = false
  const window = selectedWindow.value
  const invalid = scopeError(window)
  if (invalid) { error.value = invalid; return }
  const start = window.start.replace('T', ' ')
  // Feature 旧接口的终点包含在内；秒级采样减一秒转换为本页半开区间。
  const end = toBusinessTime(new Date(scopeMillis(window.end) - 1000))
  const request = new AbortController()
  controller = request; loading.value = true
  try {
    const feature = await getTopFeatures(window.country || 'collector', { start_time: start, end_time: end }, request.signal)
    if (current !== requestNumber || request.signal.aborted) return
    if (feature.every(point => scopeMillis(window.start) <= Date.parse(point.time) && Date.parse(point.time) < scopeMillis(window.end))) points.value = feature
    else error.value = `${period.value}特征读取失败，请重新读取`
  } catch (cause) {
    if (current === requestNumber && !request.signal.aborted) error.value = `${period.value}特征读取失败，请重新读取`
  } finally { if (current === requestNumber) loading.value = false }
}
watch([selectedWindow, () => props.refreshKey], load)
onMounted(load)
onServerPrefetch(load)
onBeforeUnmount(() => { requestNumber++; controller?.abort() })
</script>

<template>
  <section id="routing" class="core-daily-trends" aria-labelledby="core-trends-title">
    <div class="c-section-caption"><h2 id="core-trends-title">趋势分析</h2><span>{{ scopeLabel(selectedWindow) }} · {{ country || '全球' }}</span></div>
    <form class="core-trend-range" aria-label="特征图时间筛选" @submit.prevent="applyRange">
      <label>开始时间<input v-model="draft.start" type="datetime-local" step="1" :min="scopeMinimum" :max="scopeMaximum" required aria-label="特征图开始时间" /></label>
      <label>结束时间<input v-model="draft.end" type="datetime-local" step="1" :min="scopeMinimum" :max="scopeMaximum" required aria-label="特征图结束时间" /></label>
      <button type="submit">应用图表区间</button><button type="button" class="core-trend-reset" @click="resetRange">重置为首页区间</button>
      <p>北京时间 · 右端不含。仅影响下方三张图，国家跟随首页；{{ localRange ? '当前使用独立图表区间' : '当前跟随首页区间' }}。</p>
      <p v-if="rangeError" class="core-trend-range-error" role="alert">{{ rangeError }}</p>
    </form>
      <section class="core-trend-panel" aria-label="Feature 特征趋势" :aria-busy="loading">
        <header><h3>特征趋势 <span>FEATURE</span></h3><p>{{ country || '全球 · RRC25' }} · 全部地址族<span v-if="observedAt"> · 末次采样 {{ observedAt }}</span></p></header>
        <div v-for="metric in metrics" :key="metric.title" class="core-trend-metric">
          <div class="core-trend-metric-heading"><h4>{{ metric.title }}</h4><span>{{ lastValue(metric.series) }}</span></div>
          <div v-if="loading" class="core-trend-state" role="status">正在读取区间特征…</div>
          <div v-else-if="error" class="core-trend-state" role="status">{{ error }}</div>
          <LineChart v-else-if="hasValues(metric.series)" :series="metric.series" :unit="metric.unit" :height="320" :show-points="true" :show-data-zoom="true" :time-bounds="timeBounds" />
          <div v-else class="core-trend-state" role="status">{{ points.length ? `${period}未返回该指标` : `${period}没有可用特征采样` }}<small>缺失不表示数量为零</small></div>
          <p>{{ metric.unit }} · {{ metric.note }}</p>
        </div>
      </section>
    <p class="core-trend-scope">可拖动每张图底部的时间滑块放大查看。三图读取所选地区的全部地址族，显示文件末采样；缺失时段断线。首页地区或区间变化时，图表区间自动跟随重置。</p>
  </section>
</template>

<style scoped>
.core-daily-trends { margin:26px 0; scroll-margin-top:18px; }
.core-trend-range { display:flex; align-items:end; flex-wrap:wrap; gap:12px; margin-bottom:18px; padding:16px 20px; background:var(--surface); border:1px solid var(--line); border-radius:6px; }
.core-trend-range label { display:grid; gap:6px; font-size:11px; color:var(--muted); }
.core-trend-range input { padding:9px; border:1px solid var(--line); border-radius:4px; background:white; }
.core-trend-range button { padding:10px 16px; border-radius:4px; background:var(--accent); color:white; font-size:12px; }
.core-trend-range .core-trend-reset { background:transparent; color:var(--accent); border:1px solid var(--line); }
.core-trend-range p { width:100%; font-size:11px; color:var(--muted); line-height:1.7; }
.core-trend-range .core-trend-range-error { color:#945038; }
.core-trend-panel { min-width:0; background:var(--surface); border:1px solid var(--line); border-radius:6px; }
.core-trend-panel>header { padding:19px 21px 16px; border-bottom:1px solid var(--line); }
.core-trend-panel h3 { font-size:17px; font-weight:550; }
.core-trend-panel h3 span { font:11px var(--mono); color:var(--muted); margin-left:8px; }
.core-trend-panel p,.core-trend-scope { font-size:11px; line-height:1.7; color:var(--muted); }
.core-trend-panel>header p { margin-top:5px; }
.core-trend-metric { padding:17px 20px 13px; }
.core-trend-metric+.core-trend-metric { border-top:1px solid var(--line); }
.core-trend-metric-heading { display:flex; align-items:baseline; justify-content:space-between; gap:10px; flex-wrap:wrap; min-height:26px; }
.core-trend-metric-heading h4 { font-size:13px; font-weight:550; margin:0; }
.core-trend-metric-heading>span { font:12px var(--mono); }
.core-trend-state { min-height:320px; display:flex; flex-direction:column; justify-content:center; align-items:center; gap:8px; color:var(--muted); font-size:13px; text-align:center; }
.core-trend-state small { font-size:11px; }
.core-trend-scope { margin-top:12px; }
@media(max-width:700px) { .core-trend-range { padding:14px 12px; }.core-trend-range label { width:100%; }.core-trend-range input { min-width:0; }.core-trend-metric { padding:15px 12px 12px; }.core-trend-panel>header { padding:17px; } }
</style>

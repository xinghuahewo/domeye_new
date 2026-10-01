<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, onServerPrefetch, ref, watch } from 'vue'
import { getTopFeatures } from '@/api/features'
import { getResources, type ResourcePoint, type ResourceStatistics } from '@/api/resources'
import type { FeaturePoint } from '@/types/api'
import { toBusinessTime } from '@/utils/businessTime'
import { scopeMillis, scopeError, scopeLabel } from '@/utils/coreScope'
import LineChart, { type ChartSeries } from './LineChart.vue'

const props = defineProps<{ date: string; start?: string; end?: string; country?: string; refreshKey: number }>()
const selectedWindow = computed(() => ({
  start: props.start || `${props.date}T00:00:00`,
  end: props.end || toBusinessTime(new Date(Date.parse(`${props.date}T00:00:00+08:00`) + 86400000)).replace(' ', 'T'),
  country: props.country || '',
}))
const timeBounds = computed<[string, string]>(() => [selectedWindow.value.start + '+08:00', selectedWindow.value.end + '+08:00'])
const period = computed(() => props.start ? '此区间' : '此日')
const points = ref<FeaturePoint[]>([])
const loading = ref(false)
const error = ref('')
const resources = ref<ResourceStatistics | null>(null)
const resourceError = ref('')
const resourceBounds = computed<[string, string] | undefined>(() => resources.value?.query
  ? [resources.value.query.start, resources.value.query.end_exclusive] : undefined)
let controller: AbortController | undefined
let requestNumber = 0
const metrics = computed(() => [
  { title: '宣告与撤回', unit: '元素 / 文件', note: '已接受的宣告与撤回元素', series: [series('宣告', 'announce'), series('撤回', 'withdraw', '#788f58')] },
  { title: 'IPv4 资源量', unit: '/24 等价量', note: 'Feature 文件末状态 · 非去重前缀数', series: [series('IPv4 资源', 'ipv4Prefixes')] },
  { title: 'IPv6 资源量', unit: '/48 等价量', note: 'Feature 文件末状态', series: [series('IPv6 资源', 'ipv6Prefixes')] },
])
const resourceMetrics = computed(() => [
  { title: 'IPv4 去重前缀', unit: '条', note: '不同 IPv4 前缀', key: 'ipv4_prefix_count' as const },
  { title: 'IPv6 资源量', unit: '/48 块', note: 'IPv6 /48 覆盖块', key: 'ipv6_48_count' as const },
  { title: '公有 AS 数', unit: '个', note: 'Resource 尾 ASN 规则 · 与明确起源 AS 数分开', key: 'public_as_count' as const },
].map(metric => ({ ...metric, series: [resourceSeries(metric.title, metric.key)] })))
function resourceSeries(name: string, key: keyof ResourcePoint['metrics']): ChartSeries {
  return { name, color: '#967431', type: 'scatter', data: (resources.value?.points ?? []).map(point => [point.observed_at, point.metrics[key].main]) }
}
const resourceNote = computed(() => {
  const rows = resources.value?.points ?? []
  if (!rows.length) return '各 RIB 独立时点'
  const last = toBusinessTime(new Date(rows.at(-1)!.observed_at)).slice(props.start && props.start.slice(0, 10) !== props.end?.slice(0, 10) ? 5 : 11)
  return `${rows.length} 个 RIB 时点 · 末次 ${last} · 时点之间未知`
})
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
  ? toBusinessTime(new Date(points.value.at(-1)!.time)).slice(props.start && selectedWindow.value.start.slice(0, 10) !== selectedWindow.value.end.slice(0, 10) ? 5 : 11)
  : '')
async function load() {
  const current = ++requestNumber
  controller?.abort()
  points.value = []; error.value = ''; resources.value = null; resourceError.value = ''; loading.value = false
  const window = selectedWindow.value
  const invalid = scopeError(window)
  if (invalid) { error.value = resourceError.value = invalid; return }
  const start = window.start.replace('T', ' ')
  // Feature 旧接口的终点包含在内；秒级采样减一秒转换为本页半开区间。
  const end = toBusinessTime(new Date(scopeMillis(window.end) - 1000))
  const request = new AbortController()
  controller = request; loading.value = true
  try {
    const [feature, resource] = await Promise.allSettled([
      getTopFeatures(window.country || 'collector', { start_time: start, end_time: end }, request.signal),
      window.country ? Promise.resolve(null) : getResources({ start_time: start, end_time: window.end.replace('T', ' ') }, request.signal),
    ])
    if (current !== requestNumber || request.signal.aborted) return
    if (feature.status === 'fulfilled' && feature.value.every(point => scopeMillis(window.start) <= Date.parse(point.time) && Date.parse(point.time) < scopeMillis(window.end))) points.value = feature.value
    else error.value = `${period.value}特征读取失败，请重新读取`
    if (window.country) resourceError.value = '地区 RIB 统计尚未生成'
    else if (resource.status === 'fulfilled') resources.value = resource.value
    else resourceError.value = `${period.value}资源统计读取失败，请重新读取`
  } catch (cause) {
    if (current === requestNumber && !request.signal.aborted) error.value = `${period.value}特征读取失败，请重新读取`
  } finally { if (current === requestNumber) loading.value = false }
}
watch(() => `${props.date}|${props.start}|${props.end}|${props.country}|${props.refreshKey}`, load)
onMounted(load)
onServerPrefetch(load)
onBeforeUnmount(() => { requestNumber++; controller?.abort() })
</script>

<template>
  <section id="routing" class="core-daily-trends" aria-labelledby="core-trends-title">
    <div class="c-section-caption"><h2 id="core-trends-title">趋势分析</h2><span>{{ props.start ? scopeLabel(selectedWindow) : date }} · {{ country || '全球' }}</span></div>
    <div class="core-trend-columns">
      <section class="core-trend-panel" aria-label="Feature 特征趋势" :aria-busy="loading">
        <header><h3>特征趋势 <span>FEATURE</span></h3><p>{{ country || '全球 · RRC25' }} · 全部地址族<span v-if="observedAt"> · 末次采样 {{ observedAt }}</span></p></header>
        <div v-for="metric in metrics" :key="metric.title" class="core-trend-metric">
          <div class="core-trend-metric-heading"><h4>{{ metric.title }}</h4><span>{{ lastValue(metric.series) }}</span></div>
          <div v-if="loading" class="core-trend-state" role="status">正在读取区间特征…</div>
          <div v-else-if="error" class="core-trend-state" role="status">{{ error }}</div>
          <LineChart v-else-if="hasValues(metric.series)" :series="metric.series" :unit="metric.unit" :height="200" :show-points="true" :time-bounds="timeBounds" />
          <div v-else class="core-trend-state" role="status">{{ points.length ? `${period}未返回该指标` : `${period}没有可用特征采样` }}<small>缺失不表示数量为零</small></div>
          <p>{{ metric.unit }} · {{ metric.note }}</p>
        </div>
      </section>
      <section class="core-trend-panel" aria-label="Resource 资源趋势" :aria-busy="loading">
        <header><h3>资源规模 <span>RESOURCE</span></h3><p>{{ resourceNote }}</p></header>
        <div v-for="metric in resourceMetrics" :key="metric.title" class="core-trend-metric">
          <div class="core-trend-metric-heading"><h4>{{ metric.title }}</h4><span>{{ lastValue(metric.series) }}</span></div>
          <div v-if="loading" class="core-trend-state" role="status">正在读取 RIB 资源统计…</div>
          <div v-else-if="resourceError" class="core-trend-state" role="status">{{ resourceError }}</div>
          <LineChart v-else-if="hasValues(metric.series)" :series="metric.series" :unit="metric.unit" :height="200" :show-points="true" :time-bounds="resourceBounds" />
          <div v-else class="core-trend-state" role="status">{{ resources?.state === 'available' ? '该指标主值不可用' : resources?.message || `${period}没有 RIB 资源统计` }}<small>未计算或不适用不表示零</small></div>
          <p>{{ metric.note }} · {{ metric.unit }}</p>
        </div>
      </section>
    </div>
    <p class="core-trend-scope">按所选地区与时间区间读取全部地址族；异常列表的类型、等级和地址族筛选不改变此区。Feature 显示文件末采样，Resource 当前仅有全球独立 RIB 统计点，不连成连续状态或补齐缺失时段。</p>
  </section>
</template>

<style scoped>
.core-daily-trends { margin:26px 0; scroll-margin-top:18px; }
.core-trend-columns { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:20px; }
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
.core-trend-state { min-height:200px; display:flex; flex-direction:column; justify-content:center; align-items:center; gap:8px; color:var(--muted); font-size:13px; text-align:center; }
.core-trend-state small { font-size:11px; }
.core-trend-scope { margin-top:12px; }
@media(max-width:700px) { .core-trend-columns { grid-template-columns:1fr; }.core-trend-metric { padding:15px 12px 12px; }.core-trend-panel>header { padding:17px; } }
</style>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, onServerPrefetch, ref, watch } from 'vue'
import { getTopFeatures } from '@/api/features'
import type { FeaturePoint } from '@/types/api'
import { toBusinessTime } from '@/utils/businessTime'
import profile from '../../../config/data-profile.json'
import LineChart, { type ChartSeries } from './LineChart.vue'

const props = defineProps<{ date: string; refreshKey: number }>()
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
const pendingResources = [
  ['IPv4 去重前缀', '不同 IPv4 前缀 · 条'], ['IPv6 资源量', 'IPv6 /48 覆盖块 · 块'], ['公有 AS 数', 'Resource 规则统计 · 个'],
]
function series(name: string, key: keyof Omit<FeaturePoint, 'time'>, color = '#3e6f89'): ChartSeries {
  return { name, color, data: points.value.map(point => [point.time, point[key]]) }
}
const hasValues = (items: ChartSeries[]) => items.some(item => item.data.some(([, value]) => value !== null))
function lastValue(items: ChartSeries[]) {
  if (items.length !== 1) return ''
  const last = items[0]?.data.at(-1)
  return last?.[1] == null ? '末值 —' : `末值 ${last[1].toLocaleString('zh-CN')}`
}
const observedAt = computed(() => points.value.length ? toBusinessTime(new Date(points.value.at(-1)!.time)).slice(11) : '')
async function load() {
  const current = ++requestNumber
  controller?.abort()
  points.value = []; error.value = ''; loading.value = false
  const date = props.date
  const start = `${date} 00:00:00`, end = `${date} 23:59:59`
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || date < profile.window_start.slice(0, 10) || date > profile.snapshot_time.slice(0, 10)) {
    error.value = '请选择数据档内的有效日期'; return
  }
  const request = new AbortController()
  controller = request; loading.value = true
  try {
    const data = await getTopFeatures('collector', { start_time: start, end_time: end }, request.signal)
    if (current !== requestNumber || request.signal.aborted) return
    if (data.some(point => toBusinessTime(new Date(point.time)).slice(0, 10) !== date)) throw new Error('特征响应超出所选日期')
    points.value = data
  } catch (cause) {
    if (current === requestNumber && !request.signal.aborted) error.value = '此日特征读取失败，请重新读取'
  } finally { if (current === requestNumber) loading.value = false }
}
watch(() => `${props.date}|${props.refreshKey}`, load)
onMounted(load)
onServerPrefetch(load)
onBeforeUnmount(() => { requestNumber++; controller?.abort() })
</script>

<template>
  <section id="routing" class="core-daily-trends" aria-labelledby="core-trends-title">
    <div class="c-section-caption"><h2 id="core-trends-title">趋势分析</h2><span>{{ date }} · 日内采样</span></div>
    <div class="core-trend-columns">
      <section class="core-trend-panel" aria-label="Feature 特征趋势" :aria-busy="loading">
        <header><h3>特征趋势 <span>FEATURE</span></h3><p>采集点 · 全部地址族<span v-if="observedAt"> · 末次采样 {{ observedAt }}</span></p></header>
        <div v-for="metric in metrics" :key="metric.title" class="core-trend-metric">
          <div class="core-trend-metric-heading"><h4>{{ metric.title }}</h4><span>{{ lastValue(metric.series) }}</span></div>
          <div v-if="loading" class="core-trend-state" role="status">正在读取当日特征…</div>
          <div v-else-if="error" class="core-trend-state" role="status">{{ error }}</div>
          <LineChart v-else-if="hasValues(metric.series)" :series="metric.series" :unit="metric.unit" :height="200" :show-points="true" />
          <div v-else class="core-trend-state" role="status">{{ points.length ? '此日未返回该指标' : '此日没有可用特征采样' }}<small>缺失不表示数量为零</small></div>
          <p>{{ metric.unit }} · {{ metric.note }}</p>
        </div>
      </section>
      <section class="core-trend-panel" aria-label="Resource 资源趋势">
        <header><h3>资源规模 <span>RESOURCE</span></h3><p>各 RIB 时点 · 时序数据待接入</p></header>
        <div v-for="[title, unit] in pendingResources" :key="title" class="core-trend-metric">
          <div class="core-trend-metric-heading"><h4>{{ title }}</h4><span>—</span></div>
          <div class="core-trend-state" role="status">Resource 时序待接入<small>当前没有可绘制的 RIB 资源序列</small></div>
          <p>{{ unit }}</p>
        </div>
      </section>
    </div>
    <p class="core-trend-scope">Feature 按所选日期读取，保持全部地址族；异常列表筛选不改变此区。Feature 与 Resource 保留各自来源和单位。</p>
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

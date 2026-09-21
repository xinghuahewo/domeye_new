<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, onServerPrefetch, ref, watch } from 'vue'
import type { CoreOverview } from '@/api/coreOverview'
import { anomalyKinds, getAnomalySummary, type AnomalyKind, type AnomalySummary } from '@/api/coreAnomalies'

const props = defineProps<{ date: string; family: string; base: CoreOverview | null; refreshKey: number; requestLoading?: boolean }>()
const emit = defineEmits<{ select: [kind: AnomalyKind, hour?: number] }>()
const summaries = ref<Partial<Record<AnomalyKind, AnomalySummary>>>({})
const loading = ref(false)
let controller: AbortController | undefined
let requestNumber = 0
const binding = computed(() => [props.date, props.family, props.base?.version, props.base?.state,
  props.base?.query.date, props.base?.query.family, props.refreshKey].join('|'))
const matching = computed(() => props.base?.query.date === props.date && props.base?.query.family === props.family)
const usable = computed(() => matching.value && props.base?.state === 'available')
const label = (kind: AnomalyKind) => summaries.value[kind]?.count?.toLocaleString('zh-CN') ?? '—'
const note = (kind: AnomalyKind) => summaries.value[kind]?.note || (loading.value || props.requestLoading ? '正在读取' : '当日异常数据不可用')
const hours = (kind: AnomalyKind) => summaries.value[kind]?.hours
const height = (kind: AnomalyKind, value: number) => `${value / Math.max(1, ...(hours(kind) || [])) * 100}%`
const hourLabel = (hour: number) => `${String(hour).padStart(2, '0')}:00–${String(hour + 1).padStart(2, '0')}:00`

async function load() {
  const current = ++requestNumber
  controller?.abort()
  summaries.value = {}
  loading.value = false
  const base = props.base
  if (!base || !usable.value) return
  const request = new AbortController()
  controller = request
  loading.value = true
  // 最多两类同时读取，避免首次打开首页时堆积六路分页请求。
  let next = 0
  async function worker() {
    while (next < anomalyKinds.length && !request.signal.aborted) {
      const item = anomalyKinds[next++]
      if (!item) return
      const kind = item[0]
      let result: AnomalySummary
      try { result = await getAnomalySummary(base!, kind, request.signal) }
      catch { result = { count: null, hours: null, note: '读取失败；请重新读取' } }
      if (current === requestNumber && !request.signal.aborted) summaries.value[kind] = result
    }
  }
  await Promise.all([worker(), worker()])
  if (current === requestNumber) loading.value = false
}
watch(binding, load)
onMounted(load)
onServerPrefetch(load)
onBeforeUnmount(() => { requestNumber++; controller?.abort() })
</script>

<template>
  <section id="anomalies" class="core-anomalies" aria-labelledby="core-anomalies-title" :aria-busy="loading">
    <div class="c-section-caption"><h2 id="core-anomalies-title">异常态势</h2><span>六类异常 · 当日新增记录</span></div>
    <div class="core-anomaly-grid">
      <article v-for="[kind, title] in anomalyKinds" :key="kind" class="core-anomaly-card" :data-testid="`anomaly-${kind}`">
        <header><h3>{{ title }}</h3><button :aria-label="`查看${title}记录`" :disabled="!usable || !base?.metadata.kinds.includes(kind)" @click="emit('select', kind)">↗</button></header>
        <div class="core-anomaly-number"><strong>{{ label(kind) }}</strong><span v-if="summaries[kind]?.count !== null && summaries[kind]?.count !== undefined">条新增记录</span></div>
        <template v-if="hours(kind)">
          <div class="core-anomaly-bars" :aria-label="`${title}每小时新增记录`">
            <button v-for="(value, hour) in hours(kind)" :key="hour" :aria-label="`${title} ${hourLabel(hour)}，${value} 条，筛选该时段`" :title="`${hourLabel(hour)} · ${value} 条`" @click="emit('select', kind, hour)"><span :style="{ height: height(kind, value) }"></span></button>
          </div>
          <div class="core-anomaly-axis" aria-hidden="true"><span>00:00</span><span>24:00</span></div>
        </template>
        <div v-else class="core-anomaly-no-chart" aria-hidden="true">—</div>
        <p>{{ note(kind) }}</p>
      </article>
    </div>
  </section>
</template>

<style scoped>
.core-anomalies { margin:24px 0; scroll-margin-top:18px; }
.core-anomaly-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:18px; }
.core-anomaly-card { padding:18px 21px 16px; background:var(--surface); border:1px solid var(--line); border-radius:6px; min-width:0; }
.core-anomaly-card header { display:flex; align-items:center; justify-content:space-between; gap:8px; }
.core-anomaly-card h3 { font-size:15px; font-weight:550; }
.core-anomaly-card header button { color:var(--accent); font-size:23px; min-width:32px; min-height:32px; }
.core-anomaly-number { display:flex; align-items:baseline; gap:9px; flex-wrap:wrap; margin:8px 0 14px; }
.core-anomaly-number strong { font:500 32px/1.2 var(--mono); letter-spacing:-1px; }
.core-anomaly-number span,.core-anomaly-card p,.core-anomaly-axis { font-size:11px; color:var(--muted); }
.core-anomaly-bars { height:58px; display:grid; grid-template-columns:repeat(24,minmax(0,1fr)); align-items:end; gap:2px; }
.core-anomaly-bars button { height:100%; display:flex; align-items:flex-end; padding:0; min-width:0; border-radius:2px; }
.core-anomaly-bars button:hover { background:#e7eef2; }
.core-anomaly-bars button span { display:block; width:100%; background:var(--accent); border-radius:1px; }
.core-anomaly-axis { display:flex; justify-content:space-between; margin-top:6px; }
.core-anomaly-card p { margin-top:6px; }
.core-anomaly-no-chart { height:81px; display:flex; align-items:center; color:var(--muted); }
@media(max-width:700px) { .core-anomaly-grid { grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; }.core-anomaly-card { padding:13px; }.core-anomaly-card h3 { font-size:13px; }.core-anomaly-number strong { font-size:27px; } }
@media(max-width:380px) { .core-anomaly-grid { grid-template-columns:1fr; } }
</style>

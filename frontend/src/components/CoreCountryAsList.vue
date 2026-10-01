<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { getAsCandidates } from '@/api/features'
import type { AsCandidatePage } from '@/types/api'
import { errorMessage } from '@/utils/normalize'
import { toBackendTime } from '@/utils/time'
import { toBusinessTime } from '@/utils/businessTime'

const props = defineProps<{ start: string; end: string; country: string; version: string; refreshKey: number }>()
const emit = defineEmits<{ refresh: [] }>()
const result = ref<AsCandidatePage | null>(null)
const loading = ref(false)
const error = ref('')
let requestNumber = 0
let controller: AbortController | undefined
const count = (value: number | null) => value === null ? '—' : value.toLocaleString('zh-CN')
const stamp = (value: string | null) => value ? toBusinessTime(new Date(value)).slice(5, 16) : '未知'
const query = () => ({ start: props.start, end: props.end, country: props.country })
async function load() {
  const number = ++requestNumber
  controller?.abort()
  const request = new AbortController()
  controller = request
  result.value = null
  error.value = ''
  loading.value = true
  try {
    const payload = await getAsCandidates({ start_time: toBackendTime(props.start), end_time: toBackendTime(props.end) },
      { country: props.country, sort: 'activity', order: 'desc', page_size: 8, version: props.version }, request.signal)
    if (number === requestNumber) result.value = payload
  } catch (cause) {
    if (number === requestNumber && !request.signal.aborted) error.value = errorMessage(cause)
  } finally {
    if (number === requestNumber) loading.value = false
  }
}
watch(() => [props.start, props.end, props.country, props.version, props.refreshKey], load, { immediate: true })
onBeforeUnmount(() => { requestNumber++; controller?.abort() })
</script>

<template>
  <section id="networks" class="c-panel country-as-list" aria-labelledby="country-as-title" :aria-busy="loading">
    <div class="c-panel-heading">
      <div><p class="overline">NETWORKS IN VIEW</p><h2 id="country-as-title">{{ country }} · AS 观测</h2></div>
      <RouterLink :to="{ name: 'ases', query: query() }">查看全部 AS →</RouterLink>
    </div>
    <div class="country-as-context"><span>按宣告与撤回合计排序 · 最多展示 8 个</span><span v-if="result?.state === 'available'">窗口内 {{ count(result.total) }} 个 AS 有特征样本</span></div>
    <p v-if="loading" class="country-as-state" role="status">正在读取此地区的 AS 观测…</p>
    <p v-else-if="error" class="country-as-state" role="alert">AS 列表不可用：{{ error }} <button @click="emit('refresh')">重新读取首页</button></p>
    <p v-else-if="result?.state === 'window_not_observed'" class="country-as-state">此时段没有已接入观测，AS 数量未知。</p>
    <p v-else-if="!result?.items.length" class="country-as-state">此地区在选定时段没有留存的 AS 特征样本。</p>
    <div v-else class="country-as-scroll">
      <table><caption class="sr-only">{{ country }} 在选定时段的 AS 特征样本</caption>
        <thead><tr><th scope="col">网络</th><th scope="col">宣告量</th><th scope="col">撤回量</th><th scope="col">撤回占比</th><th scope="col">最后观测 · 北京时间</th><th scope="col"><span class="sr-only">档案</span></th></tr></thead>
        <tbody><tr v-for="item in result.items" :key="item.asn">
          <td><RouterLink :to="{ name: 'asn-detail', params: { asn: item.asn }, query: query() }">AS{{ item.asn }}</RouterLink><small v-if="item.asName">{{ item.asName }}</small></td>
          <td>{{ count(item.announce) }}</td><td>{{ count(item.withdraw) }}</td><td>{{ item.withdrawRate === null ? '—' : `${item.withdrawRate.toFixed(1)}%` }}</td>
          <td>{{ stamp(item.latestObservation) }}</td><td><RouterLink :to="{ name: 'asn-detail', params: { asn: item.asn }, query: query() }">查看档案 ↗</RouterLink></td>
        </tr></tbody>
      </table>
    </div>
    <p class="country-as-note"><strong v-if="result?.coverage.state === 'partial'">部分时段有数据；</strong>国家按特征记录归属，列表仅涵盖留存样本中的 AS。报文量与撤回占比不代表故障严重程度。</p>
  </section>
</template>

<style scoped>
.country-as-list { margin-top:26px; scroll-margin-top:20px; }
.c-panel-heading a { color:var(--accent); font-size:12px; text-decoration:none; }
.country-as-context { display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px; padding:12px 20px; color:var(--muted); font-size:11px; background:#fafcfd; border-bottom:1px solid var(--line); }
.country-as-scroll { overflow:auto; }
table { width:100%; min-width:640px; border-collapse:collapse; font-size:12px; }
th { text-align:left; font-size:10px; font-weight:500; color:var(--muted); }
th,td { padding:14px 20px; border-bottom:1px solid var(--line); }
td { font-family:var(--mono); font-variant-numeric:tabular-nums; }
td a { color:var(--accent); text-decoration:none; }
td a:hover { text-decoration:underline; }
td small { display:block; font-size:10px; color:var(--muted); margin-top:5px; }
.country-as-state { padding:26px 20px; font-size:13px; color:var(--muted); }
.country-as-state button { color:var(--accent); text-decoration:underline; }
.country-as-note { margin:0; padding:14px 20px; line-height:1.7; color:var(--muted); font-size:10px; }
</style>

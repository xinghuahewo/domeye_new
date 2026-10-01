<script setup lang="ts">
import { computed, onBeforeUnmount, onServerPrefetch, reactive, ref, shallowRef, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { getAsCandidates, getAsOverview, getAsRecentEvents, getASPrefixOutages, type FeatureRange } from '@/api/features'
import EventTable from '@/components/EventTable.vue'
import AsnRequestState from '@/components/AsnRequestState.vue'
import AsnEventTimeline from '@/components/AsnEventTimeline.vue'
import AsnRibSnapshot from '@/components/AsnRibSnapshot.vue'
import LineChart, { type ChartSeries } from '@/components/LineChart.vue'
import PageState from '@/components/PageState.vue'
import type { AsCandidatePage, AsOverview, EventRow, FeaturePoint, OutagePoint } from '@/types/api'
import { resultDelivery } from '@/api/health'
import { errorMessage } from '@/utils/normalize'
import { businessTimezone, businessTimeToIso, toBusinessTime } from '@/utils/businessTime'
import { scopeError, scopeFromQuery, scopeLabel, scopeMaximum, scopeMinimum, scopeMillis } from '@/utils/coreScope'
import profile from '../../../config/data-profile.json'

const route = useRoute()
const router = useRouter()
const delivered = computed(() => resultDelivery.value?.state === 'available')
const text = (value: unknown) => typeof value === 'string' ? value : ''
const fallback = { start: `${profile.snapshot_time.slice(0, 10)}T00:00:00`, end: scopeMaximum, country: '' }
const selectedAsn = computed(() => text(route.params.asn).trim().replace(/^AS/i, ''))
const asnInput = ref('')
const countryInput = ref('')
const query = reactive({ start: fallback.start, end: fallback.end })
const draft = reactive({ ...query })
const formError = ref('')
const overview = ref<AsOverview | null>(null)
const candidates = ref<AsCandidatePage | null>(null)
const prefixOutages = ref<OutagePoint[]>([])
const recentEvents = ref<EventRow[]>([])
const loading = ref(false)
const error = shallowRef<unknown>(null)
const outageLoading = ref(false)
const outageError = ref('')
const eventsLoading = ref(false)
const eventError = ref('')
const resourceFamily = ref<'ipv4' | 'ipv6'>('ipv4')
let candidateVersion = ''
let candidateScope = ''
let loadToken = 0
let controller: AbortController | undefined
let pendingLoad: Promise<void> | undefined
const selected = computed(() => overview.value?.selectedAsn ?? null)
const hasMessageSummary = computed(() => (selected.value?.sampleCount ?? 0) > 0)
const timeBounds = computed<[string, string]>(() => [businessTimeToIso(query.start), businessTimeToIso(query.end)])
const eventContext = computed(() => {
  const start = text(route.query.event_start), end = text(route.query.event_end), reference = text(route.query.event_ref)
  if (!start || !end || !reference) return null
  const startDate = new Date(start), endDate = new Date(end)
  if (!Number.isFinite(startDate.getTime()) || !Number.isFinite(endDate.getTime()) || startDate >= endDate) return null
  return { start, end, reference, startDate, endDate }
})
const incompleteEvent = computed(() => !eventContext.value && ['event_start', 'event_end', 'event_ref'].some(key => route.query[key] !== undefined))
const returnEventLink = computed(() => ({ name: 'event-detail', query: {
  ref: eventContext.value?.reference || '', focus: text(route.query.return_anchor) || 'affected-as',
  as_page: text(route.query.as_page) || undefined, as_query: text(route.query.as_query) || undefined,
  as_classification: text(route.query.as_classification) || undefined,
} }))
const listQuery = computed(() => ({ ...route.query, start: query.start, end: query.end,
  event_start: undefined, event_end: undefined, event_ref: undefined, return_anchor: undefined }))
const eventCountry = computed(() => text(route.query.country) || text(route.query.attacked_country) || undefined)
const eventListLink = computed(() => ({ name: 'events', query: { attacked_as: selectedAsn.value, attacked_country: eventCountry.value, start: query.start, end: query.end } }))
const candidateSort = computed(() => ['activity', 'latest', 'asn'].includes(text(route.query.sort)) ? text(route.query.sort) as 'activity' | 'latest' | 'asn' : 'activity')
const candidateOrder = computed(() => route.query.order === 'asc' ? 'asc' as const : 'desc' as const)
const candidatePage = computed(() => /^\d+$/.test(text(route.query.page)) ? Math.max(1, Number(route.query.page)) : 1)
const resourceUnit = computed(() => delivered.value ? resourceFamily.value === 'ipv4' ? '/24 覆盖块' : '/48 覆盖块' : '原始值 · 单位 Unknown')
function asnRoute(asn: string) {
  return { name: 'asn-detail', params: { asn }, query: eventContext.value ? { ...route.query } : { ...route.query, start: query.start, end: query.end } }
}
function rangeError(range: { start: string; end: string }) {
  return scopeError({ ...range, country: '' }) || (scopeMillis(range.end) - scopeMillis(range.start) > 45 * 86400000 ? '单次最多查看 45 天，请缩小时间范围' : '')
}
function applyRange() {
  if (eventContext.value) return
  const complete = (value: string) => value.length === 16 ? `${value}:00` : value
  const next = { start: complete(draft.start), end: complete(draft.end) }
  formError.value = rangeError(next)
  if (formError.value) return
  if (query.start === next.start && query.end === next.end) reload()
  else void router.push({ query: { ...route.query, ...next, date: undefined, page: undefined } })
}
function openAsn() {
  const asn = asnInput.value.trim().replace(/^AS/i, '')
  if (!/^\d{1,10}$/.test(asn) || Number(asn) < 1 || Number(asn) > 4294967295) {
    formError.value = '请输入 1 至 4294967295 的 ASN，例如 AS3356'
    return
  }
  formError.value = ''
  void router.push(asnRoute(String(Number(asn))))
}
function filterCandidates() {
  const q = asnInput.value.trim()
  if (q && !/^(?:AS)?\d+$/i.test(q)) { formError.value = '候选筛选请输入数字 ASN 或 AS 加数字'; return }
  formError.value = ''
  void router.push({ query: { ...route.query, start: query.start, end: query.end, q: q || undefined, country: countryInput.value.trim() || undefined, page: undefined } })
}
function sortCandidates(event: Event) {
  const [sort, order] = (event.target as HTMLSelectElement).value.split(':')
  void router.push({ query: { ...route.query, sort, order, page: undefined } })
}
function goPage(page: number) { void router.push({ query: { ...route.query, page: String(page) } }) }
function openEvent(event: EventRow) {
  if (event.detailUrl) void router.push({ name: 'event-detail', query: { ref: event.detailUrl, attacked_country: eventCountry.value, start: query.start, end: query.end } })
}
const number = (value: number | null | undefined) => value == null ? '—' : value.toLocaleString('zh-CN')
const percent = (value: number | null | undefined) => value == null ? '—' : `${value.toFixed(1)}%`
function observationTime(value: string | null) {
  if (!value) return '未知'
  try { return toBusinessTime(new Date(businessTimeToIso(value))).slice(5, 16) }
  catch { return '时间未知' }
}
function featureSeries(name: string, key: keyof Omit<FeaturePoint, 'time'>, color: string): ChartSeries {
  const data: ChartSeries['data'] = []
  const records = selected.value?.series ?? []
  records.forEach((point, index) => {
    const time = businessTimeToIso(point.time), previous = records[index - 1]
    if (previous && Date.parse(time) - Date.parse(businessTimeToIso(previous.time)) > 300000) {
      data.push([new Date(Date.parse(businessTimeToIso(previous.time)) + 300000).toISOString(), null])
    }
    data.push([time, point[key] ?? null])
  })
  return { name, color, data }
}
const messageSeries = computed(() => [featureSeries('宣告', 'announce', '#3e6f89'), featureSeries('撤回', 'withdraw', '#788f58')])
const resourceSeries = computed(() => [featureSeries(resourceFamily.value === 'ipv4' ? 'IPv4 资源' : 'IPv6 资源', resourceFamily.value === 'ipv4' ? 'ipv4Prefixes' : 'ipv6Prefixes', '#3e6f89')])
const outageSeries = computed<ChartSeries[]>(() => [{ name: '并发前缀中断', color: '#967431', data: prefixOutages.value.map(point => [businessTimeToIso(point.time), point.count]) }])
const hasValues = (series: ChartSeries[]) => series.some(row => row.data.some(([, value]) => typeof value === 'number' && Number.isFinite(value)))
function featureRange(): FeatureRange { return { start_time: query.start.replace('T', ' '), end_time: query.end.replace('T', ' ') } }
function reload() { candidateVersion = ''; void load() }
async function load() {
  const token = ++loadToken
  controller?.abort()
  const request = new AbortController()
  controller = request
  overview.value = null; candidates.value = null; prefixOutages.value = []; recentEvents.value = []
  error.value = null; outageError.value = ''; eventError.value = ''
  loading.value = false; outageLoading.value = false; eventsLoading.value = false
  const invalid = incompleteEvent.value ? '事件上下文不完整，无法读取原事件窗口'
    : eventContext.value && !selectedAsn.value ? '事件窗口必须指定 ASN' : rangeError(query)
  if (invalid) { error.value = invalid; eventError.value = invalid; outageError.value = invalid; return }
  const range = featureRange(), asn = selectedAsn.value, context = eventContext.value
  const current = () => token === loadToken && !request.signal.aborted
  loading.value = true
  if (!asn) {
    try { const result = await getAsCandidates(range, { country: text(route.query.country) || undefined, q: text(route.query.q) || undefined,
      sort: candidateSort.value, order: candidateOrder.value, page: candidatePage.value, page_size: 20, version: candidateVersion || undefined }, request.signal)
      if (current()) { candidates.value = result; candidateVersion = result.version }
    } catch (cause) { if (current()) error.value = cause }
    finally { if (current()) loading.value = false }
    return
  }
  if (!/^\d{1,10}$/.test(asn) || Number(asn) < 1 || Number(asn) > 4294967295) {
    error.value = 'ASN 无效'; eventError.value = 'ASN 无效'; outageError.value = 'ASN 无效'; loading.value = false; return
  }
  outageLoading.value = true; eventsLoading.value = true
  await Promise.allSettled([
    getAsOverview(range, asn, 6, Boolean(context), context?.reference, request.signal)
      .then(result => { if (current()) overview.value = result })
      .catch(cause => { if (current()) error.value = cause })
      .finally(() => { if (current()) loading.value = false }),
    getAsRecentEvents(asn, range, 10, Boolean(context), context?.reference, request.signal)
      .then(result => { if (current()) recentEvents.value = result.data })
      .catch(cause => { if (current()) eventError.value = errorMessage(cause) })
      .finally(() => { if (current()) eventsLoading.value = false }),
    getASPrefixOutages(asn, range, request.signal)
      .then(result => { if (current()) prefixOutages.value = result })
      .catch(cause => { if (current()) outageError.value = errorMessage(cause) })
      .finally(() => { if (current()) outageLoading.value = false }),
  ])
}
watch(() => [route.params.asn, route.query.start, route.query.end, route.query.date, route.query.event_start, route.query.event_end, route.query.event_ref,
  route.query.country, route.query.q, route.query.sort, route.query.order, route.query.page], () => {
  const scope = scopeFromQuery(route.query, fallback)
  Object.assign(query, eventContext.value ? {
    start: toBusinessTime(eventContext.value.startDate).replace(' ', 'T'), end: toBusinessTime(eventContext.value.endDate).replace(' ', 'T'),
  } : { start: scope.start, end: scope.end })
  Object.assign(draft, query)
  asnInput.value = selectedAsn.value ? `AS${selectedAsn.value}` : text(route.query.q)
  countryInput.value = text(route.query.country)
  const nextCandidateScope = [query.start, query.end, text(route.query.country), text(route.query.q)].join('|')
  if (candidateScope !== nextCandidateScope) { candidateVersion = ''; candidateScope = nextCandidateScope }
  formError.value = ''
  pendingLoad = load()
}, { immediate: true })
onServerPrefetch(() => pendingLoad)
onBeforeUnmount(() => { loadToken++; controller?.abort() })
</script>

<template>
  <article class="page asn-page">
    <section v-if="eventContext" class="event-window-context" aria-label="国家中断事件窗口">
      <div><span>国家中断事件中的网络</span><strong>{{ scopeLabel(query) }} · {{ businessTimezone }}</strong><small>事件：{{ eventContext.reference }}</small></div>
      <RouterLink :to="returnEventLink">← 返回事件中的相关 AS</RouterLink>
    </section>
    <header class="asn-heading">
      <div><p class="eyebrow">NETWORK OBSERVATORY</p><h1>{{ selectedAsn ? `AS${selectedAsn}` : '网络档案' }}<span v-if="selected">{{ selected.asName || selected.orgName || '名称未知' }}</span></h1>
        <p v-if="selected" class="asn-identity">{{ selected.orgName || '组织未知' }}<i>·</i>{{ selected.country || '国家未知' }}<i>·</i>{{ selected.asType || '类型未知' }}<b v-if="selected.important">重点网络</b></p>
        <p v-else-if="!selectedAsn" class="asn-intro">查找一个 AS，查看它在所选时段的报文、资源与异常记录。</p>
      </div>
      <RouterLink v-if="selectedAsn" class="text-action" :to="{ name: 'ases', query: listQuery }">← 全部候选网络</RouterLink>
    </header>
    <section class="asn-controls" aria-label="网络与时间范围">
      <form class="asn-search" @submit.prevent="openAsn"><label>查找网络<input v-model="asnInput" aria-label="检索 ASN" placeholder="输入 ASN，例如 AS3356" /></label><button type="submit">打开档案 →</button></form>
      <form v-if="!eventContext && !incompleteEvent" class="asn-time" @submit.prevent="applyRange">
        <label>开始时间<input v-model="draft.start" type="datetime-local" step="1" required :min="scopeMinimum" :max="scopeMaximum" aria-label="AS 开始时间" /></label>
        <label>结束时间<input v-model="draft.end" type="datetime-local" step="1" required :min="scopeMinimum" :max="scopeMaximum" aria-label="AS 结束时间" /></label>
        <button type="submit">应用区间</button>
      </form>
      <p class="asn-range-note">{{ incompleteEvent ? '事件参数不完整，需返回原事件重新打开 ASN。' : eventContext ? '沿用原事件窗口，时间不可更改。' : '北京时间 · 右端不含 · 单次最多 45 天。' }} {{ scopeLabel(query) }}</p>
      <p v-if="formError" class="asn-error" role="alert">{{ formError }}</p>
    </section>
    <p class="asn-source-line">{{ delivered ? '本批已交付结果' : '历史观测数据' }}<span>缺失时段保留未知；BGP 观测不能直接解释为实际用户影响。</span></p>
    <p v-if="overview?.deliveryCoverage?.state === 'partial'" class="chart-note" role="status">所选区间仅部分时段有已交付观测；以下数量只对应已交付部分，其余时段未知。</p>
    <AsnRequestState :loading="loading" :error="error" :event-window="Boolean(eventContext)" @retry="reload" />

    <section v-if="!selectedAsn" class="asn-candidates" aria-labelledby="candidate-title">
      <header class="section-heading"><div><p class="eyebrow">EXPLORE NETWORKS</p><h2 id="candidate-title">有特征记录的网络</h2></div><span>仅所选窗口内的候选网络</span></header>
      <form class="candidate-filters" @submit.prevent="filterCandidates"><label>国家或地区<input v-model="countryInput" aria-label="筛选候选国家或地区" placeholder="全部国家或地区" /></label><button type="submit">筛选候选</button>
        <label class="candidate-sort">排序<select aria-label="候选网络排序" :value="`${candidateSort}:${candidateOrder}`" @change="sortCandidates"><option value="activity:desc">更新量从高到低</option><option value="latest:desc">最近采样优先</option><option value="asn:asc">ASN 从小到大</option><option value="asn:desc">ASN 从大到小</option></select></label></form>
      <template v-if="candidates">
        <p v-if="candidates.coverage.state === 'partial'" class="chart-note" role="status">仅部分时段有交付数据，其余时段未知；列表中的数量只来自已有采样。</p>
        <div v-if="candidates.items.length" class="candidate-table-scroll"><table class="candidate-table"><thead><tr><th>网络</th><th>国家 / 地区</th><th>宣告</th><th>撤回</th><th>更新总量</th><th>撤回占比</th><th>最后采样 · 北京时间</th><th><span class="sr-only">打开</span></th></tr></thead><tbody>
          <tr v-for="item in candidates.items" :key="item.asn"><td><RouterLink :to="asnRoute(item.asn)"><strong>AS{{ item.asn }}</strong><small>{{ item.asName || item.orgName || '名称未知' }}</small></RouterLink></td><td>{{ item.country || item.countries.join('、') || '未知' }}</td><td>{{ number(item.announce) }}</td><td>{{ number(item.withdraw) }}</td><td>{{ number(item.updateTotal) }}</td><td>{{ percent(item.withdrawRate) }}</td><td :title="item.latestObservation || undefined">{{ observationTime(item.latestObservation) }}</td><td><RouterLink :to="asnRoute(item.asn)" :aria-label="`打开 AS${item.asn} 档案`">↗</RouterLink></td></tr>
        </tbody></table></div>
        <PageState v-else :title="candidates.state === 'window_not_observed' ? '所选区间没有已交付观测' : '所选范围没有可展示的候选网络'" detail="仅表示没有返回 Feature 样本，不能解释为这些网络没有路由或没有异常。" />
        <footer class="candidate-footer"><span>共 {{ number(candidates.total) }} 个候选 · 宣告 / 撤回为路由元素次数 · 国家按 Feature 来源字段</span><div><button :disabled="candidatePage <= 1" @click="goPage(candidatePage - 1)">上一页</button><span>{{ candidatePage }} / {{ Math.max(1, candidates.pageCount) }}</span><button :disabled="candidatePage >= candidates.pageCount" @click="goPage(candidatePage + 1)">下一页</button></div></footer>
      </template>
    </section>

    <template v-if="selectedAsn">
      <section class="asn-window-summary" aria-label="所选区间摘要">
        <div class="section-heading"><h2>区间摘要</h2><span>{{ selected?.latestObservation ? `末次采样 ${selected.latestObservation}` : '采样时点未知' }}</span></div>
        <div class="summary-metrics"><article><span>宣告</span><strong>{{ number(hasMessageSummary ? selected?.announce : null) }}</strong><small>已返回报文元素</small></article><article><span>撤回</span><strong>{{ number(hasMessageSummary ? selected?.withdraw : null) }}</strong><small>已返回报文元素</small></article><article><span>撤回占比</span><strong>{{ percent(hasMessageSummary ? selected?.withdrawRate : null) }}</strong><small>撤回 /（宣告 + 撤回）</small></article><article><span>异常记录</span><strong>{{ number(selected?.anomalyCount) }}</strong><small>所选窗口 · 六类异常</small></article></div>
      </section>
      <AsnEventTimeline v-if="eventContext && selected && overview" :profile="selected" :start-time="overview.startTime" :end-time="overview.endTime" />
      <section v-if="!eventContext" class="asn-chart-panel" aria-label="ASN 报文活动">
        <div class="section-heading"><div><p class="eyebrow">01 / MESSAGES</p><h2>报文活动</h2></div><span>宣告与撤回 · 元素</span></div>
        <PageState v-if="loading" kind="loading" title="正在读取报文活动" /><PageState v-else-if="error" kind="error" title="报文活动不可用" /><LineChart v-else-if="hasValues(messageSeries)" :series="messageSeries" :timezone="businessTimezone" unit="元素" :height="320" :time-bounds="timeBounds" show-data-zoom show-points /><PageState v-else title="所选区间没有可用报文样本" detail="缺失不表示数量为零。" />
      </section>
      <section class="asn-chart-panel" aria-label="ASN 前缀并发中断">
        <div class="section-heading"><div><p class="eyebrow">02 / PREFIX OUTAGES</p><h2>前缀并发中断</h2></div><span>3 分钟采样 · 起</span></div>
        <PageState v-if="outageLoading" kind="loading" title="正在读取前缀中断时序" /><PageState v-else-if="outageError" kind="error" title="前缀中断时序不可用" :detail="outageError" @retry="load" /><LineChart v-else-if="hasValues(outageSeries)" :series="outageSeries" :timezone="businessTimezone" unit="起" :height="300" :time-bounds="timeBounds" show-data-zoom show-points /><PageState v-else title="所选区间没有可用中断时序" detail="无可用时序不表示没有中断。" />
      </section>
      <section v-if="!eventContext" class="asn-chart-panel" aria-label="ASN 资源趋势">
        <div class="section-heading"><div><p class="eyebrow">03 / RESOURCES</p><h2>资源趋势</h2></div><div class="resource-tabs" role="group" aria-label="资源地址族"><button :aria-pressed="resourceFamily === 'ipv4'" @click="resourceFamily = 'ipv4'">IPv4</button><button :aria-pressed="resourceFamily === 'ipv6'" @click="resourceFamily = 'ipv6'">IPv6</button></div></div>
        <p class="chart-note">{{ resourceUnit }} · 按来源文件时间标签绘制，数值为文件处理后的资源记录，与独立 RIB 去重前缀数不同。</p>
        <PageState v-if="loading" kind="loading" title="正在读取资源趋势" /><PageState v-else-if="error" kind="error" title="资源趋势不可用" /><LineChart v-else-if="hasValues(resourceSeries)" :series="resourceSeries" :timezone="businessTimezone" :unit="resourceUnit" :height="320" :time-bounds="timeBounds" show-data-zoom show-points /><PageState v-else title="所选地址族没有可用资源值" detail="未知保留为空，不使用另一地址族代替。" />
      </section>
      <section class="asn-events" aria-labelledby="asn-events-title"><div class="section-heading"><div><p class="eyebrow">EVENT RECORDS</p><h2 id="asn-events-title">区间异常事件</h2></div><RouterLink :to="eventListLink">检索全部事件 →</RouterLink></div>
        <PageState v-if="eventsLoading" kind="loading" title="正在读取异常事件" /><PageState v-else-if="eventError" kind="error" title="ASN 事件不可用" :detail="eventError" @retry="load" /><PageState v-else-if="!recentEvents.length" title="当前窗口没有可展示的异常事件" /><EventTable v-else compact :events="recentEvents" @select="openEvent" /><p class="chart-note">最多展示最近 10 条记录。等级和检测分类不能直接说明实际损害或原因。</p>
      </section>
      <AsnRibSnapshot :asn="selectedAsn" />
      <details class="asn-source-details"><summary>数据来源与解释范围</summary><p>{{ delivered ? 'Feature 与异常查询读取本批已交付结果，仅已有时段可用。' : '历史特征、异常与独立 RIB 快照分别读取；不能按页面相邻位置混算。' }} 每个图表的空值与缺口保持未知。</p><p>资源保留所选地址族的独立单位；RIB 只说明标明时点的起源前缀，不代表整个窗口的连续状态。</p><p v-if="selected">名称、组织、类型与排名采用静态参考资料，历史适用性未知。全球排名 {{ number(selected.globalRank) }} · 国家排名 {{ number(selected.countryRank) }}，不是本页窗口内的网络表现排名。</p></details>
    </template>
  </article>
</template>

<style scoped>
.asn-page { --accent:var(--primary); --surface:var(--paper); max-width:1320px; margin-inline:auto; display:grid; gap:22px; }
.asn-heading { display:flex; justify-content:space-between; align-items:end; gap:24px; padding:8px 0 5px; }
.eyebrow { color:var(--muted); font:10px var(--mono); letter-spacing:.13em; margin:0 0 8px; }
.asn-heading h1 { font-size:38px; letter-spacing:-1.3px; margin:0; line-height:1.2; font-weight:550; }
.asn-heading h1 span { display:block; font-size:18px; font-weight:450; letter-spacing:0; margin-top:8px; color:var(--muted); }
.asn-identity,.asn-intro { margin-top:14px; color:var(--muted); font-size:13px; line-height:1.8; }
.asn-identity i { padding:0 9px; font-style:normal; }.asn-identity b { font-size:10px; font-weight:500; margin-left:14px; padding:3px 7px; background:#edf2df; color:#617340; }
.text-action,.section-heading>a { color:var(--accent); font-size:12px; text-decoration:none; white-space:nowrap; }
.asn-controls { background:var(--surface); border:1px solid var(--line); border-top:3px solid var(--accent); padding:18px 22px; display:flex; flex-wrap:wrap; gap:18px 32px; align-items:end; }
.asn-search,.asn-time { display:flex; flex-wrap:wrap; gap:12px; align-items:end; }.asn-search { flex:1; }.asn-search label { flex:1; min-width:180px; }
label { display:grid; gap:7px; color:var(--muted); font-size:11px; }input,select { min-height:38px; padding:8px 10px; border:1px solid var(--line); border-radius:4px; background:white; color:var(--ink); font-size:12px; min-width:0; }button { cursor:pointer; }button:disabled { cursor:default; opacity:.45; }
.asn-controls button,.candidate-filters>button { min-height:38px; padding:8px 15px; border:0; border-radius:4px; background:var(--accent); color:white; font-size:12px; }
.asn-range-note { flex-basis:100%; color:var(--muted); font-size:10px; margin:0; }.asn-error { color:#9b4538; flex-basis:100%; margin:0; font-size:12px; }
.asn-source-line { margin:0; font-size:11px; color:var(--muted); }.asn-source-line>span { margin-left:14px; }
.section-heading { display:flex; justify-content:space-between; align-items:center; gap:20px; margin-bottom:18px; }.section-heading h2 { margin:0; font-size:20px; font-weight:550; }.section-heading>span { color:var(--muted); font-size:11px; }
.asn-candidates,.asn-window-summary,.asn-chart-panel,.asn-events { min-width:0; padding:22px 24px; background:var(--surface); border:1px solid var(--line); border-radius:5px; }
.candidate-filters { display:flex; align-items:end; flex-wrap:wrap; gap:12px; margin:20px 0; }.candidate-sort { margin-left:auto; }.candidate-table-scroll { overflow:auto; }.candidate-table { width:100%; border-collapse:collapse; text-align:left; font-size:12px; }.candidate-table th { font-size:10px; color:var(--muted); font-weight:500; white-space:nowrap; }.candidate-table td,.candidate-table th { border-bottom:1px solid var(--line); padding:14px 10px; }.candidate-table td:nth-child(n+3):nth-child(-n+5) { font-family:var(--mono); }.candidate-table a { text-decoration:none; color:var(--accent); }.candidate-table strong { font:500 14px var(--mono); }.candidate-table tbody tr:hover { background:#f7fafb; }
.candidate-footer { display:flex; flex-wrap:wrap; justify-content:space-between; align-items:center; gap:16px; padding-top:18px; font-size:11px; color:var(--muted); }.candidate-footer>div { display:flex; align-items:center; gap:14px; }.candidate-footer button { padding:6px 10px; border:1px solid var(--line); background:white; border-radius:3px; color:var(--accent); }
.candidate-table td { white-space:nowrap; }.candidate-table td small { display:block; color:var(--muted); font-size:10px; margin-top:5px; max-width:190px; overflow:hidden; text-overflow:ellipsis; }
.summary-metrics { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); border-top:1px solid var(--line); padding-top:20px; }.summary-metrics article { display:grid; gap:10px; padding:0 22px; border-left:1px solid var(--line); }.summary-metrics article:first-child { border:0; padding-left:0; }.summary-metrics span { font-size:12px; color:var(--muted); }.summary-metrics strong { font:500 30px/1.2 var(--mono); letter-spacing:-1px; }.summary-metrics small { font-size:10px; color:var(--muted); }
.chart-note { font-size:11px; line-height:1.8; color:var(--muted); margin:8px 0 16px; }.resource-tabs { display:flex; border:1px solid var(--line); border-radius:4px; overflow:hidden; }.resource-tabs button { padding:7px 14px; font-size:12px; color:var(--muted); background:white; border:0; }.resource-tabs button[aria-pressed=true] { color:white; background:var(--accent); }
.asn-source-details { border-top:1px solid var(--line); padding:18px 0; color:var(--muted); font-size:11px; line-height:1.8; }.asn-source-details summary { cursor:pointer; font-size:12px; color:var(--accent); }.asn-source-details p { margin:10px 0 0; }
.event-window-context { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:18px; background:#14384a; border-left:4px solid #c19350; padding:14px 18px; color:white; }.event-window-context>div { display:grid; gap:5px; min-width:0; }.event-window-context span,.event-window-context small { color:#b6cbd4; font-size:10px; overflow-wrap:anywhere; }.event-window-context strong { font:12px var(--mono); }.event-window-context a { color:#eed2a0; font-size:12px; text-decoration:none; }
@media(max-width:1000px) { .asn-time { flex-basis:100%; }.asn-time label { flex:1; }.summary-metrics strong { font-size:25px; } }
@media(max-width:700px) { .asn-page { gap:16px; }.asn-heading { align-items:start; flex-direction:column; gap:14px; }.asn-heading h1 { font-size:32px; }.asn-controls,.asn-candidates,.asn-window-summary,.asn-chart-panel,.asn-events { padding:17px 14px; }.asn-time { display:grid; grid-template-columns:1fr; width:100%; }.asn-search { width:100%; }.summary-metrics { grid-template-columns:1fr 1fr; gap:22px 0; }.summary-metrics article { padding:0 12px; }.summary-metrics article:nth-child(3) { border-left:0; padding-left:0; }.section-heading { align-items:start; flex-wrap:wrap; gap:12px; }.candidate-sort { margin-left:0; }.asn-source-line>span { display:block; margin:7px 0 0; } }
</style>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { getCoreOverview, getCoreOverviewRecord, type CoreOverview, type CoreOverviewDetail, type CoreOverviewItem, type CoreOverviewQuery } from '@/api/coreOverview'
import { errorMessage } from '@/utils/normalize'
import { toBusinessTime, formatBusinessEndTime } from '@/utils/businessTime'
import profile from '../../../config/data-profile.json'
import './home-prototype/overview.css'
import './coreOverview.css'

const route = useRoute()
const router = useRouter()
const date = ref(typeof route.query.date === 'string' ? route.query.date : profile.snapshot_time.slice(0, 10))
const family = ref<CoreOverviewQuery['family']>('all')
const kind = ref<CoreOverviewQuery['kind']>('all')
const level = ref<CoreOverviewQuery['level']>('all')
const sort = ref<CoreOverviewQuery['sort']>('severity')
const query = ref('')
const hour = ref<number | null>(null)
const page = ref(1)
const data = ref<CoreOverview | null>(null)
const metadata = ref<CoreOverview['metadata'] | null>(null)
const loading = ref(true)
const error = ref('')
const pinnedVersion = ref('')
let requestNumber = 0
let controller: AbortController | undefined
const types = { prefix_outage: '前缀中断', as_outage: 'AS 中断', leak: '路由泄漏', hijack: '前缀劫持', sub_hijack: '子前缀劫持', country_outage: '国家中断' } as const
const levels = { high: '高', middle: '中', low: '低' } as const
const levelLabel = (item: CoreOverviewItem) => item.level_conflict ? '等级待核实' : item.level ? levels[item.level] : '未知'
const familyLabels = { all: '全部地址族', ipv4: 'IPv4', ipv6: 'IPv6', mixed: 'IPv4 + IPv6', unknown: '地址族未知' }
const familyLabel = computed(() => familyLabels[family.value || 'all'])
const ready = computed(() => !loading.value && !error.value && data.value?.state === 'available')
const scale = computed(() => ready.value ? data.value?.metadata.scale : undefined)
const scaleReady = computed(() => scale.value?.state === 'available')
const originReady = computed(() => scale.value?.state === 'available' && scale.value.origin_metric_state === 'available')
const originNote = computed(() => {
  if (!scaleReady.value || originReady.value) return scaleNote.value
  return scale.value && scale.value.state !== 'unavailable' && scale.value.origin_metric_state === 'unavailable'
    ? '起源统计校验失败' : '归属 ASN 去重 · 数据待接入'
})
const scaleNote = computed(() => {
  if (loading.value) return '正在读取'
  const value = scale.value
  if (!value) return ready.value ? '前缀条数 · 数据待验证' : '选定窗口不可用'
  if (value.state === 'unavailable') return '规模数据不可用'
  if (value.state === 'date_not_retained') return '此日无已验证的 RIB 快照'
  if (value.state === 'family_not_supported') return '未知地址族不提供规模'
  return `单 RIB · ${toBusinessTime(new Date(value.observed_at)).slice(5, 16)}（${profile.timezone}）`
})
const availableDates = computed(() => metadata.value?.available_dates ?? (metadata.value ? [metadata.value.retained_window.start.slice(0, 10)] : []))
const diagnosticDates = computed(() => metadata.value?.diagnostic_dates ?? [])
const directoryDates = computed(() => [...availableDates.value, ...diagnosticDates.value])
const availableTypes = computed(() => metadata.value?.kinds ?? [])
const typeScopeLabel = computed(() => metadata.value ? `已接入 ${availableTypes.value.length} 类异常` : '类型范围待读取')
const failureLabels = { level_conflict: '总表与明细等级冲突', invalid_time_order: '结束早于开始或持续时长为负', time_fields_conflict: '结束时间或持续时长冲突',
  source_identity_unresolved: '源 ASN 不能按单一 AS 解析', source_population_mismatch: '总表候选与独立明细集合不一致',
  start_time_conflict: '总表与明细开始时间冲突', source_read_timeout: '读取超时，未取得完整记录' } as const
const diagnosticTypes = { ...types, all: '六类异常' }
const diagnosticStage = computed(() => data.value?.diagnostic?.stage === 'source_read' ? '源数据读取未完成'
  : data.value?.diagnostic?.stage === 'source_field_validation' ? '整日源记录校验未通过' : '源字段预检；不是该日完整数据准入')
const diagnosticTitle = computed(() => data.value?.diagnostic?.stage === 'source_read' ? '源数据读取未完成' : '源记录校验失败')
const chartMax = computed(() => Math.max(4, ...((data.value?.trend?.buckets || []).map(point => point.value))))
const hasFilters = computed(() => hour.value !== null || kind.value !== 'all' || level.value !== 'all' || !!query.value)
const changeKind = ref(1)
const pathComparison = computed(() => ready.value ? data.value?.metadata.path_comparison : undefined)
const pathReady = computed(() => pathComparison.value?.state === 'available' ? pathComparison.value : undefined)
const pathNote = computed(() => {
  const value = pathComparison.value
  if (loading.value) return '正在读取路径对照'
  if (!ready.value) return '选定日期不可用'
  if (!value) return '此版本尚未绑定两次观察对照'
  if (value.state === 'unavailable') return '路径对照校验失败'
  if (value.state === 'date_not_retained') return '此日无已验证的两次观察对照'
  if (value.state === 'family_not_supported') return '未知地址族不提供路径对照'
  return '两次 RIB 观察对照'
})
const pathRatio = computed(() => pathReady.value?.metrics?.different_fraction == null ? '—'
  : `${(pathReady.value.metrics.different_fraction * 100).toFixed(2)}%`)
const changes = [
  ['可见性', '逐前缀状态数据尚未验证', '观测缺口不能当作前缀消失。'],
  ['路径', '带观测时间的路径数据尚未验证', '有路径样本，不等于能比较两个时点。'],
  ['起源', '完整的起源变化记录尚未验证', '劫持事件不能代表全部起源变化。'],
]
const time = (value: string) => toBusinessTime(new Date(value)).slice(11)
const hourLabel = (value: number) => `${String(value).padStart(2, '0')}:00–${String(value + 1).padStart(2, '0')}:00`
const count = (value: number | undefined | null) => value == null ? '—' : value.toLocaleString('zh-CN')
const objectLabel = (item: CoreOverviewItem) => item.kind === 'country_outage' && item.country_name
  ? `${item.country_name}（${item.object}）` : `${item.kind === 'as_outage' && !item.object_identity ? 'AS' : ''}${item.object}`
const endLabel = (item: CoreOverviewItem, full = false) => item.end_time.state === 'recorded' && item.end_time.value
  ? `${formatBusinessEndTime(item.end_time.value, item.start_time, full)}${full ? ` · ${profile.timezone}` : ''} · 已记录` : item.end_time.state === 'unavailable' ? '来源不提供' : '未记录'

async function load(resetVersion = false) {
  const current = ++requestNumber
  controller?.abort()
  const request = new AbortController()
  controller = request
  if (resetVersion) pinnedVersion.value = ''
  loading.value = true
  error.value = ''
  data.value = null
  try {
    const result = await getCoreOverview({ date: date.value, family: family.value, kind: kind.value,
      level: level.value, sort: sort.value, q: query.value, hour: hour.value ?? undefined,
      page: page.value, page_size: 10, version: pinnedVersion.value || undefined }, request.signal)
    if (current !== requestNumber) return
    data.value = result
    metadata.value = result.metadata
    pinnedVersion.value = result.version
    if (result.state === 'unavailable') error.value = result.message || '选定日期的留存数据不可用'
  } catch (cause) {
    if (current !== requestNumber || request.signal.aborted) return
    error.value = errorMessage(cause)
    metadata.value = null
  } finally {
    if (current === requestNumber) loading.value = false
  }
}
function resetFilters() { hour.value = null; kind.value = 'all'; level.value = 'all'; query.value = '' }
function goPage(value: number) { page.value = value; void load() }
function useRetainedWindow() { const latest = availableDates.value.at(-1); if (latest) date.value = latest }
function selectRetainedDate(event: Event) { const value = (event.target as HTMLSelectElement).value; if (value) date.value = value }
watch([date, family, kind, level, sort, query, hour], () => { page.value = 1; void load() })
watch(date, value => { void router.replace({ query: { ...route.query, date: value } }) })
watch(() => route.query.date, value => { date.value = typeof value === 'string' ? value : profile.snapshot_time.slice(0, 10) })

const dialog = ref<HTMLDialogElement>()
const dialogTitle = ref('')
const dialogRows = ref<[string, string][]>([])
const dialogNote = ref('')
const detail = ref<CoreOverviewDetail | null>(null)
const detailLoading = ref(false)
const detailError = ref('')
let detailRequest = 0
let detailController: AbortController | undefined
let trigger: HTMLElement | null = null
async function openDialog(title: string, rows: [string, string][], note: string) {
  trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null
  detail.value = null
  detailError.value = ''
  dialogTitle.value = title
  dialogRows.value = rows
  dialogNote.value = note
  await nextTick()
  dialog.value?.showModal()
}
function closeDialog() { dialog.value?.close() }
function afterClose() { detailController?.abort(); detailRequest++; detailLoading.value = false; trigger?.focus() }
function showScope() {
  void openDialog('来源与数据说明', [
    ['Collector', 'RRC25（source=r；用户确认的映射）'],
    ['来源实例', metadata.value?.source.instance || '尚未取得留存输入'],
    ['观察覆盖', '未知；不能据此计算覆盖率或漏报率'], ['历史检测版本', '未知；不追补，新检测另行记录版本'],
    ['已留存日期', metadata.value ? availableDates.value.join('、') : '尚未取得'],
    ['项目数据档', `${profile.id} · ${profile.timezone}`], ['消费版本', pinnedVersion.value || '尚未取得'],
    ['解释版本', metadata.value?.interpretation_version || '尚未取得'],
    ['当日输入解释', ready.value ? data.value?.metadata.input_interpretation_version || metadata.value?.interpretation_version || '尚未取得' : '尚未取得'],
    ['类型范围', metadata.value ? availableTypes.value.map(key => types[key]).join('、') : '尚未取得留存输入'],
    ['地址族', '按已存结构化前缀匹配；AS 混合记录在两种筛选均可出现。无法判定的单列未知。'],
  ], '统计仅描述已留存异常记录，不代表完整路由观察。缺少结束不等于持续中；BGP 记录不能直接推出实际断网、用户影响、原因或责任。')
}
function showMetric() {
  void openDialog('新增中断前缀数', [
    ['指标', 'recorded_prefix_outage_starts_distinct'], ['时间桶', `1 小时 · ${profile.timezone} · 左闭右开`],
    ['统计', '按发生时间选择前缀中断记录，再在桶内对 Prefix 去重'],
    ['输入版本', pinnedVersion.value || '尚未取得'], ['筛选', '图与列表共用日期和地址族；列表的类型、等级、搜索是局部筛选'],
  ], '同一前缀可在不同小时重复出现，柱子之和不等于全日去重数。核对柱值时选择前缀中断、清除等级及搜索筛选，比较列表的去重前缀数，不是记录条数。零仅表示留存记录中无匹配，不证明原始观测无缺口。')
}
function showDiagnostic() {
  const diagnostic = data.value?.diagnostic
  if (!diagnostic) return
  const rows: [string, string][] = [
    ['核验阶段', diagnosticStage.value],
    ['诊断日期', `${date.value} · ${profile.timezone}`],
    ['来源实例', diagnostic.source.instance], ['诊断版本', diagnostic.version], ['消费目录版本', pinnedVersion.value],
  ]
  for (const reason of diagnostic.reasons) {
    const evidence = reason.evidence
    const label = `${diagnosticTypes[reason.kind]} · ${failureLabels[reason.code]}`
    const queriedDates = 'queried_dates' in evidence ? evidence.queried_dates : undefined
    rows.push([label, reason.count === null ? '记录数未知' : `${reason.count} 条`],
      [`${label}／读取`, evidence.finished_at ? `${evidence.read_at} → ${evidence.finished_at}` : `${evidence.read_at}；未取得成功完成回执`],
      [`${label}／${queriedDates ? '查询日期外包范围' : '目标查询窗'}`, `${evidence.query_window.start} → ${evidence.query_window.end_exclusive}（右端不含）`],
      [`${label}／${diagnostic.stage === 'source_read' ? '部分原文' : '原结果'}`, evidence.source_data_sha256], [`${label}／成功回执`, evidence.receipt_sha256 ?? '未取得'],
      [`${label}／SQL`, evidence.query_sha256], [`${label}／选择清单`, evidence.selection_sha256])
    if (queriedDates) rows.push([`${label}／实际查询日期`, queriedDates.join('、')])
    if ('audit_sha256' in evidence && evidence.audit_sha256) rows.push([`${label}／离线核验`, evidence.audit_sha256])
    if ('failure_sha256' in evidence && evidence.failure_sha256) rows.push([`${label}／失败日志`, evidence.failure_sha256])
  }
  void openDialog(`${diagnosticTitle.value}的依据`, rows, diagnostic.stage === 'source_read'
    ? '目标查询窗不代表已完整读取。超时未取得整日数据或成功完成回执，不能判断异常数量或记录是否通过校验。未修改源库；观察覆盖与历史检测版本仍未知。'
    : '只说明本次已查证的问题。集合不一致的条数是独立明细与总表候选的对称差，不表示全库孤立记录。不同原因可能重叠，不能相加为受影响规模。未修改原始身份、等级、时间或源库；观察覆盖与历史检测版本仍未知。')
}
function showScale() {
  const value = scale.value
  if (!value || value.state === 'unavailable') {
    void openDialog('可见前缀数的依据', [['当前状态', scaleNote.value]],
      value?.message || '只有已验证且明确绑定的单RIB提供前缀并集；不可用不表示前缀数为零。')
    return
  }
  void openDialog('可见前缀数的依据', [
    ['当前状态', scaleNote.value], ['文件实际时点', `${toBusinessTime(new Date(value.observed_at))} · ${profile.timezone}`],
    ['统计范围', `${familyLabel.value} · RRC25 · 跨本文件 Peer 位置合并相同前缀`],
    ['规模可用日期', value.available_dates.join('、')], ['本文件有路由的 Peer 位置数', count(value.peer_position_count)],
    ['源文件', value.source.path], ['源文件 SHA256', value.source.sha256],
    ...(value.prefix_set_sha256 ? [['前缀集合 SHA256', value.prefix_set_sha256]] as [string, string][] : []),
    ['规模版本', value.version], ['消费版本', data.value?.version ?? '未知'], ['解释口径', value.interpretation_version],
  ], '这是单RIB自身时点的观察，不是整日、日末或连续RouteState。异常类型、等级、小时和搜索不改变此快照。观察覆盖未知，不能据此判断全网可达、实际断网或原因。')
}
function showOrigin() {
  const value = scale.value
  const origin = value && value.state !== 'unavailable' ? value.origin : undefined
  void openDialog('可见起源 AS 数的依据', [
    ['当前状态', originNote.value],
    ['统计规则', '从路径末端跳过私用 AS，取能明确归属的单 ASN；跨 Peer 去重，双栈取集合并集'],
    ['不计入单 ASN', '无法明确归属的集合／联盟段；不拆集合，不越过歧义猜测'],
    ['保留值边界', '沿用旧归属规则排除65535；0、23456、4294967295不作明确归属'],
    ['原始信息', '原始路径和原始末端另存，不由归属结果覆盖'],
    ['未明确归属的路由条目', count(origin?.unattributed_entries)],
    ['源文件 SHA256', value && value.state !== 'unavailable' ? value.source.sha256 : '未知'],
    ['起源统计版本', origin?.version ?? '尚未绑定'], ['消费版本', data.value?.version ?? '未知'],
  ], '只说明单RIB实际时点的明确归属ASN集合，不代表所有网络。未明确归属的条目数不是缺测ASN数。异常类型、等级、小时和搜索不重算快照。')
}
function showPaths() {
  const value = pathReady.value
  if (!value) return
  const rows: [string, string][] = [
    ['左观察时点', `${toBusinessTime(new Date(value.left.observed_at))} · ${profile.timezone}`],
    ['右观察时点', `${toBusinessTime(new Date(value.right.observed_at))} · ${profile.timezone}`],
    ['比较单位', '同一原始 Peer BGP ID、IP、ASN × AFI × SAFI × Prefix；不是独立前缀数'],
    ['可比较对象对', count(value.metrics?.comparable_pairs)], ['路径不同', `${count(value.metrics?.different)} 对 · ${pathRatio.value}`],
    ['左源 SHA256', value.left.sha256], ['右源 SHA256', value.right.sha256],
    ['比较结果版本', value.comparison_version], ['路径消费版本', value.version], ['首页消费版本', data.value?.version ?? '未知'],
    ['样本选择', '每个地址族按留存顺序最多5个路径不同对象；不是风险排序或代表性采样'],
  ]
  value.examples.forEach((example, index) => {
    const label = `样本 ${index + 1}`
    rows.push([label, `${example.prefix} · Peer ${example.peer.ip} · BGP ID ${example.peer.bgp_id} · AS${example.peer.asn}`],
      [`${label}／左路径`, example.left_path.join(' → ')], [`${label}／右路径`, example.right_path.join(' → ')],
      [`${label}／左定位`, JSON.stringify(example.left_reference)], [`${label}／右定位`, JSON.stringify(example.right_reference)])
  })
  void openDialog('两次观察的路径对照', rows,
    '保留 AS_SEQUENCE 的重复与私用 ASN；含集合、联盟等歧义的对象不参与路径相同比较。原始 Peer 属性配对不证明连续 Session；单端缺项不表示撤回。这里只描述两个时点的差异，不能推出期间次数、变化时间、异常或影响。')
}
async function showDetail(item: CoreOverviewItem) {
  if (!data.value) return
  const version = data.value.version
  const current = ++detailRequest
  detailController?.abort()
  const request = new AbortController()
  detailController = request
  detailLoading.value = true
  await openDialog(`${types[item.kind]} · ${objectLabel(item)}`, [
    ['原引用', item.reference], ['发生时间', `${toBusinessTime(new Date(item.start_time))} · ${profile.timezone}`],
    ...(item.object_identity ? [['对象身份', '对象待核实；集合原文不是确定单ASN'], ['原对象文本', item.object],
      ['记录解释', '保留一条原检测记录；不拆集合、不确认单ASN归属，也不据此判为误报']] as [string, string][] : []),
    ...(item.parent_prefix ? [['父前缀', item.parent_prefix], ['子前缀', item.object]] as [string, string][] : []),
    ['结束信息', endLabel(item, true)], ['危险等级', levelLabel(item)],
    ...(item.level_conflict ? [
      ['总表原等级', `${levels[item.level_conflict.event_level]}（${item.level_conflict.event_level}）`],
      ['明细原等级', `${levels[item.level_conflict.detail_level]}（${item.level_conflict.detail_level}）`],
      ['总表定位', `${item.level_conflict.event_table} · ${item.reference}`],
      ['等级原始输入摘要', item.level_conflict.source_input_sha256],
    ] as [string, string][] : []),
    ['记录地址族', familyLabels[item.address_family]], ['记录内容版本', item.content_version], ['消费版本', version],
  ], item.kind === 'country_outage'
    ? '国家中断是原检测分类，不代表全国实际断网。地址族未知；ASN 原数组仅供记录检索。原聚合数量与比例不保证同一时点或人口，不能用于推算影响；说明文字中的时间不作为发生时间。缺少结束不等于持续中。'
    : item.kind === 'sub_hijack'
    ? '父子前缀和角色来自同版本检测记录，不是独立确认的攻击或责任证据。此来源没有路径字段；缺少结束不等于持续中。'
    : item.kind === 'hijack'
    ? '详情来自与列表相同的留存检测记录。原角色、is_hijack和filter_reason均保留；角色及检测标记不是独立确认的攻击或责任证据。缺少结束不等于持续中。'
    : '详情来自与列表相同的留存记录。原记录中的未知状态保留；RRC25 来源映射是之后确认的解释，不反写原字段。')
  try {
    const result = await getCoreOverviewRecord(item.reference, version, request.signal)
    if (current === detailRequest) detail.value = result
  } catch (cause) {
    if (current === detailRequest && !request.signal.aborted) detailError.value = errorMessage(cause)
  } finally {
    if (current === detailRequest) detailLoading.value = false
  }
}
onMounted(() => { void load() })
onBeforeUnmount(() => { requestNumber++; controller?.abort(); detailController?.abort() })
</script>

<template>
  <div class="core-real">
    <header class="core-header">
      <RouterLink to="/" class="core-brand" aria-label="Domeye 核心态势"><svg viewBox="0 0 40 40" aria-hidden="true"><ellipse cx="20" cy="20" rx="17" ry="10"/><ellipse cx="20" cy="20" rx="10" ry="17" transform="rotate(35 20 20)"/><circle cx="20" cy="20" r="4"/></svg><strong>domeye<small>路由观测</small></strong></RouterLink>
      <nav aria-label="核心态势导航"><a href="#" class="active">核心态势</a><a href="#routing">变化趋势</a><a href="#events">路由异常</a><RouterLink to="/events">事件检索</RouterLink></nav>
      <span class="core-header-note">历史窗口 / 只读数据</span>
    </header>
    <div class="core-toolbar">
      <button @click="showScope">● RRC25 <span>来源说明 ⓘ</span></button>
      <label>日期 <input v-model="date" type="date" aria-label="观察日期" :min="profile.window_start.slice(0, 10)" :max="profile.snapshot_time.slice(0, 10)" /></label>
      <label v-if="directoryDates.length">日期目录 <select aria-label="日期目录" :value="directoryDates.includes(date) ? date : ''" @change="selectRetainedDate"><option value="" disabled>选择日期</option><optgroup :label="`已留存（${availableDates.length} 天）`"><option v-for="day in [...availableDates].reverse()" :key="day" :value="day">{{ day }}</option></optgroup><optgroup v-if="diagnosticDates.length" :label="`有失败诊断（${diagnosticDates.length} 天）`"><option v-for="day in [...diagnosticDates].reverse()" :key="day" :value="day">{{ day }} · 失败诊断</option></optgroup></select></label>
      <span class="core-zone">{{ profile.timezone }}</span>
      <label class="core-family">地址族 <select v-model="family" aria-label="地址族"><option value="all">全部</option><option value="ipv4">IPv4</option><option value="ipv6">IPv6</option><option value="unknown">未知</option></select></label>
      <button @click="load(true)" :disabled="loading">重新读取</button>
    </div>
    <main class="variant-c c-overview core-main" :aria-busy="loading">
      <div class="c-title-row"><div><p class="overline">ROUTING OVERVIEW</p><h1>路由态势</h1></div><div class="c-time-stamp"><span>选定历史窗口</span><strong>{{ date }} · 00:00–24:00</strong><small>RRC25 · 观察覆盖未知</small></div></div>
      <div v-if="data?.diagnostic" class="core-notice core-diagnostic" role="alert"><strong>{{ diagnosticTitle }}</strong><span>此日不提供统计和异常列表，不表示没有异常。</span><ul><li v-for="reason in data.diagnostic.reasons" :key="`${reason.kind}:${reason.code}`">{{ diagnosticTypes[reason.kind] }}：{{ failureLabels[reason.code] }}<template v-if="reason.count !== null">，{{ count(reason.count) }} 条</template>。</li></ul><small>核验阶段：{{ diagnosticStage }}。原始值保留，此日尚未准入。</small><button @click="showDiagnostic">核验依据与版本 ↗</button></div>
      <div v-else-if="error" class="core-notice" role="alert"><strong>数据不可用</strong><span>{{ error }}</span><button @click="load(true)">重新读取</button></div>
      <div v-else-if="data?.state === 'window_not_retained'" class="core-notice" role="status"><strong>选定日期尚未留存</strong><span>不是没有异常；本版本已留存 {{ availableDates.length }} 天，可在上方选择日期。</span><button @click="useRetainedWindow">查看最近已留存日期</button></div>
      <section aria-labelledby="c-overview-title">
        <div class="c-section-caption"><h2 id="c-overview-title">整体概况</h2><span>{{ familyLabel }} · {{ typeScopeLabel }}</span></div>
        <div class="c-metrics">
          <button class="c-metric" @click="showScale"><span class="c-metric-label">可见前缀数 <span>↗</span></span><strong :class="{ 'c-unknown-number': !scaleReady }" data-testid="core-prefix-count">{{ scaleReady ? count(data?.overview?.visible_prefixes) : '—' }} <small v-if="scaleReady">条</small></strong><span class="c-metric-note">{{ scaleNote }}</span></button>
          <button class="c-metric" @click="showOrigin"><span class="c-metric-label">可见起源 AS 数 <span>↗</span></span><strong :class="{ 'c-unknown-number': !originReady }" data-testid="core-origin-count">{{ originReady ? count(data?.overview?.visible_origin_ases) : '—' }} <small v-if="originReady">个</small></strong><span class="c-metric-note">{{ originNote }}</span></button>
          <button class="c-metric" @click="showScope"><span class="c-metric-label">新增异常记录 <span>↗</span></span><strong data-testid="core-record-count">{{ ready ? count(data?.overview?.record_count) : '—' }} <small v-if="ready">条</small></strong><span class="c-metric-note">{{ loading ? '正在读取' : ready ? `选定留存窗口 · ${availableTypes.length} 类记录` : '选定窗口不可用' }}</span></button>
        </div>
      </section>
      <div id="routing" class="c-trends">
        <section class="c-panel c-outage-panel" aria-labelledby="c-outage-title">
          <div class="c-panel-heading"><div><p class="overline">PREFIX OUTAGE</p><h2 id="c-outage-title">前缀中断</h2></div><span class="c-tag">已存异常记录</span></div>
          <div class="c-chart-caption"><span>每小时新增中断前缀数</span><span>单位：个 · {{ familyLabel }}</span></div>
          <div v-if="!ready" class="c-empty-chart" role="status"><strong>{{ loading ? '正在读取中断记录' : '中断数据不可用' }}</strong><p>不能据此判断没有中断。</p></div>
          <div v-else class="c-bar-chart" aria-label="每小时新增中断前缀，点选时段筛选异常列表">
            <div class="c-y-axis" aria-hidden="true"><span>{{ chartMax }}</span><span>{{ Math.round(chartMax / 2) }}</span><span>0</span></div>
            <div class="c-plot"><button v-for="(point, index) in data?.trend?.buckets" :key="point.start" class="c-bar-slot" :class="{ selected: hour === index }" :aria-pressed="hour === index" :aria-label="`${hourLabel(index)}，新增中断前缀 ${point.value} 个，筛选该时段`" :title="`${hourLabel(index)} · ${point.value} 个前缀`" @click="hour = hour === index ? null : index"><span class="c-bar" :style="{ height: `${point.value / chartMax * 100}%` }"></span><span class="c-bar-value">{{ point.value }}</span></button></div>
            <div class="c-x-axis" aria-hidden="true"><span>00:00</span><span>06:00</span><span>12:00</span><span>18:00</span><span>24:00</span></div>
          </div>
          <div class="c-chart-control"><label>查看时段 <select v-model="hour" aria-label="筛选异常时段" :disabled="!ready"><option :value="null">整个窗口</option><option v-for="index in 24" :key="index" :value="index - 1">{{ hourLabel(index - 1) }}</option></select></label><button class="c-text-button" @click="showMetric">统计口径 ↗</button></div>
          <p class="c-note">点选时段筛选列表；不是当前仍中断数。零表示留存记录无匹配，观察覆盖仍未知。</p>
        </section>
        <section class="c-panel c-change-panel" aria-labelledby="c-change-title">
          <div class="c-panel-heading"><div><p class="overline">ROUTE CHANGES</p><h2 id="c-change-title">路由变化</h2></div><span class="c-tag c-tag-muted">{{ changeKind === 1 && pathReady ? '两次观察' : '暂无可用数据' }}</span></div>
          <div class="c-change-tabs" role="group" aria-label="路由变化类别"><button v-for="(change, index) in changes" :key="index" :aria-pressed="changeKind === index" :class="{ active: changeKind === index }" @click="changeKind = index">{{ change[0] }}</button></div>
          <div v-if="changeKind === 1 && pathReady" class="core-path-comparison" aria-live="polite" data-testid="core-path-comparison">
            <p class="core-path-times">{{ toBusinessTime(new Date(pathReady.left.observed_at)).slice(5, 16) }} → {{ toBusinessTime(new Date(pathReady.right.observed_at)).slice(5, 16) }} <small>{{ profile.timezone }}</small></p>
            <div class="core-path-primary"><div><span>路径不同的对象对</span><strong data-testid="core-path-different">{{ count(pathReady.metrics?.different) }}</strong></div><div><span>占可比较对象对</span><strong class="core-path-ratio">{{ pathRatio }}</strong></div></div>
            <p class="core-path-denominator">可比较 {{ count(pathReady.metrics?.comparable_pairs) }} 对 · 相同 {{ count(pathReady.metrics?.same) }} 对</p>
            <dl class="core-path-other"><div><dt>不可比较</dt><dd>{{ count(pathReady.metrics?.not_comparable) }}</dd></div><div><dt>仅左侧记录</dt><dd>{{ count(pathReady.metrics?.left_only) }}</dd></div><div><dt>仅右侧记录</dt><dd>{{ count(pathReady.metrics?.right_only) }}</dd></div></dl>
            <button class="c-text-button" @click="showPaths">路径样本与依据 ↗</button>
            <p class="c-note">单位：原始 Peer × 地址族 × 前缀的对象对。不是独立前缀数，也不是期间变化次数。</p>
          </div>
          <template v-else><div class="c-change-state" aria-live="polite"><span class="c-pending-mark" aria-hidden="true">—</span><h3>{{ changeKind === 1 ? pathNote : changes[changeKind]?.[1] }}</h3><p>{{ changeKind === 1 ? '不沿用其他日期或地址族的对照。' : '现有异常记录不能代替完整路由状态。' }}</p></div>
          <div class="c-change-boundary"><span class="c-small-dot"></span><p>{{ changes[changeKind]?.[2] }}</p></div><p class="c-note">不以通告、撤回消息量替代。</p></template>
        </section>
      </div>
      <section id="events" class="c-panel c-events" aria-labelledby="c-events-title">
        <div class="c-panel-heading"><div><p class="overline">ROUTING ANOMALIES</p><h2 id="c-events-title">路由异常 <span class="c-event-count">{{ ready ? count(data?.events?.total) : '—' }}</span></h2></div><span class="c-tag">RRC25 · 留存记录</span></div>
        <div class="c-filter-row"><div class="c-event-filters">
          <label><span class="sr-only">异常类型</span><select v-model="kind" aria-label="异常类型"><option value="all">全部已接入类型</option><option v-for="key in availableTypes" :key="key" :value="key">{{ types[key] }}</option><option v-if="!availableTypes.includes('hijack')" disabled>前缀劫持 · 未接入</option><option v-if="!availableTypes.includes('sub_hijack')" disabled>子前缀劫持 · 未接入</option><option v-if="!availableTypes.includes('country_outage')" disabled>国家中断 · 未接入</option></select></label>
          <label><span class="sr-only">危险等级</span><select v-model="level" aria-label="危险等级"><option value="all">全部等级</option><option value="high">高</option><option value="middle">中</option><option value="low">低</option><option value="conflict">等级待核实</option><option value="unknown">未知</option></select></label>
          <label class="c-list-search"><span aria-hidden="true">⌕</span><input v-model="query" maxlength="120" aria-label="筛选异常对象或编号" placeholder="筛选对象 / ASN / 编号" /></label>
        </div><label class="c-sort">排序 <select v-model="sort" aria-label="异常排序"><option value="severity">等级优先 · 时间倒序</option><option value="time">发生时间倒序</option></select></label></div>
        <div class="c-list-context" aria-live="polite"><span>{{ familyLabel }} · {{ hour === null ? '整个窗口' : hourLabel(hour) }}</span><span v-if="ready && kind === 'prefix_outage'" data-testid="core-distinct-prefixes">去重前缀 {{ count(data?.events?.distinct_prefixes) }} 个</span><button v-if="hasFilters" class="c-text-button" @click="resetFilters">清除筛选 ×</button></div>
        <p v-if="ready && data?.query.excluded_unknown_family" class="c-note">本地址族筛选未纳入 {{ data.query.excluded_unknown_family }} 条无法判定地址族的记录，可切换“未知”查看。</p>
        <div v-if="!ready" class="c-events-empty" role="status"><strong>{{ loading ? '正在读取异常记录' : '事件数据不可用' }}</strong><p>不可用不能解释为没有异常。</p></div>
        <div v-else-if="!data?.events?.total" class="c-events-empty" role="status"><strong>没有匹配的留存记录</strong><p>可以清除筛选或选择其他时段。</p><button class="c-text-button" @click="resetFilters">清除筛选</button></div>
        <table v-else class="c-event-table"><caption class="sr-only">选定留存版本内的路由异常记录</caption><thead><tr><th scope="col">危险等级</th><th scope="col">异常类型</th><th scope="col">涉及对象</th><th scope="col">发生时间</th><th scope="col">结束信息</th><th scope="col"><span class="sr-only">查看详情</span></th></tr></thead><tbody><tr v-for="item in data?.events?.items" :key="item.reference"><td data-label="等级"><span class="c-severity" :class="item.level === null ? 'c-level-unknown' : `c-level-${item.level === 'high' ? 0 : item.level === 'middle' ? 1 : 2}`"><i></i>{{ levelLabel(item) }}</span></td><td data-label="类型">{{ types[item.kind] }}</td><td data-label="对象" class="c-target">{{ objectLabel(item) }}<small v-if="item.object_identity" class="core-identity-note">对象待核实</small><small v-if="item.parent_prefix">父前缀 {{ item.parent_prefix }}</small><small v-if="item.address_family === 'mixed'">已存前缀含 IPv4 + IPv6</small></td><td data-label="发生">{{ time(item.start_time) }}</td><td data-label="结束" :class="{ 'c-end-unknown': item.end_time.state !== 'recorded' }">{{ endLabel(item) }}</td><td class="c-detail-cell"><button :aria-label="`查看 ${item.object} 编号 ${item.record_number} 详情`" @click="showDetail(item)">详情 ↗</button></td></tr></tbody></table>
        <div class="c-table-footer"><p>结束未记录 ≠ 持续中。等级冲突标为待核实，详情保留两份原值；等级不代表损害概率。</p><div v-if="ready && data?.events?.total" class="c-pagination"><span>{{ data.events.page }} / {{ data.events.page_count }}</span><button aria-label="上一页异常" :disabled="page === 1" @click="goPage(page - 1)">←</button><button aria-label="下一页异常" :disabled="page >= data.events.page_count" @click="goPage(page + 1)">→</button></div></div>
      </section>
      <p class="c-bottom-note">列表局部筛选不改变上方概况和趋势。只说明 RRC25 的已存异常记录，不代表全网状态、实际断网或原因。</p>
      <footer class="core-footer"><button @click="showScope">来源、版本与数据边界 ↗</button><span :title="pinnedVersion">{{ pinnedVersion ? `${pinnedVersion.slice(0, 28)}…` : '留存版本待读取' }}</span><RouterLink to="/legacy-overview">旧 P0 页面</RouterLink></footer>
    </main>
    <dialog ref="dialog" class="core-dialog" aria-labelledby="core-dialog-title" @close="afterClose" @click="event => { if (event.target === dialog) closeDialog() }">
      <div class="core-dialog-heading"><span class="overline">SOURCE / EVIDENCE</span><button aria-label="关闭详情" @click="closeDialog">×</button></div><h2 id="core-dialog-title">{{ dialogTitle }}</h2>
      <dl><div v-for="[label, value] in dialogRows" :key="label"><dt>{{ label }}</dt><dd>{{ value }}</dd></div></dl>
      <p v-if="detailLoading" role="status">正在复读同版本详情…</p><p v-if="detailError" role="alert">详情不可用：{{ detailError }}</p>
      <details v-if="detail"><summary>完整源键、原字段与保留的解释</summary><pre>{{ JSON.stringify(detail.record.record, null, 2) }}</pre></details>
      <p class="core-detail-note">{{ dialogNote }}</p><button class="core-dialog-done" @click="closeDialog">返回首页</button>
    </dialog>
  </div>
</template>

<script setup lang="ts">
// 已选 C 的有界调整：整体概况 → 变化趋势 → 异常列表；无 ASN 主导航、无真实数据。
import { computed, ref, watch } from 'vue'
import { dateLabel, timeLabel, timezone, type Family, type Scenario, type Detail } from './fixture'
import { anomalyTypes, overviewEvents, overviewEventDetail, overviewVersion, severityOrder, clockLabel, hourLabel, isGapHour } from './overviewFixture'
import './overview.css'

const props = defineProps<{ family: Family; scenario: Scenario }>()
const emit = defineEmits<{ detail: [value: Detail] }>()
const selectedHour = ref<number | null>(null)
const typeFilter = ref('全部类型')
const levelFilter = ref('全部等级')
const query = ref('')
const sort = ref('severity')
const page = ref(1)
const pageSize = 7
const changeKind = ref(0)
const changeKinds = [
  { name: '可见性', definition: '在可比较的连续观测中，前缀由可见变为不可见，或重新可见。', missing: '逐前缀状态数据尚未验证', boundary: '观测缺口不能当作前缀消失。' },
  { name: '路径', definition: '同一前缀、同一观测关系下，AS 路径发生变化。', missing: '带观测时间的路径数据尚未验证', boundary: '有路径样本，不等于能比较两个时点。' },
  { name: '起源', definition: '同一前缀在可比较观测中的起源 AS 发生变化。', missing: '完整的起源变化记录尚未验证', boundary: '劫持事件只覆盖部分情形，不能代表全部起源变化。' },
]
const currentChange = computed(() => changeKinds[changeKind.value]!)
const scopedEvents = computed(() => props.scenario === 'unavailable' ? [] : overviewEvents.filter(event =>
  event.family === props.family && !(props.scenario === 'gap' && isGapHour(Math.floor(event.minute / 60))),
))
const outageCounts = computed(() => Array.from({ length: 24 }, (_, hour) => {
  if (props.scenario === 'unavailable' || (props.scenario === 'gap' && isGapHour(hour))) return null
  return new Set(scopedEvents.value.filter(event => event.type === '前缀中断' && Math.floor(event.minute / 60) === hour).map(event => event.target)).size
}))
const chartMax = computed(() => Math.max(4, ...outageCounts.value.map(value => value ?? 0)))
const filteredEvents = computed(() => scopedEvents.value.filter(event =>
  (selectedHour.value === null || Math.floor(event.minute / 60) === selectedHour.value) &&
  (typeFilter.value === '全部类型' || event.type === typeFilter.value) &&
  (levelFilter.value === '全部等级' || event.severity === levelFilter.value) &&
  `${event.id} ${event.target} ${event.type}`.toLowerCase().includes(query.value.trim().toLowerCase()),
).sort((a, b) => (sort.value === 'severity' ? severityOrder[a.severity] - severityOrder[b.severity] : 0) || b.minute - a.minute || a.id.localeCompare(b.id)))
const pageCount = computed(() => Math.max(1, Math.ceil(filteredEvents.value.length / pageSize)))
const pageEvents = computed(() => filteredEvents.value.slice((page.value - 1) * pageSize, page.value * pageSize))
const hasFilters = computed(() => selectedHour.value !== null || typeFilter.value !== '全部类型' || levelFilter.value !== '全部等级' || !!query.value)
watch([selectedHour, typeFilter, levelFilter, query, sort, () => props.family, () => props.scenario], () => { page.value = 1 })
watch([() => props.family, () => props.scenario], () => { selectedHour.value = null })
function resetFilters() {
  selectedHour.value = null
  typeFilter.value = '全部类型'
  levelFilter.value = '全部等级'
  query.value = ''
}
function showMetric(name: string, note: string) {
  emit('detail', {
    title: name, subtitle: '首页指标口径 / 未接入真实数据',
    rows: [['观察范围', `${props.family} · 全部示例观察点`], ['时间', `${dateLabel} · ${timezone}`], ['数据版本', overviewVersion], ['当前用途', '仅用于讨论首页结构，不代表指标已经可计算']], note,
  })
}
</script>

<template>
  <main class="variant-c prototype-main c-overview">
    <div class="c-title-row">
      <div><p class="overline">ROUTING OVERVIEW</p><h1>路由态势</h1></div>
      <div class="c-time-stamp"><span>示例数据截至</span><strong>{{ dateLabel }} {{ timeLabel }}</strong><small>{{ timezone }}</small></div>
    </div>
    <section class="c-overview-section" aria-labelledby="c-overview-title">
      <div class="c-section-caption"><h2 id="c-overview-title">整体概况</h2><span>{{ family }} · 全部示例观察点</span></div>
      <div class="c-metrics">
        <button class="c-metric" @click="showMetric('可见前缀数', '目标是可见路由前缀的去重条数，不是 IPv4 /24 或 IPv6 /48 覆盖块数量。当前未取得通过验证的总体状态数据，保留未知。')">
          <span class="c-metric-label">可见前缀数 <span>↗</span></span><strong class="c-unknown-number">—</strong><span class="c-metric-note">前缀条数 · 数据待验证</span>
        </button>
        <button class="c-metric" @click="showMetric('可见起源 AS 数', '目标是可见路由中能够明确识别的起源 AS 去重数量，不包括仅在路径中经过的 AS。它是整体规模指标，不是 ASN 介绍或排行。')">
          <span class="c-metric-label">可见起源 AS 数 <span>↗</span></span><strong class="c-unknown-number">—</strong><span class="c-metric-note">起源 AS 去重 · 数据待验证</span>
        </button>
        <button class="c-metric" @click="showMetric('新增异常事件', '按发生时间统计当前地址族、示例观察范围内的模拟事件 ID。缺口模式只统计可用时段；此卡不随下方列表的局部筛选变化。')">
          <span class="c-metric-label">新增异常事件 <span>↗</span></span><strong>{{ scenario === 'unavailable' ? '—' : scopedEvents.length }} <small v-if="scenario !== 'unavailable'">条</small></strong><span class="c-metric-note">{{ scenario === 'gap' ? '仅可用时段 · 模拟记录' : scenario === 'unavailable' ? '事件数据不可用' : '整个选定窗口 · 模拟记录' }}</span>
        </button>
      </div>
    </section>
    <div id="routing" class="c-trends">
      <section class="c-panel c-outage-panel" aria-labelledby="c-outage-title">
        <div class="c-panel-heading"><div><p class="overline">PREFIX OUTAGE</p><h2 id="c-outage-title">前缀中断</h2></div><span class="c-tag">模拟数据</span></div>
        <div class="c-chart-caption"><span>每小时新增中断前缀数</span><span>单位：个 · {{ family }}</span></div>
        <div v-if="scenario === 'unavailable'" class="c-empty-chart" role="status"><strong>中断数据不可用</strong><p>不能据此判断没有中断事件。</p></div>
        <div v-else class="c-bar-chart" aria-label="每小时新增中断前缀，点选时段筛选异常列表">
          <div class="c-y-axis" aria-hidden="true"><span>{{ chartMax }}</span><span>{{ chartMax / 2 }}</span><span>0</span></div>
          <div class="c-plot">
            <button v-for="(count, hour) in outageCounts" :key="hour" class="c-bar-slot" :class="{ selected: selectedHour === hour, missing: count === null }" :disabled="count === null" :aria-pressed="selectedHour === hour" :aria-label="`${hourLabel(hour)}，${count === null ? '观测缺口' : `新增中断前缀 ${count} 个，筛选该时段`}`" :title="`${hourLabel(hour)} · ${count === null ? '观测缺口' : `${count} 个前缀`}`" @click="selectedHour = selectedHour === hour ? null : hour">
              <span v-if="count !== null" class="c-bar" :style="{ height: `${count / chartMax * 100}%` }"></span><span v-if="count !== null" class="c-bar-value">{{ count }}</span>
            </button>
            <span v-if="scenario === 'gap'" class="c-gap-label">观测缺口</span>
          </div>
          <div class="c-x-axis" aria-hidden="true"><span>00:00</span><span>06:00</span><span>12:00</span><span>18:00</span><span>24:00</span></div>
        </div>
        <div class="c-chart-control"><label>查看时段 <select v-model="selectedHour" aria-label="筛选异常时段" :disabled="scenario === 'unavailable'"><option :value="null">整个窗口</option><option v-for="(_, hour) in outageCounts" :key="hour" :value="hour" :disabled="outageCounts[hour] === null">{{ hourLabel(hour) }}{{ outageCounts[hour] === null ? ' · 缺口' : '' }}</option></select></label><button class="c-text-button" @click="showMetric('新增中断前缀数', '每个小时内，按发生时间筛选前缀中断事件，再对前缀去重。同一前缀可在不同小时重复出现，各柱相加不等于全天去重前缀数。这不是当前仍在中断的前缀数。')">统计口径 ↗</button></div>
        <p class="c-note">{{ scenario === 'gap' ? '缺口不补零。' : '' }}点选时段可筛选下方列表；不是“当前仍在中断”的数量。</p>
      </section>
      <section class="c-panel c-change-panel" aria-labelledby="c-change-title">
        <div class="c-panel-heading"><div><p class="overline">ROUTE CHANGES</p><h2 id="c-change-title">路由变化</h2></div><span class="c-tag c-tag-muted">待数据验证</span></div>
        <div class="c-change-tabs" role="group" aria-label="路由变化类别"><button v-for="(kind, index) in changeKinds" :key="kind.name" :aria-pressed="changeKind === index" :class="{ active: changeKind === index }" @click="changeKind = index">{{ kind.name }}</button></div>
        <div class="c-change-state" aria-live="polite"><span class="c-pending-mark" aria-hidden="true">—</span><h3>{{ currentChange.missing }}</h3><p>{{ currentChange.definition }}</p></div>
        <div class="c-change-boundary"><span class="c-small-dot"></span><p>{{ currentChange.boundary }}</p></div>
        <p class="c-note">包含未被判为异常的变化；不以通告、撤回消息量替代。</p>
      </section>
    </div>
    <section id="events" class="c-panel c-events" aria-labelledby="c-events-title">
      <div class="c-panel-heading"><div><p class="overline">ROUTING ANOMALIES</p><h2 id="c-events-title">路由异常 <span class="c-event-count">{{ scenario === 'unavailable' ? '—' : filteredEvents.length }}</span></h2></div><span class="c-tag">模拟事件</span></div>
      <div class="c-filter-row">
        <div class="c-event-filters"><label><span class="sr-only">异常类型</span><select v-model="typeFilter" aria-label="异常类型"><option>全部类型</option><option v-for="type in anomalyTypes" :key="type">{{ type }}</option></select></label><label><span class="sr-only">危险等级</span><select v-model="levelFilter" aria-label="危险等级"><option>全部等级</option><option v-for="level in ['高', '中', '低']" :key="level">{{ level }}</option></select></label><label class="c-list-search"><span aria-hidden="true">⌕</span><input v-model="query" aria-label="筛选异常对象或编号" placeholder="筛选 Prefix / ASN / 编号" /></label></div>
        <label class="c-sort">排序 <select v-model="sort" aria-label="异常排序"><option value="severity">等级优先 · 时间倒序</option><option value="time">发生时间倒序</option></select></label>
      </div>
      <div class="c-list-context" aria-live="polite"><span>{{ family }} · {{ selectedHour === null ? '整个窗口' : hourLabel(selectedHour) }}{{ scenario === 'gap' ? ' · 仅显示可用时段' : '' }}</span><button v-if="hasFilters" class="c-text-button" @click="resetFilters">清除筛选 ×</button></div>
      <div v-if="scenario === 'unavailable'" class="c-events-empty" role="status"><strong>事件数据不可用</strong><p>不能将不可用状态解释为“没有异常”。</p></div>
      <div v-else-if="!filteredEvents.length" class="c-events-empty" role="status"><strong>没有匹配的示例记录</strong><p>可以清除筛选或选择其他时段。</p><button class="c-text-button" @click="resetFilters">清除筛选</button></div>
      <table v-else class="c-event-table"><caption class="sr-only">当前筛选范围内的模拟路由异常事件</caption><thead><tr><th scope="col">危险等级</th><th scope="col">异常类型</th><th scope="col">涉及对象</th><th scope="col">发生时间</th><th scope="col">结束信息</th><th scope="col"><span class="sr-only">查看详情</span></th></tr></thead><tbody><tr v-for="event in pageEvents" :key="event.id"><td data-label="等级"><span class="c-severity" :class="`c-level-${severityOrder[event.severity]}`"><i></i>{{ event.severity }}</span></td><td data-label="类型">{{ event.type }}</td><td data-label="对象" class="c-target">{{ event.target }}</td><td data-label="发生">{{ clockLabel(event.minute) }}</td><td data-label="结束" :class="{ 'c-end-unknown': event.endMinute === undefined }">{{ event.endMinute === undefined ? '未记录' : `${clockLabel(event.endMinute)} · 已记录` }}</td><td class="c-detail-cell"><button :aria-label="`查看 ${event.id} 详情`" @click="emit('detail', overviewEventDetail(event))">详情 ↗</button></td></tr></tbody></table>
      <div class="c-table-footer"><p>结束时间未记录 ≠ 持续中。等级仅为演示标签。</p><div class="c-pagination" v-if="scenario !== 'unavailable' && filteredEvents.length"><span>{{ page }} / {{ pageCount }}</span><button aria-label="上一页异常" :disabled="page === 1" @click="page--">←</button><button aria-label="下一页异常" :disabled="page >= pageCount" @click="page++">→</button></div></div>
    </section>
    <p class="c-bottom-note">图表与列表只按时间对应，不表示异常事件解释了全部路由变化。列表筛选不改变上方整体概况。</p>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { getEvents } from '@/api/events'
import { resultDelivery } from '@/api/health'
import EventTable from '@/components/EventTable.vue'
import PageState from '@/components/PageState.vue'
import { CORE_EVENT_TYPES, type EventPage, type EventRow } from '@/types/api'
import { localTime, scopeLabel } from '@/utils/coreScope'
import { eventDataRange, eventDateParameter, eventPresetRange, eventRangeError, eventRangeFromQuery, normalizeEventRange } from '@/utils/eventScope'
import { errorMessage } from '@/utils/normalize'
import { resolveDataWindow } from '@/utils/time'

const router = useRouter()
const route = useRoute()
const dataRange = eventDataRange(resolveDataWindow(import.meta.env))
const minimumTime = dataRange?.start
const maximumTime = dataRange?.end
const deliveryRange = computed(() => {
  const delivery = resultDelivery.value
  if (delivery?.state !== 'available' || !delivery.start || !delivery.end_exclusive) return null
  return { start: localTime(delivery.start), end: localTime(delivery.end_exclusive) }
})
const defaultRange = () => eventPresetRange(7, deliveryRange.value || dataRange || { start: '', end: '' })
const DATE_PRESETS = [
  { id: 'recent-7', label: '近 7 天', days: 7 },
  { id: 'recent-30', label: '近 30 天', days: 30 },
  { id: 'full-window', label: '整个数据窗口', days: null },
] as const
const queryText = (value: unknown) => typeof value === 'string' ? value.trim() : ''
function filtersFromQuery() {
  const eventType = queryText(route.query.event_type)
  const country = queryText(route.query.country)
  return {
    eventType: CORE_EVENT_TYPES.includes(eventType as (typeof CORE_EVENT_TYPES)[number]) ? eventType : '',
    level: ['high', 'middle', 'low'].includes(queryText(route.query.level)) ? queryText(route.query.level) : '',
    country: ['domestic', 'foreign'].includes(country) ? country : 'all',
    attackedCountry: queryText(route.query.attacked_country) || (!['all', 'domestic', 'foreign'].includes(country) ? country : ''),
    attackedAs: queryText(route.query.attacked_as).replace(/^AS/i, ''),
    keyword: queryText(route.query.event_info),
    ...eventRangeFromQuery(route.query, defaultRange()),
    pageSize: [10, 50, 100, 200].includes(Number(route.query.page_size)) ? Number(route.query.page_size) : 10,
  }
}
const filters = reactive(filtersFromQuery())
const page = ref(1)
const result = ref<EventPage>({ data: [], totalPage: 0, recordCount: 0 })
const loading = ref(true)
const error = ref('')
let requestId = 0

const pageLabel = computed(() => {
  const total = Math.max(1, result.value.totalPage)
  return `${page.value.toString().padStart(2, '0')} / ${total.toString().padStart(2, '0')}`
})
const presetRange = (preset: (typeof DATE_PRESETS)[number]) => preset.days === null
  ? dataRange || { start: '', end: '' }
  : eventPresetRange(preset.days, deliveryRange.value || dataRange || { start: '', end: '' })
const activeDatePreset = computed(() => DATE_PRESETS.find((preset) => {
  const range = presetRange(preset)
  return filters.start === range.start && filters.end === range.end
})?.id ?? '')
const activeDeliveryRange = computed(() => deliveryRange.value
  && filters.start === deliveryRange.value.start && filters.end === deliveryRange.value.end)

function applyDatePreset(preset: (typeof DATE_PRESETS)[number]) {
  Object.assign(filters, presetRange(preset))
  void applyFilters()
}
function applyDeliveryRange() {
  if (!deliveryRange.value) return
  Object.assign(filters, deliveryRange.value)
  void applyFilters()
}
function currentQuery(): Record<string, string> {
  return {
    start: filters.start, end: filters.end,
    ...(filters.eventType ? { event_type: filters.eventType } : {}),
    ...(filters.level ? { level: filters.level } : {}),
    ...(filters.country !== 'all' ? { country: filters.country } : {}),
    ...(filters.attackedCountry.trim() ? { attacked_country: filters.attackedCountry.trim() } : {}),
    ...(filters.attackedAs.trim() ? { attacked_as: filters.attackedAs.trim().replace(/^AS/i, '') } : {}),
    ...(filters.keyword.trim() ? { event_info: filters.keyword.trim() } : {}),
    ...(filters.pageSize !== 10 ? { page_size: String(filters.pageSize) } : {}),
  }
}
async function applyFilters() {
  Object.assign(filters, normalizeEventRange(filters))
  const invalid = eventRangeError(filters)
  if (invalid) {
    requestId += 1
    loading.value = false
    error.value = invalid
    return
  }
  const query = currentQuery()
  const target = router.resolve({ path: route.path, query, hash: route.hash })
  if (target.fullPath === route.fullPath) await load(true)
  else await router.replace({ query, hash: route.hash })
}
async function load(resetPage = false) {
  const id = ++requestId
  if (resetPage) page.value = 1
  const invalid = eventRangeError(filters)
  if (!dataRange || invalid) {
    result.value = { data: [], totalPage: 0, recordCount: 0 }
    loading.value = false
    error.value = !dataRange ? '缺少固定数据窗口配置，已阻止按当前日期查询。请重新构建并发布完整前端。' : invalid
    return
  }
  loading.value = true
  error.value = ''
  try {
    const next = await getEvents({
      page_num: page.value,
      page_size: filters.pageSize,
      event_type: filters.eventType || undefined,
      level: filters.level || undefined,
      country: filters.country,
      attacked_country: filters.attackedCountry.trim() || undefined,
      attacked_as: filters.attackedAs.trim().replace(/^AS/i, '') || undefined,
      event_info: filters.keyword.trim() || undefined,
      date: eventDateParameter(filters),
      sort_mode: 'start_timeB',
    })
    if (id === requestId) result.value = next
  } catch (cause) {
    if (id === requestId) error.value = errorMessage(cause)
  } finally {
    if (id === requestId) loading.value = false
  }
}

function changePage(next: number) {
  if (next < 1 || next > Math.max(1, result.value.totalPage)) return
  page.value = next
  void load()
}

function openEvent(event: EventRow) {
  if (!event.detailUrl) return
  void router.push({ name: 'event-detail', query: { ...currentQuery(), ref: event.detailUrl } })
}

onMounted(() => load())
watch(() => route.query, () => {
  if (route.name !== 'events') return
  Object.assign(filters, filtersFromQuery())
  void load(true)
})
</script>

<template>
  <article class="page events-page">
    <header class="page-heading">
      <div>
        <p class="eyebrow">异常监测 / Events</p>
        <h1>异常事件</h1>
      </div>
      <p class="page-heading-copy">
        按国家、ASN 和时间范围查找六类异常，点击事件查看业务事实与路径证据。
      </p>
    </header>

    <form class="filter-console" @submit.prevent="applyFilters()">
      <label>
        <span>异常类型</span>
        <select v-model="filters.eventType">
          <option value="">全部六类</option>
          <option v-for="type in CORE_EVENT_TYPES" :key="type" :value="type">{{ type }}</option>
        </select>
      </label>
      <label>
        <span>风险等级</span>
        <select v-model="filters.level">
          <option value="">全部等级</option>
          <option value="high">高风险</option>
          <option value="middle">中风险</option>
          <option value="low">低风险</option>
        </select>
      </label>
      <label>
        <span>事件范围</span>
        <select v-model="filters.country">
          <option value="all">全部事件</option>
          <option value="domestic">国内相关</option>
          <option value="foreign">国外事件</option>
        </select>
      </label>
      <label class="filter-keyword">
        <span>摘要检索</span>
        <input v-model="filters.keyword" type="search" placeholder="输入 ASN、前缀或事件描述" />
      </label>
      <label>
        <span>受影响国家</span>
        <input v-model="filters.attackedCountry" type="search" placeholder="例如：中国" />
      </label>
      <label>
        <span>受影响 ASN</span>
        <input v-model="filters.attackedAs" type="search" placeholder="例如：3356" />
      </label>
      <label>
        <span>开始时间（北京时间）</span>
        <input v-model="filters.start" type="datetime-local" step="1" :min="minimumTime" :max="maximumTime" required />
      </label>
      <label>
        <span>结束时间（不含）</span>
        <input v-model="filters.end" type="datetime-local" step="1" :min="minimumTime" :max="maximumTime" required />
      </label>
      <label>
        <span>每页数量</span>
        <select v-model.number="filters.pageSize">
          <option :value="10">10</option>
          <option :value="50">50</option>
          <option :value="100">100</option>
          <option :value="200">200</option>
        </select>
      </label>
      <button class="solid-action" type="submit">执行查询</button>
      <div class="filter-presets" role="group" aria-label="快捷时间范围">
        <span class="preset-label">快捷范围</span>
        <button v-if="deliveryRange" class="preset-chip" type="button"
          :class="{ 'is-active': activeDeliveryRange }" :aria-pressed="!!activeDeliveryRange"
          @click="applyDeliveryRange()">已接入时段</button>
        <button
          v-for="preset in DATE_PRESETS"
          :key="preset.id"
          class="preset-chip"
          :class="{ 'is-active': activeDatePreset === preset.id }"
          type="button"
          :aria-pressed="activeDatePreset === preset.id"
          @click="applyDatePreset(preset)"
        >
          {{ preset.label }}
        </button>
        <span class="preset-hint">按事件开始时间筛选 · 开始含、结束不含 · 北京时间</span>
        <span v-if="dataRange" class="range-hint">可选范围 {{ scopeLabel(dataRange) }}（结束不含）</span>
      </div>
    </form>

    <section class="data-panel">
      <div class="section-heading result-heading">
        <h2>查询结果</h2>
        <span v-if="loading">查询中 · 记录数未知</span>
        <span v-else-if="error">查询失败 · 记录数未知</span>
        <span v-else>{{ result.recordCount.toLocaleString('zh-CN') }} records · page {{ pageLabel }}</span>
      </div>
      <PageState
        v-if="loading"
        kind="loading"
        title="正在读取月度事件表"
        detail="跨月范围由后端自动合并"
      />
      <PageState
        v-else-if="error"
        kind="error"
        title="事件查询失败"
        :detail="error"
        @retry="load()"
      />
      <PageState v-else-if="result.data.length === 0" title="所选范围内没有匹配事件" />
      <EventTable v-else :events="result.data" @select="openEvent" />

      <nav v-if="!loading && !error && result.totalPage > 1" class="pagination" aria-label="事件分页">
        <button type="button" :disabled="page <= 1" @click="changePage(page - 1)">← 上一页</button>
        <span>{{ pageLabel }}</span>
        <button type="button" :disabled="page >= result.totalPage" @click="changePage(page + 1)">下一页 →</button>
      </nav>
    </section>
  </article>
</template>

<style scoped>
.filter-console {
  display: grid;
  grid-template-columns: repeat(4, minmax(140px, 1fr));
  gap: 13px;
  padding: 16px;
  background: var(--paper);
  border: 1px solid var(--line);
  border-radius: var(--radius);
  box-shadow: var(--shadow-sm);
}

.filter-console label {
  display: grid;
  gap: 7px;
}

.filter-console label span {
  color: var(--muted);
  font-size: 10px;
  font-weight: 650;
}

.filter-console input,
.filter-console select {
  width: 100%;
  min-width: 0;
  height: 38px;
  padding: 0 10px;
  color: var(--ink);
  background: #fff;
  border: 1px solid #cfd7e1;
  border-radius: 5px;
  font-size: 12px;
}

.filter-console .solid-action {
  align-self: end;
  height: 38px;
  min-height: 38px;
}

.filter-presets {
  display: flex;
  flex-wrap: wrap;
  grid-column: 1 / -1;
  align-items: center;
  gap: 8px;
  min-height: 34px;
  padding-top: 3px;
  border-top: 1px solid #e4e9ef;
}

.preset-label {
  margin-right: 2px;
  color: var(--muted);
  font-size: 10px;
  font-weight: 650;
}

.preset-chip {
  min-height: 28px;
  padding: 0 12px;
  cursor: pointer;
  color: var(--primary);
  background: #f6f8f9;
  border: 1px solid #becfd8;
  border-radius: 14px;
  font-size: 10px;
  font-weight: 650;
  transition: background-color 120ms ease, border-color 120ms ease, color 120ms ease;
}

.preset-chip:hover {
  background: #e7eef1;
  border-color: #8ca9b9;
}

.preset-chip.is-active {
  color: #fff;
  background: var(--primary);
  border-color: var(--primary);
}

.preset-hint {
  margin-left: auto;
  color: var(--muted);
  font-size: 10px;
}

.range-hint { flex-basis: 100%; color: var(--muted); font-size: 10px; }

.result-heading {
  margin: 0;
  padding: 15px 18px;
  border-bottom: 0;
}

.data-panel {
  overflow: hidden;
}

.data-panel > .page-state {
  margin: 0 18px 18px;
}

.pagination {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 14px;
  min-height: 58px;
  margin: 0;
  padding: 10px 18px;
  border-top: 1px solid var(--line);
  font-size: 10px;
}

.pagination button {
  min-height: 34px;
  cursor: pointer;
  padding: 0 13px;
  color: var(--primary);
  background: var(--paper);
  border: 1px solid #becfd8;
  border-radius: 5px;
  font-size: 10px;
}

.pagination button:disabled {
  cursor: not-allowed;
  color: #98a2b3;
  background: #f2f4f7;
  border-color: var(--line);
}

@media (max-width: 1180px) {
  .filter-console {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 560px) {
  .filter-console {
    grid-template-columns: 1fr;
    gap: 11px;
    padding: 13px;
  }

  .preset-hint {
    flex-basis: 100%;
    margin-left: 0;
  }

  .pagination {
    justify-content: space-between;
    padding: 10px 12px;
  }
}
</style>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { CountryOutageRecord } from '@/api/countryOutageRecord'
import PageState from '@/components/PageState.vue'
import { toBusinessTime } from '@/utils/businessTime'

const props = defineProps<{
  record: CountryOutageRecord | null
  loading: boolean
  error: string
}>()
defineEmits<{ retry: [] }>()
const query = ref('')
const page = ref(1)
const pageSize = 24
const filteredAsns = computed(() => {
  const needle = query.value.trim().replace(/^AS/i, '')
  return (props.record?.outageAsns ?? []).filter((asn) => asn.includes(needle))
})
const pageCount = computed(() => Math.max(1, Math.ceil(filteredAsns.value.length / pageSize)))
const visibleAsns = computed(() => filteredAsns.value.slice((page.value - 1) * pageSize, page.value * pageSize))
watch(query, () => { page.value = 1 })
watch(() => props.record?.bundle.sourceRecord.detailReference, () => { query.value = ''; page.value = 1 })
watch(pageCount, (count) => { page.value = Math.min(page.value, count) })
const valueText = (value: number | null) => value === null ? '未知' : value.toLocaleString('zh-CN')
const timeText = (value: string | null) => value ? toBusinessTime(new Date(value)) : '未知'
</script>

<template>
  <section class="event-record" aria-labelledby="event-record-title">
    <header><h2 id="event-record-title">事件基础记录</h2><span>检测结果</span></header>
    <PageState v-if="loading && !record" kind="loading" title="正在读取基础记录" />
    <PageState v-else-if="error" kind="error" title="基础记录暂不可用" :detail="error" @retry="$emit('retry')" />
    <template v-else-if="record">
      <dl class="record-facts">
        <div><dt>事件开始时间（北京时间）</dt><dd>{{ timeText(record.bundle.event.eventTimeUtc) }}</dd></div>
        <div><dt>记录中的中断 AS</dt><dd>{{ valueText(record.outageAsCount) }}</dd></div>
        <div><dt>记录中的 AS 总数</dt><dd>{{ valueText(record.totalAsCount) }}</dd></div>
        <div><dt>事件结束时间（北京时间）</dt><dd>{{ timeText(record.bundle.event.endTimeUtc) }}</dd></div>
      </dl>
      <p class="record-boundary">这是检测记录中的数量和名单，不代表全国实际断网或用户影响。下方时序按国家和所选时间单独统计。</p>
      <p v-if="record.delivery" class="record-coverage">
        本批数据覆盖：{{ timeText(record.delivery.start) }} — {{ timeText(record.delivery.endExclusive) }}（北京时间，不含终点）
        · {{ record.delivery.coverage === 'partial_window' ? '部分时段已处理' : record.delivery.coverage === 'complete_window' ? '所选时段已处理' : '覆盖状态未知' }}
      </p>
      <p v-else class="record-coverage">来源为已绑定的事件事实记录；接口未提供批次版本和实际覆盖时间。</p>
      <div class="record-as-header">
        <h3>检测记录中的中断 AS 名单</h3>
        <label v-if="record.outageAsns !== null">查找 ASN <input v-model="query" placeholder="例如 AS12345" type="search" /></label>
      </div>
      <p v-if="record.outageAsns === null">记录未提供有效的 AS 名单，不能解释为零。</p>
      <template v-else>
        <p>名单共 {{ record.outageAsns.length }} 个 AS<span v-if="query">，匹配 {{ filteredAsns.length }} 个</span>。</p>
        <p v-if="record.outageAsCount !== null && record.outageAsCount !== record.outageAsns.length" role="status">记录数量与名单长度不一致，暂不能确认名单完整性。</p>
        <ul class="record-as-list"><li v-for="asn in visibleAsns" :key="asn">AS{{ asn }}</li></ul>
        <p v-if="!filteredAsns.length">{{ query ? '没有匹配的 ASN。' : '此记录提供了空名单。' }}</p>
        <nav v-if="pageCount > 1" class="record-pagination" aria-label="中断 AS 名单分页">
          <button type="button" :disabled="page === 1" @click="page--">上一页</button>
          <span>第 {{ page }} / {{ pageCount }} 页</span>
          <button type="button" :disabled="page === pageCount" @click="page++">下一页</button>
        </nav>
      </template>
      <details class="record-source">
        <summary>查看基础记录来源</summary>
        <p>事件引用：{{ record.bundle.sourceRecord.detailReference }}</p>
        <p>观察点代码：{{ record.bundle.sourceRecord.sourceCode }}</p>
        <p v-if="record.delivery">批次版本：{{ record.delivery.version }}</p>
        <p>数据覆盖终点不等同于事件结束或恢复时间。</p>
      </details>
    </template>
  </section>
</template>

<style scoped>
.event-record { padding: 22px; border: 1px solid var(--line); background: var(--surface, #fff); border-radius: 5px; }
.event-record header, .record-as-header { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; }
.event-record h2 { margin: 0; font-size: 18px; }
.event-record header span, .record-facts dt, .record-boundary, .record-coverage { color: var(--muted, #63707c); font-size: 13px; }
.record-facts { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 18px; margin: 22px 0 14px; }
.record-facts dd { margin: 7px 0 0; font-size: 18px; font-weight: 600; overflow-wrap: anywhere; }
.record-boundary, .record-coverage { line-height: 1.7; }
.record-as-header { margin-top: 22px; }
.record-as-header h3 { margin: 0; font-size: 15px; }
.record-as-header label { display: flex; gap: 8px; align-items: center; font-size: 13px; }
.record-as-header input { width: 180px; max-width: 100%; padding: 7px; border: 1px solid var(--line); border-radius: 3px; }
.record-as-list { display: grid; grid-template-columns: repeat(auto-fill, minmax(115px, 1fr)); gap: 8px; padding: 0; list-style: none; }
.record-as-list li { padding: 9px 12px; border: 1px solid var(--line); font-variant-numeric: tabular-nums; }
.record-pagination { display: flex; justify-content: flex-end; align-items: center; gap: 12px; font-size: 13px; }
.record-pagination button { padding: 6px 12px; }
.record-source { margin-top: 18px; font-size: 12px; overflow-wrap: anywhere; }
.record-source summary { cursor: pointer; }
@media (max-width: 700px) { .record-facts { grid-template-columns: repeat(2, minmax(0, 1fr)); } .event-record { padding: 16px; } }
</style>

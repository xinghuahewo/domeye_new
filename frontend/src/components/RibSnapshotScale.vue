<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, onServerPrefetch, ref, watch } from 'vue'
import { discoverRibSnapshots, getRibSnapshot, ribDayMessage, ribBatchNote, ribErrorMessage, type RibSnapshot } from '@/api/ribSnapshots'
import { toBusinessTime } from '@/utils/businessTime'
import profile from '../../../config/data-profile.json'

const props = defineProps<{ date: string; family: string; refreshKey?: number; version?: string }>()
const emit = defineEmits<{ selected: [version: string]; reselect: [] }>()
const snapshot = ref<RibSnapshot | null>(null)
const state = ref<'loading' | 'available' | 'not_configured' | 'unavailable'>('loading')
const message = ref('正在读取共享快照')
const batchNote = ref('')
let batchNoteVersion = ''
let pinnedDate = ''
let pinnedVersion = ''
let requestNumber = 0
let controller: AbortController | undefined
const count = (value: number | undefined) => value === undefined ? '—' : value.toLocaleString('zh-CN')
const stamp = computed(() => snapshot.value ? toBusinessTime(new Date(snapshot.value.observed_at)) : '')
const asnLink = computed(() => snapshot.value ? `/ases?${new URLSearchParams({
  snapshot_version: snapshot.value.version, snapshot_date: snapshot.value.date, snapshot_family: snapshot.value.family,
})}` : '')

async function load() {
  const number = ++requestNumber
  controller?.abort()
  const request = new AbortController()
  controller = request
  const date = props.date
  if (pinnedDate !== date) { pinnedVersion = ''; pinnedDate = date; batchNote.value = '' }
  if (props.version !== undefined) pinnedVersion = props.version
  snapshot.value = null
  state.value = 'loading'
  message.value = '正在读取共享快照'
  try {
    if (props.version !== undefined && !/^rib_snapshot_v1_[0-9a-f]{64}$/.test(props.version)) throw new Error('快照版本无效')
    if (!pinnedVersion) {
      const discovery = await discoverRibSnapshots(date, request.signal)
      if (number !== requestNumber) return
      if (discovery.state === 'not_configured') { state.value = 'not_configured'; return }
      batchNote.value = ribBatchNote(discovery.days?.[0])
      const selected = discovery.snapshots[0]
      if (!selected) throw new Error(ribDayMessage(discovery.days?.[0], '此日无已登记的 RIB 快照'))
      pinnedVersion = selected.version
      batchNoteVersion = selected.version
    }
    if (pinnedVersion !== batchNoteVersion) batchNote.value = ''
    if (!['all', 'ipv4', 'ipv6'].includes(props.family)) throw new Error('未知地址族不提供快照规模')
    const result = await getRibSnapshot(pinnedVersion, props.family as RibSnapshot['family'], request.signal)
    if (number !== requestNumber) return
    if (result.date !== date) throw new Error('快照时点与所选日期不一致')
    snapshot.value = result
    state.value = 'available'
    if (props.version !== result.version) emit('selected', result.version)
  } catch (cause) {
    if (number !== requestNumber || request.signal.aborted) return
    state.value = 'unavailable'
    message.value = ribErrorMessage(cause, '共享快照校验或读取失败')
  }
}
function reselect() {
  if (props.version !== undefined) emit('reselect')
  else { pinnedVersion = ''; void load() }
}
watch(() => [props.date, props.family, props.refreshKey], load)
watch(() => props.version, () => { pinnedVersion = ''; void load() })
onMounted(load)
onServerPrefetch(load)
onBeforeUnmount(() => { requestNumber++; controller?.abort() })
</script>

<template>
  <slot v-if="state === 'not_configured'" />
  <template v-else>
    <div class="c-metric rib-shared-metric" data-testid="shared-rib-scale">
      <span class="c-metric-label">可见前缀数</span>
      <strong data-testid="core-prefix-count" :class="{ 'c-unknown-number': !snapshot }">{{ count(snapshot?.metrics.visible_prefixes) }} <small v-if="snapshot">条</small></strong>
      <span class="c-metric-note">{{ snapshot ? `单 RIB · ${stamp} · ${profile.timezone}` : message }}</span>
      <p v-if="snapshot && batchNote" class="c-note">{{ batchNote }}</p>
      <p v-if="snapshot?.metrics.visible_prefixes === 0" class="c-note">已完整核验此快照，所选地址族投影为空。</p>
      <details v-if="snapshot">
        <summary>快照来源与统计依据</summary>
        <p>RRC25 · {{ snapshot.family }} · 观察覆盖：Unknown · Session：Unknown</p>
        <p>至少一个明确起源的前缀：{{ count(snapshot.metrics.attributed_prefixes) }}</p>
        <p>完全无明确起源的前缀：{{ count(snapshot.metrics.unattributed_prefixes) }}</p>
        <p>原始观察：{{ count(snapshot.metrics.rib_entries) }} · 不可归属观察：{{ count(snapshot.metrics.unattributed_entries) }}</p>
        <p>结果版本：{{ snapshot.version }}</p><p>来源 SHA256：{{ snapshot.source.sha256 }}</p>
        <p v-for="limitation in snapshot.limitations" :key="limitation">{{ limitation }}</p>
      </details>
      <p v-else-if="state === 'unavailable'" class="c-note">共享快照不可用；数量为 Unknown。</p>
    </div>
    <div class="c-metric rib-shared-metric">
      <span class="c-metric-label">可见起源 AS 数</span>
      <strong data-testid="core-origin-count" :class="{ 'c-unknown-number': !snapshot }">{{ count(snapshot?.metrics.visible_origin_ases) }} <small v-if="snapshot">个</small></strong>
      <span class="c-metric-note">{{ snapshot ? '同一 RIB 快照 · 明确归属 ASN 去重' : message }}</span>
      <a v-if="snapshot" :href="asnLink" class="rib-asn-link">查看同一快照的 ASN →</a>
      <button v-if="snapshot || state === 'unavailable'" type="button" @click="reselect">重新选择此日已发布快照</button>
      <details v-if="snapshot"><summary>起源归属口径</summary>
        <p>从末端跳过私用 AS，取可明确归属的单 ASN；集合／联盟段不拆分猜测。原始路径和末端保留。</p>
        <p>排除65535；0、23456、4294967295不作明确归属。{{ snapshot.origin_rule }}</p>
        <p>双栈取 ASN 并集；多个起源可关联同一前缀，其前缀数之和不等于总体并集。</p>
      </details>
    </div>
  </template>
</template>

<style scoped>
.rib-shared-metric { cursor: default; }
.rib-shared-metric details { margin-top: 10px; font-size: 12px; color: #52657d; line-height: 1.7; overflow-wrap: anywhere; }
.rib-shared-metric summary { cursor: pointer; }
.rib-shared-metric p { margin: 5px 0; }
.rib-asn-link { display: inline-block; margin-top: 10px; font-size: 12px; color: #145d82; }
</style>

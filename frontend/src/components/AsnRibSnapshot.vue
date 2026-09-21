<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, onServerPrefetch, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { discoverRibSnapshots, getRibSnapshot, getRibSnapshotAsn, ribDayMessage, ribBatchNote, ribErrorMessage, RibSnapshotNotConfigured, type RibSnapshot, type RibSnapshotAsn } from '@/api/ribSnapshots'
import { toBusinessTime } from '@/utils/businessTime'
import profile from '../../../config/data-profile.json'

const props = defineProps<{ asn: string }>()
const route = useRoute()
const router = useRouter()
const snapshot = ref<RibSnapshot | RibSnapshotAsn | null>(null)
const asnSnapshot = computed(() => snapshot.value && 'asn' in snapshot.value ? snapshot.value : null)
const dateInput = ref('')
const familyInput = computed(() => typeof route.query.snapshot_family === 'string' ? route.query.snapshot_family : 'all')
const state = ref('loading')
const message = ref('正在读取 RIB 快照')
const batchNote = ref('')
let batchNoteVersion = ''
let requestNumber = 0
let controller: AbortController | undefined
const stamp = computed(() => snapshot.value ? toBusinessTime(new Date(snapshot.value.observed_at)) : '')
async function load() {
  const number = ++requestNumber
  controller?.abort()
  const request = new AbortController()
  controller = request
  snapshot.value = null
  state.value = 'loading'
  message.value = '正在读取 RIB 快照'
  dateInput.value = typeof route.query.snapshot_date === 'string' ? route.query.snapshot_date : ''
  try {
    let version = route.query.snapshot_version
    const family = route.query.snapshot_family ?? 'all'
    if (family !== 'all' && family !== 'ipv4' && family !== 'ipv6') throw new Error('快照地址族无效')
    if (version === undefined) {
      const date = route.query.snapshot_date
      if (date !== undefined && (typeof date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(date))) throw new Error('快照日期无效')
      const discovery = await discoverRibSnapshots(date, request.signal)
      if (number !== requestNumber) return
      if (discovery.state === 'not_configured') { state.value = 'not_configured'; return }
      batchNote.value = ribBatchNote(discovery.days?.[0])
      const selected = discovery.snapshots[0]
      if (!selected) throw new Error(ribDayMessage(discovery.days?.[0], date ? '此日无已发布的 RIB 快照，不跳到其他日期' : '允许范围内暂无已发布的 RIB 快照'))
      version = selected.version
      batchNoteVersion = selected.version
      await router.replace({ query: { ...route.query, snapshot_date: selected.date, snapshot_version: version, snapshot_family: family } })
      if (number !== requestNumber) return
    }
    if (typeof version !== 'string' || !/^rib_snapshot_v1_[0-9a-f]{64}$/.test(version)) throw new Error('快照版本无效')
    if (version !== batchNoteVersion) batchNote.value = ''
    if (props.asn && (!/^[0-9]{1,10}$/.test(props.asn) || Number(props.asn) < 1 || Number(props.asn) > 4294967295)) throw new Error('ASN须为1至4294967295的整数')
    const result = props.asn
      ? await getRibSnapshotAsn(version, props.asn, family, request.signal)
      : await getRibSnapshot(version, family, request.signal)
    if (number !== requestNumber) return
    snapshot.value = result
    dateInput.value = result.date
    state.value = 'available'
  } catch (cause) {
    if (number !== requestNumber || request.signal.aborted) return
    if (cause instanceof RibSnapshotNotConfigured) { state.value = 'not_configured'; return }
    state.value = 'unavailable'
    message.value = ribErrorMessage(cause, '快照读取失败；数量为 Unknown')
  }
}
function selectDate() {
  if (route.query.snapshot_date === dateInput.value && route.query.snapshot_version === undefined) void load()
  else void router.push({ query: { ...route.query, snapshot_date: dateInput.value, snapshot_version: undefined } })
}
function selectLatest() {
  if (route.query.snapshot_date === undefined && route.query.snapshot_version === undefined) void load()
  else void router.push({ query: { ...route.query, snapshot_date: undefined, snapshot_version: undefined } })
}
function selectFamily(event: Event) {
  void router.push({ query: { ...route.query, snapshot_family: (event.target as HTMLSelectElement).value } })
}
watch(() => [props.asn, route.query.snapshot_version, route.query.snapshot_date, route.query.snapshot_family], load)
onMounted(load)
onServerPrefetch(load)
onBeforeUnmount(() => { requestNumber++; controller?.abort() })
</script>

<template>
  <section v-if="state !== 'not_configured'" class="asn-rib" aria-label="RIB 时点快照">
    <header><div><p>RIB 时点快照</p><h2>明确起源前缀</h2></div><span>与五分钟资源／报文特征独立</span></header>
    <form class="rib-controls" @submit.prevent="selectDate">
      <label>快照日期<input v-model="dateInput" type="date" required :min="profile.window_start.slice(0, 10)" :max="profile.snapshot_time.slice(0, 10)" /></label>
      <button type="submit">查看所选日期</button>
      <label>快照地址族<select :value="familyInput" @change="selectFamily"><option value="all">双栈</option><option value="ipv4">IPv4</option><option value="ipv6">IPv6</option></select></label>
      <button type="button" @click="selectLatest">使用最新已发布快照</button>
    </form>
    <div v-if="snapshot" class="rib-result">
      <template v-if="asnSnapshot"><strong data-testid="asn-rib-prefix-count">{{ asnSnapshot.prefix_count.toLocaleString('zh-CN') }}</strong><span>条 · AS{{ asnSnapshot.asn }} · {{ snapshot.family }}</span></template>
      <p v-else>选择 ASN 查看同一快照的明确起源前缀</p>
      <p>{{ stamp }} · {{ profile.timezone }} · RRC25</p>
      <p v-if="batchNote">{{ batchNote }}</p>
      <p v-if="asnSnapshot?.prefix_count === 0">已完整核验此快照，该 ASN 在所选地址族内无明确起源前缀。</p>
      <p>同前缀跨 Peer 去重；多起源各自关联，各 ASN 数量之和不能代替总体前缀并集。</p>
      <details><summary>快照版本与来源依据</summary>
        <p>结果版本：{{ snapshot.version }}</p><p>来源 SHA256：{{ snapshot.source.sha256 }}</p>
        <p>从末端跳过私用 AS，取可明确归属的单 ASN。AS_SET／联盟段不拆分猜测；仅路径经过不计为起源。</p>
        <p>规则：{{ snapshot.origin_rule }}</p>
        <p v-for="limitation in snapshot.limitations" :key="limitation">{{ limitation }}</p>
      </details>
      <details v-if="asnSnapshot?.items.length"><summary>来源观察样本（{{ asnSnapshot.items.length }} 条，最多 {{ asnSnapshot.sample_limit }} 条）</summary>
        <p>按地址族、前缀文本、条目序号稳定取样；{{ asnSnapshot.sample_truncated ? '仍有更多观察，未在此加载。' : '已展示该查询的全部观察。' }}样本条数不是去重前缀总数。</p>
        <article v-for="item in asnSnapshot.items" :key="`${item.physical_record}-${item.entry_index}`" class="rib-evidence">
          <b>{{ item.prefix }}</b><p>Peer {{ item.peer.ip }} · BGP ID {{ item.peer.bgp_id }} · AS{{ item.peer.asn }}</p>
          <p>源记录 {{ item.physical_record }} · 条目 {{ item.entry_index }} · 解压偏移 {{ item.decoded_offset }}（均从零计）</p>
          <p>原末端 {{ item.raw_origin_asn ?? 'Unknown' }} → 明确起源 {{ item.attributed_origin_asn }} · {{ item.reason }}</p>
          <p>Originated Time：{{ item.originated_time_epoch }}（Unix秒）· AS_PATH原文（hex）：{{ item.as_path_hex ?? '无' }}</p>
          <p v-if="item.as4_path_hex">AS4_PATH原文（hex）：{{ item.as4_path_hex }}</p>
        </article>
      </details>
    </div>
    <p v-else role="status">{{ message }}<button v-if="state === 'unavailable'" type="button" @click="load">重试快照</button></p>
  </section>
</template>

<style scoped>
.asn-rib { padding: 20px; border: 1px solid #ccdce6; border-left: 4px solid #147d92; border-radius: 8px; background: #f6fafc; color: #233e50; }
.asn-rib header { display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap; }
.asn-rib h2 { font-size: 18px; margin: 4px 0 14px; }
.asn-rib p, .asn-rib span, .asn-rib summary { font-size: 12px; line-height: 1.7; }
.rib-result > strong { font-size: 36px; margin-right: 10px; }
.asn-rib details { margin-top: 12px; overflow-wrap: anywhere; }
.asn-rib summary { cursor: pointer; font-weight: 650; }
.rib-evidence { border-top: 1px solid #cbd9e1; padding: 10px 0; }
.asn-rib button { margin-left: 12px; cursor: pointer; }
.rib-controls { display: flex; align-items: end; flex-wrap: wrap; gap: 12px; margin: 0 0 18px; }
.rib-controls label { display: grid; gap: 5px; font-size: 12px; }
.rib-controls input, .rib-controls select, .rib-controls button { min-height: 36px; border: 1px solid #a9bdcb; border-radius: 4px; padding: 6px 10px; color: #233e50; background: #fff; margin: 0; }
</style>

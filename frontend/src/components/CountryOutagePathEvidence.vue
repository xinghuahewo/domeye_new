<script setup lang="ts">
import { computed } from 'vue'
import { businessTimezone, toBusinessTime } from '@/utils/businessTime'
import type { CountryOutageGeneralPathDownstream, CountryOutageGeneralMetadata } from '@/types/api'

const props = defineProps<{
  relation: CountryOutageGeneralPathDownstream
  metadata: CountryOutageGeneralMetadata
}>()
const differentPrefixes = computed(() => new Set(props.relation.path_samples.map(sample =>
  `${sample.address_family}:${sample.prefix}`,
)).size > 1)
const missingDirections = computed(() => props.relation.path_samples.some(sample =>
  !sample.independent_peer_asns?.length,
))
const differentDirections = computed(() => !missingDirections.value && new Set(props.relation.path_samples.map(sample =>
  [...new Set(sample.independent_peer_asns)].sort((a, b) => a - b).join(','),
)).size > 1)

function formatTime(value: string | null): string {
  if (!value || !/(Z|[+-]\d{2}:\d{2})$/.test(value)) return 'Unknown'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'Unknown' : toBusinessTime(date)
}
</script>

<template>
  <details class="path-evidence">
    <summary>查看关联路径 · {{ relation.path_samples.length }} 条样本</summary>
    <div class="evidence-body">
      <div class="comparison-status">
        <h4>无法确认前后路径变化</h4>
        <p v-if="relation.path_samples.length === 1">当前只返回 1 条样本，未提供可配对的前后路径。</p>
        <p v-if="differentPrefixes">样本的前缀或地址族不同，需要分别匹配，不能直接配成前后路径。</p>
        <p v-if="missingDirections">部分样本缺少观察方向，无法核对方向是否一致。</p>
        <p v-if="differentDirections">样本的观察方向不一致，不能直接作为同方向的前后对照。</p>
        <p>逐条观测时点：Unknown。样本未提供路径与前后时点的对应，列表顺序不代表时间顺序。</p>
      </div>
      <p>有序关联不保证直接邻接，也不表示依赖或原因。</p>
      <div class="relation-times">
        <p>关系首次同期时点：{{ formatTime(relation.first_concurrent_state_point_utc) }}</p>
        <p>关系末次同期时点：{{ formatTime(relation.last_concurrent_state_point_utc) }}</p>
        <p>关系汇总的首末同期时点不代表每条样本的持续区间，也不保证中间连续。时间均为 {{ businessTimezone }}。</p>
      </div>
      <ol class="path-samples">
        <li v-for="(sample, index) in relation.path_samples" :key="`${sample.prefix}-${sample.as_path_id}-${index}`">
          <header><b>{{ sample.prefix }}</b><span>{{ sample.address_family === 'ipv4' ? 'IPv4' : sample.address_family === 'ipv6' ? 'IPv6' : '地址族 Unknown' }}</span></header>
          <code>{{ sample.as_path_canonical }}</code>
          <p>独立观察方向：{{ sample.independent_peer_asns?.length ? sample.independent_peer_asns.map(asn => `AS${asn}`).join('、') : 'Unknown' }} · {{ sample.route_observation_count ?? 'Unknown' }} 条观测</p>
        </li>
      </ol>
      <details class="source-context">
        <summary>核对来源与比较条件</summary>
        <p>collector：{{ metadata.collector_id }} · 修订：{{ metadata.revision }}</p>
        <p>观测窗口：{{ formatTime(metadata.window_start_utc) }} → {{ formatTime(metadata.window_end_utc) }}</p>
        <p class="identity">发布身份：{{ metadata.publication_id }}</p>
        <p>确认前后变化需要同一前缀、地址族、collector、观察方向，以及有明确时间对应的两侧路径和可核对的发布身份。</p>
        <p>当前接口仅提供窗口内有限样本，完整审计路径证据未在此页接入。</p>
      </details>
    </div>
  </details>
</template>

<style scoped>
.path-evidence { min-width: 0; color: #52616b; font-size: 12px; line-height: 1.7; }
summary { cursor: pointer; color: #176d8f; font-weight: 700; }
.evidence-body { margin-top: 12px; padding: 14px; border: 1px solid #dbe2e6; background: #f8fafb; }
p { margin: 6px 0; }
.comparison-status { padding: 10px 14px; border-left: 3px solid #df6b2d; background: #fff5e9; }
h4 { margin: 0; color: #7a4b23; font-size: 14px; }
.relation-times { margin-top: 12px; padding: 10px 14px; border: 1px solid #dbe2e6; background: #fff; }
.path-samples { display: grid; gap: 10px; margin: 14px 0; padding: 0; list-style: none; }
.path-samples li { min-width: 0; padding: 12px; background: #fff; border: 1px solid #dbe2e6; }
.path-samples header { display: flex; flex-wrap: wrap; gap: 12px; align-items: baseline; color: #233b49; }
.path-samples header span { color: #6c7d87; font-size: 11px; }
code { display: block; margin: 8px 0; overflow-wrap: anywhere; color: #254e62; font: 12px/1.7 var(--mono); }
.identity { overflow-wrap: anywhere; font-family: var(--mono); font-size: 11px; }
.source-context { padding-top: 10px; border-top: 1px solid #dbe2e6; }
</style>

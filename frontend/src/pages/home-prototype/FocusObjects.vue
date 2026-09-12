<script setup lang="ts">
import { computed, ref } from 'vue'
import { objects, spark, objectDetail, type Scenario, type Detail } from './fixture'
defineProps<{ scenario: Scenario; compact?: boolean; selected?: number }>()
const emit = defineEmits<{ detail: [value: Detail]; select: [value: number] }>()
const direction = ref('all')
const rows = computed(() => objects.map((o, index) => ({ ...o, index })).filter(o => direction.value === 'all' || (direction.value === 'down' ? o.after < o.before : o.after > o.before)))
</script>
<template>
  <section class="focus-panel" aria-label="重点关注对象">
    <div class="section-head"><div><p class="overline">FOCUS / NETWORKS</p><h2>值得关注的网络</h2></div><label class="focus-filter"><span class="sr-only">变化方向</span><select v-model="direction" aria-label="变化方向"><option value="all">全部变化</option><option value="down">规模下降</option><option value="up">规模上升</option></select></label></div>
    <p class="section-note">IPv4 可见前缀变化 · 同范围起终点比较 · 示例对象</p>
    <div v-if="scenario !== 'ready'" class="empty-note">缺少可比状态，暂不生成变化排行。</div>
    <div v-else class="object-list" :class="{ compact }">
      <div class="object-header" v-if="!compact"><span>网络对象</span><span>前缀数 · 起点 → 终点</span><span>变化趋势</span><span>变化幅度</span></div>
      <button v-for="o in rows" :key="o.asn" class="object-row" :class="{ selected: selected === o.index }" @click="compact ? emit('select', o.index) : emit('detail', objectDetail(o.index))">
        <span class="object-name"><span class="network-symbol">⌘</span><span><strong>{{ o.asn }}</strong><small>{{ o.name }}</small></span></span>
        <span class="object-count">{{ o.before }} <small>→</small> {{ o.after }}</span>
        <svg viewBox="0 0 84 32" class="sparkline" aria-hidden="true"><polyline :points="spark(o.trend)" fill="none" :stroke="o.after > o.before ? '#376c5b' : '#b0764c'" stroke-width="1.6"/></svg>
        <span class="object-delta" :class="o.after > o.before ? 'positive' : 'negative'">{{ o.delta }} <span v-if="!compact">↗</span></span>
      </button>
    </div>
  </section>
</template>

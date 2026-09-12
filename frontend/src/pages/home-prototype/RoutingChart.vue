<script setup lang="ts">
import { computed, ref } from 'vue'
import { prefixIndex, activities, dateLabel, type Family, type Scenario } from './fixture'
const props = defineProps<{ family: Family; scenario: Scenario; objectIndex?: number }>()
const selected = ref(15)
const points = computed(() => {
  if (props.objectIndex === undefined) return prefixIndex
  const declines = [15.24, 7.36, 2.75, -8.82]
  const decline = declines[props.objectIndex]!
  return prefixIndex.map((_, i) => 100 - decline * Math.max(0, Math.min(1, (i - 13) / 5)))
})
const min = computed(() => props.objectIndex === undefined ? 99.6 : 80)
const max = computed(() => props.objectIndex === undefined ? 100.1 : 112)
const x = (i: number) => i === 23 ? 748 : 52 + i * 696 / 24
const y = (v: number) => 168 - (v - min.value) / (max.value - min.value) * 132
const segments = computed(() => {
  const ranges = props.scenario === 'gap' ? [[0, 9], [13, 24]] : [[0, 24]]
  return ranges.map(([from, to]) => points.value.slice(from, to).map((v, j) => `${x(from! + j)},${y(v)}`).join(' '))
})
const isMissing = computed(() => props.scenario === 'gap' && selected.value >= 9 && selected.value < 13)
const selectedTime = computed(() => selected.value === 23 ? '23:59:59' : `${String(selected.value).padStart(2, '0')}:00`)
</script>

<template>
  <section class="chart-panel" aria-label="路由变化趋势">
    <div class="section-head">
      <div><p class="overline">ROUTING / CHANGE</p><h2>{{ objectIndex === undefined ? '规模如何变化' : '所选对象的规模变化' }}</h2></div>
      <span class="chart-legend"><i></i>{{ family }} 可见前缀指数</span>
    </div>
    <div class="chart-empty" v-if="scenario === 'unavailable'"><span>∅</span><h3>暂时无法确认路由规模</h3><p>没有可用的状态输入，不代表前缀数为零。</p></div>
    <template v-else>
      <div class="chart-reading"><span>{{ dateLabel }} {{ selectedTime }}</span><strong>{{ isMissing ? '未知' : points[selected]!.toFixed(2) }}<small v-if="!isMissing"> 起点 = 100</small></strong><span class="reading-tag">{{ isMissing ? '观测缺口 · 不插值' : '模拟快照' }}</span></div>
      <svg class="routing-svg" viewBox="0 0 784 282" role="img" :aria-label="`${family} 模拟前缀规模和路由报文活动趋势；窗口起点为100`">
        <defs><linearGradient id="chart-wash" x1="0" y1="0" x2="0" y2="1"><stop stop-color="currentColor" stop-opacity=".12"/><stop offset="1" stop-color="currentColor" stop-opacity=".01"/></linearGradient><pattern id="gap-hatch" width="6" height="6" patternUnits="userSpaceOnUse"><path d="M0 6 6 0" stroke="#a7aaa3" stroke-width=".6"/></pattern></defs>
        <g v-for="tick in [min, (min + max) / 2, max]" :key="tick"><line x1="52" x2="748" :y1="y(tick)" :y2="y(tick)" class="gridline"/><text x="40" :y="y(tick) + 4" text-anchor="end">{{ tick.toFixed(1) }}</text></g>
        <line x1="52" x2="748" :y1="y(100)" :y2="y(100)" class="baseline"/>
        <polygon v-if="scenario === 'ready'" :points="`52,175 ${segments[0]} 748,175`" fill="url(#chart-wash)"/>
        <g v-if="scenario === 'gap'"><rect :x="x(9)" y="20" :width="x(13) - x(9)" height="216" fill="url(#gap-hatch)" opacity=".45"/><text :x="x(11)" y="122" text-anchor="middle">观测缺口</text></g>
        <polyline v-for="(segment, i) in segments" :key="i" :points="segment" fill="none" stroke="currentColor" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round"/>
        <line :x1="x(selected)" :x2="x(selected)" y1="22" y2="240" class="cursor-line"/>
        <circle v-if="!isMissing" :cx="x(selected)" :cy="y(points[selected]!)" r="5" fill="currentColor" stroke="var(--surface)" stroke-width="2"/>
        <text x="52" y="200" class="activity-caption">路由活动 · 模拟报文量（千条）</text>
        <g v-for="(count, i) in activities" :key="i">
          <template v-if="!(scenario === 'gap' && i >= 9 && i < 13)"><rect :x="x(i) - 5" :y="242 - count * .34" width="5" :height="count * .34" rx="1" fill="currentColor" opacity=".45"/><rect :x="x(i) + 1" :y="242 - count * .18" width="5" :height="count * .18" rx="1" fill="#bc8251" opacity=".65"/></template>
        </g>
        <text x="40" y="243" text-anchor="end">0</text>
        <text v-for="i in [0, 6, 12, 18, 23]" :key="i" :x="x(i)" y="266" :text-anchor="i === 23 ? 'end' : 'middle'">{{ i === 23 ? '23:59' : `${String(i).padStart(2, '0')}:00` }}</text>
      </svg>
      <div class="chart-controls"><label>查看时点 <input aria-label="查看趋势时点" type="range" min="0" max="23" v-model.number="selected" /></label><span><i class="legend-square"></i>宣告 <i class="legend-square withdrawal"></i>撤回</span></div>
      <p class="chart-footnote">{{ scenario === 'gap' ? '缺口不连线、不补零；这里只演示未知状态的呈现。' : '固定模拟观测范围 · 报文活动不等于状态变化或实际网络影响。' }}</p>
    </template>
  </section>
</template>

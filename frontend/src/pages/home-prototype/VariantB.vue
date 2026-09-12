<script setup lang="ts">
import ScaleMetrics from './ScaleMetrics.vue'
import RoutingChart from './RoutingChart.vue'
import RiskEvents from './RiskEvents.vue'
import FocusObjects from './FocusObjects.vue'
import { dateLabel, type Family, type Scenario, type Detail } from './fixture'
defineProps<{ family: Family; scenario: Scenario }>()
const emit = defineEmits<{ detail: [value: Detail] }>()
</script>
<template>
  <main class="variant-b prototype-main">
    <div class="brief-masthead"><span>路 由 观 察 简 报</span><span>{{ dateLabel }} / 历史窗口</span><span>示意刊 · 001</span></div>
    <div class="brief-lead"><div class="brief-story"><p class="overline">01 / THE BIG PICTURE</p><h1>{{ scenario === 'ready' ? '规模的细微变化，\n值得看清的局部信号。' : '观测不完整，\n结论需要留白。' }}</h1><div class="brief-number">{{ scenario === 'ready' ? '−0.18' : '—' }}<span v-if="scenario === 'ready'">%</span></div><p>{{ scenario === 'ready' ? `${family} 可见前缀较窗口起点的模拟变化。整体幅度较小，并不意味着每个网络都平稳。` : '尚不能可靠比较总体规模。缺失的数据，不应该被写成“没有变化”。' }}</p><a href="#routing" class="text-link">沿时间查看变化 <span>↓</span></a></div><aside class="brief-scale"><p class="overline">AT A GLANCE / 窗口末快照</p><ScaleMetrics :family="family" :scenario="scenario" @detail="emit('detail', $event)"/><p class="brief-side-note">规模取可靠可见状态的并集。<br>这是观测范围内的事实，不是互联网全貌。</p></aside></div>
    <a href="#events" class="brief-attention"><span class="risk-label">优先查看 · 模拟</span><strong>{{ scenario === 'unavailable' ? '事件数据待确认' : '3 条高风险事件' }}</strong><span>{{ scenario === 'unavailable' ? '未知不等于没有异常' : '可见性下降 / 起源变化 / 路径变化' }}</span><b>查看证据 ↓</b></a>
    <div id="routing" class="brief-chart"><span class="chapter-number">02</span><RoutingChart :family="family" :scenario="scenario"/></div>
    <RiskEvents id="events" :scenario="scenario" @detail="emit('detail', $event)"/>
    <FocusObjects id="networks" :scenario="scenario" @detail="emit('detail', $event)"/>
  </main>
</template>

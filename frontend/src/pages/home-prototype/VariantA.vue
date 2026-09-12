<script setup lang="ts">
import ScaleMetrics from './ScaleMetrics.vue'
import RoutingChart from './RoutingChart.vue'
import RiskEvents from './RiskEvents.vue'
import FocusObjects from './FocusObjects.vue'
import { dateLabel, timeLabel, type Family, type Scenario, type Detail } from './fixture'
defineProps<{ family: Family; scenario: Scenario }>()
const emit = defineEmits<{ detail: [value: Detail] }>()
</script>
<template>
  <main class="variant-a prototype-main">
    <div class="page-intro"><div><p class="overline">THE ROUTING OBSERVATORY</p><h1>看见路由的变化<span class="title-period">.</span></h1><p>从整体规模，到异常信号，再到值得深入的网络。</p></div><div class="snapshot-stamp"><span>历史快照 · {{ family }}</span><strong>{{ dateLabel }}</strong><small>{{ timeLabel }} · UTC+08</small></div></div>
    <ScaleMetrics :family="family" :scenario="scenario" @detail="emit('detail', $event)"/>
    <div class="overview-grid" id="routing"><RoutingChart :family="family" :scenario="scenario"/><RiskEvents id="events" :scenario="scenario" @detail="emit('detail', $event)"/></div>
    <FocusObjects id="networks" :scenario="scenario" @detail="emit('detail', $event)"/>
  </main>
</template>

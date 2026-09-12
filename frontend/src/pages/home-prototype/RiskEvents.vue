<script setup lang="ts">
import { events, eventDetail, type Scenario, type Detail } from './fixture'
defineProps<{ scenario: Scenario }>()
const emit = defineEmits<{ detail: [value: Detail] }>()
</script>
<template>
  <section class="risk-panel" aria-label="高风险事件">
    <div class="section-head"><div><p class="overline">PRIORITY / EVENTS</p><h2>高风险事件 <span class="count-badge">{{ scenario === 'unavailable' ? '—' : '03' }}</span></h2></div><span class="risk-dot" aria-hidden="true"></span></div>
    <p class="section-note">风险等级与证据状态分开呈现 · 模拟判定</p>
    <div v-if="scenario === 'unavailable'" class="empty-note">事件数据不可用，不能判定为“无异常”。</div>
    <div v-else class="event-list">
      <button v-for="(event, i) in events" :key="event.id" class="event-row" @click="emit('detail', eventDetail(i))">
        <span class="event-top"><time>{{ event.time }}</time><span class="risk-label">高风险</span><span class="event-arrow">↗</span></span>
        <strong>{{ event.name }}</strong><span class="event-object">{{ event.asn }} <span>· {{ event.prefixes }} 个前缀</span></span>
        <span class="evidence-badge" :class="event.color"><i></i>{{ event.support }}</span>
      </button>
    </div>
    <p class="section-end">点击事件查看证据摘要 <span>↗</span></p>
  </section>
</template>

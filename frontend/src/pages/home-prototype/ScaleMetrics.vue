<script setup lang="ts">
import { scale, timeLabel, type Family, type Scenario, type Detail } from './fixture'
defineProps<{ family: Family; scenario: Scenario }>()
const emit = defineEmits<{ detail: [value: Detail] }>()
</script>

<template>
  <div class="scale-metrics" aria-label="总体路由规模">
    <button v-for="(metric, index) in scale[family]" :key="metric.name" class="scale-metric" @click="emit('detail', {
      title: metric.name, subtitle: `${family} / 产品口径说明，尚未验证底层实现`,
      rows: [['展示时点', timeLabel], ['观察范围', '8 个 Collector · 32 个 Peer（模拟）'], ['版本', 'home-demo-v1'], ['计入标准', '至少一个已纳入视角提供可靠可见证据'], ['多视角支持', '另行表达，不重复累加总体规模']],
      note: metric.detail,
    })">
      <span class="metric-name"><i class="metric-icon">{{ ['◈','⌘','▧'][index] }}</i>{{ metric.name }}<span class="metric-info">↗</span></span>
      <span class="metric-number">{{ scenario === 'ready' ? metric.value : '—' }} <small>{{ scenario === 'ready' ? metric.unit : '未确认' }}</small></span>
      <span class="metric-caption" v-if="scenario === 'ready'"><b :class="metric.delta.startsWith('+') ? 'positive' : 'negative'">{{ metric.delta }}</b><span>{{ metric.value === '—' ? '不作数值替代' : '较窗口起点 · 示例' }}</span></span>
      <span class="metric-caption" v-else>观测不完整，暂停总体比较</span>
    </button>
  </div>
</template>

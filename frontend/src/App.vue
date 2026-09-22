<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { RouterLink, RouterView, useRoute } from 'vue-router'
import SiteHeader from '@/components/SiteHeader.vue'
import { toBusinessTime } from '@/utils/businessTime'
import type { HealthPayload } from '@/types/api'
import { getHealth } from '@/api/health'
import { resolveDataWindow } from '@/utils/time'

const route = useRoute()
const healthy = ref(false)
const healthChecked = ref(false)
const checkedAt = ref('')
const delivery = ref<HealthPayload['result_delivery']>()
const deliveryQuery = computed(() => delivery.value?.start && delivery.value?.end_exclusive ? { start: toBusinessTime(new Date(delivery.value.start)).replace(' ', 'T'), end: toBusinessTime(new Date(delivery.value.end_exclusive)).replace(' ', 'T') } : {})
const deliveryTime = (value?: string) => value ? new Date(value).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '未知'
const dataWindow = resolveDataWindow(import.meta.env)
const dataWindowLabel = dataWindow
  ? `${dataWindow.start.slice(0, 10)} 至 ${dataWindow.end.slice(0, 10)}`
  : '2026-02-01 至制品快照'
const currentPage = computed(() => String(route.meta.title || '系统状态'))
const currentSection = computed(() => String(route.meta.section || 'Domeye'))
const healthLabel = computed(() => !healthChecked.value ? 'API 检查中' : healthy.value ? 'API 正常' : 'API 异常')
let timer: number | undefined

async function checkHealth() {
  try {
    const payload = await getHealth()
    healthy.value = payload.status === 'ok'
    delivery.value = payload.result_delivery
    checkedAt.value = new Date(payload.time).toLocaleTimeString('zh-CN', { hour12: false })
  } catch {
    healthy.value = false
  } finally {
    healthChecked.value = true
  }
}
onMounted(() => {
  void checkHealth()
  timer = window.setInterval(checkHealth, 30_000)
})
onBeforeUnmount(() => { if (timer !== undefined) window.clearInterval(timer) })
</script>

<template>
  <div class="app-shell">
    <SiteHeader />
    <aside v-if="delivery" class="delivery-notice" role="status" aria-label="本批结果范围">
      <template v-if="delivery.state === 'available'">
        <strong>本批已接入 {{ delivery.files }} 份结果</strong>
        <span>北京时间 {{ deliveryTime(delivery.start) }} 至 {{ deliveryTime(delivery.end_exclusive) }}（右端不含）</span>
        <span>仅此时段有数据，窗口外未知；原任务未完成，归档暂停。</span>
        <RouterLink :to="{ name: 'home', query: { date: deliveryQuery.start?.slice(0, 10) } }">本批首页</RouterLink>
        <RouterLink :to="{ name: 'countries', query: deliveryQuery }">本批国家特征</RouterLink>
        <RouterLink :to="{ name: 'ases', query: deliveryQuery }">本批 AS 特征</RouterLink>
      </template>
      <span v-else>本批结果{{ delivery.state === 'empty' ? '尚未交付' : '暂不可读' }}，不能解释为零。</span>
    </aside>
    <RouterView v-if="route.name === 'home'" />
    <div v-else class="workspace">
      <div class="contextbar">
        <div class="breadcrumbs" aria-label="当前位置">
          <span>{{ currentSection }}</span><span aria-hidden="true">/</span><strong>{{ currentPage }}</strong>
        </div>
        <div class="contextbar-meta">
          <span class="data-window">数据范围 {{ dataWindowLabel }}</span>
          <span class="topbar-health" role="status" :title="checkedAt ? `最近成功检查 ${checkedAt}` : undefined"
            :class="{ 'is-offline': healthChecked && !healthy, 'is-checking': !healthChecked }">
            <span class="status-dot" aria-hidden="true"></span>{{ healthLabel }}
          </span>
        </div>
      </div>
      <main class="site-main"><RouterView /></main>
      <footer class="site-footer"><span>Domeye · 路由观测</span><span>Asia/Shanghai · UTC+08</span></footer>
    </div>
  </div>
</template>

<style scoped>
.delivery-notice { display: flex; flex-wrap: wrap; align-items: center; gap: .4rem 1rem; padding: .8rem 2rem; background: #fff8e8; color: #5c461d; border-bottom: 1px solid #ead9b0; font-size: .85rem; }
</style>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { RouterView, useRoute } from 'vue-router'
import SiteHeader from '@/components/SiteHeader.vue'
import { getHealth } from '@/api/health'
import { resolveDataWindow } from '@/utils/time'

const route = useRoute()
const healthy = ref(false)
const healthChecked = ref(false)
const checkedAt = ref('')
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

<script setup lang="ts">
import { RouterLink, useRoute } from 'vue-router'
import { resultDelivery } from '@/api/health'
import { scopeQuery } from '@/utils/queryScope'

const route = useRoute()
const navigation = [
  { to: '/', label: '核心态势', names: ['home'] },
  { to: '/events', label: '异常事件', names: ['events', 'event-detail'] },
  { to: '/countries', label: '国家特征', names: ['countries', 'country-detail'] },
  { to: '/ases', label: 'AS 特征', names: ['ases', 'asn-detail'] },
]
const destination = (path: string) => ({ path, query: scopeQuery(path, route.query, resultDelivery.value) })
const rangeNote = (path: string) => ['/countries', '/ases'].includes(path)
  && typeof route.query.start === 'string' && destination(path).query.start !== route.query.start
  ? '该页支持 24 小时，查看所选区间最后 24 小时' : undefined
</script>

<template>
  <header class="site-header">
    <RouterLink :to="destination('/')" class="site-brand" aria-label="Domeye 核心态势">
      <svg viewBox="0 0 40 40" aria-hidden="true">
        <ellipse cx="20" cy="20" rx="17" ry="10" />
        <ellipse cx="20" cy="20" rx="10" ry="17" transform="rotate(35 20 20)" />
        <circle cx="20" cy="20" r="4" />
      </svg>
      <strong>domeye<small>路由观测</small></strong>
    </RouterLink>
    <nav aria-label="主导航">
      <RouterLink v-for="item in navigation" :key="item.to" :to="destination(item.to)"
        :title="rangeNote(item.to)"
        :class="{ active: item.names.includes(String(route.name)) }"
        :aria-current="item.names.includes(String(route.name)) ? 'page' : undefined">
        {{ item.label }}
        <small v-if="rangeNote(item.to)" class="nav-range-note">末 24h</small>
      </RouterLink>
    </nav>
    <span class="site-header-note">历史窗口 / 只读数据</span>
  </header>
</template>

<style scoped>
.site-header { min-height: 70px; padding: 0 3.4%; display: flex; align-items: center; gap: 48px; background: var(--header); color: #e8edf0; }
.site-brand { display: flex; align-items: center; gap: 10px; flex-shrink: 0; text-decoration: none; }
.site-brand svg { width: 36px; height: 40px; stroke: var(--brand); fill: none; stroke-width: 1.4; }
.site-brand svg circle { fill: var(--brand); stroke: none; }
.site-brand strong { color: white; font-size: 25px; font-weight: 650; letter-spacing: -1px; line-height: 1.2; }
.site-brand small { display: block; font-size: 9px; font-weight: 400; letter-spacing: 2px; color: #a9bac4; margin-top: 4px; }
.site-header nav { display: flex; align-self: stretch; gap: 30px; min-width: 0; }
.site-header nav a { display: flex; align-items: center; min-height: 44px; font-size: 12px; color: #b4c2ca; border-bottom: 3px solid transparent; padding-top: 3px; text-decoration: none; white-space: nowrap; }
.site-header nav a:hover { color: white; }
.site-header nav a.active { color: white; border-color: var(--brand); }
.nav-range-note { margin-left:4px; font-size:9px; color:#b4c2ca; }
.site-header-note { margin-left: auto; font-size: 10px; color: #b4c2ca; white-space: nowrap; }
@media (max-width: 1000px) { .site-header { gap: 30px; }.site-header nav { gap: 22px; }.site-header-note { display: none; } }
@media (max-width: 760px) {
  .site-header { padding: 16px 5% 0; flex-wrap: wrap; gap: 12px; }
  .site-header nav { width: 100%; overflow-x: auto; gap: 24px; }
  .site-header nav a { flex-shrink: 0; padding: 4px 0 1px; }
  .site-header nav a:focus-visible { outline-offset: -4px; box-shadow: none; }
}
</style>

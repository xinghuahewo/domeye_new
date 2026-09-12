<script setup lang="ts">
// 设计问题：三种首页结构能否同时讲清规模、变化与优先事件？仅模拟数据，?variant=A/B/C 切换。
// 用户未授权替换正式首页、接入真实数据、提交分支、发布或部署。
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import VariantA from './VariantA.vue'
import VariantB from './VariantB.vue'
import VariantC from './VariantC.vue'
import PrototypeSwitcher from './PrototypeSwitcher.vue'
import { variants, scenarioLabels, objects, objectDetail, windowLabel, snapshot, timezone, type Variant, type Family, type Scenario, type Detail } from './fixture'
import { overviewVersion } from './overviewFixture'
const getVariant = (): Variant => {
  const key = new URLSearchParams(window.location.search).get('variant')
  return key === 'B' || key === 'C' ? key : 'A'
}
const variant = ref<Variant>(getVariant())
const family = ref<Family>('IPv4')
const scenario = ref<Scenario>('ready')
const query = ref('')
const modal = ref<HTMLDialogElement>()
const detail = ref<Detail>()
const components = { A: VariantA, B: VariantB, C: VariantC }
const currentVariant = computed(() => variants.find(v => v.key === variant.value)!)
const results = computed(() => objects.map((o, index) => ({ ...o, index })).filter(o => `${o.asn} ${o.name}`.toLowerCase().includes(query.value.toLowerCase())))
const trigger = ref<HTMLElement | null>(null)
async function showDetail(value: Detail) {
  trigger.value = document.activeElement instanceof HTMLElement ? document.activeElement : null
  detail.value = value
  query.value = ''
  await nextTick()
  modal.value?.showModal()
}
function closeDetail() { modal.value?.close() }
function restoreFocus() { trigger.value?.focus() }
function changeVariant(key: Variant) {
  variant.value = key
  const url = new URL(window.location.href)
  url.searchParams.set('variant', key)
  url.hash = ''
  history.pushState({}, '', url)
  window.scrollTo({ top: 0, behavior: 'instant' })
  document.title = `${key} · ${variants.find(v => v.key === key)!.name} / Domeye 首页原型`
}
function onPopState() {
  variant.value = getVariant()
  document.title = `${variant.value} · ${currentVariant.value.name} / Domeye 首页原型`
}
function showScope() {
  if (variant.value === 'C') {
    void showDetail({ title: '观测范围与数据说明', subtitle: `${overviewVersion} / 仅用于首页结构验证`, rows: [
      ['时间窗口', `${windowLabel} · ${timezone}`], ['窗口截至', snapshot], ['时间来源', 'config/data-profile.json'], ['观察范围', `${family.value} · 全部示例观察点；不限定 ASN`], ['示例记录', '异常列表、中断图与事件计数使用同一份模拟输入'], ['待验证指标', '可见前缀数、可见起源 AS 数、普通路由变化'], ['交互范围', '顶部地址族控制 C 的全部数据；列表筛选不改变总体卡片']], note: '时间窗口固定为配置快照所在日；此按钮仅展示说明，不提供范围切换。没有接入真实数据，也不将旧 collect 标签当成完整总体。' })
    return
  }
  void showDetail({ title: '观察范围与展示边界', subtitle: '全部内容为 home-demo-v1 模拟数据，不代表系统现状', rows: [
    ['时间窗口', `${windowLabel} · ${timezone}`], ['快照时点', snapshot], ['时间来源', 'config/data-profile.json'], ['观察点', '8 个 Collector / 32 个 Peer（人为设置，不是真实覆盖）'], ['统计口径', '指定时点、范围和版本的可靠可见并集'], ['当前示例状态', scenarioLabels[scenario.value]], ['IPv6 地址空间单位', '尚未确定，保留未知'], ['检测与数据能力', '均未在本原型中验证']], note: '本地无 API、无数据采集、无持久化。覆盖缺口下如何准入仍待验证；此处仅测试可读性。BGP 控制面观测不能直接推出实际断网、用户影响、原因或责任。' })
}
onMounted(() => {
  window.addEventListener('popstate', onPopState)
  document.title = `${variant.value} · ${currentVariant.value.name} / Domeye 首页原型`
})
onBeforeUnmount(() => window.removeEventListener('popstate', onPopState))
</script>

<template>
  <div class="home-prototype" :class="`theme-${variant.toLowerCase()}`">
    <div class="demo-banner"><span><b>设计原型</b> 所有数值、事件与对象均为模拟，不代表系统现状。</span><span>本地预览 · 未连接数据服务</span></div>
    <header class="prototype-header"><a class="prototype-brand" href="#" aria-label="Domeye 原型首页"><svg viewBox="0 0 40 40" aria-hidden="true"><ellipse cx="20" cy="20" rx="17" ry="10"/><ellipse cx="20" cy="20" rx="10" ry="17" transform="rotate(35 20 20)"/><circle cx="20" cy="20" r="4"/></svg><strong>domeye<span>路由观测</span></strong></a>
      <nav v-if="variant === 'C'" aria-label="原型内容导航"><a href="#" class="active">核心态势</a><a href="#routing">变化趋势</a><a href="#events">路由异常</a></nav>
      <nav v-else aria-label="原型内容导航"><a href="#" class="active">核心态势</a><a href="#events">高风险事件 <b class="nav-risk-count">{{ scenario === 'unavailable' ? '—' : '3' }}</b></a><a href="#networks">关注网络</a></nav>
      <span v-if="variant === 'C'" class="c-header-note">历史窗口 / 本地原型</span>
      <div v-else class="prototype-search"><label><span aria-hidden="true">⌕</span><input aria-label="搜索示例网络" placeholder="搜索示例 ASN / 网络" v-model="query" @keydown.esc="query = ''"/></label><div v-if="query" class="search-results"><button v-for="o in results" :key="o.asn" @click="showDetail(objectDetail(o.index))"><strong>{{ o.asn }}</strong><span>{{ o.name }} ↗</span></button><p v-if="!results.length">没有匹配的示例对象<br><small>可试试 AS64496</small></p></div></div>
    </header>
    <div class="observation-toolbar"><div class="scope-controls"><button class="scope-button" @click="showScope"><span class="scope-dot"></span>{{ variant === 'C' ? '模拟观测范围' : '全部模拟观察点' }} <span>{{ variant === 'C' ? 'ⓘ' : '⌄' }}</span></button><button class="window-button" @click="showScope"><span>◷</span> {{ windowLabel }} <small>UTC+08</small></button></div><div class="display-controls"><div class="segmented" aria-label="地址族"><button v-for="f in (['IPv4', 'IPv6'] as const)" :key="f" :aria-pressed="family === f" :class="{ active: family === f }" @click="family = f">{{ f }}</button></div><label class="scenario-control"><span>演示状态</span><select v-model="scenario" aria-label="演示数据状态"><option v-for="(label, value) in scenarioLabels" :key="value" :value="value">{{ label }}</option></select></label></div></div>
    <div v-if="scenario !== 'ready'" class="availability-notice" role="status"><strong>{{ scenarioLabels[scenario] }}</strong><span>{{ scenario === 'gap' ? (variant === 'C' ? '09:00–13:00 为模拟缺口，不补零；事件卡片与列表只显示可用时段。' : '部分时段不可确认。规模卡片和排行暂停比较，趋势保留缺口。') : '展示不可用状态，不补成零，也不宣称没有异常。' }}</span></div>
    <component :is="components[variant]" :family="family" :scenario="scenario" @detail="showDetail"/>
    <footer class="prototype-footer"><div><strong>domeye / {{ currentVariant.name }}</strong><p aria-live="polite">{{ currentVariant.idea }}</p></div><div><button @click="showScope">口径与数据边界 ↗</button><p>{{ variant === 'C' ? overviewVersion : 'home-demo-v1' }} · {{ family }} · {{ scenarioLabels[scenario] }} · {{ timezone }}</p><p>仅为控制面观测示意，不代表实际连通性或用户影响。</p></div></footer>
    <PrototypeSwitcher :variant="variant" @change="changeVariant"/>
    <dialog ref="modal" class="detail-dialog" aria-labelledby="detail-title" @close="restoreFocus" @click="(event) => { if (event.target === modal) closeDetail() }"><template v-if="detail"><div class="dialog-heading"><span class="overline">PROTOTYPE / 交互示意</span><button aria-label="关闭详情" @click="closeDetail">×</button></div><h2 id="detail-title">{{ detail.title }}</h2><p class="dialog-subtitle">{{ detail.subtitle }}</p><dl><div v-for="[key, value] in detail.rows" :key="key"><dt>{{ key }}</dt><dd>{{ value }}</dd></div></dl><p class="detail-note">{{ detail.note }}</p><button class="dialog-done" @click="closeDetail">返回首页</button></template></dialog>
  </div>
</template>

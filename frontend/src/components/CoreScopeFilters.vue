<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { presetScope, scopeError, scopeLabel, scopeMinimum, scopeMaximum, type CoreScope } from '@/utils/coreScope'

const props = defineProps<{ modelValue: CoreScope; countries: string[]; retained?: { start: string; end: string }; loading: boolean }>()
const emit = defineEmits<{ 'update:modelValue': [value: CoreScope]; refresh: [] }>()
const regionMenu = ref<HTMLDetailsElement>()
const regionSearch = ref('')
const custom = ref(false)
const draft = ref({ ...props.modelValue })
const error = ref('')
const regionOptions = computed(() => [...props.countries].sort((a, b) => a.localeCompare(b, 'zh-CN'))
  .filter(name => name.toLocaleLowerCase().includes(regionSearch.value.trim().toLocaleLowerCase())))
const anchor = computed(() => props.retained?.end || scopeMaximum)
watch(() => props.modelValue, value => { draft.value = { ...value }; error.value = '' })
function selectCountry(country: string) {
  emit('update:modelValue', { ...props.modelValue, country })
  if (regionMenu.value) regionMenu.value.open = false
  regionSearch.value = ''
}
function choose(hours: number) { custom.value = false; emit('update:modelValue', presetScope(props.modelValue, hours, anchor.value)) }
function active(hours: number) {
  const target = presetScope(props.modelValue, hours, anchor.value)
  return props.modelValue.start === target.start && props.modelValue.end === target.end
}
function useRetained() {
  if (props.retained) { custom.value = false; emit('update:modelValue', { ...props.modelValue, ...props.retained }) }
}
function apply() {
  const complete = (value: string) => value.length === 16 ? `${value}:00` : value
  const next = { country: props.modelValue.country, start: complete(draft.value.start), end: complete(draft.value.end) }
  error.value = scopeError(next)
  if (!error.value) emit('update:modelValue', next)
}
</script>

<template>
  <section class="core-scope" aria-label="首页范围筛选">
    <div class="core-scope-row">
      <details ref="regionMenu" class="core-region-menu" @keydown.esc="regionMenu && (regionMenu.open = false)">
        <summary aria-label="选择全球或国家地区"><svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c5 5 5 13 0 18-5-5-5-13 0-18Z"/></svg><span><small>观察地区</small><strong>{{ modelValue.country || '全球' }}</strong></span><span class="core-selector-arrow">⌄</span></summary>
        <div class="core-region-popover">
          <input v-model="regionSearch" type="search" aria-label="搜索国家或地区" placeholder="搜索国家或地区" />
          <div class="core-region-options" role="group" aria-label="地区选项">
            <button :aria-pressed="!modelValue.country" @click="selectCountry('')">全球 <span v-if="!modelValue.country">✓</span></button>
            <button v-for="name in regionOptions" :key="name" :aria-pressed="modelValue.country === name" @click="selectCountry(name)">{{ name }} <span v-if="modelValue.country === name">✓</span></button>
            <p v-if="!regionOptions.length">{{ countries.length ? '没有匹配的地区' : '地区列表待读取' }}</p>
          </div>
        </div>
      </details>
      <div class="core-time-selector"><small>时间区间 <span>北京时间 · 右端不含</span></small><button class="core-selected-range" :aria-expanded="custom" @click="custom = !custom">{{ scopeLabel(modelValue) }} <span>⌄</span></button></div>
      <button class="core-scope-refresh" :disabled="loading" @click="emit('refresh')">{{ loading ? '读取中…' : '重新读取 ↻' }}</button>
    </div>
    <div class="core-range-presets" role="group" aria-label="快捷时间区间">
      <button v-if="retained" :aria-pressed="modelValue.start === retained.start && modelValue.end === retained.end" @click="useRetained">已接入时段</button>
      <button v-for="[hours, label] in [[24, '近 24 小时'], [168, '近 7 天'], [720, '近 30 天']] as const" :key="hours" :aria-pressed="active(hours)" @click="choose(hours)">{{ label }}</button>
      <button :aria-pressed="custom" @click="custom = !custom">自定义</button>
      <span>快捷区间截至{{ retained ? '最新数据时刻' : '数据档末端' }}</span>
    </div>
    <form v-if="custom" class="core-custom-range" @submit.prevent="apply">
      <label>开始时间<input v-model="draft.start" type="datetime-local" step="1" :min="scopeMinimum" :max="scopeMaximum" required aria-label="开始时间" /></label>
      <span aria-hidden="true">→</span>
      <label>结束时间<input v-model="draft.end" type="datetime-local" step="1" :min="scopeMinimum" :max="scopeMaximum" required aria-label="结束时间" /></label>
      <button type="submit">应用区间</button><p v-if="error" role="alert">{{ error }}</p>
    </form>
  </section>
</template>

<style scoped>
.core-scope { background:var(--surface); border:1px solid var(--line); border-radius:6px; margin-bottom:22px; }
.core-scope-row { display:flex; align-items:stretch; min-height:82px; }
.core-region-menu { position:relative; min-width:220px; border-right:1px solid var(--line); }
.core-region-menu summary { display:flex; list-style:none; align-items:center; gap:12px; height:100%; padding:16px 22px; cursor:pointer; }
.core-region-menu summary::-webkit-details-marker { display:none; }
.core-region-menu svg { width:24px; height:24px; stroke:var(--accent); fill:none; stroke-width:1.4; }
.core-region-menu small,.core-time-selector>small { display:block; font-size:10px; color:var(--muted); margin-bottom:5px; }
.core-region-menu strong { font-size:17px; font-weight:550; }
.core-selector-arrow { margin-left:auto; color:var(--muted); }
.core-region-popover { position:absolute; z-index:15; top:100%; left:-1px; width:290px; background:white; border:1px solid var(--line); box-shadow:0 12px 30px #263a4618; border-radius:0 0 6px 6px; padding:12px; }
.core-region-popover input { width:100%; border:1px solid var(--line); padding:9px; border-radius:4px; }
.core-region-options { max-height:290px; overflow:auto; padding-top:8px; }
.core-region-options button { display:flex; justify-content:space-between; padding:9px 10px; width:100%; text-align:left; }
.core-region-options button:hover,.core-region-options button[aria-pressed=true] { background:#edf3f6; color:var(--accent); }
.core-region-options p { padding:12px; color:var(--muted); }
.core-time-selector { padding:16px 24px; flex:1; }
.core-time-selector>small span { margin-left:12px; color:var(--muted); }
.core-selected-range { display:flex; gap:18px; align-items:center; text-align:left; font-family:var(--mono)!important; font-size:15px!important; }
.core-scope-refresh { margin:0 22px; color:var(--accent)!important; font-size:12px!important; }
.core-range-presets { display:flex; flex-wrap:wrap; align-items:center; gap:6px; border-top:1px solid var(--line); padding:10px 18px; }
.core-range-presets button { padding:6px 12px; border-radius:4px; font-size:11px; color:var(--muted); }
.core-range-presets button[aria-pressed=true] { background:#e8f0f4; color:var(--accent); }
.core-range-presets>span { margin-left:auto; font-size:10px; color:var(--muted); }
.core-custom-range { display:flex; align-items:end; flex-wrap:wrap; gap:14px; padding:14px 22px 18px; border-top:1px solid var(--line); }
.core-custom-range label { display:grid; gap:6px; font-size:11px; color:var(--muted); }
.core-custom-range input { background:white; border:1px solid var(--line); border-radius:4px; padding:9px; min-width:0; }
.core-custom-range>span { padding-bottom:10px; color:var(--muted); }
.core-custom-range button { padding:10px 18px; background:var(--accent); color:white; border-radius:4px; }
.core-custom-range p { width:100%; color:#945038; }
@media(max-width:760px) { .core-scope-row { flex-wrap:wrap; }.core-region-menu { min-width:0; flex:1; border-right:0; }.core-region-menu summary { padding:14px 16px; }.core-time-selector { order:3; flex-basis:100%; padding:14px 16px; border-top:1px solid var(--line); }.core-selected-range { font-size:12px!important; gap:8px; }.core-range-presets { padding:8px; }.core-range-presets>span { width:100%; padding:6px 12px; }.core-custom-range { padding:14px 16px; gap:12px; }.core-custom-range label { width:100%; }.core-custom-range>span { display:none; } }
</style>

<script setup lang="ts">
import { onBeforeUnmount, onMounted } from 'vue'
import { variants, type Variant } from './fixture'
const props = defineProps<{ variant: Variant }>()
const emit = defineEmits<{ change: [value: Variant] }>()
const isDevelopment = import.meta.env.DEV
function cycle(step: number) {
  const current = variants.findIndex(v => v.key === props.variant)
  emit('change', variants[(current + step + variants.length) % variants.length]!.key)
}
function onKey(event: KeyboardEvent) {
  if (event.defaultPrevented || event.altKey || event.metaKey || event.ctrlKey || event.shiftKey) return
  if (document.querySelector('dialog[open]')) return
  if (event.target instanceof Element && event.target.closest('input,textarea,select,[contenteditable]')) return
  if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
    event.preventDefault()
    cycle(event.key === 'ArrowLeft' ? -1 : 1)
  }
}
onMounted(() => window.addEventListener('keydown', onKey))
onBeforeUnmount(() => window.removeEventListener('keydown', onKey))
</script>
<template>
  <nav v-if="isDevelopment" class="prototype-switcher" aria-label="原型方案切换">
    <span class="switcher-caption">设计比较</span><button class="cycle-button" aria-label="上一个方案" @click="cycle(-1)">←</button>
    <button v-for="v in variants" :key="v.key" :aria-pressed="variant === v.key" :class="{ active: variant === v.key }" @click="emit('change', v.key)"><b>{{ v.key }}</b><span>{{ v.name }}</span></button>
    <button class="cycle-button" aria-label="下一个方案" @click="cycle(1)">→</button>
  </nav>
</template>

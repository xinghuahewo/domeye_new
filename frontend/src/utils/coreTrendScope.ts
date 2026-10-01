import { computed, ref, watch, type ComputedRef } from 'vue'
import { scopeError, type CoreScope } from './coreScope'

/** 三张特征图共用局部区间；首页范围变化时重新跟随首页。 */
export function useCoreTrendScope(home: ComputedRef<CoreScope>) {
  const selectedWindow = ref({ ...home.value })
  const draft = ref({ start: home.value.start, end: home.value.end })
  const rangeError = ref('')
  const localRange = computed(() => selectedWindow.value.start !== home.value.start || selectedWindow.value.end !== home.value.end)

  function resetRange() {
    selectedWindow.value = { ...home.value }
    draft.value = { start: home.value.start, end: home.value.end }
    rangeError.value = ''
  }
  function applyRange() {
    const complete = (value: string) => value.length === 16 ? `${value}:00` : value
    const next = { country: home.value.country, start: complete(draft.value.start), end: complete(draft.value.end) }
    rangeError.value = scopeError(next)
    if (!rangeError.value) {
      selectedWindow.value = next
      draft.value = { start: next.start, end: next.end }
    }
  }
  watch(home, resetRange)
  return { selectedWindow, draft, rangeError, localRange, applyRange, resetRange }
}

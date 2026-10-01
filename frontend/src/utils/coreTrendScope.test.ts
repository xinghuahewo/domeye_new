import { computed, effectScope, nextTick, ref } from 'vue'
import { expect, it } from 'vitest'
import { useCoreTrendScope } from './coreTrendScope'

const initial = { start: '2026-02-20T00:00:00', end: '2026-02-27T00:00:00', country: '伊朗' }
function setup() {
  const lifetime = effectScope()
  const home = ref({ ...initial })
  const range = lifetime.run(() => useCoreTrendScope(computed(() => home.value)))!
  return { home, range, stop: () => lifetime.stop() }
}

it('图表区间独立应用、补齐秒并可重置，首页范围不被修改', () => {
  const { home, range, stop } = setup()
  try {
    range.draft.value = { start: '2026-02-24T09:00', end: '2026-02-24T10:00' }
    range.applyRange()
    expect(range.selectedWindow.value).toEqual({ start: '2026-02-24T09:00:00', end: '2026-02-24T10:00:00', country: '伊朗' })
    expect(range.localRange.value).toBe(true)
    expect(home.value).toEqual(initial)
    range.resetRange()
    expect(range.selectedWindow.value).toEqual(initial)
    expect(range.localRange.value).toBe(false)
  } finally { stop() }
})

it('无效、反向或数据档外范围保留已应用区间并显示错误', () => {
  const { range, stop } = setup()
  try {
    for (const draft of [
      { start: '2026-02-30T09:00', end: '2026-03-01T10:00' },
      { start: '2026-02-24T10:00', end: '2026-02-24T09:00' },
      { start: '2026-02-24T09:00', end: '2026-04-01T00:00:01' },
    ]) {
      range.draft.value = draft
      range.applyRange()
      expect(range.rangeError.value).not.toBe('')
      expect(range.selectedWindow.value).toEqual(initial)
    }
  } finally { stop() }
})

it('首页国家或时间变化后，清除局部范围与错误并重新跟随首页', async () => {
  const { home, range, stop } = setup()
  try {
    range.draft.value = { start: '2026-02-24T09:00', end: '2026-02-24T10:00' }
    range.applyRange()
    home.value = { ...initial, country: '中国' }
    await nextTick()
    expect(range.selectedWindow.value).toEqual(home.value)
    expect(range.localRange.value).toBe(false)
    range.draft.value.end = ''
    range.applyRange()
    expect(range.rangeError.value).not.toBe('')
    home.value = { ...home.value, start: '2026-02-26T00:00:00' }
    await nextTick()
    expect(range.selectedWindow.value).toEqual(home.value)
    expect(range.draft.value).toEqual({ start: home.value.start, end: home.value.end })
    expect(range.rangeError.value).toBe('')
  } finally { stop() }
})

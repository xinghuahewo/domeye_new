import { onBeforeUnmount, onMounted } from 'vue'

/** 定时与手动读取共用忙碌状态；切换查询仍由调用方的请求序号/取消逻辑仲裁。 */
export function useAutoRefresh<T extends unknown[]>(
  read: (...args: T) => Promise<void>,
  options: { args: () => NoInfer<T>; enabled?: () => boolean },
) {
  let active = 0
  let disposed = false
  let timer: ReturnType<typeof setTimeout> | undefined
  async function load(...args: T) {
    active++
    try { await read(...args) } finally { active-- }
  }
  function schedule() {
    if (timer) clearTimeout(timer)
    if (!disposed && document.visibilityState !== 'hidden') timer = setTimeout(tick, 30_000)
  }
  async function tick() {
    if (disposed || document.visibilityState === 'hidden') return
    try {
      if (!active && (options.enabled?.() ?? true)) await load(...options.args())
    } finally { schedule() }
  }
  function visibilityChanged() {
    if (timer) clearTimeout(timer)
    if (document.visibilityState !== 'hidden') void tick()
  }
  onMounted(() => {
    document.addEventListener('visibilitychange', visibilityChanged)
    schedule()
  })
  onBeforeUnmount(() => {
    disposed = true
    if (timer) clearTimeout(timer)
    document.removeEventListener('visibilitychange', visibilityChanged)
  })
  return load
}

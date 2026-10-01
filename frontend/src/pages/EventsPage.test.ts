import { createSSRApp, h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { resultDelivery } from '@/api/health'
import EventsPage from './EventsPage.vue'

vi.mock('@/api/events', () => ({ getEvents: vi.fn() }))

async function renderEvents(query: Record<string, string>) {
  const router = createRouter({ history: createMemoryHistory(), routes: [
    { path: '/events', name: 'events', component: EventsPage },
    { path: '/events/detail', name: 'event-detail', component: { render: () => null } },
  ] })
  await router.push({ name: 'events', query })
  return renderToString(createSSRApp({ render: () => h(EventsPage) }).use(router))
}

beforeEach(() => { resultDelivery.value = undefined })

describe('事件页时间范围', () => {
  it('显示来自跨页链接的秒级区间、国家及 ASN，并说明右端不含', async () => {
    const html = await renderEvents({ start: '2026-02-27T08:00:12', end: '2026-03-02T08:00:34', attacked_country: '伊朗', attacked_as: 'AS49666' })
    expect(html).toContain('value="2026-02-27T08:00:12"')
    expect(html).toContain('value="2026-03-02T08:00:34"')
    expect(html).toContain('type="datetime-local" step="1"')
    expect(html).toContain('value="伊朗"')
    expect(html).toContain('value="49666"')
    expect(html).toContain('按事件开始时间筛选 · 开始含、结束不含 · 北京时间')
    expect(html).toContain('近 7 天')
    expect(html).toContain('近 30 天')
    expect(html).toContain('整个数据窗口')
  })

  it('旧日期链接显示整天，国家名 country 兼容为受影响国家', async () => {
    const html = await renderEvents({ date: '2026-02-28', country: '中国' })
    expect(html).toContain('value="2026-02-28T00:00:00"')
    expect(html).toContain('value="2026-03-01T00:00:00"')
    expect(html).toContain('value="中国"')
  })

  it('缺省和已接入时段以交付时间为准，不使用浏览器当前日期', async () => {
    resultDelivery.value = { state: 'available', files: 865, start: '2026-02-27T00:00:00Z', end_exclusive: '2026-03-02T00:00:00Z' }
    const html = await renderEvents({})
    expect(html).toContain('value="2026-02-27T08:00:00"')
    expect(html).toContain('value="2026-03-02T08:00:00"')
    expect(html).toContain('已接入时段')
    expect(html).toContain('aria-pressed="true"')
  })
})

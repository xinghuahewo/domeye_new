import { createSSRApp, h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { createMemoryHistory, createRouter } from 'vue-router'
import { expect, it, vi } from 'vitest'
import AsnPage from './AsnPage.vue'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('axios', () => ({ isAxiosError: () => false, default: { create: () => ({ get, interceptors: { response: { use: vi.fn() } } }) } }))

it('实际ASN页在旧特征失败时仍独立展示RIB，并保持普通排行与返回导航选择', async () => {
  const version = `rib_snapshot_v1_${'a'.repeat(64)}`
  const query = { snapshot_version: version, snapshot_date: '2026-02-27', snapshot_family: 'ipv4' }
  get.mockImplementation(async (url: string) => {
    if (!url.startsWith('rib-snapshots')) throw new Error('旧特征fixture不可用')
    return { data: { state: 'available', version, date: '2026-02-27', observed_at: '2026-02-27T00:00:00Z',
      family: 'ipv4', asn: 13335, prefix_count: 0, unit: 'distinct_origin_prefix',
      source: { collector_id: 'rrc25', sha256: 'b'.repeat(64), coverage: 'unknown' },
      origin_rule: 'rib-attributed-origin/private-skip-v1', limitations: [], sample_limit: 20, sample_truncated: false, items: [] } }
  })
  const router = createRouter({ history: createMemoryHistory(), routes: [
    { path: '/ases/:asn', name: 'asn-detail', component: AsnPage },
    { path: '/ases', name: 'ases', component: AsnPage },
  ] })
  await router.push({ name: 'asn-detail', params: { asn: '13335' }, query })
  const app = createSSRApp({ render: () => h(AsnPage) }).use(router)
  const html = await renderToString(app)
  expect(html).toContain('RIB 时点快照')
  expect(html).toMatch(/data-testid="asn-rib-prefix-count"[^>]*>0</)
  const returnHref = router.resolve({ name: 'ases', query }).href.replaceAll('&', '&amp;')
  expect(html).toContain(`href="${returnHref}"`)
})

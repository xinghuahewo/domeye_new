import { beforeEach, expect, it, vi } from 'vitest'
import { getAsCandidates } from './features'

const { apiGet } = vi.hoisted(() => ({ apiGet: vi.fn() }))
vi.mock('./client', () => ({ apiGet }))
const range = { start_time: '2026-02-27 08:00:00', end_time: '2026-03-02 08:00:00' }
const fixture = () => ({
  state: 'available',
  query: { start: '2026-02-27T08:00:00+08:00', end_exclusive: '2026-03-02T08:00:00+08:00', country: '伊朗', page: 2 },
  metadata: { version: 'delivery_fixture', collector_id: 'rrc25', scope_kind: 'delivered_asn_feature_samples',
    country_basis: 'result_delivery.features.country', coverage: { state: 'partial', intervals: [] }, limitations: ['仅留存样本'] },
  total: 11, page_count: 2,
  items: [{ asn: '64500', country: '伊朗', countries: ['伊朗'], as_name: null, org_name: null,
    sample_count: 2, latest_observation: '2026-02-28T08:00:00+08:00', announce: 0, withdraw: null,
    update_total: null, withdraw_rate: null, anomaly_count: null }],
})
beforeEach(() => apiGet.mockReset())

it('国家、完整跨日窗口、分页版本和取消信号都传给候选接口', async () => {
  apiGet.mockResolvedValue(fixture())
  const signal = new AbortController().signal
  const options = { country: '伊朗', page: 2, page_size: 10, version: 'delivery_fixture', sort: 'activity' as const }
  const result = await getAsCandidates(range, options, signal)
  expect(apiGet).toHaveBeenCalledWith('features/ases/candidates', { params: { ...range, ...options }, signal })
  expect(result.page).toBe(2)
  expect(result.coverage.state).toBe('partial')
  expect(result.items[0]).toMatchObject({ asn: '64500', announce: 0, withdraw: null, updateTotal: null, anomalyCount: null })
})

it('无观测与有观测但无匹配保留不同状态，失败不伪装成空列表', async () => {
  apiGet.mockResolvedValue({ ...fixture(), state: 'window_not_observed', items: [], total: 0, page_count: 0 })
  expect((await getAsCandidates(range)).state).toBe('window_not_observed')
  apiGet.mockResolvedValue({ ...fixture(), items: [], total: 0, page_count: 0 })
  expect((await getAsCandidates(range)).state).toBe('available')
  apiGet.mockRejectedValue(new Error('版本已变化'))
  await expect(getAsCandidates(range)).rejects.toThrow('版本已变化')
  apiGet.mockResolvedValue({ status: false })
  await expect(getAsCandidates(range)).rejects.toThrow('响应格式异常')
})

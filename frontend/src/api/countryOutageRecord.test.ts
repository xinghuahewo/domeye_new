import { createSSRApp, h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { getCountryOutageRecord } from './countryOutageRecord'
import { apiV2GetWithResultMetadata } from './client'
import CountryOutageRecord from '@/components/CountryOutageRecord.vue'

vi.mock('./client', () => ({ apiV2GetWithResultMetadata: vi.fn() }))
const reference = 'country_outage/2026-02-24 08:10:00/ZZ/1/r'
const reply = () => ({
  result: { state: 'available', version: 'delivery_fixture', start: '2026-02-24T00:00:00Z', 'end-exclusive': '2026-02-24T04:00:00Z' },
  data: { schema_version: 'country-outage-delivery/v1', event: {
    state: 'available', version: 'delivery_fixture',
    item: { reference, kind: 'country_outage', object: 'ZZ', country_name: '测试地区',
      start_time: '2026-02-24T00:10:00Z', end_time: { state: 'unknown', value: null }, asns: ['64501', '64502'],
      lifecycle: { state: 'ongoing', observed_at: '2026-02-24T03:55:00Z' },
      country_incident: { onset_at: '2026-02-24T00:07:00Z', peak_at: '2026-02-24T01:20:00Z',
        asn_membership: { basis: 'peak_snapshot', count: 2, total: 12, ratio: 2 / 12 } },
    },
    metadata: { source: { collector_id: 'rrc25' }, result_delivery: {
      state: 'available', version: 'delivery_fixture', start: '2026-02-24T00:00:00Z', end_exclusive: '2026-02-24T04:00:00Z', coverage: 'partial_window',
    } },
  } },
})
beforeEach(() => vi.clearAllMocks())

describe('国家事件共用详情到页面', () => {
  it('一次解析呈现检测与峰值时间、峰值成员及截至观测未结束状态', async () => {
    vi.mocked(apiV2GetWithResultMetadata).mockResolvedValue(reply())
    const record = await getCountryOutageRecord(reference.replace(' ', '+'))
    const html = await renderToString(createSSRApp({ render: () => h(CountryOutageRecord, { record, loading: false, error: '' }) }))
    expect(apiV2GetWithResultMetadata).toHaveBeenCalledOnce()
    expect(apiV2GetWithResultMetadata).toHaveBeenCalledWith('events/resolve', { params: { ref: reference } })
    for (const text of ['2026-02-24 08:10:00', '2026-02-24 08:07:00', '2026-02-24 09:20:00',
      '2026-02-24 11:55:00', '尚未结束', '峰值时刻的受影响 AS 名单', '16.67%', 'AS64501', 'AS64502', 'rrc25']) expect(html).toContain(text)
  })

  it('未提供结构化国家状态时保留未知，不能仅凭空结束时间宣布仍在中断', async () => {
    const response = reply()
    response.data.event.item.lifecycle.state = 'unknown'
    Reflect.deleteProperty(response.data.event.item, 'country_incident')
    vi.mocked(apiV2GetWithResultMetadata).mockResolvedValue(response)
    const record = await getCountryOutageRecord(reference)
    const html = await renderToString(createSSRApp({ render: () => h(CountryOutageRecord, { record, loading: false, error: '' }) }))
    expect(html).toContain('此记录尚未提供可核对的峰值成员口径')
    expect(html).not.toContain('尚未结束')
    expect(html).not.toContain('峰值时刻的受影响 AS 名单')
  })

  it.each(['reference', 'body_version', 'header_version', 'coverage'])('拒绝 %s 不匹配，防止不同读取混用', async (field) => {
    const response = reply()
    if (field === 'reference') response.data.event.item.reference = reference.replace('/1/', '/2/')
    if (field === 'body_version') response.data.event.version = 'another'
    if (field === 'header_version') response.result.version = 'another'
    if (field === 'coverage') response.result['end-exclusive'] = '2026-02-24T05:00:00Z'
    vi.mocked(apiV2GetWithResultMetadata).mockResolvedValue(response)
    await expect(getCountryOutageRecord(reference)).rejects.toThrow('不一致')
  })
})

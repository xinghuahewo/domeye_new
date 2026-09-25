import { apiV2GetWithResultMetadata } from './client'
import type { components } from '@/types/openapi.generated'
import { canonicalCountryOutageEventReference } from '@/utils/countryOutageRuntime'

type Detail = components['schemas']['CoreOverviewDetail']
export interface CountryOutageRecord {
  item: Detail['item']
  source: Detail['metadata']['source']
  delivery: { version: string; start: string; endExclusive: string; coverage: string }
}

export async function getCountryOutageRecord(reference: string): Promise<CountryOutageRecord> {
  const canonical = canonicalCountryOutageEventReference(reference)
  if (!canonical) throw new Error('国家事件引用无效')
  const { data, result } = await apiV2GetWithResultMetadata<components['schemas']['DeliveredCountryOutageResolution']>(
    'events/resolve', { params: { ref: canonical } },
  )
  const event = data?.event
  if (data?.schema_version !== 'country-outage-delivery/v1' || event?.state !== 'available') {
    throw new Error('国家事件缺少数据库详情，请使用匹配的 API')
  }
  if (event.item?.kind !== 'country_outage'
    || canonicalCountryOutageEventReference(event.item.reference) !== canonical) {
    throw new Error('基础记录与请求的国家事件不一致')
  }
  const delivery = event.metadata?.result_delivery
  if (!delivery || delivery.state !== 'available' || !delivery.version || !delivery.coverage
    || event.version !== delivery.version || result.version !== event.version || result.state !== 'available'
    || !Number.isFinite(Date.parse(delivery.start ?? ''))
    || !Number.isFinite(Date.parse(delivery.end_exclusive ?? ''))
    || Date.parse(delivery.start!) >= Date.parse(delivery.end_exclusive!)
    || Date.parse(result.start ?? '') !== Date.parse(delivery.start!)
    || Date.parse(result['end-exclusive'] ?? '') !== Date.parse(delivery.end_exclusive!)) {
    throw new Error('基础记录的版本或覆盖时间不一致')
  }
  return {
    item: event.item, source: event.metadata.source,
    delivery: { version: delivery.version, start: delivery.start!, endExclusive: delivery.end_exclusive!, coverage: delivery.coverage },
  }
}

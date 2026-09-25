import { apiGetWithResultMetadata } from './client'
import type { EvidenceBundle } from '@/types/api'
import { canonicalCountryOutageEventReference } from '@/utils/countryOutageRuntime'
import { buildEvidenceEndpoint, normalizeEvidenceBundle, parseDetailUrl } from '@/utils/normalize'

export interface CountryOutageRecord {
  bundle: EvidenceBundle
  outageAsCount: number | null
  totalAsCount: number | null
  outageAsns: string[] | null
  delivery: {
    version: string
    start: string
    endExclusive: string
    coverage: string
  } | null
}

function count(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null
}

export async function getCountryOutageRecord(reference: string): Promise<CountryOutageRecord> {
  const canonical = canonicalCountryOutageEventReference(reference)
  const parsed = canonical ? parseDetailUrl(canonical) : null
  if (!parsed || !canonical) throw new Error('国家事件引用无效')
  const { data, result } = await apiGetWithResultMetadata<unknown>(buildEvidenceEndpoint(parsed))
  const bundle = normalizeEvidenceBundle(data)
  if (
    bundle.event.kind !== 'country_outage'
    || canonicalCountryOutageEventReference(bundle.sourceRecord.detailReference) !== canonical
  ) throw new Error('基础记录与请求的国家事件不一致')

  let delivery: CountryOutageRecord['delivery'] = null
  if (Object.keys(result).length) {
    if (
      result.state !== 'available' || !result.version || !result.coverage
      || !Number.isFinite(Date.parse(result.start ?? ''))
      || !Number.isFinite(Date.parse(result['end-exclusive'] ?? ''))
      || Date.parse(result.start!) >= Date.parse(result['end-exclusive']!)
    ) throw new Error('基础记录的批次身份或覆盖时间不完整')
    delivery = {
      version: result.version,
      start: result.start!,
      endExclusive: result['end-exclusive']!,
      coverage: result.coverage,
    }
  }
  const asns = bundle.factRecord.outage_ases
  const outageAsns = Array.isArray(asns)
    && asns.every((asn) => /^(?:[1-9]\d*)$/.test(String(asn)) && Number(asn) <= 4294967295)
    ? [...new Set(asns.map(String))] : null
  return {
    bundle,
    outageAsCount: count(bundle.factRecord.outage_as_num),
    totalAsCount: count(bundle.factRecord.total_as_num),
    outageAsns,
    delivery,
  }
}

import { apiGet } from './client'
import type { components } from '@/types/openapi.generated'
import type { AsCandidatePage } from '@/types/api'
import {
  normalizeAsOverview,
  normalizeCountryOverview,
  normalizeEventPage,
  normalizeFeaturePoints,
  normalizeOutagePoints,
} from '@/utils/normalize'

type OutageSeriesPayload = components['schemas']['OutageSeriesPayload']

export interface FeatureRange {
  start_time: string
  end_time: string
}

export async function getTopFeatures(target: string, range: FeatureRange, signal?: AbortSignal) {
  const payload = await apiGet<unknown>('features/top', {
    params: { target, ...range },
    signal,
  })
  return normalizeFeaturePoints(payload)
}

export async function getCountryOverview(range: FeatureRange, country?: string, limit = 6) {
  return normalizeCountryOverview(await apiGet<unknown>('features/countries/overview', {
    params: { ...range, country: country || undefined, limit },
  }))
}

export async function getAsOverview(
  range: FeatureRange,
  asn?: string,
  limit = 6,
  eventWindow = false,
  eventReference = '',
  signal?: AbortSignal,
) {
  return normalizeAsOverview(await apiGet<unknown>('features/ases/overview', {
    transitional: { clarifyTimeoutError: true },
    signal,
    params: {
      ...range,
      asn: asn || undefined,
      limit,
      event_window: eventWindow || undefined,
      event_reference: eventWindow ? eventReference : undefined,
    },
  }))
}

export async function getAsRecentEvents(
  asn: string,
  range: FeatureRange,
  pageSize = 10,
  eventWindow = false,
  eventReference = '',
  signal?: AbortSignal,
) {
  return normalizeEventPage(await apiGet<unknown>('features/ases/events', {
    signal,
    params: {
      ...range,
      asn,
      page_size: pageSize,
      event_window: eventWindow || undefined,
      event_reference: eventWindow ? eventReference : undefined,
    },
  }))
}

export async function getGlobalASOutages(range: FeatureRange) {
  return normalizeOutagePoints(await apiGet<OutageSeriesPayload>('features/outages/global-as', { params: range }))
}

export async function getGlobalPrefixOutages(range: FeatureRange) {
  return normalizeOutagePoints(await apiGet<OutageSeriesPayload>('features/outages/global-prefix', { params: range }))
}

export async function getCountryASOutages(country: string, range: FeatureRange) {
  return normalizeOutagePoints(await apiGet<OutageSeriesPayload>('features/outages/country-as', {
    params: { country, ...range },
  }))
}

export async function getCountryPrefixOutages(country: string, range: FeatureRange) {
  return normalizeOutagePoints(await apiGet<OutageSeriesPayload>('features/outages/country-prefix', {
    params: { country, ...range },
  }))
}

export async function getASPrefixOutages(asn: string, range: FeatureRange, signal?: AbortSignal) {
  return normalizeOutagePoints(await apiGet<OutageSeriesPayload>('features/outages/as-prefix', {
    params: { asn, ...range },
    signal,
  }))
}

export interface AsCandidateOptions {
  country?: string
  q?: string
  sort?: 'activity' | 'latest' | 'asn'
  order?: 'asc' | 'desc'
  page?: number
  page_size?: number
  version?: string
}

export async function getAsCandidates(range: FeatureRange, options: AsCandidateOptions = {}, signal?: AbortSignal): Promise<AsCandidatePage> {
  const payload = await apiGet<components['schemas']['AsCandidatesPayload']>('features/ases/candidates', {
    params: { ...range, ...options }, signal,
  })
  if (!payload || !['available', 'window_not_observed'].includes(payload.state)
    || !Array.isArray(payload.items) || !payload.metadata?.version) {
    throw new Error('AS 查询响应格式异常')
  }
  return {
    state: payload.state,
    version: payload.metadata.version,
    collectorId: payload.metadata.collector_id,
    scopeKind: payload.metadata.scope_kind,
    countryBasis: payload.metadata.country_basis,
    coverage: payload.metadata.coverage,
    limitations: payload.metadata.limitations,
    start: payload.query.start,
    end: payload.query.end_exclusive,
    country: payload.query.country || '',
    total: payload.total,
    page: payload.query.page,
    pageCount: payload.page_count,
    items: payload.items.map(item => ({
      asn: item.asn, country: item.country, countries: item.countries,
      asName: item.as_name, orgName: item.org_name, sampleCount: item.sample_count,
      latestObservation: item.latest_observation, announce: item.announce,
      withdraw: item.withdraw, updateTotal: item.update_total,
      withdrawRate: item.withdraw_rate, anomalyCount: item.anomaly_count,
    })),
  }
}

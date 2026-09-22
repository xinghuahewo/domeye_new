import { beforeEach, expect, it, vi } from 'vitest'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))
vi.mock('axios', () => ({ default: { create: () => ({ get }) } }))
import { getCoreOverview, getCoreOverviewRecord } from './coreOverview'

beforeEach(() => { get.mockReset() })

it('独立 RIB 规模按实际交付时段、地址族与原件绑定，不能拿 Resource 公有 AS 代替', async () => {
  const rib = { state: 'available', family: 'all', snapshot_id: `rib_statistics_v1_${'a'.repeat(64)}`,
    observed_at: '2026-02-24T00:00:00Z', collector_id: 'rrc25', source_id: 'one-rib', source_sha256: 'b'.repeat(64),
    rule: 'rib-attributed-origin/private-skip-v1', metrics: { visible_prefixes: 123, visible_origin_ases: 10 } }
  const payload = { state: 'available', version: 'delivery-statistics', query: { date: '2026-02-24', family: 'all' },
    overview: { visible_prefixes: 123, visible_origin_ases: 10 }, metadata: { source: { collector_id: 'rrc25' },
      result_delivery: { intervals: [{ start: '2026-02-24T00:00:00Z', end_exclusive: '2026-02-24T03:35:00Z' }] }, rib_statistics: rib } }
  get.mockResolvedValue({ data: payload }); expect(await getCoreOverview()).toEqual(payload)
  get.mockResolvedValue({ data: { ...payload, state: 'window_not_retained', overview: null, events: null } })
  expect((await getCoreOverview()).metadata.rib_statistics?.snapshot_id).toBe(rib.snapshot_id)
  for (const patch of [{ family: 'ipv4' }, { observed_at: '2026-02-24T03:35:00Z' }, { collector_id: 'rrc00' },
    { rule: 'resource-39578fe-v1' }, { metrics: { visible_prefixes: 123, visible_origin_ases: 11 } }]) {
    get.mockResolvedValue({ data: { ...payload, metadata: { ...payload.metadata, rib_statistics: { ...rib, ...patch } } } })
    await expect(getCoreOverview()).rejects.toThrow('规模')
  }
  get.mockResolvedValue({ data: { ...payload, overview: { visible_prefixes: null, visible_origin_ases: null },
    metadata: { ...payload.metadata, rib_statistics: { state: 'unavailable', family: 'all', message: '读取失败' } } } })
  expect((await getCoreOverview()).overview?.visible_prefixes).toBeNull()
})

it('对象待核实须保留集合原文，列表和详情都不能附带成员ASN', async () => {
  const item = { kind: 'as_outage', object: '{64496,64497}', asns: [],
    object_identity: { state: 'unresolved', reason: 'as_set_in_asn_field', label: '对象待核实' } }
  const payload = { state: 'available', version: 'identity-v3', events: { items: [item] } }
  get.mockResolvedValue({ data: payload })
  expect(await getCoreOverview()).toEqual(payload)
  for (const patch of [{ asns: ['64496'] }, { object: '64496' }, { object_identity: undefined }, { kind: 'hijack' }]) {
    get.mockResolvedValue({ data: { ...payload, events: { items: [{ ...item, ...patch }] } } })
    await expect(getCoreOverview()).rejects.toThrow('对象')
  }
  get.mockResolvedValue({ data: { state: 'available', version: 'identity-v3', item: { ...item, asns: ['64497'] } } })
  await expect(getCoreOverviewRecord('raw-ref', 'identity-v3')).rejects.toThrow('对象')
})

it('两时点路径对照必须同日同族、分母闭合，不能伪称期间次数或延续到失败日', async () => {
  const comparison = { state: 'available', version: `rib_path_consumption_v1_${'a'.repeat(64)}`,
    comparison_version: `rib_path_comparison_v1_${'b'.repeat(64)}`, interpretation_version: 'rrc25-raw-peer-rib-endpoint/as-sequence-v1',
    collector_id: 'rrc25', coverage: 'unknown', session_continuity: 'unknown', interval_change_count: null,
    left: { observed_at: '2026-03-30T16:00:00Z', sha256: 'c'.repeat(64) },
    right: { observed_at: '2026-03-31T08:00:00Z', sha256: 'd'.repeat(64) },
    available_dates: ['2026-03-31'], family: 'ipv4', unit: 'raw_peer_afi_safi_prefix',
    metrics: { same: 3, different: 1, left_only: 2, right_only: 1, not_comparable: 1, comparable_pairs: 4, different_fraction: 0.25 }, examples: [], limits: ['两次观察'] }
  const payload = { state: 'available', version: 'retained-paths', query: { date: '2026-03-31', family: 'ipv4' },
    metadata: { path_comparison: comparison }, overview: { record_count: 7, visible_prefixes: null, visible_origin_ases: null } }
  get.mockResolvedValue({ data: payload })
  expect(await getCoreOverview()).toEqual(payload)
  for (const patch of [{ interval_change_count: 1 }, { session_continuity: 'confirmed' }, { family: 'ipv6' },
    { metrics: { ...comparison.metrics, comparable_pairs: 5 } }, { metrics: { ...comparison.metrics, different_fraction: 0.5 } },
    { state: 'unavailable' }, { examples: [{ family: 'ipv4', prefix: 'nonsense' }] }]) {
    get.mockResolvedValue({ data: { ...payload, metadata: { path_comparison: { ...comparison, ...patch } } } })
    await expect(getCoreOverview()).rejects.toThrow('路径')
  }
  get.mockResolvedValue({ data: { ...payload, query: { ...payload.query, date: '2026-03-30' } } })
  await expect(getCoreOverview()).rejects.toThrow('路径')
  get.mockResolvedValue({ data: { ...payload, state: 'unavailable', overview: null, trend: null, events: null } })
  await expect(getCoreOverview()).rejects.toThrow('路径')
  get.mockResolvedValue({ data: { ...payload, metadata: { path_comparison: { ...comparison,
    metrics: { ...comparison.metrics, same: 0, different: 0, comparable_pairs: 0, different_fraction: null } } } } })
  expect((await getCoreOverview()).metadata.path_comparison?.metrics?.different_fraction).toBeNull()
})

it('起源归属规模只在同日同族且额外证据有效时为数值；缺证据不能填数', async () => {
  const scale = { state: 'available', version: `rib_scale_v1_${'a'.repeat(64)}`, observed_at: '2026-03-31T08:00:00Z',
    available_dates: ['2026-03-31'], family: 'ipv4', source: { collector_id: 'rrc25', coverage: 'unknown', sha256: 'b'.repeat(64) },
    interpretation_version: 'rib-prefix-union/v1', origin_metric_state: 'available',
    origin: { version: `rib_origin_v1_${'c'.repeat(64)}`, interpretation_version: 'rib-attributed-origin/private-skip-v1', unattributed_entries: 1 as number | null, limits: ['单RIB'] } }
  const payload = { state: 'available', version: 'retained-origin', query: { date: '2026-03-31', family: 'ipv4' },
    metadata: { scale }, overview: { record_count: 7, visible_prefixes: 3 as number | null, visible_origin_ases: 2 as number | null } }
  get.mockResolvedValue({ data: payload })
  expect(await getCoreOverview()).toEqual(payload)
  payload.query.date = '2026-03-30'
  scale.state = scale.origin_metric_state = 'date_not_retained'
  payload.overview.visible_prefixes = null
  scale.origin.unattributed_entries = null
  await expect(getCoreOverview()).rejects.toThrow('起源')
  payload.overview.visible_origin_ases = null
  expect(await getCoreOverview()).toEqual(payload)
  get.mockResolvedValue({ data: { ...payload, metadata: {}, overview: { visible_prefixes: null, visible_origin_ases: 1 } } })
  await expect(getCoreOverview()).rejects.toThrow('规模')
})

it('数值规模必须有匹配日期、地址族和来源版本的单RIB依据', async () => {
  const payload = { state: 'available', version: 'retained-scale', query: { date: '2026-03-31', family: 'ipv4' },
    metadata: {}, overview: { record_count: 7, visible_prefixes: 3, visible_origin_ases: null } }
  get.mockResolvedValue({ data: payload })
  await expect(getCoreOverview()).rejects.toThrow('规模')
})

it('同日单RIB数值可读取；缺日、未知地址族或规模损坏保留null', async () => {
  const scale = { state: 'available', version: `rib_scale_v1_${'a'.repeat(64)}`, observed_at: '2026-03-31T08:00:00Z',
    available_dates: ['2026-03-31'], family: 'ipv4', source: { collector_id: 'rrc25', coverage: 'unknown', sha256: 'b'.repeat(64) },
    interpretation_version: 'rib-prefix-union/v1', origin_metric_state: 'pending_definition' }
  const payload = { state: 'available', version: 'retained-scale', query: { date: '2026-03-31', family: 'ipv4' },
    metadata: { scale }, overview: { record_count: 7, visible_prefixes: 3 as number | null, visible_origin_ases: null } }
  get.mockResolvedValue({ data: payload })
  expect(await getCoreOverview()).toEqual(payload)
  payload.query.date = '2026-03-30'
  await expect(getCoreOverview()).rejects.toThrow('规模')
  payload.overview.visible_prefixes = null
  scale.state = 'date_not_retained'
  expect(await getCoreOverview()).toEqual(payload)
  payload.query.date = '2026-03-31'
  payload.query.family = scale.family = 'unknown'
  scale.state = 'family_not_supported'
  expect(await getCoreOverview()).toEqual(payload)
  get.mockResolvedValue({ data: { ...payload, metadata: { scale: { state: 'unavailable', message: '规模校验失败' } } } })
  expect((await getCoreOverview()).overview?.visible_prefixes).toBeNull()
})

it('读取超时保留未完成语义，不能补零或伪造成功回执', async () => {
  const diagnostic = { schema_version: 'core-overview-diagnostic/v2', stage: 'source_read',
    reasons: [{ code: 'source_read_timeout', kind: 'all', count: null,
      evidence: { format: 'failed-day-read/v1', receipt_sha256: null, audit_sha256: null, failure_sha256: 'f'.repeat(64), finished_at: null } }] }
  const payload = { state: 'unavailable', version: 'retained-4', metadata: { available_dates: ['2026-03-31'], diagnostic_dates: ['2026-03-27'] },
    query: { date: '2026-03-27' }, overview: null, trend: null, events: null, diagnostic }
  get.mockRejectedValue({ response: { status: 503, data: payload } })
  expect(await getCoreOverview()).toEqual(payload)
  Object.assign(diagnostic.reasons[0]!, { count: 0 })
  await expect(getCoreOverview()).rejects.toThrow('诊断')
})

it('等级待核实保留两份原值，不能同时显示确定危险等级', async () => {
  const item = { reference: 'as_outage/2026-03-31 00:00:00/64512/1/r', level: null,
    level_conflict: { event_table: 'event_table_202603', event_level: 'low', detail_level: 'middle', source_input_sha256: 'a'.repeat(64) } }
  const payload = { state: 'available', version: 'retained-3', events: { items: [item] } }
  get.mockResolvedValue({ data: payload })
  expect(await getCoreOverview({ level: 'conflict' })).toEqual(payload)
  get.mockResolvedValue({ data: { ...payload, events: { items: [{ ...item, level: 'middle' }] } } })
  await expect(getCoreOverview()).rejects.toThrow('等级')
  get.mockResolvedValue({ data: { state: 'available', version: 'retained-3', item: { ...item, level: 'low' } } })
  await expect(getCoreOverviewRecord(item.reference, 'retained-3')).rejects.toThrow('等级')
})

it('公开查询保留未知状态，并把选定日期、地址族及版本传给只读接口', async () => {
  const payload = { state: 'window_not_retained', version: 'retained-1', overview: null, trend: null, events: null }
  get.mockResolvedValue({ data: payload })
  const query = { date: '2026-03-31', family: 'all' as const, version: 'retained-1' }
  expect(await getCoreOverview(query)).toEqual(payload)
  expect(get).toHaveBeenCalledWith('core-overview', { params: query, signal: undefined })
})

it('详情携带原引用和同一版本，不走可变旧库详情', async () => {
  get.mockResolvedValue({ data: { state: 'available', version: 'retained-1' } })
  await getCoreOverviewRecord('prefix_outage/2026-02-27 00:00:00/192.0.2.0-24/12/r', 'retained-1')
  expect(get).toHaveBeenCalledWith('core-overview/record', { params: { ref: 'prefix_outage/2026-02-27 00:00:00/192.0.2.0-24/12/r', version: 'retained-1' }, signal: undefined })
})

it('版本不匹配或伪成功不转换成可用数据', async () => {
  get.mockResolvedValueOnce({ data: { state: 'available', version: 'other' } })
  await expect(getCoreOverview({ version: 'retained-1' })).rejects.toThrow('版本')
  get.mockResolvedValueOnce({ data: { status: true } })
  await expect(getCoreOverview()).rejects.toThrow('响应')
})

it('单日文件校验失败仍能提供同版本日期目录，但不能产生成功指标', async () => {
  const payload = { state: 'unavailable', version: 'retained-1', message: '索引校验失败',
    metadata: { available_dates: ['2026-02-27', '2026-03-31'] },
    query: { date: '2026-03-31' }, overview: null, trend: null, events: null }
  get.mockRejectedValue({ response: { status: 503, data: payload } })
  expect(await getCoreOverview({ version: 'retained-1' })).toEqual(payload)
  await expect(getCoreOverview({ version: 'other' })).rejects.toThrow('版本')
  const failure = { response: { status: 503, data: { state: 'unavailable', message: '整个目录损坏' } } }
  get.mockRejectedValue(failure)
  const caught = await getCoreOverview().then(() => null, cause => cause)
  expect(caught).toEqual(failure)
})

it('失败诊断只在同版本不可用响应中保留，不可同时携带成功指标', async () => {
  const payload = { state: 'unavailable', version: 'retained-2', message: '源记录预检失败',
    metadata: { available_dates: ['2026-02-27'], diagnostic_dates: ['2026-03-31'] },
    query: { date: '2026-03-31' }, overview: null, trend: null, events: null,
    diagnostic: { version: `overview_diagnostic_v1_${'a'.repeat(64)}`, stage: 'source_field_precheck',
      reasons: [{ code: 'level_conflict', kind: 'as_outage', count: 14, evidence: { read_at: '2026-09-10T12:00:00Z' } }] } }
  get.mockRejectedValue({ response: { status: 503, data: payload } })
  expect(await getCoreOverview({ version:'retained-2' })).toEqual(payload)
  get.mockResolvedValue({ data: { ...payload, state: 'available', overview: { record_count: 14 } } })
  await expect(getCoreOverview()).rejects.toThrow('诊断')
})

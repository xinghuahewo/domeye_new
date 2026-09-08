import { h } from 'vue'
import { renderToString } from 'vue/server-renderer'
import { describe, expect, it } from 'vitest'
import type { CountryOutageGeneralPathDownstream, CountryOutageGeneralMetadata } from '@/types/api'
import CountryOutagePathEvidence from './CountryOutagePathEvidence.vue'

function fixture() {
  const relation: CountryOutageGeneralPathDownstream = {
    affected_asn: 64501, downstream_asn: 64503,
    downstream_as_name: null, downstream_organization: null, downstream_nature: null,
    downstream_name_state: 'unknown', downstream_organization_state: 'unknown', downstream_nature_state: 'unknown',
    observed_path_count: 4, associated_fixed_prefix_count: 2, independent_direction_count: 2,
    route_observation_count: 8, concurrent_state_point_count: 2,
    first_concurrent_state_point_utc: '2026-02-27T00:15:00Z', last_concurrent_state_point_utc: '2026-02-27T00:25:00Z',
    peak_concurrent_interrupted_prefix_count: 2, peak_concurrent_ipv4_address_count: 512,
    peak_concurrent_ipv6_slash48_count: 0,
    relationship_semantics: 'observed_ordered_rrc25_path_association_not_dependency_or_cause',
    path_samples: [{ prefix: '192.0.2.0/24', address_family: 'ipv4', as_path_id: 'sample-1',
      as_path_canonical: '64500 64501 64502 64503', independent_peer_asns: [64500], route_observation_count: 1 }],
  }
  const metadata = { collector_id: 'rrc25', publication_id: 'publication-test', revision: 1,
    window_start_utc: '2026-02-27T00:10:00Z', window_end_utc: '2026-02-27T00:30:00Z' } as CountryOutageGeneralMetadata
  return { relation, metadata }
}

describe('路径样本的实际观察范围', () => {
  it('保留含中间 ASN 的单条样本，同时明确缺少前后时点和比较输入', async () => {
    const html = await renderToString(h(CountryOutagePathEvidence, fixture()))
    expect(html).toContain('64500 64501 64502 64503')
    expect(html).toContain('192.0.2.0/24')
    expect(html).toContain('IPv4')
    expect(html).toContain('AS64500')
    expect(html).toContain('无法确认前后路径变化')
    expect(html).toContain('当前只返回 1 条样本，未提供可配对的前后路径。')
    expect(html).toContain('逐条观测时点：Unknown')
    expect(html).toContain('有序关联不保证直接邻接，也不表示依赖或原因。')
    expect(html).toContain('rrc25')
    expect(html).toContain('publication-test')
  })
  it('关系首末同期时点独立展示，不充当单条样本的持续区间', async () => {
    const html = await renderToString(h(CountryOutagePathEvidence, fixture()))
    expect(html).toContain('关系首次同期时点')
    expect(html).toContain('2026-02-27 08:15:00')
    expect(html).toContain('关系末次同期时点')
    expect(html).toContain('2026-02-27 08:25:00')
    expect(html).toContain('关系汇总的首末同期时点不代表每条样本的持续区间，也不保证中间连续。')
    const samples = html.match(/<ol[\s\S]*?<\/ol>/)?.[0] ?? ''
    expect(samples).not.toContain('2026-02-27 08:15:00')
    expect(html).toContain('2026-02-27 08:10:00')
    expect(html).toContain('Asia/Shanghai')
  })

  it('不同观察方向的路径并列可读，但不能按列表先后配成变化', async () => {
    const props = fixture()
    props.relation.path_samples.push({ ...props.relation.path_samples[0]!, as_path_id: 'sample-2',
      as_path_canonical: '64510 64501 64503', independent_peer_asns: [64510] })
    const html = await renderToString(h(CountryOutagePathEvidence, props))
    expect(html).toContain('AS64510')
    expect(html).toContain('样本的观察方向不一致，不能直接作为同方向的前后对照。')
    expect(html).toContain('列表顺序不代表时间顺序')
    props.relation.path_samples.reverse()
    expect(await renderToString(h(CountryOutagePathEvidence, props))).toContain('无法确认前后路径变化')
  })

  it('前缀或地址族不同需要另行匹配，不能直接比较路径文本', async () => {
    const props = fixture()
    props.relation.path_samples.push({ ...props.relation.path_samples[0]!, prefix: '2001:db8::/32', address_family: 'ipv6' })
    const html = await renderToString(h(CountryOutagePathEvidence, props))
    expect(html).toContain('IPv6')
    expect(html).toContain('样本的前缀或地址族不同，需要分别匹配，不能直接配成前后路径。')
  })

  it('同前缀、同方向的不同路径仍需时点对应，方向集合顺序不影响判断', async () => {
    const props = fixture()
    props.relation.path_samples[0]!.independent_peer_asns = [64500, 64510]
    props.relation.path_samples.push({ ...props.relation.path_samples[0]!, as_path_id: 'sample-2',
      as_path_canonical: '64500 64501 64503', independent_peer_asns: [64510, 64500] })
    const html = await renderToString(h(CountryOutagePathEvidence, props))
    expect(html).toContain('无法确认前后路径变化')
    expect(html).toContain('逐条观测时点：Unknown')
    expect(html).not.toContain('样本的观察方向不一致')
    expect(html).not.toContain('当前只返回 1 条样本')
  })

  it.each([null, 'invalid-time', '2026-02-27T00:15:00'])('关系时点缺少或无有效时区时保留 Unknown：%s', async time => {
    const props = fixture()
    props.relation.first_concurrent_state_point_utc = time
    props.relation.last_concurrent_state_point_utc = time
    const html = await renderToString(h(CountryOutagePathEvidence, props))
    expect(html).toContain('关系首次同期时点：Unknown')
    expect(html).toContain('关系末次同期时点：Unknown')
    expect(html).not.toContain('Invalid Date')
    expect(html).not.toContain('NaN')
    expect(html).toContain('192.0.2.0/24')
  })

  it('方向名单缺失时说明信息不足，不判定为与已知方向不一致', async () => {
    const props = fixture()
    props.relation.path_samples.push({ ...props.relation.path_samples[0]!, as_path_id: 'sample-2', independent_peer_asns: [] })
    const html = await renderToString(h(CountryOutagePathEvidence, props))
    expect(html).toContain('部分样本缺少观察方向，无法核对方向是否一致。')
    expect(html).toContain('独立观察方向：Unknown')
    expect(html).not.toContain('样本的观察方向不一致')
  })

})

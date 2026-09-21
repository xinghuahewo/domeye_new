"""尾接线纯值、签名及受控失败检查；无PG、科学运行或成功AD替身。"""
from copy import deepcopy
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock
import hashlib
import inspect
import json
import pytest

from data_pipeline.jobs.migration import run_fixed
from data_pipeline.jobs.downstream import measured, full_preflight, bind_feature_checkpoints
from data_pipeline.jobs.trend_reference import read_comparison, reference_for_source, ContextUnavailable
from data_pipeline.analysis.country_trends.contract import REFERENCE_DEFINITION


def comparison():
    return dict(origin_uri='fixture://comparison', historical_applicability='unknown',
        population='fixed_endpoint_direction', definition_binding=list(REFERENCE_DEFINITION),
        projections=[dict(country='AA', denominator=12, samples=[650,700,900], values=[12,6,12],
                          quality='complete', asn_count=None, persistent_asn_count=None)])


def source():
    from data_pipeline.analysis.country_events.compute import MetricPoint
    # 仅纯值接合示例，不是Country AD或公开读取证明。
    points = [MetricPoint('sample-cohort',t,'visible_direction_count','endpoint_direction_count',
                          Fraction(v),Fraction(v),12,Fraction(v,12),'complete')
              for t,v in [(700,0),(900,3)]]
    selected = {i:NS(target=NS(sample_us=p.sample_us),value=p.value,denominator=12) for i,p in enumerate(points)}
    return NS(event=('sample-event',3),status=NS(cohort_id='sample-cohort',incident=NS(country='ZZ')),
        originals=dict(enumerate(points)),window_us=(600,900),metric=selected.__getitem__,
        denominator=lambda:NS(value=12))


def test_actual_points_and_revision_preserved_external_values_subset_only():
    s = source(); ref = reference_for_source(s, comparison(), 'a'*64, 2)
    assert ref.event == ('sample-event',3) and ref.binding == ()
    target, external = ref.projections
    assert target.samples == (700,900) and target.values == (0,3)
    assert target.cohort_id == 'sample-cohort' and target.source_ref == 'row:2:0'
    assert external.samples == (700,900) and external.values == (6,12)
    assert external.asn_count is None and external.definition_binding == REFERENCE_DEFINITION


def test_unknown_main_and_missing_external_original_point_refuse_reference():
    s = source(); s.denominator=lambda:NS(value=None)
    with pytest.raises(ContextUnavailable,match='主值'):
        reference_for_source(s,comparison(),'a'*64,0)
    raw = comparison(); raw['projections'][0]['samples']=[650,700]
    raw['projections'][0]['values']=[12,6]
    with pytest.raises(ContextUnavailable,match='禁止插值'):
        reference_for_source(source(),raw,'a'*64,0)


def test_external_unknown_is_not_filled_with_zero_or_complete():
    raw = comparison(); raw['projections'][0].update(values=[12,None,12],quality='unknown')
    ref = reference_for_source(source(),raw,'b'*64,0)
    assert ref.projections[1].values == (None,12)
    assert ref.projections[1].quality == 'unknown'


def test_fixed_source_and_independent_acceptance_gate_before_any_tail_input(monkeypatch):
    import data_pipeline.jobs.downstream as tail
    monkeypatch.setattr(tail.subprocess,'check_output',Mock(side_effect=['a'*40,'']))
    with pytest.raises(ValueError,match='尚未独立接受'):
        full_preflight(NS(input_profile='artificial/v1'),dict(enabled=True,execution_code='a'*40,tail=dict(accepted=False)))
    monkeypatch.setattr(tail.subprocess,'check_output',Mock(return_value='b'*40))
    with pytest.raises(ValueError,match='干净'):
        full_preflight(NS(input_profile='artificial/v1'),dict(enabled=True,execution_code='a'*40))


def test_incomplete_graph_refused_before_artificial_or_publication():
    from data_pipeline.jobs.downstream import combined_request
    # 故意不完整的标签只用于拒绝路径，不构造成功AD。
    artificial = Mock()
    with pytest.raises(ValueError,match='依赖缺失'):
        combined_request([(object(),dict(owner='country',admission_id='invalid-label',dependencies=['absent']))],artificial)
    artificial.verify.assert_not_called()


def test_raw_hash_and_duplicate_time_refused(tmp_path):
    p = tmp_path/'reference.json'; p.write_text(json.dumps(comparison()))
    sha = hashlib.sha256(p.read_bytes()).hexdigest()
    assert read_comparison(p,sha)==comparison()
    with pytest.raises(ValueError,match='SHA'):
        read_comparison(p,'0'*64)
    raw = comparison(); raw['projections'][0]['samples']=[650,650,900]
    p.write_text(json.dumps(raw))
    with pytest.raises(ValueError,match='投影'):
        read_comparison(p,hashlib.sha256(p.read_bytes()).hexdigest())


def test_disabled_and_real_refusal_before_files_or_pg(tmp_path):
    with pytest.raises(ValueError,match='未显式启用'):
        run_fixed(object(),dict(enabled=False),tmp_path/'out',Mock())
    assert not (tmp_path/'out').exists()
    with pytest.raises(ValueError,match='真实全天'):
        full_preflight(NS(input_profile='rrc25-fixed/v1'),dict(enabled=True))
    with pytest.raises(ValueError,match='输出根'):
        run_fixed(object(),dict(enabled=True,invocation_root=str(tmp_path/'declared')),
                  tmp_path/'different',Mock(),full=True)
    assert not (tmp_path/'different').exists()


def test_measured_failure_and_failed_receipt_keep_primary(tmp_path,monkeypatch):
    import data_pipeline.jobs.stage_runner as observation
    with pytest.raises(ValueError,match='原错'):
        with measured(tmp_path/'failed',lambda:None):raise ValueError('原错')
    value = json.loads((tmp_path/'failed/阶段观测.json').read_text())
    assert value['state']=='failed' and value['rss_coverage'] in ('sampled', 'unavailable')
    monkeypatch.setattr(observation,'save_new',Mock(side_effect=OSError('ENOSPC')))
    with pytest.raises(ValueError,match='原错') as caught:
        with measured(tmp_path/'disk',lambda:None):raise ValueError('原错')
    assert caught.value.migration_cleanup_errors[0]['message']=='ENOSPC'


def test_checkpoint_quality_uses_actual_digest_without_mutating_draft():
    from data_pipeline.bgp.archive.checkpoint import sha
    cp=dict(source_id='s',ordinal=11,raw='verified_source_eof',ingest='complete',parse='complete',
            counts=dict(messages=3,decoded=3,rejected=0,unsupported=0))
    cp['digest']=sha(cp)
    config=dict(feature_sources={'s':dict(window=dict(coverage='complete'),message_quality_state='unknown',message_quality_refs=[])})
    old=deepcopy(config)
    result=bind_feature_checkpoints(NS(sources=lambda _:['s']),dict(run_id='sample',snapshot=9,checkpoints=[cp]),config)
    assert config==old and result['feature_sources']['s']['message_quality_state']=='complete'
    assert result['feature_sources']['s']['message_quality_refs']==[f"m2:sample:9:checkpoint:11:{cp['digest']}"]
    cp['counts']['decoded']=2
    with pytest.raises(ValueError,match='完整CP'):
        bind_feature_checkpoints(NS(sources=lambda _:['s']),dict(run_id='sample',snapshot=9,checkpoints=[cp]),config)


def test_original_owner_signatures_are_available_without_runtime_or_db():
    from data_pipeline.analysis.country_events.route_source_reader import M3Source
    from data_pipeline.analysis.country_events.qualified_store import M3ComponentWriter
    from data_pipeline.analysis.country_events.qualified_index import prepare_qualified_country_index
    from data_pipeline.analysis.country_trends.runtime import ProductionRuntime
    from data_pipeline.analysis.country_trends.snapshot_inputs import register_reference
    from data_pipeline.results import Publication
    from data_pipeline.results.fixture_metadata import ArtificialInput
    assert 'scratch_root' in inspect.signature(M3Source.capture).parameters
    assert 'window_us' in inspect.signature(M3ComponentWriter).parameters
    assert 'source' in inspect.signature(prepare_qualified_country_index).parameters
    assert 'feature_selections' in inspect.signature(ProductionRuntime).parameters
    assert 'private_root' in inspect.signature(register_reference).parameters
    assert 'combined_input' in inspect.signature(Publication).parameters
    assert 'trend_reference' in inspect.signature(ArtificialInput).parameters


def test_preflight_path_isolation_and_calculation_window(tmp_path,monkeypatch):
    import data_pipeline.jobs.downstream as module
    from data_pipeline.analysis.country_events.selection_contract import QueryLimits
    from data_pipeline.analysis.country_events.qualified_reader import ResultLimits
    from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
    from dataclasses import asdict
    root=tmp_path.resolve(); raw=root/'raw';raw.mkdir();run=root/'new-run'
    p=raw/'comparison.json';p.write_text(json.dumps(comparison()))
    dsn=f'host={root} dbname=metadata_only'
    m2=root/'m2.json';m2.write_text(json.dumps(dict(dsn=dsn,read_policy='strict/v1')))
    def path(n):return str(run/n)
    country=dict(dsn=dsn,window_us=[600000000,900000000],
        grid=dict(input_start_us=300000000,input_end_us=900000000,samples_us=[700000000,900000000]),
        input_limits=dict(max_rows=1),capture=dict(scratch_root=path('capture'),max_rows=1),
        c2_limits=dict(max_rows=1),component_limits=dict(max_rows=1),audit_limits=dict(max_rows=1),
        qualification_limits=dict(max_rows=1),query_limits=asdict(QueryLimits()),
        component_root=path('component'),query_root=path('query'),c2_scratch_root=path('c2'),
        runtime=dict(allowed_roots=[str(root)],scratch_root=path('country-scratch'),limits=asdict(ResultLimits())))
    trend=dict(dsn=dsn,window_us=country['window_us'],runtime=dict(output_root=path('trend'),
        allowed_roots=[str(root)],scratch_root=path('trend-scratch'),limits=asdict(S2Limits())))
    ref=dict(dsn=dsn,window_us=country['window_us'],source_path=str(p),
             source_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),derived_root=path('derived'),limits=asdict(S2Limits()))
    q=dict(enabled=True,execution_code='a'*40,observation_dsn=dsn,run_root=str(run),
        invocation_root=path('invocation'),output_root=path('products'),m2=dict(mode='checkpoint',config=str(m2)),
        runtime={n:dict(scratch_root=path(n+'-scratch')) for n in ('resource','feature','canonical','detection')},
        tail=dict(accepted=True,country=country,trend=trend,reference=ref,
                  publication=dict(dsn=dsn,private_root=path('publication'),expected_generation=0,limits=dict(max_rows=1))))
    q['runtime']['m2']=dict(scratch_parent=path('m2-scratch'))
    windows={'D_result':['1970-01-01T00:10:00Z','1970-01-01T00:15:00Z'],
             'canonical_detection_computation':['1970-01-01T00:05:00Z','1970-01-01T00:15:00Z']}
    fixed=NS(input_profile='artificial/v1',artificial=dict(source_root=str(raw)),window=windows.__getitem__)
    monkeypatch.setattr(module.subprocess,'check_output',lambda args,**kw:'a'*40 if args[1]=='rev-parse' else '')
    # 只验证纯配置接线；既不构造Runtime，也不证明预算/AD/生产成功。
    full_preflight(fixed,q)
    assert not run.exists()
    bad=deepcopy(q);bad['tail']['reference']['derived_root']=str(raw/'derived')
    with pytest.raises(ValueError,match='冻结原件'):
        full_preflight(fixed,bad)
    bad=deepcopy(q);bad['tail']['country']['grid']['input_start_us']=400000000
    with pytest.raises(ValueError,match='原计算窗'):
        full_preflight(fixed,bad)
    assert not run.exists()
    m2doc=dict(dsn=dsn,read_policy='strict/v1')
    for location,reason in [(str(raw/'lake'),'冻结原件'),(str(root/'outside'),'独立运行根'),
                            (str(run/'country-scratch'/'lake'),'分别隔离')]:
        m2.write_text(json.dumps(dict(m2doc,catalog_data_path=location)))
        with pytest.raises(ValueError,match=reason):full_preflight(fixed,q)
    existing=run/'old-lake';existing.mkdir(parents=True)
    m2.write_text(json.dumps(dict(m2doc,catalog_data_path=str(existing))))
    with pytest.raises(ValueError,match='旧产物'):full_preflight(fixed,q)
    alias=run/'alias';alias.symlink_to(raw,target_is_directory=True)
    m2.write_text(json.dumps(dict(m2doc,catalog_data_path=str(alias/'lake'))))
    with pytest.raises(ValueError,match='别名'):full_preflight(fixed,q)
    m2.write_text(json.dumps(dict(m2doc,require_readonly_inputs=True,preflight_receipt=str(raw/'receipt.json'))))
    with pytest.raises(ValueError,match='冻结原件'):full_preflight(fixed,q)
    m2.write_text(json.dumps(dict(m2doc,require_readonly_inputs=True,preflight_receipt=path('preflight.json'))))
    full_preflight(fixed,q)  # 原CLI条件写文件合法；未创建该文件或任何producer输出。
    assert not Path(path('preflight.json')).exists()


def test_context_failure_receipt_cannot_replace_original_error(tmp_path,monkeypatch):
    import data_pipeline.jobs.trend_reference as context
    import data_pipeline.jobs.stage_runner as observation
    from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
    from dataclasses import asdict
    p=tmp_path/'raw.json';p.write_text(json.dumps(comparison()))
    cfg=dict(limits=asdict(S2Limits()),source_path=str(p),source_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),window_us=[600,900])
    original=ContextUnavailable('原上下文不足')
    # 在公开捕获起点故意失败，不返回或伪造任何成功AD/读取证明。
    monkeypatch.setattr(context,'capture_country',Mock(side_effect=original))
    monkeypatch.setattr(context,'save_new',Mock(side_effect=OSError('ENOSPC')))
    monkeypatch.setattr(observation,'save_new',Mock(side_effect=OSError('ENOSPC')))
    with pytest.raises(ContextUnavailable,match='原上下文不足') as caught:
        context.prepare_context(cfg,(None,None),(None,None),tmp_path/'out',lambda:None)
    assert caught.value is original
    assert caught.value.migration_cleanup_errors[0]['message']=='ENOSPC'

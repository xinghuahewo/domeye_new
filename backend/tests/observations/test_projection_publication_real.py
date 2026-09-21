"""Canonical real-candidate 配置边界；真实 PG 使用既有人工制品单独验收。"""
from copy import deepcopy
from pathlib import Path
import pytest
from data_pipeline.bgp.replay import snapshot_admission as p
from data_pipeline.bgp.replay import snapshot_access as io
from data_pipeline.bgp.replay.snapshot_validation import RULES, INPUT_RULES, CODEC
from data_pipeline.bgp.replay.snapshot_contract import PROFILE, TABLES


def params(tmp_path):
    root=tmp_path.resolve()
    for name in ('input','output','data','scratch'):(root/name).mkdir(exist_ok=True)
    manifest=dict(schema_version='observation-run/v1',inputs=[],window_start='1970-01-01T00:00:00Z',window_end_exclusive='1970-01-02T00:00:00Z')
    binding=dict(run_id='a'*32,snapshot=1,seal_digest='b'*64,profile=PROFILE)
    plan=dict(RULES,input_binding=dict(INPUT_RULES),input_manifest=manifest,selected_sources=[],references=[],code={'old.py':'a'*64},
              baseline_endpoints=[],limitations=['cutover_assumed','source_order_declared','session_continuity_unknown'],resource_limits={})
    d=dict(binding=binding,database={'data_root':str(root/'data')},schema='m3_'+binding['run_id'],manifest_sha='c'*64,
           control_root=str(root/'output'),control_entity={},state='complete',plan=plan,codec=CODEC,tables={t:{} for t in TABLES},files=[])
    return dict(dsn=f'host={root} dbname=synthetic',allowed_roots=(root,),scratch_root=root/'scratch',
                dependency_admissions=(),dependency_runtimes={},execution_profile='real-candidate/v1',fixture_only=False,
                input_manifest=manifest,expected_canonical_binding=dict(binding=binding,descriptor=d),output_root=root/'output',
                memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,max_rss_bytes=4*1024**3,lock_timeout_ms=2000,
                max_batch_rows=100,max_batch_bytes=1024**2,max_total_rows=1000,max_total_bytes=4*1024**2,min_free_bytes=1)


def test_explicit_required_fields_and_budgets(tmp_path):
    base=params(tmp_path);p.Runtime(**base)
    for field in ('execution_profile','input_manifest','expected_canonical_binding','output_root',*io.BUDGETS):
        with pytest.raises(ValueError):p.Runtime(**{**base,field:None})
    with pytest.raises(ValueError):p.Runtime(**{**base,'fixture_only':True})
    for value in (True,float('nan'),float('inf'),0):
        with pytest.raises(ValueError):p.Runtime(**{**base,'max_rss_bytes':value})


def test_mode_fixed_binding_and_configuration_are_checked_at_use(tmp_path):
    base=params(tmp_path)
    for field,value in [('fixture_only',True),('fixture_only',0),('execution_profile','unknown'),('dsn','host=/tmp dbname=other'),
                        ('scratch_root',tmp_path.resolve()),('allowed_roots',()),('output_root',tmp_path.resolve())]:
        rt=p.Runtime(**base);setattr(rt,field,value)
        with pytest.raises(ValueError):rt.check()
    rt=p.Runtime(**base);rt.expected_canonical_binding['binding']['snapshot']=99
    with pytest.raises(ValueError):rt.check()
    rt=p.Runtime(**base);rt.input_manifest['window_start']='1970-01-01T00:01:00Z'
    with pytest.raises(ValueError):rt.check()
    rt=p.Runtime(**base);wrong=deepcopy(rt.expected_canonical_binding);wrong['binding']['snapshot']=2
    with pytest.raises(ValueError):rt.check_binding(wrong)


def test_scratch_output_and_original_manifest_must_match(tmp_path):
    base=params(tmp_path)
    with pytest.raises(ValueError):p.Runtime(**{**base,'scratch_root':base['output_root']})
    with pytest.raises(ValueError):p.Runtime(**{**base,'output_root':tmp_path.resolve()/'data'})
    manifest={**base['input_manifest'],'window_start':'1970-01-01T00:00:01Z'}
    with pytest.raises(ValueError):p.Runtime(**{**base,'input_manifest':manifest})
    rt=p.Runtime(**base);rt.max_total_rows=2000;rt.check()  # 合法预算调整不改变固定范围。
    assert p._rules(rt)['rules_digest']!=p._rules()['rules_digest']


def test_fixture_defaults_preserved(tmp_path):
    root=tmp_path.resolve()
    rt=p.Runtime(f'host={root} dbname=fixture',(root,),root,(),{},fixture_only=True)
    assert rt.memory_bytes==256*1024**2 and rt.max_batch_rows==10000
    with pytest.raises(ValueError):p.Runtime(f'host={root} dbname=fixture',(root,),root,(),{},fixture_only=True,memory_bytes=None)
    rt.check();rt.fixture_only=False
    with pytest.raises(ValueError):rt.check()

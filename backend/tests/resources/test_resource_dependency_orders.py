"""纯原绑定接合：只替换外部准入/PG核验，不把人工字典当可信Admission。"""
from copy import deepcopy
from types import SimpleNamespace
import pytest
from data_pipeline.analysis.resources import publication as p
from data_pipeline.bgp.archive import admission as up


def bindings(monkeypatch):
    ids=['late','early','middle'];bid='whole-input-binding'
    cps=[{'source_id':sid,'ordinal':i} for i,sid in enumerate(['csv',*ids])]
    full=dict(binding={'sources':[{'source_id':sid} for sid in ids]},seal={'checkpoints':cps},manifest={'inputs':ids})
    original=dict(input_binding_id=bid,input_binding=p.canonical(full['binding']),ordered_source_ids=ids,seal=full['seal'],plan={'manifest':full['manifest']},run_id='run',snapshot=1)
    m=dict(owner='m2',admission_id='m',owner_binding=up.typed(original))
    ref=dict(owner='reference',admission_id='r',owner_binding=up.typed(dict(source_id='csv',m2_binding=original,checkpoint_ordinal=0,m2_admission_id='m')))
    manifest=dict(observation_runs={bid:full},sources=[{'context':{'source_id':sid}} for sid in ['early','middle','late']],
        observation_inputs={sid:dict(binding_id=bid,source_rank=i,checkpoint=cps[i+1]) for i,sid in enumerate(ids)},
        csv_reference={'source_id':'csv','run_id':'run','snapshot':1},csv_observation={'checkpoint':cps[0]})
    monkeypatch.setattr(up,'admission_shape',lambda a:None)
    monkeypatch.setattr(up,'verify_current',lambda *a,**kw:None)
    return SimpleNamespace(fixture_only=True,upstream_runtime=object(),dependency_admissions=(m,ref)),{'binding':manifest}


def test_nonmonotone_global_ranks_preserve_both_orders(monkeypatch):
    rt,b=bindings(monkeypatch);before=deepcopy(b)
    assert [x['admission_id'] for x in p._dependencies(rt,b,lambda:None)]==['m','r']
    assert b==before
    m=b['binding'];assert [m['observation_inputs'][s['context']['source_id']]['source_rank'] for s in m['sources']]==[1,2,0]


@pytest.mark.parametrize('bad',['rank','missing','duplicate','seal','checkpoint','fields','duplicate_admission'])
def test_wrong_dependency_still_rejected(monkeypatch,bad):
    rt,b=bindings(monkeypatch);m=b['binding']
    if bad=='rank':m['observation_inputs']['early']['source_rank']=0
    elif bad=='duplicate':m['sources'].append(deepcopy(m['sources'][0]))
    elif bad=='seal':m['observation_runs']['whole-input-binding']['seal']=dict(checkpoints=[])
    elif bad=='checkpoint':m['observation_inputs']['early']['checkpoint']={'source_id':'early','ordinal':999}
    elif bad=='fields':m['observation_runs']['whole-input-binding']['binding']={'sources':[]}
    elif bad=='duplicate_admission':rt.dependency_admissions=(*rt.dependency_admissions,rt.dependency_admissions[0])
    else:
        original=up.untyped(rt.dependency_admissions[0]['owner_binding']);original['ordered_source_ids']=['late','middle']
        rt.dependency_admissions[0]['owner_binding']=up.typed(original)
    with pytest.raises(ValueError):p._dependencies(rt,b,lambda:None)

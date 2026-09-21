"""固定 Feature M3 公开绑定及资格读取；raw可审计，主值不因模块complete而可用。"""
from contextlib import closing
import json
import hashlib
import resource
import sys
import psycopg2
from data_pipeline.analysis.features.store import read_table, TABLES as RAW_TABLES
from data_pipeline.analysis.features.qualification import PROFILE, SCHEMA_VERSION, RULE, DIMENSIONS, TABLES, ORDER_KEYS, rows_digest
from data_pipeline.bgp.archive.selection import Selection
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.bgp.archive.store import connect_duckdb, literal


def _resource_guard(callback, maximum):
    def check():
        callback()
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)>maximum:
            raise ValueError('Feature公开读取RSS保护触发')
    return check


def _coverage_rows(dsn,binding,allow_fixture,guard):
    for batch in read_table(dsn,binding['run_id'],binding['snapshot'],'qualifications',profile=PROFILE,allow_fixture=allow_fixture):
        guard()
        yield from batch.to_pylist()


def _specification(dsn, run_id, snapshot, allow_fixture):
    if not isinstance(run_id,str) or not run_id.isalnum() or type(snapshot) is not int or snapshot<0:raise ValueError('固定run/snapshot格式无效')
    with closing(psycopg2.connect(dsn)) as pg, pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT state,snapshot,schema_name,specification FROM feature.runs WHERE run_id=%s',(run_id,))
            row=c.fetchone()
    if row is None or row[:3]!=('complete',snapshot,'f_'+run_id):raise ValueError('Feature固定结果不存在/未完成/错版')
    spec=row[3]
    if spec.get('output_profile')!=PROFILE or spec.get('output_schema')!=SCHEMA_VERSION or spec.get('qualification_rule')!=RULE:
        raise ValueError('未实现或缺失的Feature资格profile')
    if not allow_fixture and spec['code_identity'].get('execution_mode')!='frozen-fresh-process':
        raise ValueError('正式读取拒绝fixture')
    with closing(psycopg2.connect(dsn)) as pg, pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT snapshot,receipt FROM feature.qualified_results WHERE run_id=%s',(run_id,))
            result=c.fetchone()
            if result is None or result[0]!=snapshot or result[1]['specification_digest']!=identity(spec):
                raise ValueError('Feature资格完成锚缺失或规格漂移')
    return spec,result[1]


def inspect_binding(dsn, run_id, snapshot, *, allow_fixture=False, guard=lambda:None, max_rss_bytes=1024**3):
    """返回可固定保存的绑定；核验当前M2封存资格、完整资格枚举和外键。

    下游调用时传回完整binding以拒绝漂移。不把观察business=not_run改成complete。
    数据集物理文件的发布封装/长期保管由发布owner负责。
    """
    guard=_resource_guard(guard,max_rss_bytes);guard()
    spec,completion=_specification(dsn,run_id,snapshot,allow_fixture)
    for bound in spec['observation_seals']:
        guard()
        selection=Selection(dsn,bound['run_id'],bound['snapshot'])
        if selection.seal!=bound['seal']:raise ValueError('输入封存身份漂移')
        connection=selection.connect();connection.close()
    with closing(psycopg2.connect(dsn)) as pg, pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT system_identifier FROM pg_control_system()');system_identifier=str(c.fetchone()[0])
            c.execute('SELECT oid FROM pg_database WHERE datname=current_database()');database_oid=c.fetchone()[0]
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+" AS lake (READ_ONLY, METADATA_SCHEMA "+literal('fl_'+run_id)+')')
        counts={t:db.execute(f'SELECT count(*) FROM lake.f_{run_id}.{t} AT (VERSION => {snapshot})').fetchone()[0] for t in {**RAW_TABLES,**TABLES}}
        if counts!=completion['actual_rows']:raise ValueError('固定输出必要表计数漂移')
    finally:db.close()
    saved={}
    for table in TABLES:
        saved[table]=[]
        for batch in read_table(dsn,run_id,snapshot,table,profile=PROFILE,allow_fixture=allow_fixture):
            guard();saved[table].extend(batch.to_pylist())
    for table,keys in ORDER_KEYS.items():
        saved[table].sort(key=lambda r:tuple(r[k] for k in keys))
        if rows_digest(saved[table])!=completion['qualification_hashes'][table]:raise ValueError('固定资格数据内容摘要漂移')
    gaps={(r['mode'],r['gap_id']):r for r in saved['input_gaps']}
    qualities={(r['mode'],r['evidence_id']):r for r in saved['source_qualities']}
    if len(gaps)!=len(saved['input_gaps']) or len(qualities)!=len(saved['source_qualities']):raise ValueError('重复资格证据')
    receipts={(r['mode'],r['source_id']):r for r in saved['qualification_receipts']}
    expected={(m,s) for m in ('ordinary','ir') for s in spec['source_ids']}
    if set(receipts)!=expected or len(receipts)!=len(saved['qualification_receipts']):raise ValueError('缺少资格来源')
    grouped={key:[] for key in expected}
    for q in saved['qualifications']:
        key=(q['mode'],q['source_id'])
        if key not in grouped:raise ValueError('资格来源未绑定')
        plain={k:v for k,v in q.items() if k not in ('qualification_id','window_role')}
        if q['qualification_id']!=identity(plain) or q['rule']!=RULE or q['coverage'] not in ('complete','partial','unknown','not_applicable'):
            raise ValueError('资格身份/版本/枚举损坏')
        for refs,available in [(q['gap_refs'],gaps),(q['quality_refs'],qualities)]:
            for ref in refs:
                evidence=available.get((q['mode'],ref))
                if evidence is None or evidence['source_rank']>q['source_rank']:raise ValueError('资格FK缺失/未来引用')
        grouped[key].append({k:v for k,v in q.items() if k!='window_role'})
    sources={s['source_id']:s for s in spec['source_bindings']}
    anchored={(r['mode'],r['source_id']):r for r in completion['qualification_receipts']}
    for key,found in receipts.items():
        if {k:v for k,v in found.items() if k!='window_role'}!=anchored.get(key):raise ValueError('独立资格回执偏离完成锚')
    for gap in saved['input_gaps']:
        raw_ref={k:gap[k] for k in ('source_id','content_sha256','record','offset','length','raw_digest')}
        natural=json.dumps([gap['gap_rule'],raw_ref,gap['interpretation_digest']],sort_keys=True,separators=(',',':'))
        if hashlib.sha256(natural.encode()).hexdigest()!=gap['gap_id']:raise ValueError('Gap原始引用身份损坏')
        source=sources.get(gap['source_id'])
        if source is None or (gap['source_rank'],gap['upstream_source_rank'],gap['binding_ref'],gap['content_sha256'])!=(source['sequence'],source['upstream_source_rank'],source['ordered_binding_ref'],source['content_sha256']):
            raise ValueError('Gap来源绑定冲突')

    for key,receipt in receipts.items():
        qs=grouped[key]
        if len(qs)!=len(DIMENSIONS) or {q['dimension'] for q in qs}!=set(DIMENSIONS):raise ValueError('资格维度缺失')
        qs.sort(key=lambda q:DIMENSIONS.index(q['dimension']))
        source=sources[key[1]]
        if (receipt['binding_ref'],receipt['upstream_source_rank'],receipt['source_rank'])!=(source['ordered_binding_ref'],source['upstream_source_rank'],source['sequence']):
            raise ValueError('来源资格映射冲突')
        if identity(qs)!=receipt['qualification_digest'] or receipt['qualifications']!=len(qs):raise ValueError('资格回执摘要冲突')
        for name,column in [('input_gaps','gaps'),('source_qualities','qualities')]:
            if sum((r['mode'],r['source_id'])==key for r in saved[name])!=receipt[column]:raise ValueError('证据回执计数冲突')
    binding=dict(specification=spec,reference_version=completion['reference_version'],catalog_ref=dict(system_identifier=system_identifier,database_oid=database_oid),completion_digest=identity(completion),profile=PROFILE,schema_version=SCHEMA_VERSION,run_id=run_id,snapshot=snapshot,
                 specification_digest=identity(spec),qualification_digest=identity(sorted(q['qualification_id'] for q in saved['qualifications'])),
                 table_counts={k:len(v) for k,v in saved.items()},input_seals=[{'run_id':b['run_id'],'snapshot':b['snapshot'],'seal_digest':b['seal']['digest']} for b in spec['observation_seals']])
    binding['binding_id']=identity(binding)
    return binding


def read_windows(dsn, binding, *, window_role=None, allow_fixture=False, batch_rows=1000, guard=lambda:None, max_rss_bytes=1024**3):
    """流式科学行附主值/各维度资格；无ASN行覆盖用read_coverage另读。

    读取起止复验绑定；调用者必须耗尽，不能将早停或尾核验失败发布为完成。
    """
    guard=_resource_guard(guard,max_rss_bytes);guard()
    run,snapshot=binding['run_id'],binding['snapshot']
    if inspect_binding(dsn,run,snapshot,allow_fixture=allow_fixture,guard=guard,max_rss_bytes=max_rss_bytes)!=binding:raise ValueError('Feature公开绑定漂移')
    coverage={(q['mode'],q['source_id'],q['dimension']):q for q in _coverage_rows(dsn,binding,allow_fixture,guard)}
    for batch in read_table(dsn,run,snapshot,'windows',profile=PROFILE,allow_fixture=allow_fixture,window_role=window_role,batch_rows=batch_rows):
        guard()
        for raw in batch.to_pylist():
            qualifications={d:coverage[raw['mode'],raw['source_id'],d] for d in DIMENSIONS}
            yield qualified_window(raw,qualifications)
    if inspect_binding(dsn,run,snapshot,allow_fixture=allow_fixture,guard=guard,max_rss_bytes=max_rss_bytes)!=binding:raise ValueError('Feature读取末资格漂移')


def read_coverage(dsn,binding,*,allow_fixture=False,guard=lambda:None,max_rss_bytes=1024**3):
    """包括零元素/无dirty/无ASN行窗口，缺失不补complete。"""
    guard=_resource_guard(guard,max_rss_bytes);guard()
    def check():
        if inspect_binding(dsn,binding['run_id'],binding['snapshot'],allow_fixture=allow_fixture,guard=guard,max_rss_bytes=max_rss_bytes)!=binding:
            raise ValueError('Feature覆盖绑定漂移')
    check()
    yield from _coverage_rows(dsn,binding,allow_fixture,guard)
    check()


def qualified_window(raw, qualifications):
    values = {k: raw[k] for k in ('v4Prefix_num', 'v6Prefix_num', 'v4IP_num', 'announ_num', 'withdraw_num')}
    available = lambda d: qualifications[d]['coverage'] == 'complete'
    main = {k: (v if available('resources') else None) for k, v in values.items() if k not in ('announ_num', 'withdraw_num')}
    for key, dimension in [('announ_num', 'announcement_attribution'), ('withdraw_num', 'withdrawal_attribution')]:
        main[key] = values[key] if available('window_counts') and (raw['scope'] == 'collect' or available(dimension)) else None
    return dict(raw=raw, raw_values=values, values=main, qualifications=qualifications)

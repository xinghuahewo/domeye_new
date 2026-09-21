"""独立 RIB 的有限资格；不推导连续路由恢复，不改写科学原值。"""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json

import psycopg2
from data_pipeline.analysis.resources.identity import identity_digest
from data_pipeline.analysis.resources.store import TABLES

PROFILE = 'resource-observation/v1'
VERSION = 'resource-qualification/v1'
EXTRA_TABLES = {
    'input_receipts': [('source_id','VARCHAR'),('binding_id','VARCHAR'),('source_rank','BIGINT'),
        ('checkpoint_digest','VARCHAR'),('attempt','VARCHAR'),('messages','BIGINT'),('elements','BIGINT'),
        ('decoded','BIGINT'),('rejected','BIGINT'),('unsupported','BIGINT'),('gaps','BIGINT'),
        ('peers','BIGINT'),('quality_records','BIGINT'),('first_epoch','BIGINT'),('last_epoch','BIGINT')],
    'peer_dependencies': [('source_id','VARCHAR'),('table_record','BIGINT'),('peer_index','BIGINT'),
        ('peer_ip','VARCHAR'),('peer_asn','BIGINT'),('bgp_id','VARCHAR'),('bgp_id_present','BOOLEAN')],
    'observation_quality': [('source_id','VARCHAR'),('record','BIGINT'),('code','VARCHAR'),('detail','VARCHAR')],
    'coverage': [('source_id','VARCHAR'),('dimension','VARCHAR'),('status','VARCHAR'),('reason','VARCHAR'),
        ('scope','VARCHAR'),('window_start','TIMESTAMPTZ'),('window_end','TIMESTAMPTZ'),('qualification_version','VARCHAR')],
    'qualifications': [('source_id','VARCHAR'),('qualification_id','VARCHAR'),('target','VARCHAR'),
        ('dimension','VARCHAR'),('bucket','VARCHAR'),('metric','VARCHAR'),('status','VARCHAR'),
        ('reason','VARCHAR'),('scope','VARCHAR'),('qualification_version','VARCHAR')],
    'qualification_dependencies': [('source_id','VARCHAR'),('qualification_id','VARCHAR'),('kind','VARCHAR'),('dependency_id','VARCHAR')],
}
ALL_TABLES = {**TABLES, **EXTRA_TABLES}
STATUSES = {'qualified','unknown','not_applicable'}


def pg_identity(dsn):
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as cur:
            cur.execute('SELECT system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),current_database() FROM pg_control_system()')
            system,oid,name=cur.fetchone()
    return dict(system_identifier=system,database_oid=oid,database_name=name)


def check_table_set(dsn,schema,snapshot):
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as cur:
            cur.execute('''SELECT t.table_name FROM public.ducklake_table t JOIN public.ducklake_schema s USING(schema_id)
                WHERE s.schema_name=%s AND s.begin_snapshot<=%s AND (s.end_snapshot IS NULL OR s.end_snapshot>%s)
                AND t.begin_snapshot<=%s AND (t.end_snapshot IS NULL OR t.end_snapshot>%s)''',
                (schema,snapshot,snapshot,snapshot,snapshot))
            names=[r[0] for r in cur.fetchall()]
    if len(names)!=len(ALL_TABLES) or set(names)!=set(ALL_TABLES):raise ValueError('实际快照表枚举不完整或含未知表')


def normalized(value):
    if isinstance(value,datetime):return ['datetime',value.astimezone(timezone.utc).isoformat()]
    if isinstance(value,Decimal):return ['decimal',str(value)]
    if isinstance(value,bytes):return ['bytes',value.hex()]
    if isinstance(value,(list,tuple)):return [normalized(v) for v in value]
    if isinstance(value,dict):return {k:normalized(v) for k,v in value.items()}
    return value


def table_inventory(db, relation, guard=lambda:None):
    """全量分批内容核对，排序由受限 DuckDB 承担；RSS 不含 PG。"""
    result={}
    for table in ALL_TABLES:
        digest=hashlib.sha256();count=0;size=0
        for batch in db.execute('SELECT * FROM '+relation(table)+' ORDER BY ALL').fetch_record_batch(4096):
            guard()
            for row in batch.to_pylist():
                raw=json.dumps(normalized(row),sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()+b'\n'
                digest.update(raw);count+=1;size+=len(raw)
        result[table]=dict(rows=count,logical_bytes=size,sha256=digest.hexdigest())
    return result


def expected_qualifications(db, relation, manifest, guard=lambda:None):
    """由持久科学行与已核对输入回执重建资格，而非相信调用方自报 qualified。"""
    def rows(table):
        cur=db.execute('SELECT * FROM '+relation(table))
        result=[]
        for batch in cur.fetch_record_batch(4096):
            guard();result.extend(batch.to_pylist())
        return result
    receipts={r['source_id']:r for r in rows('input_receipts')}
    sources={r['source_id']:r for r in rows('sources')}
    point={sid:(r['elements']>0 and r['first_epoch'] is not None and r['first_epoch']==r['last_epoch']==sources[sid]['snapshot_time'].timestamp() and not (r['rejected'] or r['unsupported'] or r['gaps'])) for sid,r in receipts.items()}
    preceding={sid:{p for p in sources if sources[p]['snapshot_time']<sources[sid]['snapshot_time']} for sid in sources}
    window=[datetime.fromisoformat(t) for t in manifest['result_window']]
    coverage=[];qualifications=[];dependencies=[];binding_id=identity_digest(manifest)
    for sid in sources:
        for dimension,status,reason,scope in (
            ('rib_point','qualified' if point[sid] else 'unknown','independent_rib_verified' if point[sid] else 'point_time_or_closed_scope_unproven','independent_rib'),
            ('historical_continuity','unknown','cold_start_not_continuous_history','declared_sample_set'),
            ('reference_history','unknown','reference_effectivity_unproven','independent_rib'),
            ('normal_history','qualified' if len(preceding[sid])>=6 and all(point[p] for p in preceding[sid]) else 'unknown','declared_predecessors_only','declared_sample_set')):
            coverage.append(dict(source_id=sid,dimension=dimension,status=status,reason=reason,scope=scope,
                window_start=window[0],window_end=window[1],qualification_version=VERSION))
    def add(sid,target,dimension,bucket,metric,ok,reason,scope,refs):
        row=dict(source_id=sid,target=target,dimension=dimension,bucket=bucket,metric=metric,
            status='qualified' if ok else 'unknown',reason=reason,scope=scope,qualification_version=VERSION)
        qid=identity_digest({'binding':binding_id,**row})
        if len(qualifications)%256==0:guard()
        qualifications.append({**row,'qualification_id':qid})
        for kind,dep in sorted(set(refs)):
            dependencies.append(dict(source_id=sid,qualification_id=qid,kind=kind,dependency_id=dep))
    from data_pipeline.analysis.resources.compute import METRIC_UNITS
    metric_rows=rows('metrics')
    metric_index={(r['source_id'],r['dimension'],r['bucket']):r for r in metric_rows}
    normal={}
    for r in rows('normal_samples'):
        sample=metric_index.get((r['sample_source'],r['dimension'],r['bucket']))
        if sample is None or r['metric'] not in METRIC_UNITS or sample[r['metric']]!=r['value'] or sample['time']!=r['sample_time']:
            raise ValueError('normal样本原值与所依赖科学行不符')
        normal.setdefault((r['source_id'],r['dimension'],r['bucket'],r['metric']),[]).append(r)
    from data_pipeline.analysis.resources.validation import validate_normal_history
    band_rows=rows('normal_bands')
    validate_normal_history(sources,metric_rows,band_rows,normal,guard)
    normal_ok={};normal_dependencies={}
    for r in band_rows:
        sid=r['source_id'];key=(sid,r['dimension'],r['bucket'],r['metric']);samples=normal.get(key,[])
        if any(s['sample_source'] not in sources or s['sample_time']!=sources[s['sample_source']]['snapshot_time'] for s in samples):
            raise ValueError('normal样本依赖缺失或时点不符')
        if len(samples)!=r['list_len'] or len({s['sample_source'] for s in samples})!=len(samples):raise ValueError('normal样本数量或唯一性不符')
        previous={s['sample_source'] for s in samples if s['sample_time']<sources[sid]['snapshot_time']}
        ok=(point[sid] and all(point[p] for p in preceding[sid]) and len(previous)>=6 and bool(samples) and all(point[s['sample_source']] for s in samples)
            and r['upper_bound'] is not None and r['lower_bound'] is not None)
        normal_ok.setdefault((sid,r['dimension'],r['bucket']),[]).append(ok)
        normal_dependencies.setdefault((sid,r['dimension'],r['bucket']),set()).update(s['sample_source'] for s in samples)
        add(sid,'normal_bands',r['dimension'],r['bucket'],r['metric'],ok,
            'six_prior_samples_verified' if ok else 'normal_preconditions_insufficient','declared_sample_set',
            [('rib',sid),*(('normal_sample',s['sample_source']) for s in samples),*(('normal_history',p) for p in preceding[sid])])
    for r in metric_rows:
        sid=r['source_id'];key=(sid,r['dimension'],r['bucket'])
        for metric in (*METRIC_UNITS,'ipv6_cidr_count','is_outlier','as_name','as_rank'):
            ok=point[sid] and r[metric] is not None
            refs=[('rib',sid)];scope='independent_rib';reason='independent_rib_verified'
            if metric=='is_outlier':
                ok=ok and bool(normal_ok.get(key)) and all(normal_ok[key]);scope='declared_sample_set';reason='normal_preconditions'
                refs.extend(('normal_sample',sample) for sample in normal_dependencies.get(key,()))
                refs.extend(('normal_history',p) for p in preceding[sid])
            elif metric in ('as_name','as_rank'):
                ok=ok and bool(r['reference_row_ref']);refs.append(('reference','csv'));reason='fixed_reference_version_only'
            add(sid,'metrics',r['dimension'],r['bucket'],metric,ok,reason,scope,refs)
    for r in rows('topology_status'):
        sid=r['source_id'];ok=point[sid] and r['country_scope']=='reference_label' and r['status'] in ('computed','no_edges')
        add(sid,'topology_status','country',r['country_cn'],'graph',ok,'fixed_reference_version_only' if ok else 'topology_or_reference_limited',
            'independent_rib',[('rib',sid),('reference','country')])
    return dict(coverage=coverage,qualifications=qualifications,qualification_dependencies=dependencies)


def validate_relations(db, relation, manifest, guard=lambda:None, *, dsn):
    def rows(table):
        cur=db.execute('SELECT * FROM '+relation(table));cols=[d[0] for d in cur.description]
        return [dict(zip(cols,r)) for r in cur.fetchall()]
    sources=rows('sources');receipts=rows('input_receipts');selected=manifest['sources']
    expected={s['context']['source_id']:s for s in selected}
    if len(sources)!=len(expected) or {s['source_id'] for s in sources}!=set(expected) or len(receipts)!=len(expected) or {r['source_id'] for r in receipts}!=set(expected):
        raise ValueError('完整所选RIB与独立输入回执枚举不符')
    for row in receipts:
        sid=row['source_id'];item=manifest['observation_inputs'][sid];cp=item['checkpoint'];counts=cp['counts']
        for key in ('messages','elements','decoded','rejected','unsupported'):
            if row[key]!=counts[key]:raise ValueError('输入回执伪计数：'+key)
        if row['rejected'] or row['unsupported'] or row['gaps']:raise ValueError('损坏RIB不能由M3容错')
        if (row['binding_id'],row['source_rank'],row['checkpoint_digest'],row['attempt'])!=(item['binding_id'],item['source_rank'],cp['digest'],cp['attempt']):
            raise ValueError('来源rank或checkpoint绑定不符')
        peer_count=db.execute('SELECT count(*) FROM '+relation('peer_dependencies')+' WHERE source_id=?',[sid]).fetchone()[0]
        if peer_count!=cp['tables']['peers']['count'] or peer_count!=row['peers']:raise ValueError('Peer表依赖不完整')
        quality_count=db.execute('SELECT count(*) FROM '+relation('observation_quality')+' WHERE source_id=?',[sid]).fetchone()[0]
        if quality_count!=cp['tables']['quality']['count'] or quality_count!=row['quality_records']:raise ValueError('原始质量记录遗漏')
        source=next(s for s in sources if s['source_id']==sid);binding=expected[sid]
        if any(source[k]!=binding['context'][k] for k in ('collector','content_sha256','origin_uri','reference_id')) or source['snapshot_time']!=datetime.fromisoformat(binding['context']['snapshot_time']):
            raise ValueError('结果RIB身份漂移')
        if (source['upstream_run'],source['upstream_snapshot'],source['purpose'])!=(binding['run_id'],binding['snapshot'],binding['purpose']):raise ValueError('结果来源用途或版本漂移')
        if (source['element_count'],source['min_epoch'],source['max_epoch'])!=(row['elements'],row['first_epoch'],row['last_epoch']):raise ValueError('RIB时点范围/计数漂移')
    from data_pipeline.analysis.resources.validation import validate_m2_dependencies
    validate_m2_dependencies(dsn,db,relation,manifest,guard)
    expected_rows=expected_qualifications(db,relation,manifest,guard)
    for table,wanted in expected_rows.items():
        key=lambda r:json.dumps(normalized(r),sort_keys=True,ensure_ascii=False)
        if sorted(map(key,rows(table)))!=sorted(map(key,wanted)):raise ValueError('资格或依赖不完整：'+table)

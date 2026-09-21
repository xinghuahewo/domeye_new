"""只接受正式模块产出的人工制品；复核实际登记、原Reader及catalog。"""
import json
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
import psycopg2
from psycopg2 import sql
from data_pipeline.bgp.archive.message_reader import ObservationReader, SourceEnd
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.analysis.resources.store import TABLES as RESOURCE_TABLES, scan_resource
from data_pipeline.analysis.resources.references import read_reference
from data_pipeline.analysis.features.inputs import FeatureInputs
from data_pipeline.analysis.features.store import TABLES as FEATURE_TABLES, DIAGNOSTICS_VERSION, read_table
from data_pipeline.results.manifest_io import encode, require, file_hash

QUALIFICATIONS = {
 'domeye.resource_runs':'run_id', 'feature.runs':'run_id',
 'domeye.runs':'run_id','domeye.run_specs':'run_id','domeye.inputs':'run_id',
 'domeye.source_receipts':'run_id','domeye.reference_inputs':'run_id',
 'domeye.resource_references':'reference_id', 'feature.source_commits':'run_id',
}


def qualification(cursor, table, key, lock=False):
    require(table in QUALIFICATIONS,'未知资格表')
    cursor.execute(sql.SQL('SELECT to_jsonb(t) FROM {} t WHERE {}=%s'+(' FOR SHARE' if lock else '')).format(
        sql.Identifier(*table.split('.')),sql.Identifier(QUALIFICATIONS[table])),(key,))
    return sorted((r[0] for r in cursor.fetchall()),key=encode)


def instance(cursor):
    cursor.execute('SELECT system_identifier::text FROM pg_control_system()'); system=cursor.fetchone()[0]
    cursor.execute('SELECT oid::text FROM pg_database WHERE datname=current_database()'); oid=cursor.fetchone()[0]
    cursor.execute("SELECT current_setting('listen_addresses')")
    require(cursor.fetchone()[0]=='','Q1仅允许无TCP监听的私有PG')
    return {'system_identifier':system,'database_oid':oid}


def catalog(cursor, metadata, schema, snapshot):
    require(metadata=='public' or (metadata.startswith('fl_') and metadata[3:].isalnum()),'未知catalog')
    ident=lambda t:sql.Identifier(metadata,t)
    cursor.execute(sql.SQL("SELECT value FROM {} WHERE key='data_path'").format(ident('ducklake_metadata')))
    root=cursor.fetchone(); require(root is not None,'目录缺失')
    root=Path(root[0]).resolve()
    cursor.execute(sql.SQL('SELECT schema_uuid,path,path_is_relative FROM {} WHERE schema_name=%s AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)').format(ident('ducklake_schema')),(schema,snapshot,snapshot))
    found=cursor.fetchall();require(len(found)==1,'schema版本缺失')
    uuid,path,relative=found[0]; schema_path=(root/path if relative else Path(path)).resolve()
    cursor.execute(sql.SQL('SELECT table_id,table_uuid,table_name,path,path_is_relative FROM {} WHERE schema_id=(SELECT schema_id FROM {} WHERE schema_uuid=%s) AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s) ORDER BY table_name').format(ident('ducklake_table'),ident('ducklake_schema')),(uuid,snapshot,snapshot))
    tables=cursor.fetchall(); files=[]
    for tid,tuuid,name,tp,rel in tables:
        table_path=(schema_path/tp if rel else Path(tp)).resolve()
        cursor.execute(sql.SQL('SELECT path,path_is_relative,file_size_bytes,record_count FROM {} WHERE table_id=%s AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s) ORDER BY path').format(ident('ducklake_data_file')),(tid,snapshot,snapshot))
        for fp,fr,size,count in cursor.fetchall():
            files.append({'table':name,'path':str((table_path/fp if fr else Path(fp)).resolve()),'bytes':size,'rows':count})
    return {'metadata_schema':metadata,'data_path':str(root),'schema':schema,'schema_uuid':uuid,
            'snapshot':snapshot,'tables':[[r[1],r[2]] for r in tables],'files':files}


def inspect(dsn, kind, receipt_path, private_root, guard=lambda:None):
    require(kind in ('resource','feature'),'Q1能力未实现')
    root=Path(private_root).resolve(); path=Path(receipt_path).resolve()
    require(path.is_relative_to(root),'回执不在本任务私有目录')
    report=json.loads(path.read_text());run=report['run_id']; snap=report['snapshot']
    require(run.isalnum() and type(snap) is int and snap>=0,'run/snapshot非法')
    require(report['state']=='ready','必须是原ready文件')
    table='domeye.resource_runs' if kind=='resource' else 'feature.runs'
    q=[]; layouts=[]; paths={str(path)}; observations={}; observed_counts={}
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            dbid=instance(c)
            def save(t,key):
                rows=qualification(c,t,key);require(bool(rows),'缺少实际资格')
                q.append({'table':t,'key':key,'rows':rows}); return rows
            actual=save(table,run);require(len(actual)==1,'模块登记不唯一'); actual=actual[0]
            require(actual['state']=='complete' and actual['snapshot']==snap,'模块未完成或错版')
            if kind=='resource':
                require(actual['dataset_id']==report['dataset_id'] and actual['binding_manifest']==report['binding_manifest'] and actual['code_digest']==report['code_digest'],'Resource登记与回执不符')
                require(actual['execution_mode']=='frozen-fresh-process' and report['code_identity']['execution_mode']=='frozen-fresh-process','拒绝非正式模块身份')
                bm=report['binding_manifest']
                for source in bm['sources']:
                    observations.setdefault((source['run_id'],source['snapshot']),[]).append(source['context']['source_id'])
                ref=bm['csv_reference']; observations.setdefault((ref['run_id'],ref['snapshot']),[]).append(ref['anchor_source_id'])
                country=bm['country_reference']; refid=country['reference_id']
                saved=save('domeye.resource_references',refid)[0]
                require(saved['state']=='complete','参考未完成')
                require(read_reference(dsn,refid,country['dataset_id'])[1]==country,'参考身份漂移')
                paths.add(saved['manifest']['raw_path'])
                layouts.append(catalog(c,'public',country['lake_schema'],country['snapshot']))
                metadata='public';schema='resource_'+run
            else:
                require(actual['specification']==report['specification'],'Feature登记与回执不符')
                spec=report['specification']
                require(spec['code_identity']['execution_mode']=='frozen-fresh-process','拒绝非正式模块身份')
                if spec.get('source_bindings'):
                    for source in spec['source_bindings']:
                        for view in [source,*source['aliases']]:
                            observations.setdefault((view['run_id'],view['snapshot']),[]).append(view['source_id'])
                    ref=spec['reference_binding']
                    observations.setdefault((ref['run_id'],ref['snapshot']),[]).extend(ref['reader_qualification_sources'])
                else:
                    observations[spec['observation_run'],spec['observation_snapshot']]=spec['source_ids']
                commits=save('feature.source_commits',run)
                require({(r['mode'],r['source_id']) for r in commits}=={(m,s) for m in ('ordinary','ir') for s in spec['source_ids']},'Feature来源完成提交缺失')
                metadata='fl_'+run;schema='f_'+run
            require(actual['schema_name']==schema,'模块schema错误')
            layouts.append(catalog(c,metadata,schema,snap))
            if kind=='feature':
                require({t[1] for t in layouts[-1]['tables']}==set(feature_tables(spec)), 'Feature固定版本表集合不符')
            for (obsrun,obssnap),ids in observations.items():
                ids=list(dict.fromkeys(ids))
                reader=ObservationReader(dsn,obsrun,obssnap,ids)
                connection=reader.connect();connection.close()
                for t in ('domeye.runs','domeye.run_specs','domeye.inputs','domeye.source_receipts'):
                    save(t,obsrun)
                if reader.manifest.get('references'):
                    refs=save('domeye.reference_inputs',obsrun)
                    require(all(r['state']=='validated' for r in refs),'参考资格未验证')
                    if kind=='feature' and (obsrun,obssnap)==reference_key(spec):
                        require(any(r['source_id']==spec['reference_sha256'] and r['row_count']==spec['reference_expected_rows'] for r in refs),'Feature参考计数漂移')
                    for ref in reader.manifest['references']:
                        for _ in reader.reference_batches(ref['sha256']):guard()
                layouts.append(catalog(c,'public','r_'+obsrun,obssnap))
                for entry in reader.manifest['inputs']:
                    require(Path(entry['path']).resolve().is_relative_to(root),'原输入不在私有目录')
                    require(entry['origin_uri'].startswith('fixture://'),'Q1禁止真实来源')
                    require(entry['source_id']==source_identity(reader.manifest['collector'],entry['origin_uri'],entry['sha256']),'来源身份错误')
                    require(file_hash(entry['path'],guard)==entry['sha256'],'原输入摘要损坏')
                    paths.add(entry['path'])
                for ref in reader.manifest.get('references',[]):
                    require(Path(ref['path']).resolve().is_relative_to(root),'参考不在私有目录')
                    require(file_hash(ref['path'],guard)==ref['sha256'],'参考原件摘要损坏');paths.add(ref['path'])
                # Reader实际消费完整消息/元素流，验证双计数和资格，不靠提交者填数。
                for record in reader.stream():
                    guard()
                    if isinstance(record,SourceEnd):
                        observed_counts[observation_key(obsrun,obssnap,record.source_id)]=[record.messages,record.elements]
    for layout in layouts:
        for f in layout['files']:
            p=Path(f['path']);require(p.resolve().is_relative_to(root),'catalog文件不在私有目录');require(p.stat().st_size==f['bytes'],'目录文件大小不符');paths.add(str(p))
    for p in paths: require(Path(p).resolve().is_relative_to(root),'制品越过私有目录')
    before_hashes={p:file_hash(p,guard) for p in sorted(paths)}
    feature_sources=None
    if kind=='feature':
        historical=feature_history(dsn,run,snap,guard)
        if spec.get('source_bindings'):
            observed_counts={view_key(view):observed_counts[observation_key(view['run_id'],view['snapshot'],view['source_id'])]
                             for source in spec['source_bindings'] for view in [source,*source['aliases']]}
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:
            validate_feature_sources(c,run,spec,observed_counts,historical)
        feature_sources={'version':'bound-views/v1','observed_counts':observed_counts,'historical':historical,
                         'source_bindings':spec.get('source_bindings'),
                         'diagnostics':validate_diagnostics(dsn,run,snap,spec,historical,guard)}
    counts={}
    for name in (RESOURCE_TABLES if kind=='resource' else feature_tables(spec)):
        guard();counts[name]=sum(batch.num_rows for batch in scan(dsn,kind,run,snap,name,scope='all'))
    require(counts==report['actual_rows'],'实际历史行数与完成回执不符')
    require(all(file_hash(p,guard)==sha for p,sha in before_hashes.items()),'扫描期间文件内容漂移')
    return {'kind':kind,'run_id':run,'snapshot':snap,'schema_name':schema,
            'catalog_ref':{**dbid,'metadata_schema':metadata},'receipt_path':str(path),
            'receipt_sha256':file_hash(path,guard),'report':report,'qualifications':q,
            'layouts':layouts,'files':sorted(paths),'file_hashes':before_hashes,'actual_rows':counts,
            'feature_sources':feature_sources}


def scan(dsn,kind,run,snapshot,table,scope='result',batch_rows=256):
    if kind=='resource':
        # 公共scan_resource使用登记snapshot；接缝额外核对调用者固定版本。
        def fixed():
            with psycopg2.connect(dsn) as pg:
                pg.set_session(readonly=True)
                with pg.cursor() as c:
                    c.execute('SELECT state,snapshot FROM domeye.resource_runs WHERE run_id=%s',(run,))
                    require(c.fetchone()==('complete',snapshot),'Resource读取错版或资格漂移')
        fixed()
        yield from scan_resource(dsn,run,table,batch_size=batch_rows,scope=scope)
        fixed()
    else:
        yield from read_table(dsn,run,snapshot,table,batch_rows=batch_rows)


def verify(cursor, binding, lock=False):
    require(instance(cursor)=={k:binding['catalog_ref'][k] for k in ('system_identifier','database_oid')},'数据库实例身份漂移')
    for q in binding['qualifications']:
        require(qualification(cursor,q['table'],q['key'],lock)==q['rows'],'固定资格撤销或漂移')
    for old in binding['layouts']:
        require(catalog(cursor,old['metadata_schema'],old['schema'],old['snapshot'])==old,'catalog目录或固定版本漂移')
    if binding['kind']=='feature':
        proof=binding.get('feature_sources')
        # 70c合法旧token未存新证明：复用其冻结上游资格，补查固定小型来源回执。
        if proof is None:
            spec=binding['report']['specification']
            upstream=next(q['rows'] for q in binding['qualifications']
                          if q['table']=='domeye.source_receipts' and q['key']==spec['observation_run'])
            proof={'observed_counts':{r['source_id']:[r['message_count'],r['element_count']] for r in upstream}}
        # 查询复用已封存关系；发布最后门禁（或70c兼容）只重读小型来源回执。
        historical=(feature_history(cursor.connection.dsn,binding['run_id'],binding['snapshot'])
                    if lock or 'historical' not in proof else proof['historical'])
        validate_feature_sources(cursor,binding['run_id'],binding['report']['specification'],
                                 proof['observed_counts'],historical)


def feature_history(dsn,run,snapshot,guard=lambda:None):
    rows=[]
    for batch in read_table(dsn,run,snapshot,'source_receipts'):
        guard();rows.extend(batch.to_pylist())
    return rows


def validate_feature_sources(cursor,run,spec,observed,historical):
    """上游Reader实际计数→上游登记→双模式提交→固定历史回执，逐源交叉核对。"""
    commits=qualification(cursor,'feature.source_commits',run)
    if spec.get('source_bindings'):
        validate_views(cursor,spec,observed,historical)
        upstream={s['source_id']:observed[view_key(s)]
                  for s in spec['source_bindings']}
    else:
        upstream=qualification(cursor,'domeye.source_receipts',spec['observation_run'])
        upstream={r['source_id']:[r['message_count'],r['element_count']] for r in upstream}
    expected={(mode,source) for mode in ('ordinary','ir') for source in spec['source_ids']}
    key=lambda r:(r['mode'],r['source_id'])
    require(len(commits)==len(expected) and {key(r) for r in commits}==expected,'Feature提交来源集合错误')
    require(len(historical)==len(expected) and {key(r) for r in historical}==expected,'Feature历史来源集合错误')
    history={key(r):r for r in historical}
    for commit in commits:
        source=commit['source_id'];receipt=history[key(commit)]
        if spec.get('source_bindings'):
            actual=upstream.get(source)
        else:
            actual=observed.get(observation_key(spec['observation_run'],spec['observation_snapshot'],source),observed.get(source))
        require(actual is not None and upstream.get(source)==actual
                and [commit['messages'],commit['elements']]==actual
                and [receipt['messages'],receipt['elements']]==actual,'Feature消息/元素与固定上游不一致')
        require(all(commit[n]==receipt[n] for n in ('previous_projection','projection_version')),
                'Feature提交与历史投影引用不一致')
    for mode in ('ordinary','ir'):
        previous=None
        for rank,source in enumerate(spec['source_ids']):
            receipt=history[mode,source]
            require(receipt['source_rank']==rank,'Feature来源顺序不一致')
            if rank:require(receipt['previous_projection']==previous,'Feature跨源投影链不一致')
            previous=receipt['projection_version']


def observation_key(run,snapshot,source):
    return encode([run,snapshot,source])


def view_key(view):
    return encode([view[n] for n in ('run_id','snapshot','source_id','source_role','calculation_role','origin_uri','content_sha256')])


def reference_key(spec):
    ref=spec.get('reference_binding')
    return (ref['run_id'],ref['snapshot']) if ref else (spec['observation_run'],spec['observation_snapshot'])


def validate_views(cursor,spec,observed,historical):
    validate_windows(spec)
    sources=spec['source_bindings']
    require([s['source_id'] for s in sources]==spec['source_ids'] and len(set(spec['source_ids']))==len(sources),
            'Feature主来源顺序或唯一性错误')
    for rank,source in enumerate(sources):
        require(type(source['sequence']) is int and source['sequence']==rank and source['aliases'],'Feature来源序号或别名缺失')
        role='initial_rib' if rank==0 else 'update'
        require(source['calculation_role']==role,'Feature计算角色错误')
        require(source['reference_sha256']==spec['reference_sha256'],'Feature来源参考版本不符')
        for view in [source,*source['aliases']]:
            require(all(view[n]==source[n] for n in ('source_id','origin_uri','content_sha256','calculation_role',
                    'reference_sha256','expected_messages','expected_elements','message_quality_refs','message_quality_state')),
                    'Feature别名身份或计算用途冲突')
            require({k:v for k,v in view['window'].items() if k!='input_version'}==
                    {k:v for k,v in source['window'].items() if k!='input_version'},'Feature别名窗口冲突')
            require(view['window']['input_version']==view['run_id']+':'+str(view['snapshot']),'Feature来源窗口错版')
            require(view['source_role'] in (('baseline','snapshot') if rank==0 else ('update',)), 'Feature原角色错误')
            manifest=qualification(cursor,'domeye.run_specs',view['run_id'])[0]['manifest']
            require(manifest['collector']==spec['collector'],'Feature业务collector不符')
            entry=next((e for e in manifest['inputs'] if e['source_id']==view['source_id']),None)
            require(entry is not None and (entry['origin_uri'],entry['sha256'],entry['role'])==
                    (view['origin_uri'],view['content_sha256'],view['source_role']),'Feature原来源身份不符')
            receipt=next((r for r in qualification(cursor,'domeye.source_receipts',view['run_id']) if r['source_id']==view['source_id']),None)
            actual=observed.get(view_key(view))
            require(receipt is not None and actual==[view['expected_messages'],view['expected_elements']]
                    ==[receipt['message_count'],receipt['element_count']],'Feature逐视图消息/元素不符')
        for row in (r for r in historical if r['source_id']==source['source_id']):
            require(all(row[a]==source[b] for a,b in [('upstream_run_id','run_id'),('upstream_snapshot','snapshot'),
                    ('source_role','source_role'),('calculation_role','calculation_role'),('origin_uri','origin_uri'),
                    ('content_sha256','content_sha256')]),'Feature历史上游身份不符')
            require(row['window_role']==source['window_role'],'Feature历史窗口用途不符')
    ref=spec['reference_binding']
    require(ref['source_sha256']==spec['reference_sha256'] and ref['expected_rows']==spec['reference_expected_rows'],
            'Feature参考绑定不符')
    manifest=qualification(cursor,'domeye.run_specs',ref['run_id'])[0]['manifest']
    require(manifest['collector']==ref['carrier_collector'],'Feature参考载体身份不符')
    receipts=qualification(cursor,'domeye.reference_inputs',ref['run_id'])
    require(any(r['source_id']==ref['source_sha256'] and r['state']=='validated' and r['row_count']==ref['expected_rows'] for r in receipts),
            'Feature独立参考资格不符')


def feature_tables(spec):
    version=spec.get('diagnostics_dataset_version')
    require(version in (None,DIAGNOSTICS_VERSION),'Feature诊断版本不支持')
    return [name for name in FEATURE_TABLES if name!='module_diagnostics' or version is not None]


def validate_diagnostics(dsn,run,snapshot,spec,historical,guard):
    feature_tables(spec)
    if spec.get('diagnostics_dataset_version') is None:
        return {'state':'not_saved','dataset_version':None}
    counts={(r['mode'],r['source_id']):0 for r in historical}
    for batch in read_table(dsn,run,snapshot,'module_diagnostics'):
        guard()
        for row in batch.to_pylist():
            key=row['mode'],row['source_id']
            require(key in counts and row['dataset_version']==DIAGNOSTICS_VERSION,'诊断来源或版本不符')
            rank=row['source_rank']
            require(type(rank) is int and 0<=rank<len(spec['source_bindings']), '诊断来源序号越界或类型错误')
            source=spec['source_bindings'][rank]
            receipt=next(r for r in historical if (r['mode'],r['source_id'])==key)
            require(type(source['sequence']) is int and type(receipt['source_rank']) is int and
                    rank==source['sequence']==receipt['source_rank'], '诊断来源序号与固定来源回执不符')
            require(source['source_id']==row['source_id'] and source['window_role']==row['window_role'], '诊断来源顺序/窗口不符')
            counts[key]+=1
    for row in historical:
        require(row['diagnostics']==counts[row['mode'],row['source_id']] and
                row['diagnostics_state']==('not_applicable' if row['source_rank']==0 else 'saved'), '诊断来源计数或保存态不符')
        require(row['source_rank']!=0 or row['diagnostics']==0,'初始基线不应含窗口诊断')
    return {'state':'saved','dataset_version':DIAGNOSTICS_VERSION,
            'sources':[{'mode':r['mode'],'source_id':r['source_id'],'count':r['diagnostics'],'state':r['diagnostics_state']} for r in historical]}


def validate_windows(spec):
    """复核Feature已有声明合同；允许缺口，不要求填满交付/比较包络。"""
    def time(value):
        require(isinstance(value,str),'窗口时间必须是带时区文本')
        try:parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        except ValueError:raise ValueError('窗口时间格式非法') from None
        require(parsed.tzinfo is not None and parsed.utcoffset() is not None,'窗口时间必须带时区')
        return parsed
    def bounds(value):
        if value is None:return None
        require(isinstance(value,(list,tuple)) and len(value)==2,'窗口边界必须为二元区间')
        pair=tuple(time(t) for t in value)
        require(pair[0]<pair[1],'窗口必须为非空半开区间')
        return pair
    result=bounds(spec['result_window']);comparison=bounds(spec['comparison_window'])
    require(comparison is None or result is not None and comparison[1]==result[0], '比较窗口必须紧邻结果窗口之前')
    sources=spec['source_bindings'];windows=spec['windows']
    require(bool(sources) and len(windows)==len(sources),'窗口摘要来源数不符')
    parsed=[]
    context=SimpleNamespace(result_window=result,comparison_window=comparison)
    for source,summary in zip(sources,windows):
        raw=source['window'];start,end=bounds([raw['start'],raw['end']]);file_time=time(raw['file_time'])
        window=SimpleNamespace(start=start,end=end,file_time=file_time);parsed.append(window)
        expected=FeatureInputs.window_role(context,SimpleNamespace(calculation_role=source['calculation_role'],window=window))
        require(source['window_role']==expected,'来源窗口用途与整体窗口不符')
        require(summary['source_id']==source['source_id'] and
                all(time(summary[n])==getattr(window,n) for n in ('start','end','file_time')) and
                summary['coverage']==raw['coverage'] and
                all(summary[n]==source[n] for n in ('message_quality_state','message_quality_refs')),
                '重复窗口摘要与固定来源不符')
    expected=(parsed[1].start,parsed[-1].end) if len(parsed)>1 else None
    require(bounds(spec['calculation_window'])==expected,'计算窗口与首末UPDATE包络不符')
    require(time(spec['initial_rib_time'])==parsed[0].file_time,'初态时间与固定来源不符')

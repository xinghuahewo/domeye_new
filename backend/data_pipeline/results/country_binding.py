"""国家发布来源资格：显式运行角色、固定封存证明及普通事务锁。"""
from contextlib import contextmanager, ExitStack
from dataclasses import asdict
import json
import hashlib
from pathlib import Path

import psycopg2
from data_pipeline.analysis.country_events.selection_contract import CountryAdmissionDescriptor, CountryRuntime, result_id, typed_decode, contract_json, contract_value, REQUIRED, VERSION
from data_pipeline.analysis.country_events.selection_admission import verify_country_admission
from data_pipeline.results import resource_feature_bindings as bindings
from data_pipeline.results.manifest_io import require, digest, file_hash, stamp


def config(p):
    cfg=p.country
    require(isinstance(cfg,dict) and set(cfg)=={'runtime','observation_dsn','detection_dsn','production_receipt'},'国家须显式绑定运行角色')
    require(type(cfg['runtime']) is CountryRuntime,'国家运行配置类型错误')
    c1=cfg['runtime'].c1
    if c1 is not None:
        for actual,expected in ((c1.reader.dsn,cfg['observation_dsn']),(c1.detection.dsn,cfg['detection_dsn'])):
            require(psycopg2.extensions.parse_dsn(actual)==psycopg2.extensions.parse_dsn(expected),'离线C1未使用显式绑定角色')
    return cfg


def identity(value):
    return {'system_identifier':value['system_id'],'database_oid':str(value['database_oid'])}


def entities(p,descriptor,*,full=False):
    require(type(descriptor) is CountryAdmissionDescriptor,'国家准入描述类型错误')
    require(descriptor.capabilities==REQUIRED and descriptor.query_version==VERSION,'国家必需view未完整实现')
    b=descriptor.read_binding
    require(type(b.component.database_oid) is int and type(b.component.snapshot) is int and b.component.database_oid>0 and b.component.snapshot>=0,'国家组件数值身份非法')
    require(b.result_id==result_id(b.component),'国家结果身份不符')
    require(bool(descriptor.entities),'国家冻结实体缺失')
    for entity in descriptor.entities:
        path=Path(entity.path).resolve()
        require(path.is_relative_to(p.root),'国家制品未在显式私有根绑定')
        current=stamp(entity.path)
        require(current[:4]==[entity.device,entity.inode,entity.size,entity.mtime_ns],'国家冻结文件实体漂移')
        if full:require(file_hash(path,p.guard)==entity.sha256,'国家冻结文件内容漂移')


def rows(c,table,run,lock):
    return bindings.qualification(c,table,run,lock)


def current(p,descriptor,stack,*,lock=False):
    """单次角色连接内批量资格；不调用C1._check或扫描业务正文/全run COUNT。"""
    cfg=config(p);b=descriptor.read_binding.component
    upstream=typed_decode(descriptor.upstream_typed)
    require(isinstance(upstream,dict) and set(upstream)=={'observation','detection','production_receipt_sha256','legacy_timezone','reference_selection_rule'},'国家正式上游绑定缺失')
    obs,det=upstream['observation'],upstream['detection']
    expected={'component':identity(asdict(b)),'observation':identity(obs),'detection':identity(det)}
    dsns={'component':cfg['runtime'].component_dsn,'observation':cfg['observation_dsn'],'detection':cfg['detection_dsn']}
    cursors={}
    for role in dsns:
        pg=psycopg2.connect(dsns[role]);stack.callback(pg.close);pg.set_session(readonly=not lock)
        c=stack.enter_context(pg.cursor());c.execute("SET LOCAL statement_timeout='10s'")
        if lock:c.execute("SET LOCAL lock_timeout='10s'")
        require(bindings.instance(c)==expected[role],'国家运行角色实际库身份不符:'+role);cursors[role]=c
    suffix=' FOR SHARE' if lock else ''
    c=cursors['component']
    c.execute('SELECT catalog_id FROM country_components.catalog WHERE singleton'+suffix)
    require(c.fetchone()==(b.catalog_id,),'国家catalog身份漂移')
    c.execute('SELECT to_jsonb(t) FROM country_components.components t WHERE component_id=%s'+suffix,(b.component_id,))
    row=c.fetchone();require(row is not None,'国家组件缺失');component=row[0]
    require(all(component[k]==v for k,v in {'schema_name':b.schema_name,'state':'complete','snapshot':b.snapshot,'root':b.root,'manifest_sha256':b.manifest_sha256}.items()),'国家组件完成身份漂移')
    c=cursors['observation'];q={}
    for table in ('domeye.runs','domeye.run_specs','domeye.inputs','domeye.source_receipts','domeye.reference_inputs'):
        q[table]=rows(c,table,obs['run_id'],lock);require(q[table],'国家来源资格缺失:'+table)
    require(len(q['domeye.runs'])==1 and q['domeye.runs'][0]['state']=='complete' and q['domeye.runs'][0]['snapshot']==obs['snapshot'],'国家观察资格漂移')
    require(q['domeye.run_specs'][0]['manifest']==obs['manifest'],'国家观察清单漂移')
    declared=obs['manifest']['inputs'];actual_inputs=q['domeye.inputs']
    require({r['source_id'] for r in actual_inputs}=={r['source_id'] for r in declared},'国家输入来源集合漂移')
    require(all(r['state']=='validated' for r in q['domeye.inputs']+q['domeye.reference_inputs']),'国家源/参考资格撤销')
    c=cursors['detection'];c.execute('SELECT state,schema_name,snapshot,scope,identity FROM detection.runs WHERE run_id=%s'+suffix,(det['run_id'],))
    actual=c.fetchone();require(actual is not None,'国家Detection来源缺失')
    original=det['identity'];expected_identity={k:v for k,v in original.items() if k not in ('_scope','_records')}
    require(actual==('complete','det_'+det['run_id'],det['snapshot'],original['_scope'],expected_identity),'国家Detection资格/身份漂移')
    require(expected_identity['input_run']==obs['run_id'] and expected_identity['input_snapshot']==obs['snapshot'],'国家同链来源错版')
    require(expected_identity['selected_sources']==[obs['manifest']['baseline_source'],*obs['manifest']['update_sources']],'国家来源顺序错版')
    references=expected_identity['reference_sources'];saved={r['source_id']:r for r in q['domeye.reference_inputs']}
    require(len(references)==11 and {r['source_id'] for r in references.values()}==set(saved),'国家参考范围错版')
    require(set(saved)=={r['sha256'] for r in obs['manifest'].get('references',[])},'国家参考与原清单不符')
    require(upstream['reference_selection_rule']=='detection-reference-39578fe/v2','国家参考解释版本不符')
    for ref in references.values():
        require(saved[ref['source_id']]['row_count']==ref['rows'] and ref['snapshot_ref']==f"{obs['run_id']}:{obs['snapshot']}",'国家参考计数/版本漂移')
    ref=descriptor.read_binding.reference
    require(ref is not None and type(ref.database_oid) is int and type(ref.snapshot) is int and type(ref.expected_rows) is int and ref.expected_rows>=0,'国家参考数值身份非法')
    require(ref is not None and (ref.system_id,str(ref.database_oid),ref.run_id,ref.snapshot)==(obs['system_id'],str(obs['database_oid']),obs['run_id'],obs['snapshot']),'国家显示参考未绑定同一来源')
    require(ref.role=='as_info' and ref.table=='references' and ref.source_id==references['as_info']['source_id'] and ref.expected_rows==references['as_info']['rows'],'国家显示参考选择漂移')
    require(ref.manifest_sha256==digest(obs['manifest']) and ref.content_sha256==ref.source_id,'国家显示参考摘要不符')
    require((ref.storage_rule,ref.interpretation_rule,ref.projection_rule)==('reference-rows/v2','detection-reference-39578fe/v2','country-static-reference/v1'),'国家参考解释规则错版')
    receipt=cfg['production_receipt'];require(Path(receipt.path).resolve().is_relative_to(p.root) and receipt.sha256==upstream['production_receipt_sha256'],'国家生产回执未在运行绑定中')
    return {'component':component,'observation':q,'detection':list(actual),'receipt_path':receipt.path,'receipt_sha256':receipt.sha256}



def catalog_identity(c,schema,snapshot):
    c.execute("SELECT value FROM public.ducklake_metadata WHERE key='data_path'")
    root=c.fetchone();require(root is not None,'国家来源湖目录缺失')
    c.execute("""SELECT s.schema_uuid::text,s.path,s.path_is_relative,t.table_uuid::text,t.table_name,t.path,t.path_is_relative,f.path,
                 f.path_is_relative,f.file_size_bytes,f.record_count
        FROM public.ducklake_schema s JOIN public.ducklake_table t USING(schema_id)
        LEFT JOIN public.ducklake_data_file f ON f.table_id=t.table_id
          AND f.begin_snapshot<=%s AND (f.end_snapshot IS NULL OR f.end_snapshot>%s)
        WHERE s.schema_name=%s AND s.begin_snapshot<=%s AND (s.end_snapshot IS NULL OR s.end_snapshot>%s)
          AND t.begin_snapshot<=%s AND (t.end_snapshot IS NULL OR t.end_snapshot>%s)
        ORDER BY t.table_name,f.path""",(snapshot,snapshot,schema,snapshot,snapshot,snapshot,snapshot))
    result=c.fetchall();require(result,'国家来源固定catalog缺失')
    return {'root':root[0],'entries':[list(r) for r in result]}


def source_files(p,descriptor):
    cfg=config(p);upstream=typed_decode(descriptor.upstream_typed);paths=set();catalogs=[]
    for role in ('observation','detection'):
        source=upstream[role];schema=('r_' if role=='observation' else 'det_')+source['run_id']
        require(source['run_id'].isalnum(),'国家来源run身份非法')
        with psycopg2.connect(cfg[role+'_dsn']) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                require(bindings.instance(c)==identity(source),'国家来源角色漂移')
                catalog=bindings.catalog(c,'public',schema,source['snapshot'])
                catalogs.append({'role':role,'schema':schema,'snapshot':source['snapshot'],'identity':catalog_identity(c,schema,source['snapshot'])})
                paths.update(f['path'] for f in catalog['files'])
    manifest=upstream['observation']['manifest']
    for entry in [*manifest['inputs'],*manifest.get('references',[])]:
        path=Path(entry['path']).resolve();require(path.is_relative_to(p.root),'国家来源原件越界')
        require(file_hash(path,p.guard)==entry['sha256'],'国家来源原件摘要漂移');paths.add(str(path))
    require(all(r['origin_uri'].startswith('fixture://') for r in manifest['inputs']),'国家profile只接受人工原件')
    result=[]
    for path in sorted(paths):
        require(Path(path).resolve().is_relative_to(p.root),'国家来源湖文件越界')
        result.append({'path':path,'stamp':stamp(path),'sha256':file_hash(path,p.guard)})
    return result,catalogs


def capture(p,descriptor):
    entities(p,descriptor)
    component=descriptor.read_binding.component
    manifest_path=Path(component.root)/'manifest.json'
    require(manifest_path.resolve().is_relative_to(p.root) and file_hash(manifest_path,p.guard)==component.manifest_sha256,'国家组件清单未封存')
    manifest=json.loads(manifest_path.read_text())
    require((manifest['upstream'],manifest['parameters'])==(descriptor.upstream_typed,descriptor.parameters_typed),'国家描述与实际组件来源不符')
    require(json.loads(descriptor.tables_json)==manifest['tables'] and json.loads(descriptor.event_index_json)==manifest['event_index'],'国家描述与实际表/事件摘要不符')
    require(hashlib.sha256(descriptor.completion_typed.encode()).hexdigest()==manifest['completion_sha256'],'国家完成证明与实际组件不符')
    with ExitStack() as stack:qualifications=current(p,descriptor,stack)
    path=qualifications['receipt_path'];require(file_hash(path,p.guard)==qualifications['receipt_sha256'],'国家生产回执内容漂移')
    files,catalogs=source_files(p,descriptor)
    return {'kind':'country','descriptor':contract_json(descriptor),'source_files':files,'catalogs':catalogs,
            'qualifications':qualifications,'receipt_stamp':stamp(path),'entity_stamps':{e.path:stamp(e.path) for e in descriptor.entities}}


@contextmanager
def verify(p,binding,*,lock=False):
    descriptor=contract_value(binding['descriptor']);entities(p,descriptor)
    for path,expected in binding['entity_stamps'].items():require(stamp(path)==expected,'国家封存实体变更')
    with ExitStack() as stack:
        require(current(p,descriptor,stack,lock=lock)==binding['qualifications'],'国家冻结资格漂移')
        stack.enter_context(verify_country_admission(config(p)['runtime'].component_dsn,descriptor.read_binding,descriptor.admission_proof,lock=lock))
        for file in binding['source_files']:
            p.guard();require(stamp(file['path'])==file['stamp'],'国家来源冻结实体漂移')
        cfg=config(p)
        for catalog in binding['catalogs']:
            pg=psycopg2.connect(cfg[catalog['role']+'_dsn']);stack.callback(pg.close);pg.set_session(readonly=True)
            c=stack.enter_context(pg.cursor())
            require(catalog_identity(c,catalog['schema'],catalog['snapshot'])==catalog['identity'],'国家来源catalog漂移')
        require(stamp(binding['qualifications']['receipt_path'])==binding['receipt_stamp'],'国家生产回执实体漂移')
        yield
        if not lock:
            with verify_country_admission(cfg['runtime'].component_dsn,descriptor.read_binding,descriptor.admission_proof):pass
            entities(p,descriptor)
            for path,expected in binding['entity_stamps'].items():require(stamp(path)==expected,'国家读取期间封存实体变更')
            require(current(p,descriptor,stack)==binding['qualifications'],'国家读取尾部资格漂移')
            for file in binding['source_files']:require(stamp(file['path'])==file['stamp'],'国家读取期间来源实体漂移')
            require(stamp(binding['qualifications']['receipt_path'])==binding['receipt_stamp'],'国家读取期间回执漂移')

"""Resource私有ASN国家参考：完整原件、逐键原位置与非标准token无损保存。"""
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import uuid

import psycopg2
from psycopg2.extras import Json

from data_pipeline.analysis.resources.identity import execution_identity, identity_digest

RULE = 'resource-as-country-pairs-token/v1'


@dataclass
class Constant:
    token: str


class Pairs(list):
    pass


def tagged(value):
    if isinstance(value, Constant):
        return {'kind':'nonstandard_constant','token':value.token,'value_state':'non_finite'}
    if isinstance(value, Pairs):
        return {'kind':'object','pairs':[[k,tagged(v)] for k,v in value]}
    if isinstance(value,list):
        return {'kind':'array','items':[tagged(v) for v in value]}
    if isinstance(value,float) and not math.isfinite(value):
        raise ValueError('标准数字溢出，不接受隐式Infinity')
    return {'kind':'scalar','value':value}


def quality(value, location='$'):
    found=[]
    if isinstance(value,Constant):
        found.append({'location':location,'status':'nonstandard_constant','raw_token':value.token})
    elif isinstance(value,Pairs):
        seen=set()
        for index,(key,item) in enumerate(value):
            child=f'{location}/pairs/{index}/{key}'
            if key in seen:found.append({'location':child,'status':'duplicate_key_last_wins','key':key})
            seen.add(key);found.extend(quality(item,child))
    elif isinstance(value,list):
        for index,item in enumerate(value):found.extend(quality(item,f'{location}/{index}'))
    return found


def parse_rows(raw, guard=lambda:None):
    """顶层逐键原字节切片；整份raw另存，所有内部重复键与token在tagged树保留。"""
    text=raw.decode('utf-8')
    decoder=json.JSONDecoder(object_pairs_hook=Pairs,parse_constant=Constant)
    pos=0;byte_pos=0;seen=set();ordinal=0
    def whitespace(at):
        while at<len(text) and text[at] in ' \t\r\n':at+=1
        return at
    pos=whitespace(pos)
    if pos>=len(text) or text[pos]!='{':raise ValueError('参考顶层必须是对象')
    pos+=1
    if whitespace(pos)<len(text) and text[whitespace(pos)]=='}':
        if text[whitespace(pos)+1:].strip():raise ValueError('参考尾部多余内容')
        return
    while True:
        start=whitespace(pos)
        key,key_end=decoder.raw_decode(text,start)
        if not isinstance(key,str):raise ValueError('参考键必须是字符串')
        colon=whitespace(key_end)
        if colon>=len(text) or text[colon]!=':':raise ValueError('参考缺少冒号')
        value_start=whitespace(colon+1)
        value,end=decoder.raw_decode(text,value_start)
        guard()
        # 顺序累计字节位置，不逐行重扫全文。
        byte_pos+=len(text[pos:start].encode('utf-8')) if ordinal else len(text[:start].encode('utf-8'))
        offset=byte_pos
        segment=text[start:end].encode('utf-8')
        statuses=quality(value)
        if key in seen:statuses.append({'location':'$','status':'duplicate_key_last_wins','key':key})
        seen.add(key)
        if not isinstance(value,Pairs):raise ValueError('ASN参考值必须是对象')
        fields=dict(value)
        expected_fields={'asn','aut_name','country','country_cn','org_name'}
        if set(fields)!=expected_fields:statuses.append({'location':'$','status':'field_set_difference','fields':sorted(fields)})
        if fields.get('asn')!=key:statuses.append({'location':'asn','status':'key_value_mismatch'})
        country=fields.get('country_cn')
        if not isinstance(country,str):country=None
        scope='legacy_unknown' if country=='未知' else 'unknown' if not country else 'reference_label'
        if scope!='reference_label':statuses.append({'location':'country_cn','status':scope})
        yield {'ordinal':ordinal,'key':key,'byte_offset':offset,'byte_length':len(segment),
               'raw_entry':segment.decode('utf-8'),'value':tagged(value),'quality':statuses,
               'country_cn':country,'country_scope':scope}
        ordinal+=1;byte_pos+=len(segment);pos=end
        next_pos=whitespace(pos)
        if next_pos>=len(text):raise ValueError('参考对象未结束')
        if text[next_pos]=='}':
            if text[next_pos+1:].strip():raise ValueError('参考尾部多余内容')
            break
        if text[next_pos]!=',':raise ValueError('参考缺少逗号')
        byte_pos+=len(text[pos:next_pos+1].encode('utf-8'));pos=next_pos+1



COLUMNS = (('ordinal','BIGINT'),('raw_key','VARCHAR'),('byte_offset','BIGINT'),('byte_length','BIGINT'),
           ('raw_entry','VARCHAR'),('value','VARCHAR'),('quality','VARCHAR'),('country_cn','VARCHAR'),('country_scope','VARCHAR'))


def _json(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _history_row(row):
    return {**{k:v for k,v in row.items() if k!='key'},'raw_key':row['key']}


def _row_hash(digest,row):
    digest.update((_json(row)+'\n').encode('utf-8'))


def _fsync_directory(path):
    import os
    fd=os.open(str(path),os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def register_reference(dsn, path, output, *, origin_uri, content_sha256, fixture_only=False,
                       max_bytes=64*1024**2, max_rss_bytes=2*1024**3, min_free_bytes=1024**3,
                       batch_bytes=4*1024**2):
    """文件原件→DuckLake完整历史→PG完成登记；同catalog，不覆盖任何既有制品。"""
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    import os
    import pyarrow as pa
    import resource
    import shutil
    import sys
    if max_rss_bytes<=0 or min_free_bytes<0 or batch_bytes<=0:raise ValueError('参考资源保护无效')
    if not 1<=max_bytes<=1024**3:raise ValueError('参考大小保护无效')
    base_identity=execution_identity(fixture_only=fixture_only)
    if fixture_only and not origin_uri.startswith('fixture://'):raise ValueError('fixture参考须显式fixture来源')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=False)
    reference_id=uuid.uuid4().hex
    pg=psycopg2.connect(dsn);db=None
    catalog_path=None
    file_binding={'origin_uri':origin_uri,'cache_path':str(Path(path).resolve()),'expected_sha256':content_sha256,
                  'capture_state':'not_started','output':str(root)}
    def guard():
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        if rss>max_rss_bytes:raise ValueError('参考RSS资源保护触发')
        for target in (root,Path(catalog_path) if catalog_path else root):
            while not target.exists():target=target.parent
            if shutil.disk_usage(target).free<min_free_bytes:raise ValueError('参考磁盘资源保护触发')
    try:
        with pg,pg.cursor() as c:
            c.execute('CREATE SCHEMA IF NOT EXISTS domeye')
            c.execute("""CREATE TABLE IF NOT EXISTS domeye.resource_references (
                reference_id TEXT PRIMARY KEY,state TEXT,origin_uri TEXT,content_sha256 TEXT,
                rule_version TEXT,dataset_id TEXT,execution_mode TEXT,code_identity JSONB,
                row_count BIGINT,quality JSONB,reason TEXT,manifest JSONB)""")
            # 旧a0制品保持原样；新版本不再向旧BYTEA/PG完整行字段写入。
            c.execute('ALTER TABLE domeye.resource_references ADD COLUMN IF NOT EXISTS manifest JSONB')
            c.execute('INSERT INTO domeye.resource_references (reference_id,state,origin_uri,content_sha256,rule_version,execution_mode,code_identity) VALUES (%s,%s,%s,%s,%s,%s,%s)',
                      (reference_id,'candidate',origin_uri,content_sha256,RULE,base_identity['execution_mode'],Json(base_identity)))
        try:
            with pg,pg.cursor() as c:
                c.execute("SELECT value FROM public.ducklake_metadata WHERE key='data_path'")
                catalog_path=c.fetchone()[0]
            guard()
            with Path(path).open('rb') as stream:raw=stream.read(max_bytes+1)
            file_binding.update(read_bytes=len(raw),capture_state='read_not_archived')
            if len(raw)>max_bytes:
                file_binding.update(capture_state='not_archived_size_limit',actual_sha256=None,
                                    reason='读取max_bytes+1已超限，未继续确认EOF或归档')
                raise ValueError('参考文件超过显式大小保护；未确认完整读取或归档')
            actual_sha=hashlib.sha256(raw).hexdigest()
            file_binding.update(actual_sha256=actual_sha,size=len(raw))
            guard()
            matched=actual_sha==content_sha256
            original=root/('original' if matched else 'quarantine');original.mkdir()
            raw_path=original/'as_dict.json'
            with raw_path.open('xb') as f:
                f.write(raw);f.flush();os.fsync(f.fileno())
            raw_path.chmod(0o444);_fsync_directory(original);_fsync_directory(root)
            file_binding.update(raw_path=str(raw_path),capture_state='complete',content_sha256=actual_sha)
            with pg,pg.cursor() as c:
                c.execute('UPDATE domeye.resource_references SET manifest=%s WHERE reference_id=%s',(Json(file_binding),reference_id))
            if not matched:raise ValueError('参考原件SHA不符；实际完整原件已隔离，非预期合格版本')
            db=connect_duckdb(str(root/'reference-staging.duckdb'))
            db.execute("SET memory_limit='512MB'");db.execute('SET threads=2')
            db.execute('LOAD ducklake');db.execute('LOAD postgres')
            db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (DATA_INLINING_ROW_LIMIT 0)')
            schema='reference_'+reference_id
            db.execute('CREATE SCHEMA lake.'+schema)
            db.execute('CREATE TABLE lake.'+schema+'.rows ('+','.join('"'+n+'" '+t for n,t in COLUMNS)+')')
            extensions=dict(db.execute("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded ORDER BY extension_name").fetchall())
            identity={**base_identity,'extensions':extensions}
            count=0;issue_count=0;pending=[];pending_bytes=0;digest=hashlib.sha256()
            def flush():
                nonlocal pending_bytes
                if not pending:return
                guard()
                types=pa.schema([(n,pa.int64() if t=='BIGINT' else pa.string()) for n,t in COLUMNS])
                db.register('reference_batch',pa.Table.from_pylist(pending,schema=types))
                db.execute('INSERT INTO lake.'+schema+'.rows SELECT * FROM reference_batch')
                db.unregister('reference_batch');pending.clear();pending_bytes=0
                guard()
            for parsed in parse_rows(raw,guard=guard):
                row=_history_row(parsed);_row_hash(digest,row)
                count+=1;issue_count+=len(row['quality'])
                projected={**row,'value':_json(row['value']),'quality':_json(row['quality'])}
                size=sum(len(v.encode('utf-8')) if isinstance(v,str) else 16 for v in projected.values())
                if pending and pending_bytes+size>batch_bytes:flush()
                pending.append(projected);pending_bytes+=size
                guard()
                # 单大条独占批且完整保存；RSS超限则失败，不截断任何字段。
                if len(pending)>=1000 or pending_bytes>=batch_bytes:flush()
            flush()
            if not count:raise ValueError('参考为空')
            snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
            with pg,pg.cursor() as c:
                c.execute("SELECT value FROM public.ducklake_metadata WHERE key='data_path'")
                catalog_path=c.fetchone()[0]
                c.execute('SELECT path,path_is_relative FROM public.ducklake_schema WHERE schema_name=%s AND end_snapshot IS NULL',(schema,))
                schema_path,is_relative=c.fetchone()
            binding={**file_binding,'schema_version':'resource-reference/v2','reference_id':reference_id,
                     'rule_version':RULE,'lake_schema':schema,'snapshot':snapshot,'row_count':count,
                     'normalized_sha256':digest.hexdigest(),'code_identity':identity,
                     'resource_limits':{'max_bytes':max_bytes,'max_rss_bytes':max_rss_bytes,'min_free_bytes':min_free_bytes,'batch_bytes':batch_bytes},
                     'storage_layout':{'catalog_data_path':catalog_path,'schema_path':schema_path,'path_is_relative':is_relative},
                     'quality':{'issue_count':issue_count,'historical_effectivity':'Unknown','generation_chain':'Unknown','duplicate_rule':'last_wins'}}
            # 发布前重新读持久化文件与精确湖快照；不是只核对行数。
            guard()
            _validate_original(binding)
            _validate_history(dsn,binding,guard=guard)
            guard()
            if execution_identity(fixture_only=fixture_only)!=base_identity:raise ValueError('参考登记实现发生变化')
            dataset_id=identity_digest(binding)
            with pg,pg.cursor() as c:
                c.execute("UPDATE domeye.resource_references SET state='validated',row_count=%s,quality=%s,manifest=%s,code_identity=%s WHERE reference_id=%s",
                          (count,Json(binding['quality']),Json(binding),Json(identity),reference_id))
            report={**binding,'dataset_id':dataset_id}
            with (root/'execution.json').open('x') as f:
                json.dump({**report,'state':'ready'},f,ensure_ascii=False,allow_nan=False)
                f.flush();os.fsync(f.fileno())
            _fsync_directory(root)
            with pg,pg.cursor() as c:
                c.execute("UPDATE domeye.resource_references SET state='complete',dataset_id=%s WHERE reference_id=%s AND state='validated'",(dataset_id,reference_id))
                if c.rowcount!=1:raise ValueError('参考状态变化')
            return {**report,'state':'complete'}
        except BaseException as exc:
            pg.rollback()
            with pg,pg.cursor() as c:
                c.execute("UPDATE domeye.resource_references SET state='failed',reason=%s,manifest=coalesce(manifest,%s) WHERE reference_id=%s AND state!='complete'",(str(exc),Json(file_binding),reference_id))
                c.execute('SELECT state FROM domeye.resource_references WHERE reference_id=%s',(reference_id,))
                state=c.fetchone()[0]
            if state=='complete':
                return {**json.loads((root/'execution.json').read_text()),'state':'complete','commit_confirmation':'verified_after_error'}
            (root/'failure.json').write_text(_json({'reference_id':reference_id,'state':state,'reason':str(exc),'capture':file_binding}))
            raise
    finally:
        if db is not None:db.close()
        pg.close()


def _binding(dsn,reference_id,dataset_id,allow_fixture):
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT state,dataset_id,execution_mode,manifest FROM domeye.resource_references WHERE reference_id=%s',(reference_id,))
            row=c.fetchone()
    if row is None or row[:2]!=('complete',dataset_id):raise ValueError('补充参考未完成或错版本')
    if row[2]!='frozen-fresh-process' and not (allow_fixture and row[2]=='synthetic-fixture-api'):
        raise ValueError('补充参考非正式版本')
    binding=row[3]
    if (binding is None or binding.get('schema_version')!='resource-reference/v2'
            or binding.get('reference_id')!=reference_id or identity_digest(binding)!=dataset_id):
        raise ValueError('参考文件/快照登记身份不符')
    if not reference_id.isalnum() or binding['lake_schema']!='reference_'+reference_id:raise ValueError('参考湖身份无效')
    return binding


def _validate_original(binding):
    path=Path(binding['raw_path'])
    if path.is_symlink() or path.resolve()!=path:raise ValueError('参考原件目录身份不符')
    if path.stat().st_size!=binding['size']:raise ValueError('已保存参考原件摘要不符')
    raw=path.read_bytes()
    if len(raw)!=binding['size'] or hashlib.sha256(raw).hexdigest()!=binding['content_sha256']:
        raise ValueError('已保存参考原件摘要不符')
    return raw


def _history(dsn,binding,batch_rows=1000,guard=lambda:None):
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute("SELECT value FROM public.ducklake_metadata WHERE key='data_path'")
            root=c.fetchone()[0]
            c.execute('SELECT path,path_is_relative FROM public.ducklake_schema WHERE schema_name=%s AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)',(binding['lake_schema'],binding['snapshot'],binding['snapshot']))
            row=c.fetchone()
    expected=binding['storage_layout']
    if row is None or (root,*row)!=(expected['catalog_data_path'],expected['schema_path'],expected['path_is_relative']):
        raise ValueError('参考catalog或schema数据目录身份不符')
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        query=f"SELECT * FROM lake.{binding['lake_schema']}.rows AT (VERSION => {int(binding['snapshot'])}) ORDER BY ordinal"
        for batch in db.execute(query).fetch_record_batch(batch_rows):
            guard()
            for row in batch.to_pylist():
                yield {**row,'value':json.loads(row['value']),'quality':json.loads(row['quality'])}
    finally:db.close()


def _validate_history(dsn,binding,guard=lambda:None):
    digest=hashlib.sha256();count=0
    for row in _history(dsn,binding,guard=guard):
        if row['ordinal']!=count:raise ValueError('参考历史序号不连续')
        _row_hash(digest,row);count+=1
    if count!=binding['row_count'] or digest.hexdigest()!=binding['normalized_sha256']:
        raise ValueError('参考规范历史内容或计数不符')


def scan_reference_rows(dsn, reference_id, dataset_id, *, allow_fixture=False, batch_rows=1000):
    """只读精确完成历史；先验证文件与全部规范行内容，不接受同计数改值。"""
    if not 1<=batch_rows<=10000:raise ValueError('参考批大小无效')
    binding=_binding(dsn,reference_id,dataset_id,allow_fixture)
    _validate_original(binding);_validate_history(dsn,binding)
    digest=hashlib.sha256();count=0
    for row in _history(dsn,binding,batch_rows):
        _row_hash(digest,row);count+=1
        yield row
    if count!=binding['row_count'] or digest.hexdigest()!=binding['normalized_sha256']:
        raise ValueError('参考输出流内容发生漂移')
    if _binding(dsn,reference_id,dataset_id,allow_fixture)!=binding:raise ValueError('参考读取期间登记漂移')


def read_reference(dsn, reference_id, dataset_id, *, allow_fixture=False):
    countries={}
    for row in scan_reference_rows(dsn,reference_id,dataset_id,allow_fixture=allow_fixture):
        countries[row['raw_key']]=(row['country_cn'],f"{reference_id}:{dataset_id}:{row['ordinal']}",row['country_scope'])
    binding=_binding(dsn,reference_id,dataset_id,allow_fixture)
    return countries,{'reference_id':reference_id,'dataset_id':dataset_id,'content_sha256':binding['content_sha256'],
                     'origin_uri':binding['origin_uri'],'rule_version':binding['rule_version'],'quality':binding['quality'],
                     'lake_schema':binding['lake_schema'],'snapshot':binding['snapshot'],'normalized_sha256':binding['normalized_sha256']}


def read_reference_original(dsn, reference_id, dataset_id, *, allow_fixture=False):
    binding=_binding(dsn,reference_id,dataset_id,allow_fixture)
    raw=_validate_original(binding);_validate_history(dsn,binding)
    if _binding(dsn,reference_id,dataset_id,allow_fixture)!=binding:raise ValueError('参考读取期间登记漂移')
    return raw

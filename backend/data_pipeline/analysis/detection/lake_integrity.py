"""湖单写完整验收；独立固定快照读取，不登记公共P1 Admission。"""
import hashlib
import json
import time
import resource
import sys
import os
from datetime import datetime, timezone
from data_pipeline.analysis.detection._results import plain, stable
from data_pipeline.analysis.detection.qualification_contract import validate_entry, validate_identity

from data_pipeline.analysis.detection.lake_contract import INTEGRITY_VERSION as VERSION, STATE_HEADERS, expected_indexes
STATE_COLUMNS = (('ordinal','BIGINT'),('family','VARCHAR'),('attribute','VARCHAR'),
                 ('key_json','VARCHAR'),('container','VARCHAR'),('value_json','VARCHAR'))


def row_bytes(row):
    # TIMESTAMPTZ按同一UTC时刻编码，其余字符串（包括原JSON文本）不重写。
    value={k:(v.astimezone(timezone.utc) if isinstance(v,datetime) else v) for k,v in row.items()}
    return json.dumps(plain(value),ensure_ascii=False,sort_keys=True,allow_nan=False,separators=(',',':')).encode()


class Inventory:
    """只保留每表计数/字节/摘要；摘要绑定字段名、值、类型标签及原顺序。"""
    def __init__(self):
        self.values={name:[0,0,hashlib.sha256()] for name in ('records','state_entries','m3_entries')}

    def add(self, table, row):
        data=row_bytes(row)
        v=self.values[table];v[0]+=1;v[1]+=len(data)
        v[2].update(len(data).to_bytes(8,'big'));v[2].update(data)

    def result(self):
        return {name:dict(rows=v[0],typed_bytes=v[1],sha256=v[2].hexdigest()) for name,v in self.values.items()}


def require(value, message):
    if not value:raise ValueError(message)


def validate_snapshot(dsn,run_id,snapshot,identity,expected,*,source_binding,batch_rows=256,guard=lambda:None, validated_connection=None, compare_inventory=True, audit_sink=lambda **event:None, scope=None):
    """一次三表完整扫描，可由未来admit复用；来源证据须来自调用方实际只读核验。"""
    from data_pipeline.analysis.detection.store import COLUMNS, connect_duckdb, literal
    from data_pipeline.analysis.detection.qualification_store import DDL
    started=time.monotonic()
    require(isinstance(run_id,str) and run_id.isascii() and run_id.isalnum() and type(snapshot) is int and snapshot>=0, '固定快照身份非法')
    require(type(batch_rows) is int and 1<=batch_rows<=10000,'完整验收批次非法')
    validate_identity(identity,allow_synthetic=True)
    require(isinstance(source_binding,dict),'缺少实际来源完整核验')
    for field in ('input_binding','reference_sources','reference_checkpoints','reference_interpretation'):
        require(source_binding.get(field)==identity.get(field),'实际源/参考绑定不符')
    window = None
    if 'result_window_rule' in identity or 'result_window' in identity or 'window_coverage' in identity:
        from data_pipeline.analysis.detection.result_window import identity_windows, time_coverage as new_time_coverage, observe_message
        window = identity_windows(identity, scope)
        time_coverage = new_time_coverage(identity['selected_sources'],source_binding['input_binding']['sources'])
        from data_pipeline.bgp.ordered_reader import _boundary, _validate_binding
        from data_pipeline.bgp.record_types import InputBinding, SourceBinding
        # 同一实际来源绑定和共用边界解释；不从原microsecond=None另造fallback。
        ordered_binding = InputBinding(**{**source_binding['input_binding'],
            'sources': tuple(SourceBinding(**s) for s in source_binding['input_binding']['sources'])})
        _validate_binding(ordered_binding, tuple(identity['selected_sources']))
        ordered_sources = {s.source_id: (rank, s) for rank, s in enumerate(ordered_binding.sources)}
    selected=identity['selected_sources']
    original={s['source_id']:s for s in source_binding['input_binding']['sources']}
    require(len(selected)==len(set(selected)) and all(s in original for s in selected),'来源选择非法')
    refs=source_binding['reference_sources'];checkpoints=source_binding['reference_checkpoints']
    require(set(refs)==set(checkpoints),'参考角色集合不符')
    for role,ref in refs.items():
        cp=checkpoints[role]
        require(cp['source_id']==ref['source_id'] and cp['counts']['references']==ref['rows'] and
                ref['snapshot_ref']==f"{identity['input_run']}:{identity['input_snapshot']}",'参考原CP绑定不符')
    definitions={'records':COLUMNS,'state_entries':STATE_COLUMNS,
                 'm3_entries':tuple(tuple(s.strip().split()) for s in DDL.split(','))}
    last_records={};revisions={}
    inventory=Inventory();events=set();qualified=set();gaps=set();gap_counts={};starts=[];ends=[];coverage=[]
    current_source=None;message_count=0;end_rows={};headers={};entry_keys={};completion=None;schemas={}
    db=validated_connection if validated_connection is not None else connect_duckdb()
    try:
        if validated_connection is None:
            db.execute('LOAD ducklake');db.execute('LOAD postgres')
            db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        for table,columns in definitions.items():
            guard()
            order='sequence' if table=='records' else 'ordinal'
            query=f'SELECT * FROM lake.det_{run_id}.{table} AT (VERSION => {int(snapshot)}) ORDER BY {order}'
            description=db.execute('DESCRIBE '+query).fetchall()
            actual=tuple((r[0],r[1].replace('TIMESTAMP WITH TIME ZONE','TIMESTAMPTZ')) for r in description)
            require(actual==tuple(columns),'湖表完整类型不符：'+table)
            schemas[table]=list(map(list,actual))
            audit_sink(kind="full_table_scan",table=table)
            batches=db.execute(query).fetch_record_batch(batch_rows)
            primary = None
            try:
                for batch in batches:
                    guard()
                    audit_sink(kind="validated_batch",table=table,rows=batch.num_rows,arrow_bytes=batch.nbytes)
                    for row in batch.to_pylist():
                        require(row[order]==inventory.values[table][0],'湖表顺序不连续：'+table)
                        inventory.add(table,row)
                        if table=='records':
                            legacy=json.loads(row['legacy_json']);evidence=json.loads(row['evidence_json']);attrs=json.loads(row['attributes_json'])
                            require(isinstance(legacy,dict) and isinstance(attrs,dict) and isinstance(evidence,dict),'记录完整载荷非法')
                            if window is not None: require(attrs.get('scope')==scope,'记录与实际计算窗scope不符')
                            indexes=expected_indexes(attrs,legacy,evidence)
                            for field,value in indexes.items():
                                require(row[field]==value and (field=='observed_at' or type(row[field]) is type(value)),
                                        '科学typed索引与原正文/固定规则不符：'+field)
                            kind=row['record_kind']
                            require(attrs.get('kind')==kind,'记录类型与完整原值不符')
                            if kind=='business_revision':
                                key=row['incident_id'],row['revision']
                                require(isinstance(key[0],str) and type(key[1]) is int and key[1]>0 and key not in events,'科学revision重复/非法')
                                require((attrs.get('incident_id'),attrs.get('revision'),attrs.get('event_kind'))==(*key,row['event_kind']),'科学正文/索引不符')
                                require(key[1]==revisions.get(key[0],0)+1,'科学revision序列不连续')
                                events.add(key);last_records[key[0]]=stable(legacy);revisions[key[0]]=key[1]
                            elif kind=='source_start':
                                sid=legacy['source_id'];s=original[sid]
                                require(current_source is None and sid not in starts,'来源重复/未关闭')
                                require((legacy['run_id'],legacy['snapshot'],legacy['content_sha256'],legacy['role'],legacy['expected_messages'],legacy['expected_elements'])==
                                        (identity['input_run'],identity['input_snapshot'],s['content_sha256'],s['role'],s['messages'],s['elements']),'来源开始与实际绑定不符')
                                starts.append(sid);current_source=sid;message_count=0
                            elif kind=='source_message':
                                require(current_source==legacy['source_id'],'消息来源次序不符');message_count+=1
                                if window is not None and original[current_source]['role']=='update':
                                    rank, source = ordered_sources[current_source]
                                    boundary = _boundary(legacy, ordered_binding, rank, source)
                                    observe_message(dict(legacy, epoch=boundary.raw_time.epoch,
                                                         microsecond=boundary.raw_time.microsecond), window, time_coverage)
                            elif kind=='source_end':
                                sid=legacy['source_id'];s=original[sid]
                                require(current_source==sid and legacy['messages']==s['messages']==message_count and legacy['elements']==s['elements'] and
                                        (legacy['run_id'],legacy['snapshot'])==(identity['input_run'],identity['input_snapshot']),'来源结束与实际绑定不符')
                                ends.append(sid);end_rows[sid]=legacy;current_source=None
                            elif kind=='input_completion':
                                require(completion is None,'重复完成记录');completion=legacy
                        elif table=='state_entries':
                            key=row['family'],row['attribute'];container=row['container']
                            k=json.loads(row['key_json']);v=json.loads(row['value_json'])
                            if container=='entry':
                                require(key in headers and headers[key] in ('dict','list','set'),'终态条目缺容器')
                                encoded=json.dumps(k,sort_keys=True)
                                require(encoded not in entry_keys[key],'终态重复键')
                                if headers[key] in ('list','set'):require(type(k) is int and k==len(entry_keys[key]),'终态序列键不连续')
                                entry_keys[key].add(encoded)
                                if key==('output','last_records'):require(k in last_records and v==last_records[k],'终态last_records与原科学正文不符')
                                if key==('output','revisions'):require(k in revisions and type(v) is int and v==revisions[k],'终态revision与科学表不符')
                            else:
                                require(container in ('dict','list','set','value') and key not in headers and k is None,'终态容器非法/重复')
                                if container!='value':require(v is None,'终态容器头带值')
                                require(STATE_HEADERS.get(key)==container,'终态族/属性/容器与固定合同不符')
                                headers[key]=container;entry_keys[key]=set()
                        else:
                            p=validate_entry(row,run_id,identity)
                            if row['kind']=='scope_gap':
                                require(row['gap_id'] not in gaps,'重复Gap');gaps.add(row['gap_id'])
                                gap_counts[row['source_id']]=gap_counts.get(row['source_id'],0)+1
                            elif row['kind']=='event_qualification':
                                key=row['incident_id'],row['revision'];require(key in events,'资格科学FK损坏');qualified.add(key)
                            else:
                                sid=row['source_id'];coverage.append(sid)
                                require(p['receipt']['raw']==end_rows[sid] and p['receipt']['parse_counts']['gaps']==gap_counts.get(sid,0) and
                                        all(p['receipt']['parse_counts'][k]==original[sid][k] for k in ('decoded','rejected','unsupported')),'资格源回执/Gap计数不符')
                            require(all(g in gaps for g in p.get('gap_refs',[])),'资格Gap FK损坏')
            except BaseException as exc: primary = exc; raise
            finally:
                # 原冻结生产快照没有公共P1上游模块；保持生产验收自包含。
                try: batches.close()
                except BaseException as exc:
                    if primary is None: raise
                    primary.cleanup_errors = (*getattr(primary, 'cleanup_errors', ()), exc)
        if window is not None: require(time_coverage==identity['window_coverage'],'实际RawTime覆盖证明不符')
        if compare_inventory:
            require(inventory.result()==expected,'湖完整typed内容/计数与生产流不符')
        require(current_source is None and starts==ends==coverage==selected,'完整来源枚举不符')
        require(completion is not None and completion['sources']==[end_rows[s] for s in selected],'输入完成来源不符')
        require(qualified==events,'科学revision资格覆盖不完整')
        require(headers==STATE_HEADERS and list(headers)==list(STATE_HEADERS),'终态必需族/属性/顺序不完整')
        scientific_keys={json.dumps(k,sort_keys=True) for k in last_records}
        require(entry_keys[('output','last_records')]==entry_keys[('output','revisions')]==scientific_keys,'终态科学键集合未闭合')
    finally:
        if validated_connection is None: db.close()
    return dict(version=VERSION,snapshot=snapshot,inventory=inventory.result(),schemas=schemas,
                qualification_counts=dict(entries=inventory.values['m3_entries'][0],gaps=len(gaps),sources=len(coverage),revisions=len(events)),
                full_table_scans=3,wall_seconds=time.monotonic()-started,pid=os.getpid(),
                process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
                memory_scope='生产进程启动至完整验收结束累计峰值；不含PG服务',
                source_binding_sha256=hashlib.sha256(row_bytes(source_binding)).hexdigest(),
                state='verified',public_admission='not_performed')

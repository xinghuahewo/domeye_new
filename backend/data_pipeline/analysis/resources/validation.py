"""同格式依赖实值核验；只验证既有结果，不产生新科学值或资格身份。"""
from collections import Counter
from dataclasses import asdict
from datetime import timedelta
import hashlib
import json

from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.ordered_reader import binding_from_reader
from data_pipeline.bgp.archive.checkpoint import KEYS, sha
from data_pipeline.analysis.resources.compute import ITEMS


def validate_m2_dependencies(dsn,db,relation,manifest,guard):
    """独立读取指定attempt的原行，并核checkpoint摘要；不能用输出自证输出。"""
    def output(table,sid,columns):
        result=[]
        for batch in db.execute('SELECT '+','.join(columns)+' FROM '+relation(table)+' WHERE source_id=?',[sid]).fetch_record_batch(4096):
            guard();result.extend(tuple(row[c] for c in columns) for row in batch.to_pylist())
        return Counter(result)
    for sid,item in manifest['observation_inputs'].items():
        full=manifest['observation_runs'][item['binding_id']];binding=full['binding'];cp=item['checkpoint']
        reader=ObservationReader(dsn,binding['observation_run'],binding['seal_snapshot'],[sid],profile='observation',guard=guard)
        actual=binding_from_reader(reader)
        selected=next(c for c in reader.selection.checkpoints if c['source_id']==sid)
        if (actual.binding_id!=item['binding_id'] or json.loads(json.dumps(asdict(actual)))!=binding
                or selected!=cp or reader.selection.seal!=full['seal'] or reader.manifest!=full['manifest']):
            raise ValueError('依赖原行不在指定M2封存/checkpoint/attempt')
        source=reader.connect()
        try:
            raw={}
            for table in ('peers','quality'):
                digest=hashlib.sha256();rows=[]
                for batch in source.execute('SELECT * FROM '+reader.bound_table(table,sid)+' ORDER BY '+KEYS[table]).fetch_record_batch(512):
                    guard()
                    for row in batch.to_pylist():
                        if row['source_id']!=sid:raise ValueError('原行来源与所选RIB不符')
                        digest.update(bytes.fromhex(sha(row)));rows.append(row)
                expected=cp['tables'][table]
                if len(rows)!=expected['count'] or digest.hexdigest()!=expected['digest']:
                    raise ValueError('实际M2依赖原行与checkpoint摘要不符：'+table)
                raw[table]=rows
            peers=Counter((r['source_id'],r['table_record'],r['index'],r['ip'],r['asn'],r['bgp_id'],r['bgp_id_present']) for r in raw['peers'])
            if output('peer_dependencies',sid,('source_id','table_record','peer_index','peer_ip','peer_asn','bgp_id','bgp_id_present'))!=peers:
                raise ValueError('Peer依赖位置或原属性与实际M2不符')
            # 用实际消息核record映射，同时要求原message_id与其source/record自洽。
            messages=dict(source.execute('SELECT m.message_id,m.record FROM '+reader.bound_table('messages',sid)+' m JOIN '+
                reader.bound_table('quality',sid)+' q USING(message_id)').fetchall())
            quality=[]
            for row in raw['quality']:
                mid=row['message_id'];record=messages.get(mid)
                if mid is not None and (record is None or mid!=f'{sid}:{record}'):
                    raise ValueError('质量记录的实际消息位置不符')
                quality.append((sid,record,row['code'],row['detail']))
            if output('observation_quality',sid,('source_id','record','code','detail'))!=Counter(quality):
                raise ValueError('质量依赖位置或原值与实际M2不符')
            reader.check_sources(reader.starts)
        finally:source.close()


def validate_normal_history(sources,metric_rows,band_rows,normal,guard):
    """仅重建旧正常样本的选择/保留关系，不重算或写回mean/std/边界。"""
    bands={}
    for row in band_rows:
        key=(row['source_id'],row['dimension'],row['bucket'],row['metric'])
        if key in bands:raise ValueError('重复normal边界')
        bands[key]=row
    if set(normal)-set(bands):raise ValueError('normal样本没有对应边界')
    by_source={sid:[] for sid in sources}
    for row in metric_rows:
        if row['source_id'] not in sources or row['time']!=sources[row['source_id']]['snapshot_time']:
            raise ValueError('normal科学行来源或时点不符')
        by_source[row['source_id']].append(row)
    histories={};retained={};seen=set()
    sample_key=lambda r:(r['sample_source'],r['sample_time'],r['value'])
    for rib_count,source in enumerate(sorted(sources.values(),key=lambda r:r['snapshot_time']),1):
        guard();sid=source['source_id'];time=source['snapshot_time'];buckets=set()
        for row in by_source[sid]:
            bucket=(row['dimension'],row['bucket'])
            if bucket in buckets or row['dimension'] not in ('first_path_asn','peer_asn'):
                raise ValueError('normal科学行重复或维度未知')
            buckets.add(bucket);history=histories.setdefault(bucket,{})
            history[time]=row
            for metric in ITEMS if row['dimension']=='first_path_asn' else ITEMS[:2]:
                key=(sid,*bucket,metric);seen.add(key)
                if key not in bands:raise ValueError('normal边界遗漏')
                actual=normal.get(key,[])
                if any(r['sample_time']>time for r in actual):raise ValueError('normal样本不能引用未来RIB')
                selected=[(old['source_id'],stamp,old[metric]) for stamp,old in history.items() if stamp!=time and not old['is_outlier']]
                state_key=(*bucket,metric);previous=retained.get(state_key,())
                # 对齐compute.py：前6次可纳当前；后续不足6样本或长度变小则沿用旧band。
                if len(selected)>5 or rib_count<7:
                    if len(selected)<=5:selected.append((sid,time,row[metric]))
                    if len(selected)>=len(previous):previous=tuple(selected)
                retained[state_key]=previous
                if bands[key]['list_len']!=len(previous) or Counter(map(sample_key,actual))!=Counter(previous):
                    raise ValueError('normal样本不符合实际历史筛选/保留关系')
            # 原算法在本桶本次比较之后清理；保留band可继续引用此前样本。
            for stamp in list(history):
                if stamp<time-timedelta(days=3):del history[stamp]
    if seen!=set(bands):raise ValueError('normal边界含非实际科学行')

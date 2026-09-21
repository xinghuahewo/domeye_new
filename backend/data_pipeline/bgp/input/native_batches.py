"""原生有序段的计算视图；完整属性只保存在 Observation，不再次组装全文件。"""
from collections import OrderedDict, defaultdict
from dataclasses import replace
from pathlib import Path
import struct
import sys

import pyarrow.parquet as pq

from data_pipeline.bgp.archive import message_reader as consumer
from data_pipeline.bgp.archive.checkpoint import file_sha
from data_pipeline.bgp.ordered_reader import SourceCursor, _ELEMENT_COLS
from data_pipeline.bgp.record_types import SourceBinding, SourceStart
from data_pipeline.bgp.archive.store import TABLES
from data_pipeline.bgp.state.path_dictionary import TextPool

# 热循环只使用这些解释字段。原始 AS_SET、AS4_PATH、属性字节仍在同段归档路径表。
PATH_FIELDS = ('path_key', 'as_path_text', 'attributed_origin_asn', 'raw_origin_asn',
               'as4_path_text', 'attributes_digest', 'reason')
MESSAGE_FIELDS = ('source_id', 'content_sha256', 'record', 'epoch', 'microsecond',
                  'mrt_type', 'mrt_subtype', 'local_message', 'local_ip', 'local_asn', 'interface')


class HotPaths:
    """窄路径编号索引：定长字节保存解释字段，完整属性由 Observation 保存。

    文本仅在 TextPool 中共享一份，展开行缓存有界；编号索引随不同路径数增长，
    计入主任务 RSS 上限。失败后整体丢弃，从核验过的归档重建，不单独落库。
    """
    _ROW = struct.Struct('>qqqIIB32s')

    def __init__(self, *, cache_entries=8192, cache_bytes=8*1024**2):
        self.disk_peak=0;self.texts=TextPool()
        self.index={};self.rows=bytearray();self.index_bytes=0
        self.values=[None];self.value_ids={None:0};self.value_bytes=0
        self.cache=OrderedDict();self.bytes=0
        self.max_entries,self.max_bytes=cache_entries,cache_bytes
        self.queries=self.peak_entries=self.peak_bytes=self.peak_memory_bytes=0

    def _value_id(self,value):
        ident=self.value_ids.get(value)
        if ident is None:
            ident=len(self.values);self.values.append(value);self.value_ids[value]=ident
            self.value_bytes+=sys.getsizeof(value)+sys.getsizeof(ident)
        return ident

    @property
    def memory_bytes(self):
        return (sys.getsizeof(self.rows)+sys.getsizeof(self.index)+self.index_bytes+
                sys.getsizeof(self.values)+sys.getsizeof(self.value_ids)+self.value_bytes+
                self.texts.memory_bytes+self.bytes)

    def register(self,table):
        if table is None or not len(table):return
        # Arrow 列在段内按小块解包，不为每个属性项构造并编码完整 Python 字典。
        for batch in table.select(PATH_FIELDS).to_batches(max_chunksize=8192):
            columns=[column.to_pylist() for column in batch.columns]
            self.texts.register(columns[1])
            for key,text,origin,raw,as4,attributes,reason in zip(*columns):
                binary=bytes.fromhex(key)
                if len(binary)!=32:raise ValueError('路径引用不是 SHA256')
                attr=bytes(32) if attributes is None else bytes.fromhex(attributes)
                if len(attr)!=32:raise ValueError('属性摘要不是 SHA256')
                packed=self._ROW.pack(self.texts.ident(text),-1 if origin is None else origin,
                    -1 if raw is None else raw,self._value_id(as4),self._value_id(reason),
                    attributes is not None,attr)
                offset=self.index.get(binary)
                if offset is not None:
                    if self.rows[offset:offset+self._ROW.size]!=packed:
                        raise ValueError('跨段或跨文件路径解释冲突')
                else:
                    offset=len(self.rows);self.index[binary]=offset;self.rows.extend(packed)
                    self.index_bytes+=sys.getsizeof(binary)+sys.getsizeof(offset)
            self.peak_memory_bytes=max(self.peak_memory_bytes,self.memory_bytes)

    def prepare(self,keys,*,max_result_bytes=None):
        result={};result_bytes=0
        for key in dict.fromkeys(keys):
            cached=self.cache.get(key)
            if cached is not None:
                row,size=cached;self.cache.move_to_end(key)
            else:
                offset=self.index.get(bytes.fromhex(key))
                if offset is None:raise ValueError('批次引用的历史路径缺失')
                text,origin,raw,as4,reason,present,attr=self._ROW.unpack_from(self.rows,offset)
                row=dict(zip(PATH_FIELDS,(key,self.texts.value(text),None if origin==-1 else origin,
                    None if raw==-1 else raw,self.values[as4],attr.hex() if present else None,self.values[reason])))
                size=sys.getsizeof(row)+sum(sys.getsizeof(k)+sys.getsizeof(v) for k,v in row.items())+sys.getsizeof(key)+256
                if size<=self.max_bytes:
                    self.cache[key]=(row,size);self.bytes+=size
                    while len(self.cache)>self.max_entries or self.bytes>self.max_bytes:
                        _,(_,removed)=self.cache.popitem(last=False);self.bytes-=removed
                    self.peak_entries=max(self.peak_entries,len(self.cache));self.peak_bytes=max(self.peak_bytes,self.bytes)
            result[key]=row;result_bytes+=size
            if max_result_bytes is not None and result_bytes>max_result_bytes:
                raise ValueError('批次路径准备超过内存上限')
        self.peak_memory_bytes=max(self.peak_memory_bytes,self.memory_bytes)
        return result

    def close(self):
        self.index.clear();self.rows.clear();self.values.clear();self.value_ids.clear();self.cache.clear()
        self.texts.texts.clear();self.texts.ids.clear();self.texts.tokens.clear()


def persisted_paths(cp):
    """已提交计算只需重建路径编号索引，不读取历史元素或再次组装消息。"""
    for item in cp['files']:
        if Path(item['path']).stem!='paths':continue
        if file_sha(item['path'])!=item['sha256']:raise ValueError('历史路径归档摘要漂移')
        yield pq.read_table(item['path'],columns=list(PATH_FIELDS))


def persisted_segments(cp):
    """只读取固定 checkpoint 引用的 Parquet，按原生段号重放；不执行全文件 JOIN。"""
    groups = defaultdict(dict)
    for item in cp['files']:
        path = Path(item['path'])
        if not path.parent.name.startswith('segment-'): raise ValueError('直接恢复需要原生分段归档')
        ordinal = int(path.parent.name.removeprefix('segment-'))
        if path.stem in groups[ordinal]: raise ValueError('分段归档表重复')
        groups[ordinal][path.stem] = item
    if sorted(groups) != list(range(len(groups))): raise ValueError('分段归档不连续')
    for ordinal in sorted(groups):
        tables = {}
        for name, item in groups[ordinal].items():
            if file_sha(item['path']) != item['sha256']: raise ValueError('分段归档摘要漂移')
            tables[name] = pq.read_table(item['path']).drop(['attempt'])
        if 'messages' not in tables: raise ValueError('分段归档缺少消息')
        records = tables['messages']['record'].to_pylist()
        yield dict(segment=ordinal, first_record=records[0], next_record=records[-1]+1,
                   counts={name:len(table) for name,table in tables.items()}), tables


def peer_rows(table):
    if table is None: return []
    return [tuple(row[k] for k in ('ip','asn','bgp_id','table_record','index')) for row in table.to_pylist()]


class SegmentStream:
    """当前段 Arrow 与至多 1024 个轻量元素；文件末只能由已核验 checkpoint 确认。"""
    def __init__(self, binding, rank, entry, paths, *, batch_rows=1024):
        self.binding, self.entry, self.paths = binding, entry, paths
        self.rib_batches = False
        self.update_batches = False
        self.update_rows = True
        self.update_boundary = None
        self.batch_rows = batch_rows; self.next_segment = 0; self.endpoints = {}; self.peers = []
        source = SourceBinding(entry['source_id'],entry['sha256'],entry['role'],None,None,None,None,None,None)
        start = consumer.SourceStart(binding.observation_run,None,source.source_id,source.content_sha256,source.role,None,None)
        self.start = SourceStart(binding.binding_id,rank,start)
        self.cursor = SourceCursor(binding,rank,source,start,provisional=True)
        # 候选视图明确只含热字段；公开 Reader 仍要求原来的完整列合同。
        omitted = {name for name,_ in TABLES['paths']} - set(PATH_FIELDS)
        self.cursor.element_columns = [(name,typ) for name,typ in _ELEMENT_COLS if name not in omitted]
        self.path_columns = [(name, typ) for name, typ in TABLES['paths'] if name in PATH_FIELDS]

    def segment(self, segment, tables):
        if segment['segment'] != self.next_segment or segment['first_record'] != self.cursor.record+1:
            raise ValueError('直接批次重复、遗漏或乱序')
        self.paths.register(tables.get('paths'))
        messages = tables['messages'].to_pylist()
        by_message = {m['message_id']:m for m in messages}
        if len(by_message)!=len(messages): raise ValueError('重复消息')
        self.peers.extend(peer_rows(tables.get('peers')))
        if len(self.peers)>1000000: raise ValueError('Peer 证据容量上限')
        for m in messages:
            m.update(eor=[],quality=[],peers=[])
            if m['peer_ip'] is not None and not m['local_message']:
                key=tuple(m[n] for n in ('peer_ip','peer_asn','local_ip','local_asn','interface'))
                prior=self.endpoints.get(key)
                self.endpoints[key]=(min(prior[0],m['message_id']),prior[1]+1) if prior else (m['message_id'],1)
        if len(self.endpoints)>65536: raise ValueError('端点集合容量上限')
        source_quality=[]
        for name,fields in (('eor',('afi','safi')),('quality',('code','detail'))):
            table=tables.get(name)
            if table is None:continue
            for row in table.to_pylist():
                if name=='quality' and row['message_id'] is None:source_quality.append(row);continue
                if row['message_id'] not in by_message:raise ValueError('诊断或 EOR 缺少本段消息')
                by_message[row['message_id']][name].append({k:row[k] for k in fields})
        for ip,asn,bgp_id,record,index in peer_rows(tables.get('peers')):
            mid=self.entry['source_id']+':'+str(record)
            if mid not in by_message:raise ValueError('Peer 表缺消息')
            by_message[mid]['peers'].append(dict(ip=ip,asn=asn,bgp_id=bgp_id,bgp_id_present=True,peer_index=index,table_record=record))
        # 附属无位置集合沿旧 Reader 的稳定顺序；路由元素和消息本身绝不排序。
        for m in messages:
            m['quality'].sort(key=lambda q:(q['code'],q['detail']))
            m['eor'].sort(key=lambda q:(q['afi'],q['safi']))
            m['peers'].sort(key=lambda p:(p['table_record'],p['peer_index']))
        mi=0
        elements=tables.get('elements')
        # 混合 RIB 消息仍沿旧入口解释，不借批次优化收紧原来允许的消息组合。
        if (self.update_batches and self.entry['role']=='update'
                and not any(m['kind']=='rib' for m in messages)):
            from data_pipeline.bgp.state.update_columns import segments
            yield from segments(self, messages, elements, source_quality)
            if self.cursor.record+1!=segment['next_record']:raise ValueError('分段实际结束位置不符')
            self.next_segment+=1
            return
        if (self.rib_batches and self.entry['role']=='baseline' and not self.cursor.parse['gaps']
                and all(m['kind'] in ('rib','peer_index_table') and not m['local_message'] for m in messages)):
            from data_pipeline.bgp.state.rib_columns import segments
            yield from segments(self, messages, elements, source_quality)
            if self.cursor.record+1!=segment['next_record']:raise ValueError('分段实际结束位置不符')
            self.next_segment+=1
            return
        batches=() if elements is None else elements.to_batches(max_chunksize=self.batch_rows)
        for batch in batches:
            rows=batch.to_pylist(); paths=self.paths.prepare([r['path_key'] for r in rows])
            for row in rows:
                m=by_message.get(row['message_id'])
                if m is None:raise ValueError('元素引用非本段消息')
                row.update(paths[row['path_key']]);row.update({k:m[k] for k in MESSAGE_FIELDS})
            end=mi
            while end<len(messages) and messages[end]['record']<=rows[-1]['record']:end+=1
            yield from self.cursor.batch(consumer.MessageBatch(self.binding.observation_run,None,self.entry['source_id'],
                tuple(messages[mi:end]),tuple(rows),batch.nbytes,tuple(source_quality)))
            source_quality=[];mi=end
        if mi<len(messages) or source_quality:
            yield from self.cursor.batch(consumer.MessageBatch(self.binding.observation_run,None,self.entry['source_id'],
                tuple(messages[mi:]),(),0,tuple(source_quality)))
        if self.cursor.record+1!=segment['next_record']:raise ValueError('分段实际结束位置不符')
        self.next_segment+=1

    def end(self,cp):
        if cp['source_id']!=self.entry['source_id'] or cp['source_sha']!=self.entry['sha256']:
            raise ValueError('计算文件与归档身份不符')
        source=replace(self.cursor.source,checkpoint_digest=cp['digest'],
            **{k:cp['counts'][k] for k in ('messages','elements','decoded','rejected','unsupported')})
        raw=consumer.SourceEnd(self.binding.observation_run,cp['snapshot'],self.entry['source_id'],**self.cursor.counts)
        return self.cursor.end(raw,source)

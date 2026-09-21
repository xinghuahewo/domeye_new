"""同版Arrow观察的有界暂存及Resource投影；不读取或执行旧项目。"""
from datetime import datetime, timezone
import json
import csv
import io

import pandas as pd
from pathlib import Path
import sys

import duckdb

from data_pipeline.analysis.resources.compute import ReferenceAs, RibContext, RibElement

COLUMNS = ('source_id', 'content_sha256', 'event_id', 'message_id', 'record', 'ordinal', 'offset', 'epoch',
           'microsecond', 'action', 'afi', 'safi', 'prefix', 'as_path_text', 'path_key',
           'peer_asn', 'peer_ip', 'bgp_id', 'bgp_id_present', 'peer_table_record', 'peer_index',
           'attributes_digest', 'attributes_offset', 'attributes_length', 'attributed_origin_asn')


class ObservationInput:
    """磁盘暂存只保存引用和计算投影；每份来源显式选取，不按SHA顺序推进时间。"""

    def __init__(self, path):
        if Path(path).exists():
            raise ValueError('拒绝覆盖暂存文件')
        self.db = duckdb.connect(str(path))
        self.db.execute("SET memory_limit='512MB'")
        self.db.execute('SET threads=2')
        self.loaded = False

    def load(self, batches):
        for batch in batches:
            if set(COLUMNS) - set(batch.schema.names):
                raise ValueError('观察批缺少冻结字段')
            self.db.register('incoming', batch.select(COLUMNS))
            if not self.loaded:
                self.db.execute('CREATE TABLE observations AS SELECT * FROM incoming LIMIT 0')
                self.loaded = True
            self.db.execute('INSERT INTO observations SELECT * FROM incoming')
            self.db.unregister('incoming')
        if not self.loaded:
            raise ValueError('观察输入为空')
        if self.db.execute('SELECT count(*)-count(DISTINCT event_id) FROM observations').fetchone()[0]:
            raise ValueError('重复原始元素身份')

    def range(self, source):
        row = self.db.execute('SELECT min(epoch),max(epoch),count(*) FROM observations WHERE source_id=?', [source]).fetchone()
        if not row[2]:
            raise ValueError('指定RIB没有已完成观察')
        return row

    def content_sha256(self, source):
        rows=self.db.execute('SELECT DISTINCT content_sha256 FROM observations WHERE source_id=?',[source]).fetchall()
        if len(rows)!=1 or rows[0][0] is None:
            raise ValueError('RIB内容版本缺失或冲突')
        return rows[0][0]

    def snapshot_epoch(self, source):
        row=self.db.execute('SELECT epoch FROM observations WHERE source_id=? ORDER BY record,ordinal LIMIT 1',[source]).fetchone()
        if row is None:
            raise ValueError('RIB来源没有元素')
        return row[0]

    def elements(self, source, *, batch_size=10000):
        reader = self.db.execute('SELECT * FROM observations WHERE source_id=? ORDER BY record,ordinal', [source]).fetch_record_batch(batch_size)
        previous = None
        for batch in reader:
            for row in batch.to_pylist():
                if row['action'] != 'rib_snapshot':
                    raise ValueError('Resource输入包含非RIB元素')
                order = (row['record'], row['ordinal'])
                if previous is not None and order <= previous:
                    raise ValueError('RIB元素顺序重复或倒退')
                previous = order
                if row['as_path_text'] is None or row['peer_asn'] is None:
                    raise ValueError('RIB缺少路径或Peer ASN，不补空值')
                # 同一前缀/路径在不同Peer重复时共享字符串，不复制规范原属性。
                yield RibElement(source, row['message_id'], row['ordinal'],
                    sys.intern(row['prefix']), sys.intern(row['as_path_text']), str(row['peer_asn']),
                    row['peer_ip'], row['bgp_id'] if row['bgp_id_present'] else None,
                    row['path_key'], row['afi'], row['safi'], row['action'], row['attributed_origin_asn'])

    def close(self):
        self.db.close()


def reference_projection(batches, *, csv_source, country_source):
    """CSV首ASN行保留、JSON同键最后值兼容；原整行仍在上游引用。"""
    selected = []
    for batch in batches:
        for row in batch.to_pylist():
            if row['source_id'] in (csv_source, country_source):
                selected.append(row)
    selected.sort(key=lambda row: (row['source_id'], row['row']))
    header = None
    csv_rows = []
    csv_refs = []
    names = {}
    countries = {}
    for row in selected:
        raw = json.loads(row['raw_row'])
        ref = f"{row['source_id']}:{row['location']}:{row['row']}"
        if row['source_id'] == csv_source:
            if row['location'] != 'csv' or not isinstance(raw, list):
                raise ValueError('AS参考CSV格式不符')
            if row.get('rule')=='reference-rows/v2':
                kind=row.get('csv_record_kind')
                if kind in ('blank_line','whitespace_line'):
                    continue
                if kind!='record':
                    raise ValueError('新版CSV参考缺少词法分类')
            if not raw:
                continue  # v1也能明确表示的空记录；原始行仍留在上游。
            if row.get('rule')!='reference-rows/v2' and len(raw)==1 and raw[0] and not raw[0].strip(' \t'):
                raise ValueError('CSV空白字段缺少词法依据，需要新版参考规范')
            if header is None:
                header = raw
                if not {'asn', 'as_name', 'global_rank'} <= set(header):
                    raise ValueError('AS参考缺必需列')
                csv_rows.append(raw)
                continue
            csv_rows.append(raw)
            csv_refs.append(ref)
        else:
            if not isinstance(raw, dict) or '__object_pairs__' not in raw:
                raise ValueError('国家参考缺整行对象')
            value = dict(raw['__object_pairs__'])
            countries[row['location']] = (value.get('country_cn'), ref)
    if header is None or not countries:
        raise ValueError('指定参考来源缺失')
    text=io.StringIO()
    writer=csv.writer(text,quoting=csv.QUOTE_ALL)
    writer.writerows(csv_rows)
    text.seek(0)
    frame=pd.read_csv(text,keep_default_na=False,usecols=['asn','as_name','global_rank'])
    if len(frame)!=len(csv_refs):
        raise ValueError('CSV解释行数与原始记录引用不一致')
    frame['raw_row_ref']=csv_refs
    frame.drop_duplicates(subset=['asn'],keep='first',inplace=True)
    for _,row in frame.iterrows():
        names[str(row['asn'])]=(row['as_name'],row['global_rank'],row['raw_row_ref'])
    return {asn: ReferenceAs(*(names.get(asn, ('', None, None))[:2]),
                country_cn=countries.get(asn, (None, None))[0],
                raw_row_ref=names.get(asn, ('', None, None))[2])
            for asn in names.keys() | countries.keys()}

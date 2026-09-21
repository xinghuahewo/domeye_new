"""C3公开固定定位读取接缝；完整准入证明由C4 inspect验证，资格由owner注入。"""
from collections import Counter, OrderedDict
from pathlib import Path
import pyarrow.parquet as pq

from data_pipeline.analysis.country_events.snapshot_store import lake_connect, cleanup
from data_pipeline.analysis.country_events.snapshot_schema import SCHEMAS, TABLES, row_decode


class FixedComponentAccess:
    tables, schemas = TABLES, SCHEMAS
    decode_row = staticmethod(row_decode)
    connect = staticmethod(lake_connect)

    def __init__(self, dsn, binding, manifest, *, limits, guard):
        self.binding, self.manifest, self.limits, self.guard = binding, manifest, limits, guard
        self.stats = Counter(); self.cache = OrderedDict(); self.cache_bytes = 0
        self.files = {}; self.handles = {}
        try:
            if binding.schema_name!='country_'+binding.component_id or not binding.component_id.isalnum() or type(binding.snapshot) is not int or binding.snapshot<0:
                raise ValueError('C4_component_binding_scope')
            db = self.connect(dsn)
            try:
                for table in self.tables:
                    links = db.execute("SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files('lake',?,schema=>?,snapshot_version=>?)",
                                       [table, binding.schema_name, binding.snapshot]).fetchall()
                    self.stats['layout_queries'] += 1
                    expected = manifest['lake_files'][table]
                    got = [[str(Path(p).relative_to(binding.root)), n] for p, n, d in links if d is None]
                    if got != expected or any(d is not None for _, _, d in links): raise ValueError('C4_lake_layout')
                    cols = db.execute(f'DESCRIBE SELECT * FROM lake.{binding.schema_name}.{table} AT (VERSION => {binding.snapshot})').fetchall()
                    self.stats['layout_queries'] += 1
                    if [(r[0],r[1]) for r in cols] != [(n,t) for n,t,_ in self.schemas[table]]: raise ValueError('C4_lake_schema')
            finally: db.close()
            for table in self.tables:
                path = Path(binding.root)/'data'/(table+'.parquet')
                handle = path.open('rb'); self.handles[table] = handle
                handle.seek(-8,2); footer=handle.read(8); handle.seek(0)
                if footer[4:]!=b'PAR1': raise ValueError('C4_parquet_footer')
                self.stats['footer_bytes']+=int.from_bytes(footer[:4],'little')
                if self.stats['footer_bytes']>limits.max_cache_bytes: raise ValueError('resource_limit:C4_footer_cache')
                self.files[table] = pq.ParquetFile(handle)
                self.stats['footer_opens'] += 1
            self.guard()
        except BaseException as error:
            cleanup((self.close,),error); raise

    def read(self, locators):
        """locator=(table,group,offset,sequence,hash)；整页I/O先检查后读取。"""
        self.guard()
        groups = set((x[0],x[1]) for x in locators)
        if len(groups) > self.limits.max_row_groups: raise ValueError('resource_limit:C4_row_groups')
        physical = sum(self.files[t].metadata.row_group(g).total_byte_size for t,g in groups if (t,g) not in self.cache)
        if physical > self.limits.max_read_bytes: raise ValueError('resource_limit:C4_read_bytes')
        result = {}
        # 同组所有命中行一次提取，即使该组大于缓存预算也不重复加载。
        for t,g in sorted(groups):
            key = (t,g)
            if key in self.cache:
                batch = self.cache.pop(key); self.cache[key] = batch; self.stats['cache_hits'] += 1
            else:
                self.guard(); batch = self.files[t].read_row_group(g)
                self.stats['row_groups_read'] += 1
                self.stats['uncompressed_read_bytes'] += self.files[t].metadata.row_group(g).total_byte_size
                self.stats['compressed_read_bytes'] += sum(self.files[t].metadata.row_group(g).column(i).total_compressed_size for i in range(self.files[t].metadata.num_columns))
                while self.cache and self.cache_bytes + batch.nbytes + self.stats['footer_bytes'] > self.limits.max_cache_bytes:
                    _, old = self.cache.popitem(last=False); self.cache_bytes -= old.nbytes
                if batch.nbytes + self.stats['footer_bytes'] <= self.limits.max_cache_bytes:
                    self.cache[key] = batch; self.cache_bytes += batch.nbytes
                self.stats['peak_cache_bytes'] = max(self.stats['peak_cache_bytes'], self.cache_bytes)
            for table,group,offset,sequence,sha in locators:
                if (table,group) != key: continue
                row = batch.slice(offset,1).to_pylist()
                if len(row)!=1 or (row[0]['_sequence'],row[0]['_row_hash']) != (sequence,sha): raise ValueError('C4_locator_mismatch')
                result[sequence] = self.decode_row(table,row[0]); self.stats['rows_decoded'] += 1
            self.guard()
        return result

    def close(self):
        try:
            cleanup((self.cache.clear,*(handle.close for handle in self.handles.values()),self.files.clear))
        finally:
            self.cache_bytes=0; self.handles.clear(); self.files.clear()

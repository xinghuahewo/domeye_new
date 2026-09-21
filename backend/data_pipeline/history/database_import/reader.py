"""有界离线 typed 批流；暂定数据与正常耗尽的完成回执分开。"""
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
import hashlib
from itertools import islice
from pathlib import Path

from data_pipeline.history.database_import.codec import canonical
from data_pipeline.history.database_import.freeze import Guard, digest, sha_file


def code_identity():
    return {p.name: sha_file(p) for p in sorted(Path(__file__).parent.glob('*.py'))}


class BulkReader:
    """必须用 with 或 finally/close；不正常耗尽则 receipt 始终为 None。

    数据批只含一个原表，按表索引及ordinal排序。批字节使用规范raw加换行，
    Arrow和Python双表示另由History读取缓冲、RSS门禁限制。
    """
    def __init__(self, history, token, *, table_indices, purpose, batch_rows):
        if purpose not in ('audit', 'business'): raise ValueError('未知读取用途')
        cap = history.limits.batch_rows if batch_rows is None else batch_rows
        if type(cap) is not int or not 1 <= cap <= history.limits.batch_rows: raise ValueError('输出批行预算无效')
        if table_indices is not None:
            table_indices = tuple(islice(table_indices, history.limits.max_tables+1))
            if not table_indices or len(table_indices) > history.limits.max_tables or any(type(i) is not int or i < 0 for i in table_indices) or tuple(sorted(set(table_indices))) != table_indices:
                raise ValueError('表子集须显式非空、唯一且按表索引排序')
        self._history, self._token = history, token
        self._tables, self._purpose, self._cap = table_indices, purpose, cap
        self._limits = history.limits
        self._receipt = None
        self._state = 'pending'
        self._iterator = self._run()

    @property
    def receipt(self): return deepcopy(self._receipt)

    @property
    def state(self): return self._state

    def __enter__(self): return self

    def __exit__(self, *error): self.close()

    def __iter__(self): return self

    def __next__(self): return next(self._iterator)

    def close(self):
        self._iterator.close()
        if self._state in ('pending', 'provisional'): self._state = 'incomplete'

    def _check_budget(self):
        if self._history.limits != self._limits: raise ValueError('批量会话预算漂移')

    def _run(self):
        h, token = self._history, self._token
        self._state = 'provisional'
        try:
            self._check_budget()
            code = code_identity()
            self._check_budget()
            component = h.component(token); manifest = component['original']
            tables = tuple(range(len(manifest['tables']))) if self._tables is None else self._tables
            if any(i >= len(manifest['tables']) for i in tables): raise ValueError('未知原表')
            selected_rows = sum(manifest['tables'][i]['rows'] for i in tables)
            selected_bytes = sum(block['bytes'] for i in tables for block in manifest['tables'][i]['blocks'])
            if selected_rows > self._limits.max_total_rows or selected_bytes > self._limits.max_total_bytes:
                raise ValueError('声明表集合超总扫描预算')
            if self._purpose == 'business' and manifest['availability']['status'] != 'available': raise ValueError('隔离来源禁止业务旁路')
            guard = Guard(h.root, h.limits); results = []; batches = 0
            for ti in tables:
                self._check_budget()
                table = manifest['tables'][ti]; pending = []; size = count = 0; content = hashlib.sha256()
                with closing(h._stream(component, token, ti)) as rows:
                    while True:
                        # for会先拉取下一行；须在恢复源流/下一Arrow批之前拒绝漂移。
                        self._check_budget()
                        try: row = next(rows)
                        except StopIteration: break
                        self._check_budget()
                        encoded = canonical(row['values']); guard.row(encoded)
                        if pending and (len(pending) == self._cap or size+len(encoded)+1 > h.limits.batch_bytes):
                            batches += 1
                            yield {'qualification':'provisional', 'table_index':ti, 'rows':pending, 'raw_bytes':size}
                            self._check_budget()
                            pending = []; size = 0
                        pending.append(row); size += len(encoded)+1; count += 1; content.update(encoded+b'\n')
                        guard.max_batch_rows = max(guard.max_batch_rows, len(pending))
                        guard.max_batch_bytes = max(guard.max_batch_bytes, size)
                    if pending:
                        batches += 1
                        yield {'qualification':'provisional', 'table_index':ti, 'rows':pending, 'raw_bytes':size}
                        self._check_budget()
                if count != table['rows'] or content.hexdigest() != table['content_sha256']: raise ValueError('批量读取整表计数/摘要不符')
                results.append({'table_index':ti, 'rows':count, 'content_sha256':content.hexdigest()})
            # 完成资格与代码身份只在正常耗尽时签发；不按批重复扫描全文件。
            self._check_budget()
            with h._delivery_qualification(token):
                self._check_budget()
                if code_identity() != code: raise ValueError('批量会话执行代码版本漂移')
                receipt = {'contract':'history-bulk/v1', 'qualification':'complete', 'token':asdict(token),
                           'scope':'whole_archive' if self._tables is None else 'declared_table_subset',
                           'table_indices':list(tables), 'tables':results, 'purpose':self._purpose,
                           'availability':manifest['availability'], 'code_sha256':code,
                           'import_code_sha256':component['binding']['code_sha256'],
                           'scan_plan':{'selected_rows':selected_rows,'selected_raw_bytes':selected_bytes,
                                        'width_aggregation_tables':len(tables),'ordered_table_scans':len(tables),
                                        'full_qualification_passes':2,
                                        'bound_parquet_bytes_per_pass':sum(f['bytes'] for f in component['binding']['files'])},
                           'limits':asdict(h.limits), 'output_batch_rows':self._cap, 'batches':batches, 'resources':guard.report()}
                if len(canonical(receipt)) > h.limits.max_metadata_bytes: raise ValueError('完成回执元数据超限')
                receipt['receipt_sha256'] = digest(receipt)
                guard.check()
            self._receipt = receipt
            self._state = 'complete'
        except GeneratorExit:
            self._state = 'incomplete'
            raise
        except BaseException:
            self._state = 'failed'
            raise

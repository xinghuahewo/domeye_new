"""Q3-B预算暂停点回归：仅本任务健康旧Token，探针透传实际读取。"""
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

import pytest

from data_pipeline.history.database_import import History, Limits, Token
from data_pipeline.history.database_import.codec import canonical
import data_pipeline.history.database_import.store as store_module


@pytest.fixture(scope='module')
def saved_history():
    location = os.environ.get('Q3_PRIVATE_ROOT')
    if not location: pytest.skip('仅显式本任务私有PG及其旧Token，不自动造数或连接其他库')
    root = Path(location).resolve()
    index = root/'交付索引.json'
    if not index.exists(): pytest.skip('缺本任务b1人工Token证据')
    token = Token(**json.loads(index.read_text())['资源与组件']['pg_component']['token'])
    dsn = f'host={root / "socket"} port=28763 dbname=postgres'
    h = History(dsn, root)
    component = h.component(token)
    assert [t['rows'] for t in component['original']['tables']][:2] == [4, 1]
    out = root/'q3-b'/'budget-ec'/('trace-'+uuid.uuid4().hex); out.mkdir(parents=True)
    print('Q3_BUDGET_EVIDENCE='+str(out))
    return root, dsn, token, component, out


class ActualReads:
    """记录尝试发起真实读取的边界；close也透传并单独记录。"""
    def __init__(self, history, monkeypatch):
        self.events = []; self.open_readers = self.open_databases = 0
        self.owner = history; self.table = None
        component, width, lake = history._component, history._read_width, history._lake
        def qualify(*args, **kwargs):
            self.events.append('component_exit' if kwargs.get('lock') else 'component_entry')
            return component(*args, **kwargs)
        def sizing(*args, **kwargs):
            self.table = args[1]; self.events.append('width_'+str(self.table))
            return width(*args, **kwargs)
        def connect(*args, **kwargs):
            db = lake(*args, **kwargs); self.open_databases += 1
            probe = self
            class Database:
                def __getattr__(self, key): return getattr(db, key)
                def execute(self, *a, **kw): db.execute(*a, **kw); return self
                def fetch_record_batch(self, requested):
                    table = probe.table
                    probe.events.append('arrow_open_'+str(table))
                    reader = db.fetch_record_batch(requested); iterator = iter(reader)
                    probe.open_readers += 1
                    class Reader:
                        @property
                        def schema(self): return reader.schema
                        def __iter__(self): return self
                        def __next__(self):
                            probe.events.append('arrow_next_'+str(table))
                            return next(iterator)
                        def close(self):
                            reader.close(); probe.open_readers -= 1
                            probe.events.append('arrow_close_'+str(table))
                    return Reader()
                def close(self):
                    db.close(); probe.open_databases -= 1
                    probe.events.append('database_close')
            return Database()
        monkeypatch.setattr(history, '_component', qualify)
        monkeypatch.setattr(history, '_read_width', sizing)
        monkeypatch.setattr(history, '_lake', connect)


@pytest.mark.parametrize('mode', ['before_first', 'between_tables', 'same_table', 'last_yield'])
def test_budget_change_stops_before_actual_read(saved_history, monkeypatch, mode):
    root, dsn, token, _, out = saved_history
    h = History(dsn, root, replace(Limits(), batch_rows=1) if mode=='same_table' else Limits())
    probe = ActualReads(h, monkeypatch)
    reader = h.bulk(token, batch_rows=1 if mode=='same_table' else 1000,
                    table_indices=[0] if mode=='last_yield' else None)
    seen = []
    try:
        if mode != 'before_first': seen.append(next(reader))
        probe.events.append('limits_changed')
        h.limits = replace(h.limits, max_total_rows=1)
        with pytest.raises(ValueError, match='预算漂移') as error: seen.extend(reader)
    finally: reader.close()
    after = probe.events[probe.events.index('limits_changed')+1:]
    record = {'mode':mode, 'token':asdict(token), 'actual_calls':probe.events,
              'calls_after_change':after, 'provisional_batches':len(seen), 'error':str(error.value),
              'receipt':reader.receipt, 'state':reader.state,
              'open_readers':probe.open_readers, 'open_databases':probe.open_databases}
    (out/(mode+'.json')).write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n')
    assert reader.receipt is None and reader.state=='failed'
    assert probe.open_readers==probe.open_databases==0
    assert not [e for e in after if e.startswith(('component_', 'width_', 'arrow_open_', 'arrow_next_'))], record


def test_fixed_small_plan_rejects_before_width(saved_history, monkeypatch):
    root, dsn, token, _, out = saved_history
    h=History(dsn,root,replace(Limits(),max_total_rows=1)); probe=ActualReads(h,monkeypatch)
    with pytest.raises(ValueError,match='总扫描预算'):
        with h.bulk(token) as reader: list(reader)
    assert probe.events==['component_entry'] and reader.receipt is None
    (out/'fixed_small_plan.json').write_text(json.dumps({'actual_calls':probe.events,'receipt':reader.receipt},ensure_ascii=False,indent=2)+'\n')


@pytest.mark.parametrize('cap', [1, 2, 1000])
def test_fixed_budget_batching_and_qualification_cost(saved_history, monkeypatch, cap):
    root, dsn, token, component, out = saved_history
    h=History(dsn+" options='-c log_statement=all'",root); probe=ActualReads(h,monkeypatch)
    original_sha=store_module.sha_file; files=[]
    def sha(path,*args): files.append(str(path)); return original_sha(path,*args)
    monkeypatch.setattr(store_module,'sha_file',sha)
    log=root/'pg.log'; log_start=log.stat().st_size
    with h.bulk(token,batch_rows=cap) as reader: batches=list(reader)
    with log.open('rb') as file: file.seek(log_start); protocol=file.read().decode()
    (out/('normal_'+str(cap)+'.log')).write_text(protocol)
    statements=len(re.findall(r'statement:|execute [^:]+:',protocol))
    assert statements>0
    for ti, table in enumerate(component['original']['tables']):
        rows=[r for batch in batches if batch['table_index']==ti for r in batch['rows']]
        assert [r['occurrence']['ordinal'] for r in rows]==list(range(table['rows']))
        assert hashlib.sha256(b''.join(canonical(r['values'])+b'\n' for r in rows)).hexdigest()==table['content_sha256']
    assert reader.receipt['qualification']=='complete' and probe.open_readers==probe.open_databases==0
    assert probe.events.count('component_entry')==probe.events.count('component_exit')==1
    assert [e for e in probe.events if e.startswith('width_')]==['width_0','width_1','width_2']
    assert len(files)==2*(2+len(component['binding']['files']))
    for previous in out.glob('normal_*.json'):
        assert statements==json.loads(previous.read_text())['pg_statements']
    (out/('normal_'+str(cap)+'.json')).write_text(json.dumps({'actual_calls':probe.events,'carrier_sha_calls':files,'pg_statements':statements,'receipt':reader.receipt},ensure_ascii=False,indent=2)+'\n')

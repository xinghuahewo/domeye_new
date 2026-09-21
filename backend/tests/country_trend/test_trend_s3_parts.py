"""分片后继直接验收：隔离合法行、清单/实体损坏与逻辑摘要，无PG。"""
from dataclasses import replace
from pathlib import Path
import hashlib
import json
import pytest
from data_pipeline.analysis.country_trends.contract import row
from data_pipeline.analysis.country_trends import stream_schema as schema, part_manifest as parts, parquet_metadata as parquet
from data_pipeline.analysis.country_trends.stream_files import write_body, read_body
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from tests.country_trend.test_country_trend_s3_files import fixture


@pytest.fixture(autouse=True)
def short_part_boundaries(request, monkeypatch):
    # 损坏用例保持三行三片，避免为相同缺片错误反复生产257行。
    if request.node.name not in ('test_original_257_rows_complete_with_batch_1_and_2',
                                  'test_written_byte_target_rolls_before_group_cap'):
        monkeypatch.setattr(parquet, 'MAX_ROW_GROUPS', 1)


def rows(n=257):
    return tuple(replace(row('context_source',(),(str(i),),source_ref=str(i),role='feature',raw_typed=schema.encode(i)),result_id='fixture') for i in range(n))


def logical(proof):
    return proof['rows'],proof['sha256'],{k:(v['rows'],v['sha256']) for k,v in proof['tables'].items()}


def test_original_257_rows_complete_with_batch_1_and_2(tmp_path):
    values=rows();proofs=[]
    for batch in (1,2):
        limits=replace(S2Limits(),max_batch_rows=batch,max_rows=1,max_total_bytes=1,max_references=1)
        root=tmp_path/str(batch);proof=write_body(iter(values),root,limits=limits)
        assert tuple(read_body(root,limits=limits))==values
        assert proof['tables']['context_source']['parts']==(2 if batch==1 else 1)
        assert len(list(parts.iter_parts(root,'context_source',limits,lambda:None)))==(2 if batch==1 else 1)
        import pyarrow.parquet as pq
        with pq.ParquetFile(root/parts.part_name('context_source',0)) as file:
            assert file.metadata.num_row_groups==(256 if batch==1 else 129)
        proofs.append(logical(proof))
    assert proofs[0]==proofs[1]
    assert proofs[0][1]=='d940699728996492c028f21747e3e7296e4faf5b83d25892da798570612ffe20'


@pytest.mark.parametrize('damage',['missing','duplicate','reordered','truncated'])
def test_part_inventory_damage_refuses_complete_read(tmp_path,damage):
    root=tmp_path/'body';limits=replace(S2Limits(),max_batch_rows=1)
    write_body(iter(rows(3)),root,limits=limits)
    path=root/parts.manifest_name('context_source');lines=path.read_bytes().splitlines(keepends=True)
    if damage=='missing':(root/parts.part_name('context_source',1)).unlink()
    elif damage=='duplicate':path.write_bytes(lines[0]+lines[0]+lines[2])
    elif damage=='reordered':path.write_bytes(lines[1]+lines[0]+lines[2])
    else:path.write_bytes(b''.join(lines[:-1]))
    with pytest.raises(ValueError,match='part_|missing'):
        tuple(read_body(root,limits=limits))


def test_fk_is_still_global_across_files(tmp_path):
    limits=replace(S2Limits(),max_batch_rows=1)
    values=fixture();write_body(iter(values),tmp_path/'good',limits=limits)
    assert tuple(read_body(tmp_path/'good',limits=limits))==values
    bad=tuple(r for r in values if r.kind!='country_qualification_source')
    with pytest.raises(ValueError,match='fk'):
        write_body(iter(bad),tmp_path/'bad',limits=limits)


def test_table_stream_early_close_does_not_mark_full_validation(tmp_path):
    root=tmp_path/'body';limits=replace(S2Limits(),max_batch_rows=1)
    write_body(iter(rows(3)),root,limits=limits)
    stream=parts.iter_table(root,'context_source',limits,lambda:None)
    next(stream);stream.close()
    # 尾损坏不能被前一页读取当成成功；新的完整读必须拒绝。
    (root/parts.part_name('context_source',2)).unlink()
    with pytest.raises(ValueError,match='missing'):
        list(parts.iter_table(root,'context_source',limits,lambda:None))


def test_p1_original_stream_and_tail_with_substantive_fixture_file_validation(tmp_path,monkeypatch):
    """只测试原P1流控制，使用无准入资格的fixture标识；不冒充真实AD/current。"""
    from types import SimpleNamespace
    from data_pipeline.analysis.country_trends import result_admission as owner
    root=tmp_path/'candidate';root.mkdir();limits=replace(S2Limits(),max_batch_rows=2)
    values=rows(5);proof=write_body(iter(values),root/'data',limits=limits)
    binding={'binding':{'result_id':'fixture'},'proof':{'body':proof}}
    admission={'owner_binding':schema.encode(binding),'admission_id':'unqualified-fixture-control-only'}
    runtime=SimpleNamespace(output_root=root,limits=limits,guarded=lambda g:g,path=lambda p:Path(p))
    checks=[]
    def validate_files(rt,ad,*,guard):
        assert ad==admission
        parts.current_parts(rt,binding,guard)
        checks.append('actual-file-check')
    monkeypatch.setattr(owner,'verify_current',validate_files)
    request=dict(view='context_source',codec_version=owner.CODEC,batch_rows=2,batch_bytes=limits.max_batch_bytes,
        scope_typed=schema.encode(dict(event=None,key_typed=None,after_sequence=-1,stop_sequence=None)))
    with owner.open_reader(runtime,admission,request,guard=lambda:None) as session:
        actual=[schema.restore('context_source',item['row']) for batch in session for item in schema.decode(batch['rows_typed'])]
    assert actual==list(values) and session.receipt['rows']==5 and len(checks)==2
    with owner.open_reader(runtime,admission,request,guard=lambda:None) as early:
        next(early)
    assert early.receipt is None
    with pytest.raises(ValueError,match='missing'):
        with owner.open_reader(runtime,admission,request,guard=lambda:None) as changed:
            list(changed)
            (root/'data'/parts.part_name('context_source',2)).unlink()
    assert changed.receipt is None


def test_resigned_physical_part_cannot_duplicate_global_sequence(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    root=tmp_path/'body';limits=replace(S2Limits(),max_batch_rows=1)
    write_body(iter(rows(3)),root,limits=limits)
    path=root/parts.part_name('context_source',1)
    table=pq.read_table(path);batch=table.to_pylist();batch[0]['sequence']=0
    pq.write_table(pa.Table.from_pylist(batch,schema=table.schema),path)
    manifest=root/parts.manifest_name('context_source')
    entries=[json.loads(line) for line in manifest.read_bytes().splitlines()]
    summary=dict(rows=1,first=0,last=0,sha256=schema.row_hash(schema.restore('context_source',batch[0])))
    entries[1]=parts.describe(path,'context_source',1,summary,lambda:None)
    raw=b''.join(parts.encode(entry)+b'\n' for entry in entries);manifest.write_bytes(raw)
    summary=parts.read_summary(root,limits,lambda:None)
    summary['tables']['context_source']['parts_sha256']=hashlib.sha256(raw).hexdigest()
    (root/parts.SUMMARY).write_bytes(parts.encode(summary))
    with pytest.raises(ValueError,match='part_sequence'):
        tuple(read_body(root,limits=limits))


@pytest.mark.parametrize('damage',[None,'missing','duplicate','order'])
def test_fresh_audit_lake_inventory_is_streamed_and_exact(tmp_path,damage):
    from data_pipeline.analysis.country_trends.stream_audit import compare_lake_files
    root=tmp_path/'candidate';root.mkdir();limits=replace(S2Limits(),max_batch_rows=1)
    body=write_body(iter(rows(3)),root/'data',limits=limits)
    values=[(str(root/'data'/p['path']),p['entity']['size'],None)
            for p in parts.iter_parts(root/'data','context_source',limits,lambda:None)]
    if damage=='missing':values.pop()
    elif damage=='duplicate':values.append(values[-1])
    elif damage=='order':values[0],values[1]=values[1],values[0]
    class Lake:
        def execute(self,sql,args):
            assert sql.endswith('ORDER BY data_file')
            self.values=iter(values);return self
        def fetchone(self):return next(self.values,None)
        def fetchall(self):pytest.fail('unbounded catalog list')
    binding=dict(root=str(root),schema_name='unqualified-fixture',snapshot=1)
    if damage:
        with pytest.raises(ValueError,match='lake_file_binding'):
            compare_lake_files(Lake(),binding,body,'context_source',limits,lambda:None)
    else:compare_lake_files(Lake(),binding,body,'context_source',limits,lambda:None)


def test_written_byte_target_rolls_before_group_cap(tmp_path,monkeypatch):
    limits=replace(S2Limits(),max_batch_rows=1)
    values=rows(3)
    normal=write_body(iter(values),tmp_path/'normal',limits=limits)
    assert normal['tables']['context_source']['parts']==1
    monkeypatch.setattr(parquet,'TARGET_FILE_BYTES',1)
    rolled=write_body(iter(values),tmp_path/'rolled',limits=limits)
    assert rolled['tables']['context_source']['parts']==3
    assert logical(normal)==logical(rolled)
    assert tuple(read_body(tmp_path/'rolled',limits=limits))==values

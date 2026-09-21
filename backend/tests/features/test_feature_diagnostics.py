"""人工正式观察链路的完整诊断、完成门禁与旧版本可用性。"""
from dataclasses import asdict
import gzip
import hashlib
import json
import struct
import pyarrow as pa
import psycopg2
import pytest
from tests.features.test_feature_storage import prepared, formal_call, table
from tests.observations.test_observation_consumer import feature_dsn
from tests.observations.test_observation_mrt import update, attr, mrt
from data_pipeline.analysis.features.projection import FeatureAdapter
from data_pipeline.analysis.features.reference import load_reference
from data_pipeline.analysis.features.store import FeatureStore, read_table, DIAGNOSTICS_VERSION
from data_pipeline.analysis.features.run import run_fixture
from data_pipeline.bgp.archive.message_reader import MessageBatch
from data_pipeline.bgp.input.path_decoding import decode_element


def expected_diagnostics(reader, plan, sha, ref):
    reference = load_reference(reader,sha,ref)
    saved = {s.source_id:[] for s in plan.sources}
    for record in reader.stream():
        if isinstance(record,MessageBatch):
            saved[record.source_id].extend(decode_element(r) for r in record.elements)
    expected = []
    for mode in ('ordinary','ir'):
        adapter = FeatureAdapter(mode,reference,plan,'pure-expected')
        for rank,binding in enumerate(plan.sources):
            rows = saved[binding.source_id]
            result = adapter.consume_source(binding,[pa.RecordBatch.from_pylist(rows)] if rows else [])
            if result:
                expected.extend((mode,rank,i,asdict(d)) for i,d in enumerate(result.diagnostics))
    return expected


@pytest.mark.parametrize('prefix_count',[64,256])
def test_formal_diagnostics_match_pure_and_fixed_snapshot(tmp_path,feature_dsn,prefix_count,monkeypatch):
    # /8 形成真实数量阈值；/0 进入既有跳过规则，不改纯计算。
    raw = update(ann=b''.join(bytes([8,n]) for n in range(prefix_count))+b'\x00',attrs=attr())
    raw = struct.pack('!I',200)+raw[4:]
    import tests.features.test_feature_storage as test_feature_storage
    from data_pipeline.bgp.input.mrt_reader import source_identity
    original_manifest = test_feature_storage.fixture_manifest
    def with_oversized_rib(root):
        manifest = original_manifest(root)
        entry = manifest['inputs'][0]
        path = root/'rib.gz'
        attrs = attr()
        rib = struct.pack('!I',1)+b'\x07\x80'+struct.pack('!HHIH',1,0,90,len(attrs))+attrs
        path.write_bytes(gzip.compress(gzip.decompress(path.read_bytes())+mrt(rib,2,13),mtime=0))
        entry['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        entry['size'] = path.stat().st_size
        entry['source_id'] = source_identity('rrc25',entry['origin_uri'],entry['sha256'])
        manifest['baseline_source'] = entry['source_id']
        return manifest
    monkeypatch.setattr(test_feature_storage,'fixture_manifest',with_oversized_rib)
    reader,plan,sha,ref = prepared(tmp_path,feature_dsn,a_bytes=raw,empty_last=True)
    expected = expected_diagnostics(reader,plan,sha,ref)
    reports = []
    outputs = []
    for batch in (1,512):
        result = formal_call(tmp_path/f'formal-{batch}',reader,plan,sha,ref,batch_rows=batch,max_rss_bytes=2*1024**3)
        assert result.returncode == 0,result.stderr
        report = json.loads(result.stdout); reports.append(report)
        rows = [r for b in read_table(feature_dsn,report['run_id'],report['snapshot'],'module_diagnostics') for r in b.to_pylist()]
        rows.sort(key=lambda r:(r['mode'],r['source_rank'],r['sequence']))
        actual = []
        for row in rows:
            diagnostic = {k:row[k] for k in ('scope','identifier','reason','raw_prefix','normalized_prefix','sample_prefixes')}
            diagnostic['sample_prefixes'] = tuple(diagnostic['sample_prefixes'])
            actual.append((row['mode'],row['source_rank'],row['sequence'],diagnostic))
            assert row['dataset_version'] == DIAGNOSTICS_VERSION
            assert row['observation_version'] == plan.observation_version
            binding = plan.sources[row['source_rank']]
            assert row['source_id'] == binding.source_id
            assert (row['start'],row['end'],row['file_time']) == tuple(v.isoformat() for v in (binding.window.start,binding.window.end,binding.window.file_time))
            assert row['reference_version'] and row['projection_version'] and row['rule_id']
            if row['metric']:
                assert len(row['sample_prefixes']) == 10
                assert row['sample_prefixes'] == sorted(f'{n}.0.0.0/8' for n in range(prefix_count))[:10]
                assert row['value'] == ((prefix_count*65536+(1 if prefix_count==64 else 0)) * (1 if row['metric']=='v4Prefix_num' else 256))
                assert row['threshold'] == {'v4prefix_num_reached_full_ipv4_c_segments':2**24,'v4ip_num_reached_full_ipv4_space':2**32,'v4ip_num_exceeds_suspicious_threshold':10**9}[row['reason']]
                assert row['comparison'] == '>=' and row['value'] >= row['threshold']
                assert row['unit'] in ('ipv4_24_union_blocks','ipv4_24_union_blocks_x256')
            else:
                assert row['threshold'] is None and row['value'] is None and row['unit'] is None
        assert actual == sorted(expected,key=lambda r:r[:3])
        assert any(r['scope']=='aggregate' for r in rows)
        assert {r['mode'] for r in rows} == {'ordinary','ir'}
        assert report['actual_rows']['module_diagnostics'] == len(rows)
        assert sum(r['diagnostics'] for r in table(feature_dsn,report,'source_receipts')) == len(rows)
        outputs.append([{k:v for k,v in r.items() if k!='projection_version'} for r in rows])
    assert outputs[0] == outputs[1]
    # 后续运行完成后，首个固定快照仍是原始完整数据。
    old = table(feature_dsn,reports[0],'module_diagnostics')
    old.sort(key=lambda r:(r['mode'],r['source_rank'],r['sequence']))
    assert [{k:v for k,v in r.items() if k!='projection_version'} for r in old] == outputs[0]


def test_saved_empty_and_old_not_saved_are_distinct(tmp_path,feature_dsn):
    reader,plan,sha,ref = prepared(tmp_path,feature_dsn,a_bytes=b'',empty_last=True)
    report = run_fixture(reader,plan,sha,ref,tmp_path/'feature',max_rss_bytes=2*1024**3)
    assert table(feature_dsn,report,'module_diagnostics') == []
    receipts = table(feature_dsn,report,'source_receipts')
    assert all(r['diagnostics']==0 and r['diagnostics_state']=='saved' for r in receipts if r['source_rank'])
    windows = table(feature_dsn,report,'windows')
    # 构造旧版规格：没有诊断版本声明；旧表仍按同一快照读取。
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE feature.runs SET specification=specification-'diagnostics_dataset_version' WHERE run_id=%s",(report['run_id'],))
    assert table(feature_dsn,report,'windows') == windows
    with pytest.raises(ValueError,match='not_saved'):
        table(feature_dsn,report,'module_diagnostics')


def test_missing_diagnostics_blocks_complete(tmp_path,feature_dsn,monkeypatch):
    raw = update(ann=b'\x00')
    reader,plan,sha,ref = prepared(tmp_path,feature_dsn,a_bytes=struct.pack('!I',200)+raw[4:])
    append = FeatureStore.append
    def omit(self,name,row):
        if name != 'module_diagnostics': append(self,name,row)
    monkeypatch.setattr(FeatureStore,'append',omit)
    with pytest.raises(ValueError,match='诊断必要输出不完整'):
        run_fixture(reader,plan,sha,ref,tmp_path/'feature',max_rss_bytes=2*1024**3)
    assert not (tmp_path/'feature'/'execution.json').exists()
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT state FROM feature.runs')
        assert c.fetchall() == [('failed',)]

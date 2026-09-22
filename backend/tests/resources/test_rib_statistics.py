"""独立时点统计与既有逐元素 Resource / 起源规则对照；只使用人工 Parquet。"""
from datetime import datetime, timezone
import hashlib
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_pipeline.analysis.resources.compute import ResourceComputer, RibContext, RibElement, METRIC_UNITS
from data_pipeline.results.rib_statistics import compute_statistics, digest_file, verify_archive


def path_bytes(values, kind=2):
    return bytes([kind, len(values)]) + b''.join(struct.pack('!I', x) for x in values)


def fixture_archive(tmp_path):
    source = tmp_path/'source.gz'; source.write_bytes(b'fixture-source-binding')
    sha = digest_file(source); source_id = 'fixture-rib'
    samples = [
        ('0.0.0.0/0', path_bytes([64501]), None, '64501'),
        ('::/0', path_bytes([64501]), None, '64501'),
        ('192.0.2.0/24', path_bytes([64500,65000]), None, '64500 65000'),
        ('192.0.2.0/24', path_bytes([64501]), None, '64501'),
        ('2001:db8::/48', path_bytes([64501]), None, '64501'),
        ('198.51.100.0/25', path_bytes([64501,64502], 1), None, '{64501,64502}'),
        ('203.0.113.0/24', path_bytes([64502]), path_bytes([64503]), '64502'),
        ('2001:db8:1::/49', path_bytes([64507], 3), None, '(64507)'),
        ('203.0.114.0/24', None, None, ''),
    ]
    attempt = 'a'*32; directory=tmp_path/attempt/'segment-000000';directory.mkdir(parents=True)
    stamp=int(datetime(2026,2,24,tzinfo=timezone.utc).timestamp())
    messages=[{'message_id':'m0','record':0,'epoch':stamp,'kind':'peer_index_table','source_id':source_id,'content_sha256':sha}]
    elements=[];paths={};reference=[]
    for i,(prefix,raw,as4,text) in enumerate(samples,1):
        key=hashlib.sha256((raw or b'')+b'|'+(as4 or b'')).hexdigest()
        messages.append({'message_id':f'm{i}','record':i,'epoch':stamp,'kind':'rib','source_id':source_id,'content_sha256':sha})
        elements.append({'message_id':f'm{i}','action':'rib_snapshot','afi':2 if ':' in prefix else 1,'safi':1,'prefix':prefix,'path_key':key})
        paths[key]={'path_key':key,'as_path_raw':raw,'as4_path_raw':as4,'as_path_text':text,'asn_width':4}
        reference.append(RibElement(source_id,f'm{i}',0,prefix,text,'64510'))
    peers=[{'index':0}]
    tables={'messages':messages,'elements':elements,'paths':list(paths.values()),'peers':peers}
    files=[]
    for name,rows in tables.items():
        p=directory/(name+'.parquet');pq.write_table(pa.Table.from_pylist(rows),p)
        files.append({'path':str(p),'table':name,'size':p.stat().st_size,'sha256':digest_file(p),'rows':len(rows)})
    counts={k:len(v) for k,v in tables.items()}
    binding={'schema_version':'retained-native-rib/v1','collector_id':'rrc25','archive_attempt':attempt,
             'source':{'path':str(source),'sha256':sha,'source_id':source_id},
             'source_counts':counts,'source_receipt_sha256':'f'*64,
             'segments':[{'ordinal':0,'input':{'segment':0,'first_record':0,'next_record':len(messages),'counts':counts},'files':files}]}
    return binding,reference


def test_statistics_matches_existing_resource_rules_and_keeps_canonical_separate(tmp_path):
    binding,items=fixture_archive(tmp_path)
    body=compute_statistics(binding,tmp_path/'result',memory_limit='128MB',threads=1,min_free_bytes=1)
    context=RibContext('fixture-rib','rrc25',datetime(2026,2,24,tzinfo=timezone.utc),'none','actual_mrt')
    old=ResourceComputer({},topology_enabled=False).compute(context,items).rows[0]
    for name,unit in METRIC_UNITS.items():
        assert body['resource']['metrics'][name] == {'raw':getattr(old,name),'main':getattr(old,name),
                'qualification':'qualified','reason':'verified_independent_rib','unit':unit}
    canonical=body['canonical']['by_family']
    assert canonical['all']['visible_prefixes']==8  # 含两条默认路由；重复 /24 仅一条
    assert canonical['ipv4']['visible_origin_ases']==2  # 跳过私有尾 ASN，AS_SET / AS4 不猜
    assert canonical['ipv6']['visible_origin_ases']==1
    assert canonical['all']['visible_origin_ases']==2  # 双栈 AS 并集，不相加
    assert body['resource']['metrics']['public_as_count']['main']==2  # 旧尾 AS 64501、64502
    assert body['resource']['metrics']['private_as_count']['main']==1
    assert canonical['all']['unattributed_entries']==4
    assert (tmp_path/'result/statistics.json').exists()


def test_archive_rejects_missing_segment_and_changed_bytes(tmp_path):
    binding,_=fixture_archive(tmp_path)
    binding['segments'][0]['input']['first_record']=1
    with pytest.raises(ValueError,match='不连续'):verify_archive(binding)
    binding['segments'][0]['input']['first_record']=0
    p=tmp_path/binding['archive_attempt']/'segment-000000/elements.parquet'
    with p.open('ab') as f:f.write(b'changed')
    with pytest.raises(ValueError,match='摘要'):verify_archive(binding)


def test_legacy_path_manifest_uses_footer_and_still_checks_segment_count(tmp_path):
    binding,_=fixture_archive(tmp_path)
    next(f for f in binding['segments'][0]['files'] if f['table']=='paths').pop('rows')
    assert verify_archive(binding)[1]['paths'] == binding['source_counts']['paths']
    binding['segments'][0]['input']['counts']['paths'] += 1
    with pytest.raises(ValueError,match='段计数'):verify_archive(binding)


def test_empty_snapshot_is_not_published_as_zero(tmp_path):
    binding,_=fixture_archive(tmp_path)
    item=next(f for f in binding['segments'][0]['files'] if f['table']=='elements')
    path=tmp_path/binding['archive_attempt']/'segment-000000/elements.parquet'
    table=pq.read_table(path).slice(0,0);pq.write_table(table,path)
    item.update(rows=0,size=path.stat().st_size,sha256=digest_file(path))
    binding['source_counts']['elements']=0
    with pytest.raises(ValueError,match='空 RIB'):verify_archive(binding)

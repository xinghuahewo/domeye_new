import hashlib
import json
import struct

import pyarrow as pa
import pytest

from data_pipeline.analysis.resources.adapter import COLUMNS, ObservationInput, reference_projection
from data_pipeline.analysis.resources import ResourceComputer
from tests.resources.test_resource_computation import context, element


def test_arrow_source_order_and_observation_identity(tmp_path):
    def row(source,record,prefix):
        return {**dict.fromkeys(COLUMNS), 'source_id':source,'content_sha256':'content-hash','record':record,'ordinal':0,
            'message_id':source+str(record),'event_id':source+str(record)+':0','epoch':100,
            'action':'rib_snapshot','prefix':prefix,'as_path_text':'9808 {100}',
            'peer_asn':200,'peer_ip':'1.2.3.4','bgp_id_present':False,'afi':1,'safi':1,'path_key':'attributes:1'}
    input=ObservationInput(tmp_path/'staged.db')
    try:
        input.load([pa.RecordBatch.from_pylist([row('z',2,'2.0.0.0/24'),row('a',1,'3.0.0.0/24'),row('z',1,'1.0.0.0/24')])])
        elements=list(input.elements('z',batch_size=1))
        assert [e.prefix for e in elements]==['1.0.0.0/24','2.0.0.0/24']
        assert elements[0].path=='9808 {100}'
        assert elements[0].attributes_ref=='attributes:1'
        assert input.range('z')==(100,100,2)
        assert input.snapshot_epoch('z')==100
        assert input.content_sha256('z')=='content-hash'
        with pytest.raises(ValueError):input.range('missing')
    finally:input.close()


def test_reference_first_csv_duplicate_json_last_and_raw_refs():
    data=[{'source_id':'csv','row':0,'location':'csv','raw_row':json.dumps(['asn','as_name','global_rank','unused'])},
        {'source_id':'csv','row':1,'location':'csv','raw_row':json.dumps(['200','first','1.9','kept upstream'])},
        {'source_id':'csv','row':2,'location':'csv','raw_row':json.dumps(['200','second','2','also kept'])},
        {'source_id':'json','row':0,'location':'200','raw_row':json.dumps({'__object_pairs__':[['country_cn','甲'],['other','retained']]})},
        {'source_id':'json','row':1,'location':'200','raw_row':json.dumps({'__object_pairs__':[['country_cn',None],['country_cn','乙']]})}]
    refs=reference_projection([pa.RecordBatch.from_pylist(data[::-1])],csv_source='csv',country_source='json')
    assert refs['200'].as_name=='first'
    assert refs['200'].country_cn=='乙'
    assert refs['200'].raw_row_ref=='csv:csv:1'
    assert refs['200'].global_rank==1.9


def test_externalized_members_and_streamed_decisions_equal_in_memory():
    a,b=ResourceComputer({}),ResourceComputer({})
    memberships={}
    decisions=[]
    def save(row):
        key=f'{row.source_id}/{row.dimension}/{row.bucket}/{row.time}'
        memberships[key]={n:set(getattr(row,n)) for n in ('ipv4_prefix','ipv6_prefix','vp_set','private_as','public_as','path')}
        return key
    for hour in range(8):
        values=[element(),element('2.0.0.0/24',path='9808 {100}',ordinal=1)]
        plain=a.compute(context(hour),values)
        streamed=b.compute(context(hour),values,decision_sink=decisions.append,membership_sink=save)
        assert not streamed.decisions
        for left,right in zip(plain.rows,streamed.rows):
            assert left.ipv4_prefix_count==right.ipv4_prefix_count
            assert left.is_outlier==right.is_outlier
            assert left.path==memberships[right.membership_ref]['path']
            assert not right.path
        assert plain.state.normal_range==streamed.state.normal_range
    assert len(decisions)==16
    with pytest.raises(ValueError):b.compute(context(8),[],decision_sink=decisions.append)


def test_stream_failure_does_not_commit_work_state():
    computer=ResourceComputer({})
    decisions=[]
    def failed_members(row):
        raise OSError('fixture member write failed')
    with pytest.raises(OSError):
        computer.compute(context(),[element()],decision_sink=decisions.append,membership_sink=failed_members)
    assert computer.export_state().rib_count==0
    assert len(decisions)==1  # 候选写出不表示计算工作态已提交。


def test_real_csv_blank_empty_fields_short_extra_and_source_references(tmp_path):
    import io
    import pandas as pd
    from data_pipeline.bgp.input.reference_reader import rows
    raw='\nasn,as_name,global_rank\n200,first,1\n\n""\n,,\n201,short\n202,extra,3,unused\n'
    path=tmp_path/'as.csv';path.write_text(raw)
    source=hashlib.sha256(path.read_bytes()).hexdigest()
    normalized=list(rows(path,source))
    country=tmp_path/'country.json';country.write_text('{"200":{"country_cn":"甲"}}')
    country_source=hashlib.sha256(country.read_bytes()).hexdigest()
    references=reference_projection([pa.RecordBatch.from_pylist(normalized+list(rows(country,country_source)))],csv_source=source,country_source=country_source)
    expected=pd.read_csv(io.StringIO(raw),keep_default_na=False,usecols=['asn','as_name','global_rank']).drop_duplicates(subset=['asn'],keep='first')
    assert {asn:(ref.as_name,ref.global_rank) for asn,ref in references.items()}=={
        str(row['asn']):(row['as_name'],row['global_rank']) for _,row in expected.iterrows()}
    assert references[''].raw_row_ref==f'{source}:csv:4'  # ""保留，不能当空行丢弃。
    assert references['201'].raw_row_ref==f'{source}:csv:6'
    result=ResourceComputer(references).compute(context(),[element(peer='201')])
    assert result.vp_rows[0].as_name=='short' and result.vp_rows[0].as_rank is None
    assert result.vp_rows[0].reference_row_ref==f'{source}:csv:6'
    assert json.loads(normalized[0]['raw_row'])==[]
    assert json.loads(normalized[5]['raw_row'])==['','','']


def test_v1_whitespace_ambiguity_is_not_silently_dropped(tmp_path):
    from data_pipeline.bgp.input.reference_reader import rows
    for index,blank in enumerate(('   ', '"   "')):
        path=tmp_path/f'as-{index}.csv';path.write_text('asn,as_name,global_rank\n'+blank+'\n200,name,1\n')
        source=hashlib.sha256(path.read_bytes()).hexdigest()
        country=tmp_path/'country.json';country.write_text('{"200":{"country_cn":"甲"}}')
        other=hashlib.sha256(country.read_bytes()).hexdigest()
        normalized=[{k:v for k,v in row.items() if k not in ('csv_record_kind','csv_physical_start','csv_physical_end','raw_byte_offset','raw_byte_length','raw_record_sha256')} for row in rows(path,source)]
        for row in normalized:row['rule']='reference-rows/v1'
        assert json.loads(normalized[1]['raw_row'])==['   ']
        with pytest.raises(ValueError,match='词法依据'):
            reference_projection([pa.RecordBatch.from_pylist(normalized+list(rows(country,other)))],csv_source=source,country_source=other)


@pytest.mark.parametrize('field', ['\f','\v','\u00a0','\u2003'])
def test_non_ascii_or_control_whitespace_is_data(tmp_path,field):
    import io
    import pandas as pd
    from data_pipeline.bgp.input.reference_reader import rows
    raw='asn,as_name,global_rank\n'+field+'\n200,name,1\n'
    path=tmp_path/'as.csv';path.write_text(raw)
    source=hashlib.sha256(path.read_bytes()).hexdigest()
    other=tmp_path/'country.json';other.write_text('{"200":{"country_cn":"甲"}}')
    country=hashlib.sha256(other.read_bytes()).hexdigest()
    references=reference_projection([pa.RecordBatch.from_pylist(list(rows(path,source))+list(rows(other,country)))],csv_source=source,country_source=country)
    expected=pd.read_csv(io.StringIO(raw),keep_default_na=False,usecols=['asn','as_name','global_rank'])
    assert list(expected['asn'])==[field,'200']
    assert references[field].raw_row_ref==f'{source}:csv:1'
    assert references[field].as_name==''


def test_v2_actual_csv_lexical_rows_to_resource_and_references(tmp_path):
    import io
    import pandas as pd
    from data_pipeline.bgp.input.reference_reader import rows
    records=[('', 'blank_line'),('   ','whitespace_line'),('\t','whitespace_line'),
        ('""','record'),('"   "','record'),('"\t"','record'),(',,','record'),
        ('\f','record'),('\v','record'),('\u00a0','record'),('\u2003','record'),
        ('200,name,12.5','record'),('201,short','record'),('202,extra,3,unused','record'),
        ('203,"multi\nline",4','record')]
    raw='asn,as_name,global_rank\n'+'\n'.join(r for r,_ in records)+'\n'
    path=tmp_path/'as.csv';path.write_text(raw)
    source=hashlib.sha256(path.read_bytes()).hexdigest()
    normalized=list(rows(path,source))
    assert all(row['rule']=='reference-rows/v2' for row in normalized)
    assert [row['csv_record_kind'] for row in normalized[1:]]==[kind for _,kind in records]
    content=path.read_bytes()
    for row in normalized:
        piece=content[row['raw_byte_offset']:row['raw_byte_offset']+row['raw_byte_length']]
        assert hashlib.sha256(piece).hexdigest()==row['raw_record_sha256']
    country=tmp_path/'country.json';country.write_text('{"200":{"country_cn":"甲"}}')
    other=hashlib.sha256(country.read_bytes()).hexdigest()
    references=reference_projection([pa.RecordBatch.from_pylist(normalized+list(rows(country,other)))],csv_source=source,country_source=other)
    expected=pd.read_csv(io.StringIO(raw),keep_default_na=False,usecols=['asn','as_name','global_rank']).drop_duplicates(subset=['asn'],keep='first')
    assert {asn:(r.as_name,r.global_rank) for asn,r in references.items()}=={
        str(r['asn']):(r['as_name'],r['global_rank']) for _,r in expected.iterrows()}
    assert references[''].raw_row_ref==f'{source}:csv:4'
    assert references['   '].raw_row_ref==f'{source}:csv:5'
    assert references['\t'].raw_row_ref==f'{source}:csv:6'
    assert references['200'].raw_row_ref==f'{source}:csv:12'
    result=ResourceComputer(references).compute(context(),[element(peer='200')])
    assert result.vp_rows[0].as_name=='name' and result.vp_rows[0].as_rank==12
    assert result.vp_rows[0].reference_row_ref==f'{source}:csv:12'

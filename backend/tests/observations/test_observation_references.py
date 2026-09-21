from hashlib import sha256

import hashlib
import json
from data_pipeline.bgp.input.reference_reader import rows, safe_list


def test_raw_csv_multiline_and_safe_lists(tmp_path):
    p=tmp_path/'ref.csv';p.write_text('id,value\n1,"line one\nline two"\n1,\n')
    result=list(rows(p,hashlib.sha256(p.read_bytes()).hexdigest()))
    assert len(result)==3
    assert json.loads(result[1]['raw_row'])==['1','line one\nline two']
    assert json.loads(result[2]['raw_row'])==['1','']
    assert safe_list("__import__('os').system('false')")== (None,'invalid_literal')
    assert safe_list('[1,"2"]')==([1,'2'],'parsed')


def test_json_duplicate_keys_retained(tmp_path):
    p=tmp_path/'ref.json';p.write_text('{"1":{"peer":[2]},"1":{"peers":["2"]}}')
    result=list(rows(p,hashlib.sha256(p.read_bytes()).hexdigest()))
    assert len(result)==2
    assert result[0]['location']==result[1]['location']=='1'
    assert 'peer' in result[0]['raw_row'] and 'peers' in result[1]['raw_row']


def test_legal_csv_field_above_16mib_is_lossless(tmp_path):
    # 真实触发结构的最小复现：合法引号字段，比旧任意上限多一个字符。
    value='x'*(16*1024**2+1)
    p=tmp_path/'large.csv'
    p.write_text('prefix,domain_auth\n192.0.2.0/24,"'+value+'"\n')
    result=list(rows(p,hashlib.sha256(p.read_bytes()).hexdigest()))
    assert len(result)==2
    assert json.loads(result[1]['raw_row'])==['192.0.2.0/24',value]


def test_csv_lexical_bytes_and_blank_boundaries(tmp_path):
    data=b'\xef\xbb\xbfh,v\r\n\r\n \t\r"   "\n""\n,,\n"a\r\nb",c\r\n\x0c\n\x0b\n'+ '\u00a0\n\u2003\n'.encode()
    p=tmp_path/'lexical.csv';p.write_bytes(data)
    result=list(rows(p,sha256(data).hexdigest()))
    assert [r['csv_record_kind'] for r in result]==['record','blank_line','whitespace_line']+['record']*8
    offset=0
    for r in result:
        assert r['raw_byte_offset']==offset
        raw=data[offset:offset+r['raw_byte_length']]
        assert sha256(raw).hexdigest()==r['raw_record_sha256']
        offset+=len(raw)
    assert offset==len(data)
    assert json.loads(result[0]['raw_row'])==['h','v']
    assert result[6]['csv_physical_start']==7 and result[6]['csv_physical_end']==8


def test_malformed_quote_rejected_with_coordinates_and_limit_restored(tmp_path):
    import csv
    import pytest
    p=tmp_path/'bad.csv';p.write_text('h,v\n"unterminated')
    old=csv.field_size_limit()
    with pytest.raises(ValueError,match='row=1 physical_start=2'):
        list(rows(p,sha256(p.read_bytes()).hexdigest()))
    assert csv.field_size_limit()==old


def test_field_at_original_byte_bound(tmp_path):
    p=tmp_path/'bound.csv';p.write_bytes(b'x'*100)
    result=list(rows(p,sha256(p.read_bytes()).hexdigest()))
    assert json.loads(result[0]['raw_row'])==['x'*100]

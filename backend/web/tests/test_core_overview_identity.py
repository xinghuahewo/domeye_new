"""对象待核实的公开留存与HTTP行为；只使用合成源行。"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest
from data_pipeline.anomaly_records import convert_anomaly_record, serialize_record
from test_core_overview_api import retained, PROFILE, URL
from test_core_overview_scale import scaled, put


@pytest.mark.parametrize('text',['{64496}','{64496, 64497}'])
@pytest.mark.parametrize('indexed',[False,True])
def test_identity_pending_retains_raw_text_and_one_record_without_inventing_single_asn(client,retained,tmp_path,monkeypatch,text,indexed):
    path, manifest, records = retained
    original = records[4]
    record = original['record']
    ref = record['identity']['legacy_reference'].split('/')
    ref[2] = text
    ref = '/'.join(ref)
    row = {**record['raw_fields'],'asn':text}
    records[4] = convert_anomaly_record(ref,[row],source={**record['source'],'read_at':original['read_at']},
                                      data_profile=PROFILE,preserve_unresolved_as_set=True)
    payload = b''.join(serialize_record(result)+b'\n' for result in records)
    (path.parent/'records.jsonl').write_bytes(payload)
    manifest.update(interpretation_version='recorded-anomaly-overview/v3',level_conflicts={})
    manifest['records']['sha256'] = hashlib.sha256(payload).hexdigest()
    path.write_text(json.dumps(manifest))
    if indexed:
        output = tmp_path/'index'
        run = subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/index-core-overview-inputs.py'),
                              '--input',str(path),'--output',str(output)],capture_output=True,text=True,timeout=15)
        assert run.returncode == 0, run.stderr
        monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST',str(output/'manifest.json'))
    response = client.get(URL,query_string={'date':'2026-02-27','q':ref})
    assert response.status_code == 200
    data = response.get_json()
    assert data['overview']['record_count'] == 7 and data['events']['total'] == 1
    item = data['events']['items'][0]
    assert item['object'] == text and item['asns'] == []
    assert item['object_identity'] == {'state':'unresolved','reason':'as_set_in_asn_field','label':'对象待核实'}
    detail = client.get(URL+'/record',query_string={'ref':ref,'version':data['version']}).get_json()
    assert detail['item'] == item and detail['record']['record']['raw_fields'] == row
    assert detail['record']['record']['identity']['legacy_reference'] == ref
    assert detail['record']['record']['mapping_version'] == 'legacy-anomaly-mapping/v2'
    assert detail['record']['record']['common']['object'] == {'kind':'unresolved_asn','value':text}
    assert client.get(URL,query_string={'date':'2026-02-27','q':'AS64496'}).get_json()['events']['total'] == 0


def test_new_catalog_can_explicitly_reuse_old_daily_bytes_and_interpretation(client,scaled):
    folder,manifest,_ = scaled
    before = client.get(URL,query_string={'date':'2026-02-27'}).get_json()
    original = (folder/'2026-02-27.sqlite3').read_bytes()
    old_rule = manifest['interpretation_version']
    manifest['interpretation_version'] = 'recorded-anomaly-overview/v3'
    manifest['days']['2026-02-27']['interpretation_version'] = old_rule
    put(folder/'manifest.json',manifest)
    response = client.get(URL,query_string={'date':'2026-02-27'})
    assert response.status_code == 200
    after = response.get_json()
    assert after['metadata']['input_interpretation_version'] == old_rule
    assert after['events']==before['events'] and after['trend']==before['trend'] and after['overview']==before['overview']
    assert (folder/'2026-02-27.sqlite3').read_bytes() == original
    assert after['version'] != before['version']
    manifest['days']['2026-02-27']['interpretation_version'] = 'recorded-anomaly-overview/v3'
    put(folder/'manifest.json',manifest)
    assert client.get(URL,query_string={'date':'2026-02-27'}).status_code == 503


def test_identity_opt_in_does_not_reinterpret_numeric_records(retained):
    _,_,records = retained
    original = records[4]
    record = original['record']
    result = convert_anomaly_record(record['identity']['legacy_reference'],[record['raw_fields']],
        source={**record['source'],'read_at':original['read_at']},data_profile=PROFILE,preserve_unresolved_as_set=True)
    assert result == original and serialize_record(result)==serialize_record(original)


@pytest.mark.parametrize('value',['{64496 64497}','(64496)','{}','{4294967296}','{x}','64496,64497'])
def test_identity_rule_does_not_relax_other_unparseable_asn_values(retained,value):
    _,_,records = retained
    result=records[4]
    record=result['record']
    parts=record['identity']['legacy_reference'].split('/')
    parts[2]=value
    with pytest.raises(ValueError):
        convert_anomaly_record('/'.join(parts),[{**record['raw_fields'],'asn':value}],
            source={**record['source'],'read_at':result['read_at']},data_profile=PROFILE,preserve_unresolved_as_set=True)

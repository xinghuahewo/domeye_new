"""已确认的两次RIB观察对照：只读HTTP与离线绑定的公开行为。"""
import hashlib
import json
from pathlib import Path
import pytest
import subprocess
import sys
from datetime import datetime

from test_core_overview_scale import scaled, retained, put, PROFILE, URL


@pytest.fixture()
def path_bound(scaled):
    output, manifest, _ = scaled
    folder = output/'paths'
    folder.mkdir()
    summary = {
        'schema_version':'core-rib-path-summary/v1',
        'interpretation_version':'rrc25-raw-peer-rib-endpoint/as-sequence-v1',
        'comparison_version':'rib_path_comparison_v1_'+'d'*64,'data_profile':PROFILE,
        'collector_id':'rrc25','coverage':'unknown','session_continuity':'unknown','interval_change_count':None,
        'left':{'observed_at':'2026-02-26T16:00:00Z','sha256':'a'*64},
        'right':{'observed_at':'2026-02-27T08:00:00Z','sha256':'b'*64},
        'families':{
            'ipv4':{'same':2,'different':1,'not_comparable':1,'left_only':2,'right_only':3},
            'ipv6':{'same':0,'different':1,'not_comparable':0,'left_only':0,'right_only':0},
            'all':{'same':2,'different':2,'not_comparable':1,'left_only':2,'right_only':3}},
        'examples':[], 'limits':['只描述两端路径差异，不是期间变化次数。'],
    }
    package = {'schema_version':'core-rib-path-package/v1',
               'summary':{'file':'summary.json','sha256':put(folder/'summary.json',summary)},
               'source_manifest_sha256':'d'*64,'binding_code_sha256':'e'*64}
    manifest['path_comparison'] = {'file':'paths/manifest.json','sha256':put(folder/'manifest.json',package)}
    put(output/'manifest.json',manifest)
    return output,manifest,summary


def test_http_exposes_two_endpoint_pair_counts_without_replacing_anomaly_or_scale_metrics(client,path_bound):
    response = client.get(URL,query_string={'date':'2026-02-27'})
    assert response.status_code == 200
    data = response.get_json()
    comparison = data['metadata']['path_comparison']
    assert comparison['state'] == 'available'
    assert comparison['left']['observed_at'] == '2026-02-26T16:00:00Z'
    assert comparison['right']['observed_at'] == '2026-02-27T08:00:00Z'
    assert comparison['metrics'] == {'same':2,'different':2,'not_comparable':1,'left_only':2,'right_only':3,
                                     'comparable_pairs':4,'different_fraction':0.5}
    assert comparison['interval_change_count'] is None
    assert comparison['session_continuity'] == 'unknown'
    assert data['overview'] == {'record_count':7,'visible_prefixes':5,'visible_origin_ases':None}
    assert data['trend']['metric'] == 'recorded_prefix_outage_starts_distinct'


def test_offline_binding_verifies_source_and_exposes_bounded_raw_path_examples(client, scaled, tmp_path, monkeypatch):
    from test_rib_path_comparison import source, path, invoke, PEERS
    left = source(tmp_path, 'left.gz', int(datetime.fromisoformat('2026-02-26T16:00:00+00:00').timestamp()), PEERS,
                  [('192.0.2.0/24', [(0, path((2, [64496, 64512])), None)], False)])
    right = source(tmp_path, 'right.gz', int(datetime.fromisoformat('2026-02-27T08:00:00+00:00').timestamp()), PEERS,
                   [('192.0.2.0/24', [(0, path((2, [64496, 64497, 64512])), None)], False)])
    comparison = tmp_path/'comparison'
    run = invoke(left, right, comparison)
    assert run.returncode == 0, run.stderr
    before = client.get(URL, query_string={'date':'2026-02-27'}).get_json()
    target = tmp_path/'bound'
    command = [sys.executable, str(Path(__file__).resolve().parents[3]/'scripts/bind-core-overview-paths.py'),
               '--index',str(scaled[0]/'manifest.json'),'--comparison',str(comparison/'manifest.json'),'--output',str(target)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=15)
    assert run.returncode == 0, run.stderr
    assert client.get(URL, query_string={'date':'2026-02-27'}).get_json() == before
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST',str(target/'manifest.json'))
    after = client.get(URL, query_string={'date':'2026-02-27'}).get_json()
    value = after['metadata']['path_comparison']
    assert value['metrics']['different'] == 1 and value['metrics']['different_fraction'] == 1
    assert value['examples'][0]['prefix'] == '192.0.2.0/24'
    assert value['examples'][0]['left_path'] == [64496,64512]
    assert value['examples'][0]['right_path'] == [64496,64497,64512]
    assert value['examples'][0]['peer'] == {'bgp_id':'192.0.2.1','ip':'192.0.2.1','asn':64496}
    assert after['events'] == before['events'] and after['trend'] == before['trend'] and after['overview'] == before['overview']
    assert after['version'] != before['version']
    assert not (target/'paths/left.mrt').exists()
    assert subprocess.run(command,capture_output=True,text=True,timeout=15).returncode != 0


def change_summary(bound, mutate):
    folder, manifest, summary = bound
    mutate(summary)
    package = json.loads((folder/'paths/manifest.json').read_bytes())
    package['summary']['sha256'] = put(folder/'paths/summary.json', summary)
    manifest['path_comparison']['sha256'] = put(folder/'paths/manifest.json',package)
    put(folder/'manifest.json',manifest)


@pytest.mark.parametrize('damage',['hash','path','duplicate_json','collector','time','cross_day','family_sum',
                                   'boolean','claims_continuity','interval_count','bad_example','many_examples'])
def test_corrupt_path_comparison_is_isolated_from_available_anomalies_and_scale(client,path_bound,damage):
    folder,manifest,_ = path_bound
    before = client.get(URL,query_string={'date':'2026-02-27'}).get_json()
    if damage == 'hash':
        (folder/'paths/summary.json').write_bytes(b'corrupt')
    elif damage == 'path':
        manifest['path_comparison']['file'] = '../paths/manifest.json'
        put(folder/'manifest.json',manifest)
    elif damage == 'duplicate_json':
        raw = (folder/'paths/manifest.json').read_bytes().replace(b'{',b'{"schema_version":"wrong",',1)
        (folder/'paths/manifest.json').write_bytes(raw)
        manifest['path_comparison']['sha256'] = hashlib.sha256(raw).hexdigest()
        put(folder/'manifest.json',manifest)
    else:
        changes = {
            'collector': lambda value:value.update(collector_id='rrc00'),
            'time': lambda value:value['right'].update(observed_at='2026-02-26T16:00:00Z'),
            'cross_day': lambda value:value['left'].update(observed_at='2026-02-25T16:00:00Z'),
            'family_sum': lambda value:value['families']['all'].update(different=3),
            'boolean': lambda value:value['families']['ipv6'].update(same=False),
            'claims_continuity': lambda value:value.update(session_continuity='confirmed'),
            'interval_count': lambda value:value.update(interval_change_count=2),
            'bad_example': lambda value:value.update(examples=[{'family':'ipv4','prefix':'anything'}]),
            'many_examples': lambda value:value.update(examples=[{}]*11),
        }
        change_summary(path_bound,changes[damage])
    response = client.get(URL,query_string={'date':'2026-02-27'})
    after = response.get_json()
    assert response.status_code == 200 and after['state'] == 'available'
    assert after['overview'] == before['overview'] and after['events'] == before['events'] and after['trend'] == before['trend']
    assert after['metadata']['path_comparison']['state'] == 'unavailable'
    assert after['metadata']['path_comparison']['metrics'] is None and after['metadata']['path_comparison']['examples'] == []


@pytest.mark.parametrize('family,different,state',[('ipv4',1,'available'),('ipv6',1,'available'),('unknown',None,'family_not_supported')])
def test_comparison_follows_family_not_event_list_filters(client,path_bound,family,different,state):
    data = client.get(URL,query_string={'date':'2026-02-27','family':family,'q':'no-match','hour':'5','level':'high'}).get_json()
    value = data['metadata']['path_comparison']
    assert data['events']['total'] == 0 and value['state'] == state
    assert (value['metrics']['different'] if value['metrics'] else None) == different


def test_no_comparable_pairs_has_null_ratio_not_zero_change(client,path_bound):
    def empty_comparable(summary):
        for family in summary['families'].values():
            family.update(same=0,different=0)
    change_summary(path_bound,empty_comparable)
    metric = client.get(URL,query_string={'date':'2026-02-27'}).get_json()['metadata']['path_comparison']['metrics']
    assert metric['comparable_pairs'] == 0 and metric['different_fraction'] is None


def test_comparison_cannot_leak_into_other_or_failed_anomaly_dates(client,path_bound):
    folder,_,_ = path_bound
    missing = client.get(URL,query_string={'date':'2026-02-28'}).get_json()
    assert missing['state'] == 'window_not_retained' and 'path_comparison' not in missing['metadata']
    change_summary(path_bound,lambda value:(value['left'].update(observed_at='2026-03-30T16:00:00Z'),
                                          value['right'].update(observed_at='2026-03-31T08:00:00Z')))
    other = client.get(URL,query_string={'date':'2026-02-27'}).get_json()['metadata']['path_comparison']
    assert other['state'] == 'date_not_retained' and other['metrics'] is None and other['examples'] == []
    (folder/'2026-02-27.sqlite3').chmod(0o600)
    (folder/'2026-02-27.sqlite3').write_bytes(b'corrupt')
    failed = client.get(URL,query_string={'date':'2026-02-27'})
    assert failed.status_code == 503 and 'path_comparison' not in failed.get_json()['metadata']

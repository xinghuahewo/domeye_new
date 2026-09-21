"""Q2附加正式人工异常链与定向反例，不冒充主P/D输入。"""
import json
import os
import uuid
from pathlib import Path
import psycopg2
import pytest
from data_pipeline.results import Publication
from data_pipeline.results.manifest_io import encode
from data_pipeline.results.detection_queries import aliases
from data_pipeline.analysis.detection.store import read_stored_rows
from tests.publication.test_publication_q2 import generate, all_pages, SELECTOR, test_message_coverage_actual_pg_and_lake as coverage_probe, test_message_proof_tail_qualification as tail_probe


def check_q2(pub,ready_path,expected):
    ready=json.loads(Path(ready_path).read_text())
    token=pub.prepare_detection(ready_path);pub.publish(token,expected_generation=0,selector=SELECTOR)
    assert pub.discover(SELECTOR)==(token,1)
    original=list(read_stored_rows(pub.detection['output_dsn'],ready['run_id'],ready['snapshot'],'records'))
    audited=all_pages(pub,token,'records')
    assert [encode(r) for r in audited]==[encode(r) for r in original]
    proof=pub.query_detection(token,'records',page_size=1,scan_budget=1000000)['verified_source_messages']
    events=pub.query_detection(token,'events',page_size=100)
    revisions=[r for r in original if r['record_kind']=='business_revision']
    assert (events['total'],len(revisions),pub.query_detection(token,'decisions',page_size=100)['total'],pub.query_detection(token,'leak_outputs')['total'])==expected
    from data_pipeline.results import detection_binding
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(detection_binding,'rows',lambda *a,**kw:pytest.fail('业务页不得湖扫描'))
        assert pub.query_detection(token,'events',page_size=100)==events
        assert pub.query_detection(token,'decisions')['total']==expected[2]
        leak=pub.query_detection(token,'leak_outputs');assert leak['total']==expected[3]
        for event in events['items']:
            rs=pub.query_detection(token,'revisions',component=events['component'],incident=event['incident_id'],page_size=100)
            expected_rows=[r for r in revisions if r['incident_id']==event['incident_id']]
            assert [encode(r) for r in rs['items']]==[encode(r) for r in expected_rows]
            for row in expected_rows:
                detail=pub.query_detection(token,'detail',component=events['component'],incident=row['incident_id'],revision=row['revision'])
                assert [encode(r) for r in detail['items']]==[encode(row)]
                for alias in aliases(row):assert pub.query_detection(token,'alias',alias=alias)['resolution']=='resolved'
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
        c.execute('SELECT mode,count(*) FROM publication_q1.rows WHERE build_id=%s GROUP BY mode',(token.build_id,));counts=dict(c.fetchall())
        assert set(counts)<= {'business_revision','rule_decision','leak_event_record'}
        assert sum(counts.values())==sum(expected[1:])
    result={'token':token.__dict__,'events':events,'source_message_proof':proof,'control_counts':counts,'typed_records':len(original),'views_expected':expected}
    return result


@pytest.fixture(scope='module')
def q2_setup(tmp_path_factory):
    base=os.environ['DOMEYE_FEATURE_TEST_DSN'];root=tmp_path_factory.mktemp('q2-formal')
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv('DOMEYE_DETECTION_TEST_DSN',base);generate(root,patch)
    req=json.loads((root/'request.json').read_text())
    admin=psycopg2.connect(base);admin.autocommit=True;name='q2_joint_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+name)
    admin.close();dsn=base+' dbname='+name
    setup={'dsn':dsn,'root':str(root),'ready':str(root/'frozen-output/business/ready.json'),
           'detection':{'output_dsn':req['detection_dsn'],'observation_dsn':req['observation_dsn']}}
    pub=Publication(dsn,root,detection=setup['detection']);pub.initialize();pub.migrate_profiles()
    (root/'joint-binding.json').write_text(json.dumps(setup))
    return setup


@pytest.fixture
def q2_pub(q2_setup):
    pub=Publication(q2_setup['dsn'],q2_setup['root'],detection=q2_setup['detection'])
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:c.execute('DELETE FROM publication_q1.head WHERE selector=%s',(SELECTOR,))
    return pub


def test_q2_formal_full_views(q2_pub,q2_setup):
    result=check_q2(q2_pub,q2_setup['ready'],(12,16,37,2))
    assert result['source_message_proof']['total']==17
    Path(q2_setup['root'],'q2-full-evidence.json').write_text(encode(result))


def test_q2_synced_message_replacement(q2_pub,q2_setup):
    coverage_probe(q2_pub,q2_setup,{},'replace')


def test_q2_source_prepare_tail(q2_pub,q2_setup,monkeypatch):
    tail_probe(q2_pub,q2_setup,monkeypatch,'prepare','source')


def test_q2_output_audit_tail(q2_pub,q2_setup,monkeypatch):
    tail_probe(q2_pub,q2_setup,monkeypatch,'audit','output')

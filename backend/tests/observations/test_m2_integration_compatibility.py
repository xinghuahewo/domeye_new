"""显式绑定原人工 Q2 制品，只读验证升级前 Token；不重新生产或发布。"""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path

import psycopg2
import pytest

from data_pipeline.results import Publication, Token
from data_pipeline.results.manifest_io import encode
from data_pipeline.analysis.detection.store import read_stored_rows
from tests.publication.test_publication_q2 import all_pages, SELECTOR


def test_original_q1_q2_tokens_remain_readable(tmp_path):
    root = os.environ.get('DOMEYE_M2_OLD_ARTIFACT_ROOT')
    if not root:
        pytest.skip('需要显式绑定已有人工制品')
    root = Path(root)
    def hashes():
        return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(root.rglob('*')) if p.is_file()}
    before = hashes()
    request = json.loads((root / 'detection-request.json').read_text())
    q1 = json.loads((root / 'q1-evidence.json').read_text())
    q2 = json.loads((root / 'q2-joint-evidence.json').read_text())
    evidence = json.loads((root / 'three-module-evidence.json').read_text())
    def state():
        with psycopg2.connect(request['observation_dsn']) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT version FROM publication_q1.schema_version')
                versions = c.fetchall()
                c.execute('SELECT * FROM publication_q1.head ORDER BY selector')
                return {'versions': versions, 'heads': c.fetchall()}
    initial = state()
    assert initial['versions'] in ([(2,)], [(3,)])
    pub = Publication(request['observation_dsn'], root,
                      detection={'observation_dsn': request['observation_dsn'],
                                 'output_dsn': request['detection_dsn']})
    assert pub.discover() == (Token(**q1['second']), 2)
    pages = 0
    for saved in q1['old_pages']:
        for page in saved['pages']:
            assert pub.query(Token(**q1['first']), saved['kind'], mode=saved['mode'],
                             page=page['page'], page_size=page['page_size']) == page
            pages += 1
    token = Token(**q2['token'])
    assert pub.discover(SELECTOR) == (token, 1)
    assert pub.query_detection(token, 'events', page_size=100) == q2['events']
    d = evidence['detection']
    original = list(read_stored_rows(request['detection_dsn'], d['run_id'], d['snapshot'], 'records'))
    records = all_pages(pub, token, 'records')
    assert Counter(encode(r) for r in records) == Counter(encode(r) for r in original)
    assert len(records) == 26
    assert state() == initial
    assert hashes() == before
    (tmp_path / '旧Token只读复核.json').write_text(json.dumps({
        'original_root': str(root), 'tokens': [q1['first'], q1['second'], q2['token']],
        'q1_pages': pages, 'q2_records': len(records), 'q2_events': q2['events']['total'],
        'state': initial, 'files_unchanged': before,
    }, ensure_ascii=False, indent=2, default=str))

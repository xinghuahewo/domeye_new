"""只验证新增控制目录/拒绝路径，不构造假Admission或成功组合。"""
from contextlib import closing
from copy import deepcopy
import json
import os
from pathlib import Path
import uuid
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
import pytest
from data_pipeline.results import Publication, Token, profiles
from data_pipeline.results.manifest_contract import CONTRACT, PROFILES
from data_pipeline.results.published_reader import candidate


def test_incomplete_candidate_refused_before_control_connection(tmp_path):
    p = Publication('dbname=not_used',tmp_path,combined={})
    request = dict(contract=CONTRACT,profile=deepcopy(PROFILES['fixture-m3-combined-country-trend/v1']),
                   dependency_revisions={},components=[],dependencies=[],edges=[],role_graph=[])
    with pytest.raises(ValueError,match='主体缺失'):
        with candidate(p,request): pytest.fail('不得进入候选')
    assert not list(tmp_path.iterdir())


def test_actual_catalog_extension_and_unknown_token_rollback(tmp_path):
    config = os.environ.get('DOMEYE_COMBINED_CONTROL_CONTEXT')
    if not config: pytest.skip('须绑定本任务私有PG；只建临时控制库')
    dsn = json.loads(Path(config).read_text())['dsn']
    database = 'publication_control_'+uuid.uuid4().hex
    with closing(psycopg2.connect(dsn)) as admin:
        admin.autocommit = True
        with admin.cursor() as c: c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        try:
            target = make_dsn(dsn,dbname=database)
            p = Publication(target,tmp_path,combined={})
            p.initialize(); p.migrate_profiles(); p.migrate_profiles()
            for selector in ('fixture:m3:combined-base','fixture:m3:combined-country-trend'):
                with pytest.raises(ValueError,match='未知发布'):
                    p.publish(Token('q1_missing','missing','missing'),expected_generation=0,selector=selector)
                with pytest.raises(ValueError,match='没有已发布'):
                    p.discover(selector)
            with closing(psycopg2.connect(target)) as pg, pg.cursor() as c:
                c.execute('SELECT selector,profile FROM publication_q1.profiles')
                assert dict(c.fetchall()) == profiles.SELECTORS
                c.execute('SELECT count(*) FROM publication_q1.head'); assert c.fetchone() == (0,)
                c.execute('SELECT count(*) FROM publication_q1.builds'); assert c.fetchone() == (0,)
        finally:
            # Publication既有方法由psycopg对象退出事务，本用例无长期连接持有。
            import gc
            gc.collect()
            with admin.cursor() as c: c.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))

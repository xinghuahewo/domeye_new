"""唯一发布元数据层的固定人工profile；迁移只由显式离线调用执行。"""
from psycopg2 import sql
from psycopg2.extras import Json
from data_pipeline.results.manifest_io import require, encode
from data_pipeline.analysis.country_events.selection_contract import PROFILE as COUNTRY_ID, SELECTOR as COUNTRY_SELECTOR, REQUIRED
from data_pipeline.results.manifest_contract import PROFILES as COMBINED_PROFILES

Q1={'id':'q1-resource-feature/v1','intent':'module_scoped','data_kind':'fixture',
    'required':['resource.metrics','feature.ordinary.windows','feature.ir.windows']}
Q2={'id':'q2-detection/v1','intent':'module_scoped','data_kind':'fixture',
    'required':['detection.records','detection.state_entries','detection.revisions','detection.decisions','detection.aliases']}
COUNTRY={'id':COUNTRY_ID,'intent':'module_scoped','data_kind':'fixture','required':list(REQUIRED)}
SELECTORS={'fixture:resource-feature:q1':Q1,'fixture:detection:q2':Q2,COUNTRY_SELECTOR:COUNTRY}
SELECTORS.update({'fixture:m3:combined-base':COMBINED_PROFILES['fixture-m3-combined-base/v1'],
                  'fixture:m3:combined-country-trend':COMBINED_PROFILES['fixture-m3-combined-country-trend/v1']})
SELECTORS.update({'real:m3:combined-base':COMBINED_PROFILES['real-m3-combined-base/v1'],
                  'real:m3:combined-country-trend':COMBINED_PROFILES['real-m3-combined-country-trend/v1']})

SELECTORS.update({'artificial:m3:combined-'+suffix:COMBINED_PROFILES['artificial-m3-combined-'+suffix+'/v1']
                  for suffix in ('base','country-trend')})


def selector_for(profile):
    matches=[s for s,p in SELECTORS.items() if p==profile]
    require(len(matches)==1,'未知或被修改的固定profile')
    return matches[0]


def migrate(c):
    c.execute('LOCK TABLE publication_q1.head IN ACCESS EXCLUSIVE MODE')
    c.execute('CREATE TABLE IF NOT EXISTS publication_q1.schema_version(singleton BOOLEAN PRIMARY KEY CHECK(singleton),version INTEGER NOT NULL CHECK(version=2))')
    c.execute('INSERT INTO publication_q1.schema_version SELECT true,2 WHERE NOT EXISTS (SELECT 1 FROM publication_q1.schema_version)')
    c.execute('SELECT version FROM publication_q1.schema_version');require(c.fetchall() in ([(2,)],[(3,)]),'未知发布元数据版本，拒绝隐式降级')
    clause=sql.SQL(' OR ').join(sql.SQL('(selector={} AND profile={}::jsonb)').format(sql.Literal(s),sql.Literal(encode(p))) for s,p in SELECTORS.items())
    c.execute(sql.SQL('CREATE TABLE IF NOT EXISTS publication_q1.profiles(selector TEXT PRIMARY KEY,profile JSONB NOT NULL CHECK ({}))').format(clause))
    # v2既有CHECK不会由CREATE IF NOT EXISTS更新；同一事务显式迁为v3。
    c.execute('ALTER TABLE publication_q1.schema_version DROP CONSTRAINT IF EXISTS schema_version_version_check')
    c.execute('UPDATE publication_q1.schema_version SET version=3 WHERE singleton')
    c.execute('ALTER TABLE publication_q1.schema_version ADD CONSTRAINT schema_version_version_check CHECK(version=3)')
    c.execute('ALTER TABLE publication_q1.profiles DROP CONSTRAINT IF EXISTS profiles_check')
    c.execute(sql.SQL('ALTER TABLE publication_q1.profiles ADD CONSTRAINT profiles_check CHECK ({})').format(clause))
    for s,p in SELECTORS.items():
        c.execute('INSERT INTO publication_q1.profiles VALUES (%s,%s) ON CONFLICT DO NOTHING',(s,Json(p)))
    c.execute('SELECT selector,profile FROM publication_q1.profiles');require(dict(c.fetchall())==SELECTORS,'固定profile目录漂移')
    c.execute('ALTER TABLE publication_q1.head DROP CONSTRAINT IF EXISTS head_selector_check')
    c.execute("SELECT 1 FROM pg_constraint WHERE conrelid='publication_q1.head'::regclass AND conname='head_profile_fk'")
    if not c.fetchone():c.execute('ALTER TABLE publication_q1.head ADD CONSTRAINT head_profile_fk FOREIGN KEY(selector) REFERENCES publication_q1.profiles(selector)')
    c.execute('''CREATE TABLE IF NOT EXISTS publication_q1.aliases(
        build_id TEXT NOT NULL, alias JSONB NOT NULL, alias_digest TEXT NOT NULL,
        target_kind TEXT NOT NULL CHECK(target_kind='detection'), target_sequence BIGINT NOT NULL,
        row_digest TEXT NOT NULL, PRIMARY KEY(build_id,alias_digest,target_sequence),
        FOREIGN KEY(build_id,target_kind,target_sequence) REFERENCES publication_q1.rows(build_id,kind,ordinal))''')

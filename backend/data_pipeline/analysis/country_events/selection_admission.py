"""C4构建验收目录。信任已配置PG控制目录，不信制品自报hash；不保存历史正文。"""
from contextlib import contextmanager
import psycopg2
from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.analysis.country_events.selection_contract import contract_json

VERSION = 'country-read-admission/v1'


def _record(binding,proof):
    return (VERSION,binding.component.component_id,contract_json(binding),contract_json(proof),
            binding.manifest_sha256,proof.index_validation_sha256,'accepted')


def _register_country_admission(dsn,binding,proof,*,before_commit):
    """仅实际builder的末步调用；无导入proof、补签、UPSERT或提权入口。"""
    pg=psycopg2.connect(dsn);error=None
    try:
        with pg:
            with pg.cursor() as cur:
                cur.execute('''CREATE TABLE IF NOT EXISTS country_components.read_admissions(
                    read_model_id TEXT PRIMARY KEY,admission_version TEXT NOT NULL,
                    component_id TEXT NOT NULL REFERENCES country_components.components(component_id),
                    binding_json TEXT NOT NULL,proof_json TEXT NOT NULL,
                    manifest_sha256 TEXT NOT NULL,validation_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('accepted','revoked')))''')
                cur.execute('''INSERT INTO country_components.read_admissions
                    (read_model_id,admission_version,component_id,binding_json,proof_json,manifest_sha256,validation_sha256,state)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',(binding.read_model_id,*_record(binding,proof)))
                # ready落盘/最后实体复验失败时事务回滚；没有有效锚。
                before_commit()
    except BaseException as caught:
        error=caught;raise
    finally:cleanup((pg.close,),error)


@contextmanager
def verify_country_admission(component_dsn,binding,proof,*,lock=False):
    """lock=True普通事务FOR SHARE；持有至with退出。发布锁序由调用方固定。"""
    pg=psycopg2.connect(component_dsn)
    error=None
    try:
        pg.set_session(readonly=not lock)
        with pg.cursor() as cur:
            cur.execute("SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()),to_regclass('country_components.read_admissions')::text FROM pg_control_system()")
            system,oid,table=cur.fetchone()
            if (system,oid)!=(binding.component.system_id,binding.component.database_oid):raise ValueError('C4_admission_database')
            if table is None:raise ValueError('C4_admission_missing')
            cur.execute('''SELECT admission_version,component_id,binding_json,proof_json,manifest_sha256,validation_sha256,state
                FROM country_components.read_admissions WHERE read_model_id=%s'''+(' FOR SHARE' if lock else ''),(binding.read_model_id,))
            row=cur.fetchone()
            if row is None:raise ValueError('C4_admission_missing')
            if row!=_record(binding,proof):raise ValueError('C4_admission_mismatch')
        yield
    except BaseException as caught:
        error=caught;raise
    finally:
        # 无写入，rollback结束普通读事务与行锁；任一清理失败仍关闭连接。
        cleanup((pg.rollback,pg.close),error)

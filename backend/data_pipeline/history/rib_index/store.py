"""H2领域原生湖表；复用既有目录、目标绑定与批预算。"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re
import uuid
import pyarrow as pa
from psycopg2 import sql
from data_pipeline.history.event_index import History as CoreHistory
from data_pipeline.history.event_index.cleanup import owner, release
from data_pipeline.history.event_collection.children import checked_lake, directory_bytes
from data_pipeline.history.event_collection.freeze import safe_path
from data_pipeline.history.database_import.freeze import read_json, write_json
from data_pipeline.history.rib_index.model import RibToken, TABLES, SCHEMAS, ORDER, RULE, code_identity, rule_sha, row_bytes, freeze_identity, valid_freeze_identity
from data_pipeline.history.rib_index.project import Projector
from data_pipeline.history.rib_index.mrt import require


def rows(db,token,name,budget,cap=None):
    table=f'lake.history.t{TABLES.index(name)} AT (VERSION => {int(token.snapshot)})'
    width=db.execute('SELECT coalesce(max(octet_length(encode(to_json(t)))),0) FROM (SELECT * FROM '+table+') t').fetchone()[0]
    count=min(cap or budget.limits.batch_rows,budget.limits.batch_rows,max(1,budget.limits.batch_bytes//max(1,width*8+len(SCHEMAS[name])*256)))
    with owner(db.execute('SELECT * FROM '+table+' ORDER BY '+ORDER[name]).fetch_record_batch(count),'H2 Arrow',getattr(budget,'cleanup_errors',None)) as reader:
        require(reader.schema==SCHEMAS[name],'typed schema不符')
        for batch in reader:
            budget.check();budget.add('arrow_batches',1);require(batch.nbytes<=budget.limits.batch_bytes,'Arrow读批超限')
            for row in batch.to_pylist():
                data=row_bytes(row);require(len(data)<=budget.limits.max_row_bytes,'typed单行超限')
                budget.add('typed_rows',1,budget.limits.max_total_rows);budget.add('typed_bytes',len(data),budget.limits.max_total_bytes)
                yield row


class History(CoreHistory):
    def import_collection(self,manifest_path):
        manifest=read_json(Path(manifest_path),self.collection_limits.metadata_bytes)
        require(valid_freeze_identity(manifest.get('h2_freezer_code')),'H2冻结角色/代码不符')
        return super().import_collection(manifest_path)
    def project_rib(self,collection):
        budget=self._budget(); identity=code_identity();rule=rule_sha()
        self._qualify_collection(collection,budget)
        pid=uuid.uuid4().hex;out=self.data_root/pid;out.mkdir(exist_ok=False)
        primary=None;db=pg=None;registered=False;token=None
        try:
            pg=self._connect()
            with pg,pg.cursor() as c:
                c.execute('''CREATE TABLE IF NOT EXISTS history_q3.rib_profiles(profile_id TEXT PRIMARY KEY,collection_id TEXT NOT NULL,rule_sha TEXT NOT NULL,state TEXT NOT NULL,snapshot BIGINT,ready_sha TEXT,private_root TEXT NOT NULL,reason TEXT)''')
                c.execute("INSERT INTO history_q3.rib_profiles VALUES (%s,%s,%s,'candidate',NULL,NULL,%s,NULL)",(pid,collection.collection_id,rule,str(self.root)))
            registered=True
            with owner(Projector(self,collection,out,budget),'H2 projection') as projection:
                projection.prepare();input_resources=projection.input_resources
                db=checked_lake(self._lake(pid,False),out,budget,self.collection_limits.temporary_bytes-2*self.limits.max_metadata_bytes)
                db.execute('CREATE SCHEMA lake.history');counts=[]
                for i,name in enumerate(TABLES):
                    schema=SCHEMAS[name];db.register('h2_schema',pa.Table.from_batches([],schema=schema))
                    db.execute(f'CREATE TABLE lake.history.t{i} AS SELECT * FROM h2_schema');db.unregister('h2_schema')
                    pending=[];size=count=0;digest=hashlib.sha256()
                    def flush():
                        nonlocal size
                        if not pending:return
                        budget.check();batch=pa.Table.from_pylist(pending,schema=schema);require(batch.nbytes<=self.limits.batch_bytes,'Arrow写批超限')
                        db.register('h2_batch',batch)
                        try:db.execute(f'INSERT INTO lake.history.t{i} SELECT * FROM h2_batch')
                        finally:db.unregister('h2_batch')
                        budget.add('arrow_batches',1);pending.clear();size=0
                        used=directory_bytes(out,budget);budget.counts['temporary_bytes']=used;budget.counts['profile_candidate_disk_peak_bytes']=max(used,budget.counts.get('profile_candidate_disk_peak_bytes',0))
                    for row in projection.rows(name):
                        data=row_bytes(row)
                        if pending and (len(pending)>=self.limits.batch_rows or size+len(data)>self.limits.batch_bytes):flush()
                        require(len(data)<=min(self.limits.max_row_bytes,self.limits.batch_bytes),'写前单行超限')
                        budget.add('profile_write_rows',1,self.limits.max_total_rows);budget.add('profile_write_bytes',len(data),self.limits.max_total_bytes)
                        pending.append(row);size+=len(data);count+=1;digest.update(data+b'\n')
                    flush();counts.append({'name':name,'rows':count,'sha256':digest.hexdigest()})
            (out/'projection.sqlite').unlink()
            snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
            provisional=RibToken(pid,collection,rule,snapshot,'')
            for info in counts:
                count=0;digest=hashlib.sha256()
                with owner(rows(db,provisional,info['name'],budget)) as stream:
                    for row in stream:count+=1;digest.update(row_bytes(row)+b'\n')
                require((count,digest.hexdigest())==(info['rows'],info['sha256']),'全typed回读不符')
            files=self._files(db,{'tables':counts},snapshot,out,budget.guard)
            # 先释放实际写owner；有关闭错误不能登记complete。
            released,db=db,None
            error=release(released,'H2 DuckDB writer')
            if error is not None:raise error
            budget.counts['temporary_bytes']=directory_bytes(out,budget)
            ready={'profile_id':pid,'collection':asdict(collection),'rule_sha256':rule,'snapshot':snapshot,'tables':counts,'files':files,
                   'code_sha256':identity,'scope':'H2_artificial_single_and_endpoint','admission':'profile_validated',
                   'session_continuity':'unknown','interval_change_count':None,'H3':'unresolved_external',
                   'resources':budget.report(),'collection_input_resources':input_resources}
            write_json(out/'ready.json',ready);require((out/'ready.json').stat().st_size<=self.collection_limits.metadata_bytes,'完成元数据超限')
            digest=budget.hash(out/'ready.json')
            with pg:
                self._qualify_collection(collection,budget,pg=pg,lock=True)
                require(identity==code_identity(),'H2角色/解释代码漂移')
                for f in files:require(budget.hash(f['path'])==f['sha256'],'最终Parquet漂移')
                with pg.cursor() as c:
                    c.execute("UPDATE history_q3.rib_profiles SET state='complete',snapshot=%s,ready_sha=%s WHERE profile_id=%s AND state='candidate'",(snapshot,digest,pid));require(c.rowcount==1,'登记竞争')
            token=RibToken(pid,collection,rule,snapshot,digest)
        except BaseException as error:
            primary=error
            if registered:
                try:
                    pg.rollback()
                    with pg,pg.cursor() as c:c.execute("UPDATE history_q3.rib_profiles SET state='failed',reason=%s WHERE profile_id=%s",(str(error),pid))
                except BaseException as failure:error.add_note('H2失败登记错误: '+str(failure))
            write_json(out/'FAILED.json',{'state':'failed','reason':str(error)})
        finally:
            primary=release(db,'H2 DuckDB writer',primary)
            primary=release(pg,'H2 catalog writer',primary)
        if primary is not None:
            if token is not None:
                # 登记提交后owner关闭仍可能失败；撤销本次候选，不返回可用Token。
                try:
                    with owner(self._connect(), 'H2 failed-registration cleanup') as cleanup_pg, cleanup_pg, cleanup_pg.cursor() as c:
                        c.execute("UPDATE history_q3.rib_profiles SET state='failed',reason=%s WHERE profile_id=%s", (str(primary),pid))
                except BaseException as failure:primary.add_note('H2关闭失败后撤销未完成: '+str(failure))
                write_json(out/'FAILED.json', {'state':'failed','reason':str(primary),'scope':'目录owner关闭失败，未交付RibToken'})
            raise primary.with_traceback(primary.__traceback__)
        return token
    def _qualify_rib(self,token,budget,pg=None,lock=False):
        require(isinstance(token,RibToken) and re.fullmatch('[0-9a-f]{32}',token.profile_id),'缺固定RibToken')
        owned=pg is None;primary=None
        if owned:pg=self._connect()
        try:
            self._qualify_collection(token.collection,budget,pg=pg,lock=lock)
            with pg.cursor() as c:
                c.execute('SELECT collection_id,rule_sha,state,snapshot,ready_sha,private_root FROM history_q3.rib_profiles WHERE profile_id=%s'+(' FOR SHARE' if lock else ''),(token.profile_id,))
                require(c.fetchone()==(token.collection.collection_id,token.rule_sha256,'complete',token.snapshot,token.ready_sha256,str(self.root)),'候选/错版/撤回')
                c.execute(sql.SQL('SELECT snapshot_id FROM {}.ducklake_snapshot WHERE snapshot_id=%s').format(sql.Identifier('hl_'+token.profile_id)),(token.snapshot,));require(c.fetchone()==(token.snapshot,),'快照不存在')
            budget.add('sql_calls',2);out=self.data_root/token.profile_id
            require(budget.hash(out/'ready.json')==token.ready_sha256,'ready变化');ready=read_json(out/'ready.json',self.collection_limits.metadata_bytes)
            require(ready['collection']==json.loads(row_bytes(asdict(token.collection))) and ready['code_sha256']==code_identity() and token.rule_sha256==rule_sha() and ready['admission']=='profile_validated','H2解释/来源身份变化')
            total=0
            for f in ready['files']:
                total+=f['bytes'];require(total<=self.limits.max_total_bytes,'H2全部Parquet预算超限')
                path=safe_path(f['path'],(out/'parquet',));require(path.stat().st_size==f['bytes'] and budget.hash(path)==f['sha256'],'H2 Parquet变化')
            return ready
        except BaseException as error:primary=error;raise
        finally:
            if owned:
                error=release(pg,'H2 qualification PostgreSQL',primary,getattr(budget,'cleanup_errors',None))
                if primary is None and error is not None:raise error
    def rib(self,token):
        from data_pipeline.history.rib_index.query import RibSession
        return RibSession(self,token)

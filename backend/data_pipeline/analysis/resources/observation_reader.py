"""有限公开只读合同：原值、资格和可用主值分别返回。"""
import re
import json

import psycopg2
from data_pipeline.bgp.archive.store import connect_duckdb, literal
from data_pipeline.analysis.resources.identity import identity_digest
from data_pipeline.analysis.resources.qualification import PROFILE, VERSION, ALL_TABLES, pg_identity, table_inventory, validate_relations, check_table_set


class ResourceObservationReader:
    def __init__(self,dsn,run_id,expected_snapshot,expected_dataset_id,*,allow_fixture=False,guard=lambda:None):
        if not re.fullmatch('[a-f0-9]{32}',run_id) or type(expected_snapshot) is not int or expected_snapshot<0:
            raise ValueError('必须绑定精确Resource运行与快照')
        self.dsn=dsn;self.run_id=run_id;self.snapshot=expected_snapshot;self.dataset_id=expected_dataset_id
        self.allow_fixture=allow_fixture;self.guard=guard
        self.binding=self._registration();self.receipt=self.binding[-1]
        if self.receipt['profile']!=PROFILE or self.receipt['qualification_version']!=VERSION:raise ValueError('未知Resource资格版本')
        if identity_digest(dict(run_id=run_id,snapshot=self.snapshot,receipt=self.receipt))!=self.dataset_id:raise ValueError('Resource完成回执身份不符')
        if set(self.receipt['inventory'])!=set(ALL_TABLES):raise ValueError('完整表枚举不符')
        if self.receipt['binding_manifest']!=self.binding[-2] or self.receipt['code_digest']!=self.binding[4]:raise ValueError('Resource输入/代码绑定不符')
        self.manifest=self.receipt['binding_manifest']
        if self.manifest['pg_identity']!=pg_identity(dsn):raise ValueError('PG系统/库身份不符')

    def _registration(self):
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as cur:
                cur.execute('SELECT schema_name,snapshot,dataset_id,schema_version,code_digest,execution_mode,binding_manifest,observation_receipt FROM domeye.resource_runs WHERE run_id=%s AND state=%s',(self.run_id,'complete'))
                row=cur.fetchone()
        if row is None or row[:4]!=('resource_'+self.run_id,self.snapshot,self.dataset_id,PROFILE):raise ValueError('候选/失败/错版Resource不可读')
        if row[5]!='frozen-fresh-process' and not (self.allow_fixture and row[5]=='synthetic-fixture-api'):raise ValueError('fixture读取须显式声明')
        if row[-1] is None:raise ValueError('缺少独立资格完成回执')
        return row

    def _check(self):
        if self._registration()!=self.binding or pg_identity(self.dsn)!=self.manifest['pg_identity']:raise ValueError('Resource固定读取身份漂移')

    def _relation(self,table):
        if table not in ALL_TABLES:raise ValueError('不在有限公开表枚举中')
        return f'lake.resource_{self.run_id}.{table} AT (VERSION => {self.snapshot})'

    def _check_inputs(self):
        from data_pipeline.bgp.archive.message_reader import ObservationReader
        from data_pipeline.bgp.ordered_reader import binding_from_reader
        from data_pipeline.analysis.resources.references import read_reference
        checked=set()
        for sid,item in self.manifest['observation_inputs'].items():
            full=self.manifest['observation_runs'][item['binding_id']]
            binding=full['binding'];key=(binding['observation_run'],binding['seal_snapshot'])
            if key in checked:continue
            r=ObservationReader(self.dsn,*key,[sid],profile='observation',guard=self.guard)
            actual=binding_from_reader(r)
            from dataclasses import asdict
            if json.loads(json.dumps(asdict(actual)))!=binding or r.selection.seal!=full['seal'] or r.manifest!=full['manifest']:
                raise ValueError('Resource上游固定封存依赖漂移')
            db=r.connect();db.close();checked.add(key)
        csv=self.manifest['csv_reference']
        r=ObservationReader(self.dsn,csv['run_id'],csv['snapshot'],[csv['anchor_source_id']],profile='observation',guard=self.guard)
        from data_pipeline.analysis.resources.observation import reference_metadata
        if json.loads(json.dumps(reference_metadata(r,csv['source_id'])))!=self.manifest['csv_observation']:raise ValueError('参考封存依赖漂移')
        for batch in r.reference_batches(csv['source_id']):self.guard()
        r.selection.check_sources([csv['source_id']])
        country=self.manifest['country_reference']
        if read_reference(self.dsn,country['reference_id'],country['dataset_id'],allow_fixture=self.allow_fixture)[1]!=country:
            raise ValueError('独立参考依赖漂移')

    def _connect(self):
        self._check();self._check_inputs();db=connect_duckdb()
        try:
            db.execute("SET memory_limit='512MB'");db.execute('SET threads=2')
            db.execute('LOAD ducklake');db.execute('LOAD postgres')
            db.execute('ATTACH '+literal('ducklake:postgres:'+self.dsn)+' AS lake (READ_ONLY)')
            # 目录实体不能仅靠同名run迁移到另一个PG或数据根。
            with psycopg2.connect(self.dsn) as pg:
                pg.set_session(readonly=True)
                with pg.cursor() as cur:
                    cur.execute("SELECT value FROM public.ducklake_metadata WHERE key='data_path'");path=cur.fetchone()[0]
                    cur.execute('SELECT path,path_is_relative FROM public.ducklake_schema WHERE schema_name=%s AND end_snapshot IS NULL',('resource_'+self.run_id,));schema=cur.fetchone()
            layout=self.receipt['storage_layout']
            if path!=layout['catalog_data_path'] or schema!=(layout['schema_path'],layout['path_is_relative']):raise ValueError('Resource目录身份漂移')
            check_table_set(self.dsn,'resource_'+self.run_id,self.snapshot)
            inventory=table_inventory(db,self._relation,self.guard)
            if inventory!=self.receipt['inventory']:raise ValueError('Resource全表内容或计数漂移')
            validate_relations(db,self._relation,self.manifest,self.guard,dsn=self.dsn)
            return db
        except BaseException:db.close();raise

    def _finish_read(self,db):
        if table_inventory(db,self._relation,self.guard)!=self.receipt['inventory']:raise ValueError('读取期间Resource内容漂移')
        self._check();self._check_inputs()

    def inputs(self):
        db=self._connect()
        try:
            result={'binding':self.manifest,'receipt':self.receipt,'run_id':self.run_id,'snapshot':self.snapshot,'dataset_id':self.dataset_id}
            self._finish_read(db);return result
        finally:db.close()

    def scan(self,table,*,scope='result',batch_size=4096):
        if table not in ALL_TABLES or scope not in ('result','all') or type(batch_size) is not int or not 1<=batch_size<=100000:raise ValueError('公开读取参数无效')
        db=self._connect()
        try:
            query='SELECT * FROM '+self._relation(table)
            if scope=='result':
                selected="SELECT source_id FROM "+self._relation('sources')+" WHERE purpose='result'"
                if table=='rendered_paths':query+=" WHERE path_digest IN (SELECT member FROM "+self._relation('memberships')+" WHERE kind='path' AND source_id IN ("+selected+'))'
                else:query+=' WHERE source_id IN ('+selected+')'
            for batch in db.execute(query).fetch_record_batch(batch_size):self.guard();yield batch
            self._finish_read(db)
        finally:db.close()

    def coverage(self,*,scope='result'):
        return self.scan('coverage',scope=scope)

    def values(self,*,target='metrics',scope='result'):
        """每个原始metric单元返回独立资格；Unknown保留raw而main=None。"""
        if scope not in ('result','all') or target not in ('metrics','normal_bands','topology_status'):raise ValueError('未知读取范围或主值目标')
        db=self._connect()
        try:
            q=db.execute('SELECT * FROM '+self._relation('qualifications')).fetch_arrow_table().to_pylist()
            qualified={}
            def key(raw):
                if target=='topology_status':return (raw['source_id'],'country',raw['country_cn'])
                if target=='normal_bands':return (raw['source_id'],raw['dimension'],raw['bucket'],raw['metric'])
                return (raw['source_id'],raw['dimension'],raw['bucket'])
            for row in q:
                if row['target']==target:
                    k=(row['source_id'],row['dimension'],row['bucket'])
                    if target=='normal_bands':k+= (row['metric'],)
                    qualified.setdefault(k,[]).append(row)
            query='SELECT * FROM '+self._relation(target)
            if scope=='result':query+=" WHERE source_id IN (SELECT source_id FROM "+self._relation('sources')+" WHERE purpose='result')"
            for batch in db.execute(query).fetch_record_batch(4096):
                self.guard()
                for raw in batch.to_pylist():
                    qualifications=qualified[key(raw)]
                    if target=='metrics':main={q['metric']:raw[q['metric']] if q['status']=='qualified' else None for q in qualifications}
                    else:
                        fields=('upper_bound','lower_bound','mean','population_std') if target=='normal_bands' else ('status','node_count','edge_count')
                        main={name:raw[name] if qualifications[0]['status']=='qualified' else None for name in fields}
                    yield {'raw':raw,'qualification':qualifications,'main':main}
            self._finish_read(db)
        finally:db.close()

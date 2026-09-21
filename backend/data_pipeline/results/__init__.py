"""共享人工发布：Q1资源/特征及Q2事件，固定catalog，不接HTTP或真实selector。"""
from contextlib import ExitStack
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import uuid
import psycopg2
from psycopg2.extras import Json, execute_values
from data_pipeline.analysis.resources.compute import RULE, METRIC_UNITS
from data_pipeline.results import resource_feature_bindings as bindings, profiles, detection_binding, detection_queries, country_binding, country_queries as country
from data_pipeline.results import published_reader as combined_publication
from data_pipeline.results.manifest_io import Limits, digest, encode, require, seal, stamp, write_sealed, file_hash, fsync_dir

PROFILE = {'id':'q1-resource-feature/v1','intent':'module_scoped','data_kind':'fixture',
           'required':['resource.metrics','feature.ordinary.windows','feature.ir.windows']}
SELECTOR = 'fixture:resource-feature:q1'


@dataclass(frozen=True)
class Token:
    publication_id: str
    build_id: str
    profile_digest: str


class Publication:
    """初始化/prepare/publish仅显式离线调用；查询永不建表或补建。"""
    def __init__(self, dsn, private_root, *, limits=Limits(), detection=None, country=None, combined=None, combined_input=None):
        self.dsn=dsn;self.root=Path(private_root).resolve();self.limits=limits;self.detection=detection;self.country=country
        self.combined=None if combined is None else dict(combined)
        self.combined_input=combined_input
        require(self.root.is_dir(),'先明确已有私有根目录')

    def guard(self, rows=0): self.limits.check(self.root,rows)

    def initialize(self):
        self.guard()
        with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
            identity=bindings.instance(c)
            c.execute('CREATE SCHEMA IF NOT EXISTS publication_q1')
            c.execute('CREATE TABLE IF NOT EXISTS publication_q1.owner (singleton BOOLEAN PRIMARY KEY CHECK(singleton), identity JSONB NOT NULL, root TEXT NOT NULL)')
            c.execute('INSERT INTO publication_q1.owner VALUES (true,%s,%s) ON CONFLICT DO NOTHING',(Json(identity),str(self.root)))
            self._owner(c)
            c.execute('''CREATE TABLE IF NOT EXISTS publication_q1.builds (
                build_id TEXT PRIMARY KEY, state TEXT NOT NULL, input_digest TEXT NOT NULL,
                manifest JSONB, manifest_path TEXT, manifest_sha TEXT, publication_id TEXT UNIQUE)''')
            c.execute('''CREATE TABLE IF NOT EXISTS publication_q1.rows (
                build_id TEXT NOT NULL REFERENCES publication_q1.builds, kind TEXT NOT NULL,
                mode TEXT NOT NULL, sort_key TEXT NOT NULL, ordinal BIGINT NOT NULL,
                payload JSONB NOT NULL, row_digest TEXT NOT NULL,
                PRIMARY KEY(build_id,kind,ordinal))''')
            c.execute('CREATE INDEX IF NOT EXISTS q1_order ON publication_q1.rows(build_id,kind,mode,sort_key,ordinal)')
            c.execute('''CREATE TABLE IF NOT EXISTS publication_q1.head (
                selector TEXT PRIMARY KEY CHECK(selector='fixture:resource-feature:q1'),
                publication_id TEXT NOT NULL, build_id TEXT NOT NULL, generation BIGINT NOT NULL)''')

    def migrate_profiles(self):
        self.guard()
        with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
            self._owner(c);profiles.migrate(c)

    def prepare_combined(self,request,*,max_rows,max_bytes,progress=None):
        from data_pipeline.results.build_manifest import prepare
        return prepare(self,request,max_rows=max_rows,max_bytes=max_bytes,progress=progress)

    def prepare_detection(self,ready_path):
        return detection_queries.prepare(self,ready_path)

    def open_combined_component(self,token,owner,request,*,max_rows,max_bytes):
        return combined_publication.open_component(self,token,owner,request,max_rows=max_rows,max_bytes=max_bytes)

    def query_detection(self,token,view,**kwargs):
        return detection_queries.query(self,token,view,**kwargs)

    def prepare_country(self,read_binding,admission_proof,*,reference_binding,profile):
        return country.prepare(self,read_binding,admission_proof,reference_binding=reference_binding,profile=profile)

    def select_country(self,token,*,result_id,incident_id,revision,cohort_id):
        return country.select(self,token,result_id=result_id,incident_id=incident_id,revision=revision,cohort_id=cohort_id)

    def verify_country_qualification(self,descriptor,*,lock=False,selection=None):
        return country.verify_qualification(self,descriptor,lock=lock,selection=selection)

    def resolve_alias(self,token,*,alias,page=1,page_size=20):
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                self._owner(c);profile=self._manifest(c,token,{'published'})['profile']
        if profile==profiles.Q2:return self.query_detection(token,'alias',alias=alias,page=page,page_size=page_size)
        require(profile==profiles.COUNTRY,'该profile没有统一alias能力')
        return country.resolve_alias(self,token,alias,page=page,page_size=page_size)

    def _verify_component(self,c,component,stack,lock=False):
        if component['kind']=='detection':stack.enter_context(detection_binding.verify(self,component,lock))
        elif component['kind']=='country':stack.enter_context(country_binding.verify(self,component,lock=lock))
        else:bindings.verify(c,component,lock)

    def _owner(self,c):
        c.execute('SELECT identity,root FROM publication_q1.owner WHERE singleton')
        require(c.fetchone()==(bindings.instance(c),str(self.root)),'私有库/根目录身份不匹配')

    def _checkpoint(self, name):
        """普通故障注入接缝；不负责恢复或重试。"""

    def prepare(self, resource_receipt, feature_receipt, *, profile=None):
        profile=json.loads(encode(PROFILE if profile is None else profile))
        require(profile==PROFILE,'Q1只接受预声明人工module_scoped profile')
        self.guard()
        with psycopg2.connect(self.dsn) as pg,pg.cursor() as c: self._owner(c)
        components=[bindings.inspect(self.dsn,k,p,self.root,self.guard) for k,p in
                    [('resource',resource_receipt),('feature',feature_receipt)]]
        require(components[0]['catalog_ref']!=components[1]['catalog_ref'],'必须两个独立catalog')
        require({x['catalog_ref']['metadata_schema'] for x in components}=={'public','fl_'+components[1]['run_id']},'catalog角色错误')
        projection_code={p.name:file_hash(p) for p in sorted(Path(__file__).parent.glob('*.py'))}
        input_digest=digest({'profile':profile,'components':components,'projection_code':projection_code})
        build=uuid.uuid4().hex
        output=self.root/'q1-builds'/build;output.mkdir(parents=True)
        with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
            c.execute('INSERT INTO publication_q1.builds(build_id,state,input_digest) VALUES (%s,%s,%s)',(build,'candidate',input_digest))
        totals={}; content={}; capability_counts={k:0 for k in profile['required']}
        row_count=0
        for binding in components:
            kind=binding['kind'];name='metrics' if kind=='resource' else 'windows'
            report=binding['report'];sources={}
            if kind=='resource':
                for batch in bindings.scan(self.dsn,kind,binding['run_id'],binding['snapshot'],'sources',batch_rows=self.limits.batch_rows):
                    sources.update({r['source_id']:r for r in batch.to_pylist()})
            else:
                sources={s['source_id']:s for s in report['specification'].get('source_bindings',[])}
            ordinal=0; pending=[];size=0
            with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
                for batch in bindings.scan(self.dsn,kind,binding['run_id'],binding['snapshot'],name,batch_rows=self.limits.batch_rows):
                    for row in batch.to_pylist():
                        row_count+=1;self.guard(row_count); mode=row.get('mode','')
                        if kind=='resource':
                            key=[row['time'].isoformat(),row['source_id'],row['dimension'],row['bucket']]
                            calculation={'kind':'calculated','basis':sources[row['source_id']], 'rule':RULE,
                                'units':METRIC_UNITS,'assumptions':report.get('limitations',[])}
                            capability_counts['resource.metrics']+=1
                        else:
                            require(mode in ('ordinary','ir'),'未知Feature模式')
                            key=[row['start'],row['source_rank'],mode,row['scope'],row['subject'],row['country']]
                            spec=report['specification']
                            if sources:
                                source=sources.get(row['source_id'])
                                require(source is not None and row['source_rank']==source['sequence'] and
                                        row['window_role']==source['window_role'] and
                                        all(row[n]==source['window'][n] for n in ('start','end','file_time')),
                                        'Feature窗口与固定来源不符')
                            calculation={'kind':'calculated','basis':{'observation_version':spec['observation_version'],
                                'reference_version':report['reference_version'],'windows':spec['windows'],
                                'diagnostics':binding['feature_sources']['diagnostics'],
                                **({k:spec[k] for k in ('source_bindings','reference_binding','result_window','comparison_window','calculation_window')}
                                   if spec.get('source_bindings') else {})},
                                'rule':spec['algorithm_versions'][mode],'assumptions':spec['limitations'],
                                'row_presence':row['row_presence'],'resource_status':row['resource_status'],
                                'units':{'v4Prefix_num':'IPv4 /24等效段','v6Prefix_num':'IPv6 /48等效段','v4IP_num':'IPv4地址量',
                                         'announ_num':'宣告记录','withdraw_num':'撤回记录'}}
                            capability_counts['feature.'+mode+'.windows']+=1
                        payload=json.loads(encode({'value':row,'calculation':calculation}))
                        raw=encode(payload).encode();require(len(raw)<=self.limits.max_row_bytes,'单行字节保护触发')
                        if pending and (len(pending)>=self.limits.batch_rows or size+len(raw)>self.limits.batch_bytes):
                            self._insert(c,pending);pending=[];size=0
                        pending.append((build,kind,mode,encode(key),ordinal,Json(payload),digest([kind,mode,encode(key),ordinal,payload])))
                        size+=len(raw);ordinal+=1
                if pending:self._insert(c,pending)
            totals[kind]=ordinal
            with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
                content[kind]=self._content(c,build,kind)
            self._checkpoint('component_written')
        require(all(v>0 for v in capability_counts.values()),'必需输出为空，不能自动known_empty')
        files={}
        for component in components:
            for path in component['files']:
                if path not in files:files[path]=seal(path,self.guard)
                require(files[path]['sha256']==component['file_hashes'][path],'构建期间文件内容漂移')
        for path in files:
            parent=Path(path).parent
            while parent.is_relative_to(self.root):
                fsync_dir(parent)
                if parent==self.root:break
                parent=parent.parent
        # chmod导致ctime变化在这里统一保存，不混用封存前实体戳。
        self._checkpoint('files_sealed')
        with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
            for component in components: bindings.verify(c,component)
        manifest={'contract':'q1-publication/v1','profile':profile,'input_binding_digest':input_digest,
                  'build_id':build,'projection_version':'q1-rows/v1','projection_code':projection_code,'components':components,
                  'files':files,'counts':totals,'content':content,'capability_counts':capability_counts,
                  'availability':'available','coverage':'declared_fixture_only'}
        publication='q1_'+digest(manifest)
        artifact=write_sealed(output/'manifest.json',manifest)
        fsync_dir(output.parent);fsync_dir(self.root)
        self._checkpoint('manifest_sealed')
        with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
            c.execute("UPDATE publication_q1.builds SET state='ready',manifest=%s,manifest_path=%s,manifest_sha=%s,publication_id=%s WHERE build_id=%s AND state='candidate'",
                      (Json(manifest),artifact['path'],artifact['sha256'],publication,build))
            require(c.rowcount==1,'候选状态漂移')
        return Token(publication,build,digest(profile))

    def _insert(self,c,rows):
        self.guard()
        execute_values(c,'INSERT INTO publication_q1.rows VALUES %s',rows,page_size=self.limits.batch_rows)

    def _content(self,c,build,kind):
        # 命名游标避免把整份副本缓存在客户端内存。
        h=hashlib.sha256();n=0
        with c.connection.cursor(name='q1_'+uuid.uuid4().hex) as stream:
            stream.execute('SELECT kind,mode,sort_key,ordinal,payload,row_digest FROM publication_q1.rows WHERE build_id=%s AND kind=%s ORDER BY sort_key COLLATE "C",ordinal',(build,kind))
            while True:
                rows=stream.fetchmany(self.limits.batch_rows)
                if not rows:break
                for row_kind,mode,key,ordinal,payload,sha in rows:
                    require(digest([row_kind,mode,key,ordinal,payload])==sha,'PG副本行内容损坏')
                    h.update((sha+'\n').encode());n+=1
                self.guard(n)
        return {'count':n,'sha256':h.hexdigest()}

    def _manifest(self,c,token,states):
        require(isinstance(token,Token),'必须固定发布令牌')
        c.execute('SELECT state,manifest,manifest_path,manifest_sha,publication_id FROM publication_q1.builds WHERE build_id=%s',(token.build_id,))
        row=c.fetchone();require(row is not None and row[0] in states,'候选/未知发布不可读')
        _,m,path,sha,p=row
        require(p==token.publication_id=='q1_'+digest(m) and token.profile_digest==digest(m['profile']) and m['profile'] in profiles.SELECTORS.values(),'发布令牌身份错误')
        if m['profile']==profiles.Q2:detection_binding.require_message_proof(m['components'][0])
        if m['profile'] in combined_publication.PROFILES.values():combined_publication.validate_manifest(m)
        require(m['build_id']==token.build_id,'build错版')
        require(file_hash(path,self.guard)==sha and json.loads(Path(path).read_text())==m,'发布清单损坏')
        return m

    def publish(self,token,*,expected_generation,selector=SELECTOR):
        require(selector in profiles.SELECTORS,'拒绝真实/full selector')
        require(type(expected_generation) is int and expected_generation>=0,'无效generation')
        self._checkpoint('before_publish')
        with ExitStack() as stack, psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
            self._owner(c)
            # 包括不存在head的首次提交；一个私有写入者锁覆盖本片所有selector操作。
            c.execute('SELECT pg_advisory_xact_lock(7194301)')
            m=self._manifest(c,token,{'ready','published'})
            require(profiles.selector_for(m['profile'])==selector,'selector与固定profile不符')
            c.execute('SELECT publication_id,build_id,generation FROM publication_q1.head WHERE selector=%s FOR UPDATE',(selector,))
            old=c.fetchone()
            if old and old[:2]==(token.publication_id,token.build_id):
                if m['contract']==combined_publication.CONTRACT:
                    c.execute('SELECT state FROM publication_q1.builds WHERE build_id=%s FOR UPDATE',(token.build_id,))
                    require(c.fetchone()[0]=='published','组合head状态不符')
                    stack.enter_context(combined_publication.graph(self,m).locked())
                return token
            c.execute('SELECT state FROM publication_q1.builds WHERE build_id=%s FOR UPDATE',(token.build_id,))
            require(c.fetchone()[0]=='ready','已发布版本不得重新选择旧head')
            require((old[2] if old else 0)==expected_generation,'generation冲突')
            if m['contract']==combined_publication.CONTRACT:
                # 复用同一个控制事务。全部单锁/current先完成；stack最后退出，
                # 因而锁持有跨越下方head写入与pg.__exit__提交。
                stack.enter_context(combined_publication.graph(self,m).locked())
            else:
                for component in m['components']:self._verify_component(c,component,stack,lock=True)
                if m['profile']==profiles.Q2:require(detection_queries.alias_content(self,c,token.build_id)==m['aliases'],'旧引用完整性损坏')
                for f in m['files'].values():
                    self.guard();require(stamp(f['path'])==f['stamp'] and file_hash(f['path'],self.guard)==f['sha256'],'封存文件损坏')
                for kind,expected in m['content'].items():require(self._content(c,token.build_id,kind)==expected,'PG副本计数/内容损坏')
            self._checkpoint('before_visible')
            c.execute("UPDATE publication_q1.builds SET state='published' WHERE build_id=%s AND state='ready'",(token.build_id,))
            require(c.rowcount==1,'发布状态冲突')
            c.execute('INSERT INTO publication_q1.head VALUES (%s,%s,%s,%s) ON CONFLICT(selector) DO UPDATE SET publication_id=excluded.publication_id,build_id=excluded.build_id,generation=excluded.generation',
                      (selector,token.publication_id,token.build_id,expected_generation+1))
        return token

    def discover(self,selector=SELECTOR):
        require(selector in profiles.SELECTORS,'Q1不提供真实发布')
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                self._owner(c)
                c.execute('SELECT publication_id,build_id,generation FROM publication_q1.head WHERE selector=%s',(selector,))
                row=c.fetchone();require(row is not None,'没有已发布版本')
                token=Token(row[0],row[1],digest(profiles.SELECTORS[selector]));self._manifest(c,token,{'published'})
                return token,row[2]

    def query(self,token,kind,*,mode='',page=1,page_size=20):
        require(kind in ('resource','feature'),'能力未实现')
        require((kind=='resource' and mode=='') or (kind=='feature' and mode in ('ordinary','ir')),'查询模式非法')
        require(type(page) is int and page>=1 and type(page_size) is int and 1<=page_size<=100,'分页非法')
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True,isolation_level='REPEATABLE READ')
            with pg.cursor() as c:
                c.execute("SET LOCAL statement_timeout='10s'")
                self._owner(c);m=self._manifest(c,token,{'published'})
                binding=next(x for x in m['components'] if x['kind']==kind)
                bindings.verify(c,binding)
                # 副本查询不重扫Parquet正文SHA；只检查此组件所依赖文件的实体戳。
                for path in binding['files']:require(stamp(path)==m['files'][path]['stamp'],'所需文件缺失或实体改变')
                c.execute('SELECT count(*) FROM publication_q1.rows WHERE build_id=%s AND kind=%s',(token.build_id,kind))
                require(c.fetchone()[0]==m['counts'][kind],'PG副本行数损坏')
                c.execute('SELECT count(*) FROM publication_q1.rows WHERE build_id=%s AND kind=%s AND mode=%s',(token.build_id,kind,mode));total=c.fetchone()[0]
                capability='resource.metrics' if kind=='resource' else 'feature.'+mode+'.windows'
                require(total==m['capability_counts'][capability],'PG副本请求能力行数损坏')
                c.execute('SELECT kind,mode,sort_key,ordinal,payload,row_digest FROM publication_q1.rows WHERE build_id=%s AND kind=%s AND mode=%s ORDER BY sort_key COLLATE "C",ordinal LIMIT %s OFFSET %s',(token.build_id,kind,mode,page_size,(page-1)*page_size))
                rows=c.fetchall()
                require(all(digest(list(row[:5]))==row[5] for row in rows),'PG副本页内容损坏')
                return {'publication_id':token.publication_id,'build_id':token.build_id,'component':kind,
                        'mode':mode,'page':page,'page_size':page_size,'total':total,'items':[r[4] for r in rows],
                        'availability':'available','coverage':'declared_fixture_only',
                        'binding_report':binding['report'],'source':{
                            'catalog_ref':binding['catalog_ref'],'run_id':binding['run_id'],'snapshot':binding['snapshot']},
                        'limitations':['计算值不是直接观测值','无行不证明零；稀疏与write_disabled保留',
                                       '未请求页不逐行校验；Parquet未重扫SHA，不保证离线损坏即时发现']}

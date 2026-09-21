"""C4唯一发布适配；正文、索引与解释均通过国家公开入口读取。"""
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import uuid

import psycopg2
from psycopg2.extras import Json
from data_pipeline.analysis.country_events import selection_contract as contract
from data_pipeline.analysis.country_events.event_aggregation import EventStatus
from data_pipeline.results import country_binding, profiles
from data_pipeline.results.manifest_io import require, digest, write_sealed, fsync_dir, file_hash


def api():
    # 运行实现由C4 owner交付；缺实现明确报错，不建立fixture假成功路径。
    from data_pipeline.analysis.country_events import selection_index as query_index
    return query_index


def limits(p):
    return contract.QueryLimits(batch_rows=p.limits.batch_rows,batch_bytes=p.limits.batch_bytes,
        max_row_bytes=p.limits.max_row_bytes,max_rows=p.limits.max_rows,max_rss_bytes=p.limits.max_rss_bytes)


def prepare(p,read_binding,admission_proof,*,reference_binding,profile):
    from data_pipeline.results import Token
    require(profile==contract.PROFILE,'国家只允许固定C4模块profile')
    require(read_binding.reference==reference_binding and reference_binding is not None,'国家参考须显式同版绑定')
    cfg=country_binding.config(p)
    for root in (read_binding.root,read_binding.component.root):
        require(Path(root).resolve().is_relative_to(p.root),'国家制品未在显式私有根绑定')
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        p._owner(c);c.execute('SELECT version FROM publication_q1.schema_version');require(c.fetchall()==[(3,)],'先显式迁移发布元数据v3')
        c.execute('SELECT profile FROM publication_q1.profiles WHERE selector=%s',(contract.SELECTOR,));require(c.fetchone()==(profiles.COUNTRY,),'国家profile目录漂移')
    descriptor=api().inspect_country(read_binding,admission_proof,runtime=cfg['runtime'],limits=limits(p))
    require(descriptor.read_binding==read_binding and descriptor.admission_proof==admission_proof,'国家公开inspect返回错版')
    component=country_binding.capture(p,descriptor)
    events=0;receipt=None
    from contextlib import closing
    with closing(api().iter_country_events(descriptor,limits=limits(p))) as stream:
        for item in stream:
            require(receipt is None,'国家事件回执后仍有正文')
            if type(item) is contract.QueryReadReceipt:receipt=item
            else:
                require(type(item) is EventStatus,'国家事件枚举类型错误');validate_status(item);events+=1;p.guard(events)
    require(receipt is not None and receipt.rows==events==json.loads(descriptor.event_index_json)['rows'],'国家事件枚举未完整结束')
    build=uuid.uuid4().hex;output=p.root/'q1-builds'/build;output.mkdir(parents=True)
    code={f.name:file_hash(f) for f in sorted(Path(__file__).parent.glob('*.py'))}
    manifest={'contract':'country-publication/v1','profile':profiles.COUNTRY,'build_id':build,
        'components':[component],'component_key':'country:'+read_binding.result_id,
        'files':{},'content':{},'projection_code':code,'coverage':'declared_fixture_only'}
    input_sha=digest(manifest)
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute('INSERT INTO publication_q1.builds(build_id,state,input_digest) VALUES (%s,%s,%s)',(build,'candidate',input_sha))
    publication='q1_'+digest(manifest);artifact=write_sealed(output/'manifest.json',manifest);fsync_dir(output.parent)
    p._checkpoint('manifest_sealed')
    with country_binding.verify(p,component):pass
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE publication_q1.builds SET state='ready',manifest=%s,manifest_path=%s,manifest_sha=%s,publication_id=%s WHERE build_id=%s AND state='candidate'",(Json(manifest),artifact['path'],artifact['sha256'],publication,build))
        require(c.rowcount==1,'国家候选状态漂移')
    return Token(publication,build,digest(profiles.COUNTRY))


def manifest(p,c,token):
    m=p._manifest(c,token,{'published'});require(m['profile']==profiles.COUNTRY,'非国家固定profile')
    require(len(m['components'])==1 and m['components'][0]['kind']=='country','国家组件范围错误')
    return m


def select(p,token,*,result_id,incident_id,revision,cohort_id):
    require(type(incident_id) is str and bool(incident_id) and type(revision) is int and revision>0,'国家事件选择必须明确修订')
    with psycopg2.connect(p.dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            p._owner(c);m=manifest(p,c,token);b=m['components'][0];descriptor=contract.contract_value(b['descriptor'])
            require(descriptor.read_binding.result_id==result_id,'国家结果不属于该P')
            with country_binding.verify(p,b):
                event=api().read_country_event(descriptor,incident_id=incident_id,revision=revision)
                require(event.incident.incident_id==incident_id and event.incident.revision==revision and event.cohort_id==cohort_id,'国家事件/修订/cohort错版')
                validate_status(event)
            require(manifest(p,c,token)==m,'国家选择期间P资格漂移')
            return contract.CountrySelection(token.publication_id,token.build_id,token.profile_digest,m['component_key'],descriptor,incident_id,revision,cohort_id,contract.typed_encode(event))


def validate_status(event):
    require(event.state in ('available','partial','unavailable'),'国家事件状态不支持')
    if event.state=='unavailable':require(event.cohort_id is None and bool(event.reasons) and set(event.reasons)<=set(contract.UNAVAILABLE),'国家不可用原因不符合profile')
    elif event.state=='partial':require(bool(event.reasons) and set(event.reasons)<=set(contract.PARTIAL),'国家partial原因不符合profile')
    else:require(not event.reasons,'国家available仍有未处理原因')


@contextmanager
def verify_qualification(p,descriptor,*,lock=False,selection=None):
    from data_pipeline.results import Token
    if selection is None:
        # 离线入口仅核来源；不能借此取得published查询许可。
        with ExitStack() as stack:
            country_binding.entities(p,descriptor)
            country_binding.current(p,descriptor,stack,lock=lock)
            yield
        return
    require(type(selection) is contract.CountrySelection and selection.descriptor==descriptor,'国家selection/描述错版')
    token=Token(selection.publication_id,selection.build_id,selection.profile_digest)
    with psycopg2.connect(p.dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            p._owner(c);m=manifest(p,c,token);b=m['components'][0]
            require(m['component_key']==selection.component_key and b['descriptor']==contract.contract_json(descriptor),'国家selection不属于真实P')
            with country_binding.verify(p,b,lock=lock):
                event=api().read_country_event(descriptor,incident_id=selection.incident_id,revision=selection.revision)
                require(event.cohort_id==selection.cohort_id and contract.typed_encode(event)==selection.event_status_typed,'国家selection事件不属于固定结果')
                yield
                require(manifest(p,c,token)==m,'国家查询尾部P资格漂移')


def resolve_alias(p,token,alias,*,page=1,page_size=20):
    from contextlib import closing
    require(type(page) is int and page>=1 and type(page_size) is int and 1<=page_size<=100,'国家alias分页非法')
    require(type(alias) is dict and set(alias)=={'source','table','ref_kind','id'},'国家alias须固定四项来源键')
    filters=contract.canonical(alias)
    with psycopg2.connect(p.dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            p._owner(c);m=manifest(p,c,token);binding=m['components'][0];descriptor=contract.contract_value(binding['descriptor'])
            count=0;receipt=None;items=[];targets={};start=(page-1)*page_size
            with country_binding.verify(p,binding):
                try:
                    with closing(api().iter_country_aliases(descriptor,filters_json=filters,limits=limits(p))) as stream:
                        for item in stream:
                            require(receipt is None,'国家alias尾回执后仍有行')
                            if type(item) is contract.QueryReadReceipt:receipt=item;continue
                            require(type(item) is dict and all(item.get(k)==v for k,v in alias.items()),'国家alias返回错集合')
                            key=(item['incident_id'],item['revision'],item['cohort_id'])
                            require(type(key[0]) is str and type(key[1]) is int and key[1]>0,'国家alias目标身份非法')
                            if key not in targets:
                                event=api().read_country_event(descriptor,incident_id=key[0],revision=key[1])
                                require(event.cohort_id==key[2],'国家alias目标cohort错误')
                                targets[key]={'component_key':m['component_key'],'result_id':descriptor.read_binding.result_id,'incident_id':key[0],'revision':key[1],'cohort_id':key[2]}
                            if start<=count<start+page_size:items.append(item)
                            count+=1;p.guard(count)
                except ValueError as error:
                    if str(error)!='C4_alias_not_available':raise
                    require(count==0,'国家alias读取中失去来源')
                    require(manifest(p,c,token)==m,'国家alias读取期间P资格漂移')
                    return {'resolution':'not_available','total':None,'items':[],'reason':'no_frozen_alias_source'}
                require(receipt is not None and receipt.rows==count,'国家alias读取未完整结束')
            require(manifest(p,c,token)==m,'国家alias尾部P资格漂移')
            return {'publication_id':token.publication_id,'result_id':descriptor.read_binding.result_id,
                'resolution':'not_found' if not targets else 'resolved' if len(targets)==1 else 'ambiguous_reference',
                'total':count,'page':page,'page_size':page_size,'items':items,'candidates':[targets[k] for k in sorted(targets,key=lambda k:(k[0],k[1],k[2] or ''))]}

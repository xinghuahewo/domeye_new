"""Detection实际查询副本、实例折叠和多值旧引用；共享原有build/head。"""
from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import uuid
import psycopg2
from psycopg2.extras import Json,execute_values
from data_pipeline.results import detection_binding as binding_api
from data_pipeline.results.manifest_io import encode, digest, require, file_hash, seal, stamp, write_sealed, fsync_dir
from data_pipeline.results.profiles import Q2, selector_for

PG_KINDS={'business_revision','rule_decision','leak_event_record'}

KINDS={'prefix_outage','as_outage','country_outage','hijack','sub_hijack','leak','moas'}


def aliases(row):
    attrs=json.loads(row['attributes_json']);legacy=json.loads(row['legacy_json']);ref=attrs.get('legacy_ref')
    if not ref:return []
    value={k:ref.get(k) for k in ('source','table','id','onset','object')}
    value.update(ref_kind='leak_phenomenon' if row['event_kind']=='leak' else row['event_kind'],
                 current_table=ref.get('current_legacy_table'))
    result=[value]
    if row['event_kind']=='leak' and legacy.get('legacy_event_id') is not None:
        result.append({**value,'ref_kind':'leak_event','id':legacy['legacy_event_id'],'table':legacy.get('leak_event_table')})
    return result



def leak_link(row):
    attrs=json.loads(row['attributes_json']);legacy=json.loads(row['legacy_json'])
    if row['record_kind']=='leak_event_record':
        return [legacy.get('source'),legacy.get('leak_event_table'),legacy.get('event_id'),legacy.get('phenomenon_id'),legacy.get('prefix')]
    return [attrs['scope']['source'],legacy.get('leak_event_table'),legacy.get('legacy_event_id'),attrs.get('legacy_ref',{}).get('id'),attrs.get('object')]


def resolve_targets(targets):
    # 按原对象身份分组，绝不按详情值去重；历史carrier以后也须交独立行身份。
    identities=sorted(set(targets))
    return {'resolution':'not_found' if not identities else 'resolved' if len(identities)==1 else 'ambiguous_reference',
            'candidate_incidents':identities}


def alias_content(p,c,build):
    h=hashlib.sha256();count=0
    with c.connection.cursor(name='q2_alias_'+uuid.uuid4().hex) as stream:
        stream.execute('SELECT alias,alias_digest,target_sequence,row_digest FROM publication_q1.aliases WHERE build_id=%s ORDER BY alias_digest,target_sequence',(build,))
        while True:
            rows=stream.fetchmany(p.limits.batch_rows)
            if not rows:break
            for alias,sha,sequence,rowsha in rows:
                require(digest(alias)==sha and digest([alias,sequence])==rowsha,'旧引用副本损坏')
                h.update((rowsha+'\n').encode());count+=1;p.guard(count)
    return {'count':count,'sha256':h.hexdigest()}


def prepare(p,ready):
    from data_pipeline.results import Token
    p.guard()
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        p._owner(c);c.execute('SELECT version FROM publication_q1.schema_version');require(c.fetchall() in ([(2,)],[(3,)]),'先显式迁移发布元数据v2')
        c.execute('SELECT profile FROM publication_q1.profiles WHERE selector=%s',(selector_for(Q2),));require(c.fetchone()==(Q2,),'先显式迁移固定profile目录')
    b=binding_api.inspect(p,ready);report=b['report'];component='detection:'+b['run_id']
    code={f.name:file_hash(f) for f in sorted(Path(__file__).parent.glob('*.py'))}
    input_sha=digest({'profile':Q2,'component':b,'code':code});build=uuid.uuid4().hex
    output=p.root/'q1-builds'/build;output.mkdir(parents=True)
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute('INSERT INTO publication_q1.builds(build_id,state,input_digest) VALUES (%s,%s,%s)',(build,'candidate',input_sha))
    kinds=Counter();pg_kinds=Counter();latest={};revisions={};ends=[];starts=[];completion=[];leak_outputs=set();pending=[];bytes_pending=0
    counts={};stream_sha={};alias_rows=[];state_headers={};state_bindings=set();read_count=0
    messages=binding_api.MessageProof(report['identity']['selected_sources'])
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        for table,ordinal_name in [('records','sequence'),('state_entries','ordinal')]:
            h=hashlib.sha256();count=0
            with closing(binding_api.rows(p,b,table)) as stored_rows:
                for raw in stored_rows:
                    read_count+=1;p.guard(read_count);require(type(raw[ordinal_name]) is int and raw[ordinal_name]==count,'Detection原序号缺失/重复/错序')
                    value=json.loads(encode(raw));h.update((digest(value)+'\n').encode());count+=1
                    if table=='state_entries':
                        pair=raw['family'],raw['attribute'];container=raw['container']
                        key_value=json.loads(raw['key_json']);state_value=json.loads(raw['value_json'])
                        if container=='entry':require(state_headers.get(pair) in ('dict','set','list'),'状态项缺容器声明')
                        else:
                            require(container in ('dict','set','list','value') and pair not in state_headers and key_value is None,'状态容器声明重复/无效')
                            state_headers[pair]=container
                        if pair==('run','baseline') and key_value=='baseline_ref':
                            require(state_value==report['scope']['input_version']+'/'+report['identity']['selected_sources'][0],'状态基线引用不符')
                            state_bindings.add('baseline')
                        if pair==('run','references') and key_value in ('version','raw_rows','row_refs'):
                            from data_pipeline.analysis.detection.reference_view import ReferenceView
                            expected={'version':report['identity']['reference_version'],'raw_rows':report['identity']['reference_sources'],
                                      'row_refs':{'snapshot_ref':report['scope']['input_version'],'selection_rule':ReferenceView.version_rule}}
                            require(state_value==expected[key_value],'状态参考身份/规则不符');state_bindings.add(key_value)
                        continue
                    require(set(raw)=={n for n,_ in binding_api.COLUMNS},'Detection typed列集合不符')
                    attrs=json.loads(raw['attributes_json']);legacy=json.loads(raw['legacy_json']);evidence=json.loads(raw['evidence_json'])
                    require(attrs['kind']==raw['record_kind'] and attrs['scope']==report['scope'],'Detection原记录类型/scope不符')
                    require(attrs['reference_version']==report['identity']['reference_version'],'Detection原记录参考版本不符')
                    mode=raw['record_kind'];kinds[mode]+=1
                    if mode=='source_message':messages.add(legacy)
                    if mode=='source_start':starts.append(legacy['source_id'])
                    if mode=='source_end':ends.append(legacy)
                    if mode=='input_completion':completion.append(legacy)
                    if mode=='leak_event_record':
                        leak_outputs.add(encode(leak_link(raw)))
                    if mode=='business_revision':
                        inc=raw['incident_id'];rev=raw['revision'];kind=raw['event_kind']
                        require(isinstance(inc,str) and inc and type(rev) is int and rev>0 and kind in KINDS,'Detection修订身份无效')
                        require(all(raw[n]==attrs[n] for n in ('incident_id','revision','event_kind')) and raw['subject_key']==str(attrs['object']),'Detection typed修订与原属性不符')
                        require(rev==revisions.get(inc,0)+1,'Detection修订重复/缺失')
                        revisions[inc]=rev
                        if inc in latest:require(latest[inc]['event_kind']==kind,'事件kind跨修订漂移')
                        latest[inc]={'sequence':raw['sequence'],'revision':rev,'event_kind':kind,'legacy_event_output':None,
                                     'leak_key':leak_link(raw) if kind=='leak' else None}
                        for alias in aliases(raw):alias_rows.append((build,Json(alias),digest(alias),'detection',raw['sequence'],digest([alias,raw['sequence']])))
                    if raw['classification_state']=='ambiguous':require(raw['classified_hijack'] is None and raw['attacker_asn'] is None and raw['victim_asn'] is None,'歧义分类被提升为确定角色')
                    if mode not in PG_KINDS:continue
                    pg_kinds[mode]+=1
                    payload={'value':value};key=encode([raw['sequence']]);size=len(encode(payload).encode())
                    require(size<=p.limits.max_row_bytes,'Detection查询单行超过预算')
                    if pending and (len(pending)>=p.limits.batch_rows or bytes_pending+size>p.limits.batch_bytes):
                        p._insert(c,pending);pending=[];bytes_pending=0
                    pending.append((build,'detection',mode,key,raw['sequence'],Json(payload),digest(['detection',mode,key,raw['sequence'],payload])));bytes_pending+=size
            counts[table]=count;stream_sha[table]=h.hexdigest()
            require(count==report[table],'Detection实际湖计数与ready不符')
        if pending:p._insert(c,pending)
        require(messages.result()==binding_api.require_message_proof(b),'保存消息与上游真实消息覆盖不符')
        require(starts==report['identity']['selected_sources'] and encode(ends)==encode(b['source_ends']),'Detection来源结束回执不符')
        require(len(completion)==1 and encode(completion[0]['sources'])==encode(ends) and completion[0]['baseline_count']==ends[0]['elements'],'Detection缺完整输入证明')
        require(counts['records']>0 and counts['state_entries']>0,'Detection必需原记录/状态缺失')
        require(state_bindings=={'baseline','version','raw_rows','row_refs'},'Detection状态基线/参考绑定缺失')
        require({'outage','hijack','subhijack','leak','projection','output','run'}<={family for family,_ in state_headers},'Detection必需状态族缺失')
        require({('run','baseline'),('run','references'),('run','processed'),('run','last_observation'),('run','seen_observations'),
                 ('output','identities'),('output','revisions'),('projection','prefix_dict')}<=set(state_headers),'Detection必需状态声明缺失')
        batch=[];size=0
        for entry in alias_rows:
            p.guard();n=len(encode(entry[1].adapted).encode())
            require(n<=p.limits.max_row_bytes,'旧引用单行超过预算')
            if batch and (len(batch)>=p.limits.batch_rows or size+n>p.limits.batch_bytes):
                execute_values(c,'INSERT INTO publication_q1.aliases VALUES %s',batch,page_size=p.limits.batch_rows);batch=[];size=0
            batch.append(entry);size+=n
        if batch:execute_values(c,'INSERT INTO publication_q1.aliases VALUES %s',batch,page_size=p.limits.batch_rows)
        content=p._content(c,build,'detection');alias_sha=alias_content(p,c,build)
    for item in latest.values():
        if item['event_kind']=='leak':
            item['legacy_event_output']=encode(item['leak_key']) in leak_outputs
            item['legacy_event_ref']=item['leak_key'][:3] if item['legacy_event_output'] else None
        del item['leak_key']
    files={}
    for f in b['files']:
        files[f]=seal(f,p.guard);require(files[f]['sha256']==b['file_hashes'][f],'Detection构建期间文件漂移')
        fsync_dir(Path(f).parent)
    with binding_api.verify(p,b):pass
    manifest={'contract':'q2-publication/v1','profile':Q2,'build_id':build,'input_binding_digest':input_sha,'projection_version':'q2-events/v1',
              'projection_code':code,'components':[b],'files':files,'counts':{'detection':sum(pg_kinds.values())},'content':{'detection':content},
              'component_key':component,'table_counts':counts,'table_digests':stream_sha,'record_counts':dict(pg_kinds),'raw_record_counts':dict(kinds),'latest':latest,
              'revision_counts':revisions,'aliases':alias_sha,'availability':'available','coverage':'declared_fixture_only'}
    publication='q1_'+digest(manifest);artifact=write_sealed(output/'manifest.json',manifest);fsync_dir(output.parent);fsync_dir(p.root)
    p._checkpoint('manifest_sealed')
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE publication_q1.builds SET state='ready',manifest=%s,manifest_path=%s,manifest_sha=%s,publication_id=%s WHERE build_id=%s AND state='candidate'",(Json(manifest),artifact['path'],artifact['sha256'],publication,build));require(c.rowcount==1,'候选状态漂移')
    return Token(publication,build,digest(Q2))


def query(p,token,view,*,component=None,incident=None,revision=None,event_kind=None,record_kind=None,alias=None,page=1,page_size=20,scan_budget=None):
    require(view in ('records','events','revisions','detail','decisions','leak_outputs','alias'),'未知Detection查询')
    require(type(page) is int and page>=1 and type(page_size) is int and 1<=page_size<=100,'分页非法')
    with psycopg2.connect(p.dsn) as pg:
        pg.set_session(readonly=True,isolation_level='REPEATABLE READ')
        with pg.cursor() as c:
            c.execute("SET LOCAL statement_timeout='10s'");p._owner(c);m=p._manifest(c,token,{'published'});require(m['profile']==Q2,'非Detection固定profile')
            require(component is None or component==m['component_key'],'Detection component错版')
            with binding_api.verify(p,m['components'][0]):pass
            for f in m['files'].values():require(stamp(f['path'])==f['stamp'],'Detection所需文件漂移')
            if view=='records':return audit_records(p,m,token,record_kind,page,page_size,scan_budget)
            c.execute("SELECT mode,count(*) FROM publication_q1.rows WHERE build_id=%s AND kind='detection' GROUP BY mode",(token.build_id,));require(dict(c.fetchall())==m['record_counts'],'Detection请求类别计数损坏')
            condition='';params=[];expected=None
            if view=='alias':
                require(isinstance(alias,dict) and {'source','table','ref_kind','id'}<=set(alias)<= {'source','table','ref_kind','id','onset','object','current_table'},'原引用须明确来源/表/类型/ID')
                require(alias_content(p,c,token.build_id)==m['aliases'],'旧引用完整性损坏')
                c.execute('SELECT target_sequence FROM publication_q1.aliases WHERE build_id=%s AND alias @> %s ORDER BY target_sequence',(token.build_id,Json(alias)));sequences=[r[0] for r in c.fetchall()]
                condition=' AND ordinal=ANY(%s)';params=[sequences];expected=len(set(sequences))
            elif view in ('events','revisions','detail'):
                condition=" AND mode='business_revision'"
                if view=='events':
                    require(event_kind is None or event_kind in KINDS,'未知事件类别')
                    selected=[v['sequence'] for v in m['latest'].values() if event_kind is None or v['event_kind']==event_kind]
                    condition+=' AND ordinal=ANY(%s)';params=[selected];expected=len(selected)
                else:
                    require(component==m['component_key'] and isinstance(incident,str),'修订/详情必须固定component和incident')
                    condition+=" AND payload->'value'->>'incident_id'=%s";params=[incident];expected=m['revision_counts'].get(incident,0)
                    if view=='detail':
                        require(type(revision) is int and revision>0,'详情必须显式revision')
                        condition+=" AND (payload->'value'->>'revision')::bigint=%s";params.append(revision);expected=int(0<revision<=expected)
            else:
                if view=='decisions':record_kind='rule_decision'
                if view=='leak_outputs':record_kind='leak_event_record'
                if record_kind is not None:condition=' AND mode=%s';params=[record_kind];expected=m['record_counts'].get(record_kind,0)
                else:expected=m['table_counts']['records']
            base=" FROM publication_q1.rows WHERE build_id=%s AND kind='detection'"+condition
            c.execute('SELECT count(*)'+base,[token.build_id,*params]);total=c.fetchone()[0];require(total==expected,'Detection固定请求集合计数损坏')
            c.execute('SELECT kind,mode,sort_key,ordinal,payload,row_digest'+base+' ORDER BY ordinal LIMIT %s OFFSET %s',[token.build_id,*params,page_size,(page-1)*page_size]);rows=c.fetchall()
            require(all(digest(list(r[:5]))==r[5] for r in rows),'Detection查询页损坏')
            items=[r[4]['value'] for r in rows]
            result={'publication_id':token.publication_id,'component':m['component_key'],'view':view,'total':total,'page':page,'page_size':page_size,'items':items,
                    'availability':'available' if total else 'known_empty','scope':m['components'][0]['report']['scope'],
                    'identity':m['components'][0]['report']['identity'],'audit_record_count':m['table_counts']['records'],
                    'interpretation':'指定观察范围内旧启发式候选，不证明攻击或责任'}
            if view=='events':
                result['instance_metadata']={r['incident_id']:m['latest'][r['incident_id']] for r in items}
                result['counts_by_kind']=dict(Counter(v['event_kind'] for v in m['latest'].values()))
                result['leak_phenomenon_instances']=sum(v['event_kind']=='leak' for v in m['latest'].values())
                result['leak_legacy_event_output_instances']=len({encode(v['legacy_event_ref']) for v in m['latest'].values() if v['legacy_event_output'] is True})
            if view=='detail' and not total:result['reason']='not_found_in_fixed_component'
            if view=='alias':
                # 引用解析必须考虑全部匹配实例，不能用第一页决定唯一性。
                incidents=set()
                with c.connection.cursor(name='q2_resolve_'+uuid.uuid4().hex) as stream:
                    stream.execute('SELECT kind,mode,sort_key,ordinal,payload,row_digest'+base,[token.build_id,*params])
                    while True:
                        candidates=stream.fetchmany(p.limits.batch_rows)
                        if not candidates:break
                        for candidate in candidates:
                            p.guard();require(digest(list(candidate[:5]))==candidate[5],'旧引用目标副本损坏')
                            incidents.add(candidate[4]['value']['incident_id'])
                result.update(resolve_targets(incidents))
            return result


def audit_records(p,m,token,record_kind,page,page_size,scan_budget):
    total_rows=m['table_counts']['records']
    require(type(scan_budget) is int and total_rows<=scan_budget<=p.limits.max_rows,
            '原记录审计需显式足够的scan_budget；每次完整扫描固定湖表，不用于业务分页')
    selected=0;count=0;items=[];h=hashlib.sha256();start=(page-1)*page_size
    expected_messages=binding_api.require_message_proof(m['components'][0])
    messages=binding_api.MessageProof(m['components'][0]['report']['identity']['selected_sources'])
    with closing(binding_api.rows(p,m['components'][0],'records')) as rows:
        for raw in rows:
            count+=1;p.guard(count);require(count<=scan_budget and raw['sequence']==count-1,'原审计序号/扫描预算不符')
            value=json.loads(encode(raw));h.update((digest(value)+'\n').encode())
            if raw['record_kind']=='source_message':messages.add(json.loads(raw['legacy_json']))
            if record_kind is None or raw['record_kind']==record_kind:
                if start<=selected<start+page_size:items.append(value)
                selected+=1
    require(messages.result()==expected_messages,'审计消息与固定上游消息覆盖不符')
    require(count==total_rows and h.hexdigest()==m['table_digests']['records'],'原审计湖表计数/内容不符')
    require(selected==(m['raw_record_counts'].get(record_kind,0) if record_kind is not None else total_rows),'原审计类别计数不符')
    with binding_api.verify(p,m['components'][0]):pass
    for f in m['files'].values():require(stamp(f['path'])==f['stamp'],'原审计期间文件漂移')
    return {'publication_id':token.publication_id,'component':m['component_key'],'view':'records','total':selected,
            'page':page,'page_size':page_size,'items':items,'availability':'available' if selected else 'known_empty',
            'scanned_rows':count,'verified_source_messages':messages.result(),'storage':'fixed_detection_lake','cost':'每次扫描全部固定records；业务列表/详情/判定不走此路径'}

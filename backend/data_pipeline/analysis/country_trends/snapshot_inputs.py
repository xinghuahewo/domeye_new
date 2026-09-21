"""S2正式strict输入；资格取公开C3/C4与既有来源核查，不能注入fixture许可。"""
from contextlib import contextmanager,ExitStack,closing
from dataclasses import dataclass,replace,asdict
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
import hashlib,json
import psycopg2
from data_pipeline.analysis.country_events.selection_contract import CountryRuntime, contract_json, contract_value
from data_pipeline.analysis.country_events.selection_index import inspect_country
from data_pipeline.analysis.country_events.snapshot_store import database_identity, upstream_binding, cleanup
from data_pipeline.results import country_binding, resource_feature_bindings as bindings
from data_pipeline.results.manifest_io import encode as json_encode, stamp, file_hash
from data_pipeline.analysis.country_trends.contract import ActivityWindow, ReferenceInput, REFERENCE_DEFINITION
from data_pipeline.analysis.country_trends.snapshot_schema import encode, decode, PROFILE

REFERENCE_PROFILE='country-trend-reference-artificial/v1'


@dataclass(frozen=True)
class Runtime:
    private_root: str
    component_dsn: str
    country: CountryRuntime
    observation_dsn: str
    detection_dsn: str
    production_receipt: object
    feature_dsn: str | None = None
    reference_dsn: str | None = None

    def country_port(self,guard):
        return SimpleNamespace(root=Path(self.private_root).resolve(),guard=guard,country=dict(
            runtime=self.country,observation_dsn=self.observation_dsn,detection_dsn=self.detection_dsn,
            production_receipt=self.production_receipt))


def strict_country(descriptor,runtime,guard):
    if runtime.country.c1 is None:raise ValueError('trend_c1_required')
    original=upstream_binding(runtime.country.c1)
    if not original or original['observation']['manifest'].get('interpretation_policy',{}).get('profile','strict')!='strict':
        raise ValueError('trend_strict_country_required')
    actual=inspect_country(descriptor.read_binding,descriptor.admission_proof,runtime=runtime.country)
    if contract_json(actual)!=contract_json(descriptor):raise ValueError('trend_country_descriptor')
    anchor=country_binding.capture(runtime.country_port(guard),descriptor)
    with country_binding.verify(runtime.country_port(guard),anchor):pass
    return anchor


def register_reference(dsn,path,private_root,guard=lambda:None):
    """登记实际保存的人工独立投影原件；不造历史有效性或从Feature推投影。"""
    path=Path(path).resolve()
    if not path.is_relative_to(Path(private_root).resolve()):raise ValueError('trend_reference_root')
    before=stamp(path)
    if path.stat().st_size>8*1024**2:raise ValueError('trend_reference_bytes')
    sha=file_hash(path,guard)
    doc=json.loads(path.read_text())
    if set(doc)!={'profile','version','origin_uri','historical_applicability','references'} or doc['profile']!=REFERENCE_PROFILE or not doc['version'] or not doc['origin_uri'].startswith('fixture://') or doc['historical_applicability']!='unknown':raise ValueError('trend_reference_profile')
    refs=decode(doc['references'])
    if type(refs) is not tuple or not refs:raise ValueError('trend_reference_empty')
    events=set()
    for i,ref in enumerate(refs):
        if type(ref) is not ReferenceInput or ref.binding or ref.definition!=REFERENCE_DEFINITION[0] or ref.event in events:raise ValueError('trend_reference_contract')
        events.add(ref.event);countries=set()
        for j,p in enumerate(ref.projections):
            if p.source_ref!=f'row:{i}:{j}' or not p.cohort_id or p.definition_binding!=REFERENCE_DEFINITION or p.country in countries:raise ValueError('trend_reference_projection_identity')
            countries.add(p.country)
        if ref.target not in countries:raise ValueError('trend_reference_target')
    if stamp(path)!=before:raise ValueError('trend_reference_changed')
    identity={**database_identity(dsn),'profile':REFERENCE_PROFILE,'path':str(path),'sha256':sha,'stamp':before,'version':doc['version'],'origin_uri':doc['origin_uri'],'historical_applicability':'unknown','events':len(refs)}
    binding=json_encode(identity)
    pg=psycopg2.connect(dsn)
    try:
        with pg,pg.cursor() as c:
            c.execute('CREATE SCHEMA IF NOT EXISTS country_trends')
            c.execute('CREATE TABLE IF NOT EXISTS country_trends.references(reference_id TEXT PRIMARY KEY,binding TEXT NOT NULL,state TEXT NOT NULL)')
            c.execute("INSERT INTO country_trends.references VALUES (%s,%s,'complete')",(sha,binding))
    finally:pg.close()
    return identity


@contextmanager
def verify_reference(dsn,binding,*,lock=False,full=False,guard=lambda:None):
    if database_identity(dsn)!={k:binding[k] for k in ('system_id','database_oid')}:raise ValueError('trend_reference_database')
    pg=psycopg2.connect(dsn);error=None
    try:
        pg.set_session(readonly=not lock)
        with pg.cursor() as c:
            c.execute('SELECT binding,state FROM country_trends.references WHERE reference_id=%s'+(' FOR SHARE' if lock else ''),(binding['sha256'],))
            if c.fetchone()!=(json_encode(binding),'complete'):raise ValueError('trend_reference_registration')
        if stamp(binding['path'])!=binding['stamp'] or full and file_hash(binding['path'],guard)!=binding['sha256']:raise ValueError('trend_reference_entity')
        yield
        if stamp(binding['path'])!=binding['stamp']:raise ValueError('trend_reference_tail')
    except BaseException as e:error=e;raise
    finally:cleanup((pg.rollback,pg.close),None if isinstance(error,GeneratorExit) else error)


def reference_context(binding,runtime,guard):
    with verify_reference(runtime.reference_dsn,binding,full=True,guard=guard):
        refs=decode(json.loads(Path(binding['path']).read_text())['references'])
    result=[];sources=[]
    for i,r in enumerate(refs):
        projections=[]
        for j,p in enumerate(r.projections):
            source=f"reference:{binding['sha256']}:{i}:{j}"
            projections.append(replace(p,source_ref=source))
            sources.append((source,'reference',encode(p)))
        result.append(replace(r,binding=(REFERENCE_PROFILE,json_encode(binding)),projections=tuple(projections)))
    return tuple(result),sources


def feature_context(receipt,selections,runtime,guard,budget):
    binding=bindings.inspect(runtime.feature_dsn,'feature',receipt,runtime.private_root,guard)
    spec=binding['report']['specification']
    if spec.get('output_profile') or any(v.get('profile','complete')!='complete' for v in spec.get('source_bindings',[])):
        raise ValueError('trend_feature_m3_rejected')
    names_by_code={};codes_by_name={}
    with closing(bindings.scan(runtime.feature_dsn,'feature',binding['run_id'],binding['snapshot'],'reference_rows',scope='all',batch_rows=budget.limits.max_batch_rows)) as stream:
        for batch in stream:
            for raw in batch.to_pylist():
                budget.charge(encode(raw).encode())
                if raw['selected']:
                    name,code=(json.loads(raw[k]) if raw[k] is not None else None for k in ('legacy_country','legacy_country_code'))
                    if type(name) is not str or type(code) is not str:continue
                    names_by_code.setdefault(code,set()).add(name);codes_by_name.setdefault(name,set()).add(code)
    seen=set();windows=[];sources=[]
    # 选择为(event, country, mode)，只用于实际country范围完整窗，不以file_time猜样本。
    if len(set(selections))!=len(selections) or len(selections)>budget.limits.max_events:raise ValueError('trend_feature_duplicate_selection')
    for event,country,mode in selections:
        if type(event) is not tuple or len(event)!=2 or mode not in ('ordinary','ir'):raise ValueError('trend_feature_selection')
    with closing(bindings.scan(runtime.feature_dsn,'feature',binding['run_id'],binding['snapshot'],'windows',scope='all',batch_rows=budget.limits.max_batch_rows)) as stream:
        for batch in stream:
            for raw in batch.to_pylist():
                budget.charge(encode(raw).encode())
                for event,country,mode in selections:
                    if raw['scope']!='country' or raw['mode']!=mode or raw['country'] not in names_by_code.get(country,set()) or codes_by_name.get(raw['country'])!={country}:continue
                    key=(raw['mode'],raw['source_id'],raw['scope'],raw['subject'],raw['window_role'])
                    if (event,key) in seen:raise ValueError('trend_feature_window_duplicate')
                    seen.add((event,key))
                    source='feature:'+hashlib.sha256(encode((binding['run_id'],binding['snapshot'],key,raw)).encode()).hexdigest()
                    sources.append((source,'feature',encode(raw)))
                    source_spec=next(s for s in spec.get('source_bindings',spec.get('sources',[])) if s['source_id']==raw['source_id']) if spec.get('source_bindings',spec.get('sources')) else None
                    quality='complete' if source_spec and source_spec.get('message_quality_state')=='complete' and source_spec.get('window',{}).get('coverage')=='complete' else 'unknown'
                    def micros(value):
                        dt=datetime.fromisoformat(value)
                        if dt.tzinfo is None:raise ValueError('trend_feature_naive_window')
                        delta=dt.astimezone(timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc)
                        return (delta.days*86400+delta.seconds)*1000000+delta.microseconds
                    for metric,population in (('announ_num','accepted_announce_elements'),('withdraw_num','accepted_withdraw_elements')):
                        window=ActivityWindow(event,mode,metric,population,micros(raw['start']),micros(raw['end']),raw[metric],quality,source,raw['source_rank'],raw['source_id'],raw['window_role'])
                        budget.refs(1);budget.charge(encode(window).encode());windows.append(window)
    binding['entity_stamps']={p:stamp(p) for p in binding['files']}
    return binding,tuple(windows),sources


@contextmanager
def verify_inputs(anchor,runtime,*,lock=False,guard=lambda:None):
    with ExitStack() as stack:
        stack.enter_context(country_binding.verify(runtime.country_port(guard),anchor['country'],lock=lock))
        f=anchor['feature']
        if f is not None:
            pg=psycopg2.connect(runtime.feature_dsn);stack.callback(pg.close);pg.set_session(readonly=not lock)
            c=stack.enter_context(pg.cursor());bindings.verify(c,f,lock=lock)
            for path,old in f['entity_stamps'].items():
                if stamp(path)!=old:raise ValueError('trend_feature_entity')
        if anchor['reference'] is not None:stack.enter_context(verify_reference(runtime.reference_dsn,anchor['reference'],lock=lock,guard=guard))
        yield
        if not lock and f is not None:
            bindings.verify(c,f)
            for path,old in f['entity_stamps'].items():
                if stamp(path)!=old:raise ValueError('trend_feature_tail')

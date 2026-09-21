"""固定 v1 的正文、规则及 M2 对照验证；只读消费既有观察，不解析 MRT。"""
from contextlib import ExitStack
from dataclasses import asdict
import hashlib
import json
from types import SimpleNamespace

from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
from data_pipeline.bgp.ordered_reader import binding_from_reader, ordered
from data_pipeline.bgp.record_types import INTERPRETATION_VERSION, NATIVE_INTERPRETATION_VERSION
from data_pipeline.bgp.replay.snapshot_contract import TABLES, COLUMNS, PROFILE, encode, decode
from data_pipeline.bgp.replay.archive_input import build_mapping
from data_pipeline.bgp.replay.route_replay import ReplayPlan, identity
from data_pipeline.bgp.replay.calculation_window import window_from_plan, calculation_window, WINDOW_RULE

CODEC='canonical-typed-json/v1'
# 支持范围是明确的语义合同，不使用运行时 producer 声明，也不按代码 hash 排除历史实现。
RULES=dict(algorithm='calculation-replay/v1',mapping_rule='full-input-unique-calculation-endpoint/v1',
           ordered_rule='ordered-observation/v1',gap_rule='payload-gap/v1')
INPUT_RULES=dict(profile='observation',schema_version='observation-checkpoint/v1',
                 interpretation_version='mrt-interpretation/v1',ordered_version='ordered-observation/v1')
FIELDS={
    'baseline_mappings':'baseline_source peer_ip peer_asn status rule peer_refs endpoint_evidence input_sources',
    'changes':'event_id object_key source_id message_id position raw',
    'invalidations':'object_key source_id message_id raw position',
    'scope_gap':'gap_id source_id message_id raw',
    'source_coverage':'source_id source_rank raw parse_counts derived_counts declared_window first_observation last_observation inherited_gap_ids execution parse_coverage current_coverage continuity',
    'source_quality':'source_id raw source_rank',
    'current_routes':'object_key scope current last_known last_position qualification',
    'legacy_state':'prefix vp path_text',
    'legacy_prefixes':'prefix paths origins',
    'legacy_seen_vps':'vp',
    'projection_metadata':'plan plan_version legacy_baseline_epoch cursor limits legacy_country_filter',
    'reference_binding':'source_id checkpoint_digest source_sha format rows rows_digest execution',
}


def normalized(value):return json.loads(json.dumps(value))


def validate_rules(plan,*,profile,codec,tables):
    if profile!=PROFILE or codec!=CODEC or not isinstance(tables,(dict,tuple,list)) or set(tables)!=set(TABLES):raise ValueError('不支持的投影 profile/codec/schema')
    required={'input_manifest','input_binding','selected_sources','references','code','baseline_endpoints','limitations','resource_limits',*RULES}
    if isinstance(plan,dict) and 'calculation_window' in plan:
        required |= {'calculation_window','calculation_window_rule'}
    if not isinstance(plan,dict) or set(plan)!=required or any(plan.get(k)!=v for k,v in RULES.items()):raise ValueError('不支持或缺失的投影算法/规则')
    b=plan.get('input_binding')
    if (not isinstance(b,dict) or any(b.get(k)!=v for k,v in INPUT_RULES.items() if k!='interpretation_version') or
            b.get('interpretation_version') not in (INTERPRETATION_VERSION,NATIVE_INTERPRETATION_VERSION)):
        raise ValueError('不支持或缺失的输入 schema/规则')
    if not isinstance(plan['input_manifest'],dict) or plan['input_manifest'].get('schema_version')!='observation-run/v1':raise ValueError('不支持的输入 manifest schema')
    if plan['limitations']!=['cutover_assumed','source_order_declared','session_continuity_unknown']:raise ValueError('投影限制声明与支持规则不符')
    window_from_plan(plan)
    code=plan['code']
    if not isinstance(code,dict) or not code or any(type(k) is not str or type(v) is not str or len(v)!=64 or any(c not in '0123456789abcdef' for c in v) for k,v in code.items()):raise ValueError('代码身份缺失或格式无效')


def validate_row(table,row,*,physical=None,expected=None):
    """同一函数用于 emit、完成、审计、扫描；expected 来自固定 M2 的原算法输出。"""
    if table not in TABLES or type(row) is not dict:raise ValueError('投影正文表/类型无效')
    fields=FIELDS.get(table)
    if table=='qualification_change':
        variant=(row.get('target'),row.get('dimension'))
        fields={('scope','canonical_current_and_continuity'):'gap_id source_id message_id target position status dimension scope',
                ('object','current'):'gap_id object_key source_id message_id target position status dimension overlap',
                ('object','current_only'):'event_id object_key source_id message_id target position status dimension historical_gap_ids'}.get(variant)
        if row.get('status') not in ('known','unknown','not_applicable'):raise ValueError('资格状态枚举无效')
    if table=='source_coverage' and 'window_qualification' in row:
        fields += ' window_qualification'
        qualification=row['window_qualification']
        if (type(qualification) is not dict or set(qualification)!={'rule','role','checked_messages'}
                or qualification['rule']!=WINDOW_RULE
                or qualification['role'] not in ('baseline_complete_cutover_assumed','update_half_open')
                or type(qualification['checked_messages']) is not int or qualification['checked_messages']<0):
            raise ValueError('计算窗覆盖资格字段无效')
        calculation_window(row['declared_window'])
    if table=='projection_metadata' and 'calculation_window' in row:
        fields += ' calculation_window calculation_window_rule'
        if row.get('calculation_window_rule')!=WINDOW_RULE:raise ValueError('计算窗元数据规则无效')
        calculation_window(row['calculation_window'])
    if fields is None or set(row)!=set(fields.split()):raise ValueError('投影正文必需字段/有限 schema 不符: '+table)
    if 'raw' in row and type(row['raw']) is not dict:raise ValueError('原 typed 正文不是对象')
    for key in COLUMNS[1:-1]:
        if key in row and type(row[key]) is not str:raise ValueError('投影引用类型无效: '+key)
    if physical is not None and any(physical[k]!=row.get(k) for k in COLUMNS[1:-1]):raise ValueError('投影物理引用与正文不符: '+table)
    if table in ('changes','invalidations','current_routes'):
        raw=row if table=='current_routes' else row['raw']
        scope=raw.get('scope')
        if type(scope) is not tuple or len(scope)!=11 or type(scope[9]) is not bool or (scope[9] and type(scope[10]) is not int) or (not scope[9] and scope[10] is not None):raise ValueError('投影 typed scope 无效')
        if row['object_key']!=identity(scope):raise ValueError('投影 scope 与 object_key 不符')
        if table!='current_routes' and raw.get('object_key')!=row['object_key']:raise ValueError('原正文对象引用不符')
    if table=='changes':
        raw=row['raw']
        if raw.get('event_id')!=row['event_id'] or row['event_id']!=f"{row['message_id']}:{raw.get('ordinal')}" or row['message_id']!=f"{row['source_id']}:{raw.get('record')}":raise ValueError('原正文事件/来源引用不符')
    if table=='invalidations' and row['raw'].get('source')!=row['message_id']:raise ValueError('失效原来源不符')
    if table=='scope_gap' and (row['raw'].get('gap_id')!=row['gap_id'] or row['raw'].get('rule_version')!=RULES['gap_rule']):raise ValueError('Gap 原引用/规则不符')
    if table=='baseline_mappings' and row['rule']!=RULES['mapping_rule']:raise ValueError('映射原规则不符')
    if table in ('source_coverage','reference_binding') and row['execution']!='complete':raise ValueError('来源执行枚举无效')
    if expected is not None and encode(row)!=encode(expected):raise ValueError('投影正文与绑定 M2 推导不符: '+table)


def reference_rows(reader,binding,guard):
    from data_pipeline.bgp.replay.snapshot_store import closing
    for cp in reader.selection.checkpoints:
        if cp['source_id'] in binding.ordered_source_ids:continue
        hashed=hashlib.sha256();count=0
        with closing(iter(reader.reference_batches(cp['source_id']))) as batches:
            for batch in batches:
                for row in batch.to_pylist():
                    guard();hashed.update(encode(row).encode()+b'\n');count+=1
        reader.selection.check_sources([cp['source_id']])
        if count!=cp['counts']['references']:raise ValueError('参考消费计数不符')
        yield dict(source_id=cp['source_id'],checkpoint_digest=cp['digest'],source_sha=cp['source_sha'],
                   format=cp['format'],rows=count,rows_digest=hashed.hexdigest(),execution='complete')


def expected_rows(reader,plan,guard):
    from data_pipeline.bgp.replay.snapshot_store import closing
    binding=binding_from_reader(reader)
    selected=(reader.manifest['baseline_source'],*reader.manifest['update_sources'])
    if tuple(reader.sources)!=selected or normalized(selected)!=plan['selected_sources'] or normalized(asdict(binding))!=plan['input_binding'] or normalized(reader.manifest)!=plan['input_manifest']:raise ValueError('投影输入计划与真实 M2 不符')
    mappings=[]
    def append(name,row):guard();mappings.append(row)
    sink=SimpleNamespace(dsn=reader.dsn,flush=guard,append=append)
    endpoints=build_mapping(sink,selected[0],selected[1:],reader=reader)
    if normalized(endpoints)!=plan['baseline_endpoints']:raise ValueError('映射计划与真实 M2 不符')
    for row in mappings:yield 'baseline_mappings',row
    references=[]
    for row in reference_rows(reader,binding,guard):
        references.append(row);yield 'reference_binding',row
    if normalized(references)!=plan['references']:raise ValueError('参考计划与真实 M2 不符')
    core=CanonicalReplay(ReplayPlan(binding.collector,selected[0],tuple(selected[1:]),endpoints),binding,selected,
                         window={k:reader.manifest[k] for k in ('window_start','window_end_exclusive')},
                         calculation_window=window_from_plan(plan))
    with closing(iter(ordered(reader))) as flow:
        for item in flow:
            guard()
            for table,row in core.apply(item):guard();yield table,row
    for table,row in core.export():guard();yield table,row


def validate_contents(db,schema,snapshot,reader,plan,guard,*,max_row_bytes=4*1024**2):
    """13 个独立 SQL 游标逐行对照；不保存全表副本，不建立另一状态平台。"""
    from data_pipeline.bgp.replay.snapshot_store import closing, relation
    validate_rules(plan,profile=PROFILE,codec=CODEC,tables=TABLES)
    guard()
    columns=db.execute("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_catalog='lake' AND table_schema=? ORDER BY table_name,ordinal_position",[schema]).fetchall()
    expected_columns=sorted((t,[(k,'BIGINT' if k=='seq' else 'VARCHAR') for k in COLUMNS]) for t in TABLES)
    if columns!=[(t,k,v) for t,cols in expected_columns for k,v in cols]:raise ValueError('投影物理 schema 不支持')
    with ExitStack() as resources:
        cursors={};counts={t:0 for t in TABLES};payload_bytes=0
        def next_row(table):
            nonlocal payload_bytes
            guard()
            if table not in cursors:
                cursor=resources.enter_context(closing(db.cursor()))
                cursor.execute(f'SELECT * FROM {relation(schema,snapshot,table)} ORDER BY seq')
                cursors[table]=cursor
            value=cursors[table].fetchone()
            if value is None:return None
            physical=dict(zip(COLUMNS,value))
            if type(physical['payload']) is not str or len(physical['payload'].encode())>max_row_bytes:raise ValueError('投影验证单行字节资源上限')
            if physical['seq']!=counts[table]:raise ValueError('投影行序不符')
            counts[table]+=1;payload_bytes+=len(physical['payload'].encode())
            return physical
        with closing(expected_rows(reader,plan,guard)) as expected:
            for table,row in expected:
                physical=next_row(table)
                if physical is None:raise ValueError('投影缺少绑定 M2 派生正文: '+table)
                validate_row(table,decode(physical['payload']),physical=physical,expected=row)
        for table in TABLES:
            if next_row(table) is not None:raise ValueError('投影出现额外正文: '+table)
    guard();reader.selection.check()
    binding=binding_from_reader(reader)
    return dict(projection_rows=sum(counts.values()),projection_payload_bytes=payload_bytes,
                selected_m2_messages=sum(s.messages for s in binding.sources if s.source_id in reader.sources),
                selected_m2_elements=sum(s.elements for s in binding.sources if s.source_id in reader.sources),
                reference_rows=sum(r['rows'] for r in plan['references']),
                scope='fixed_m2_typed_reconsumption_no_mrt_decode')

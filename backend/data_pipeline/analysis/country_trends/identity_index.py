"""S3身份的磁盘索引与共享成员页；不读取参考原件或建立独立资格。"""
import hashlib
import json
from itertools import groupby

from data_pipeline.bgp.archive.value_codec import typed, untyped

TABLES = (('feature_reference_interpretation', 'ordinal'),
          ('feature_identity_groups', 'kind,key'), ('feature_identity_pages', 'kind,key,page'))


def install_progress(db, budget):
    from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, install_progress as streaming_progress
    if isinstance(budget, StreamBudget):
        return streaming_progress(db, budget)
    # 兼容未迁移调用者，不能替其取消原累计门禁。
    quantum = min(1000, budget.limits.max_index_steps)
    def progress():
        budget.stats['sqlite_steps'] += quantum
        return int(budget.stats['sqlite_steps'] > budget.limits.max_index_steps)
    db.set_progress_handler(progress, quantum)
    db.execute('PRAGMA cache_size=-2048')
    db.execute('PRAGMA temp_store=FILE')


def initialize(db):
    db.executescript('''
        CREATE TABLE feature_reference_interpretation(
            ordinal INTEGER PRIMARY KEY,payload TEXT,nkey TEXT,ckey TEXT,reason TEXT);
        CREATE TABLE feature_identity_groups(kind TEXT,key TEXT,first_key TEXT,multiple INTEGER,
            bad INTEGER,member_count INTEGER,member_digest TEXT,PRIMARY KEY(kind,key)) WITHOUT ROWID;
        CREATE TABLE feature_identity_pages(kind TEXT,key TEXT,page INTEGER,ordinals TEXT,
            next_page INTEGER,PRIMARY KEY(kind,key,page)) WITHOUT ROWID;
        CREATE INDEX identity_name ON feature_reference_interpretation(nkey,ordinal,ckey,reason) WHERE nkey IS NOT NULL;
        CREATE INDEX identity_code ON feature_reference_interpretation(ckey,ordinal,nkey,reason) WHERE ckey IS NOT NULL;''')


def classify(row):
    name=json.loads(row['legacy_country']) if row['legacy_country'] is not None else None
    code=json.loads(row['legacy_country_code']) if row['legacy_country_code'] is not None else None
    reason = ('not_selected' if row['selected'] is not True else
              'unknown_name' if type(name) is not str or not name or name in ('未知','Unknown','unknown') else
              'unknown_code' if type(code) is not str or len(code)!=2 or not code.isascii() or not code.isalpha() or not code.isupper() else None)
    selected=row['selected'] is True
    return name,code,reason,typed(name) if selected and type(name) is str else None,typed(code) if selected and type(code) is str else None


def save_interpretation(db, ordinal, row, budget):
    name,code,reason,nkey,ckey=classify(row)
    payload=typed(dict(original=row,name=name,code=code))
    # 完整留存计入总量；单个同时存活的解释值仍受上下文限制。
    size=len(payload.encode())+len((nkey or '').encode())+len((ckey or '').encode())
    if size>budget.limits.max_context_bytes:raise ValueError('trend_feature_identity_context_budget')
    budget.charge(typed((ordinal,payload,nkey,ckey,reason)).encode());budget.refs(2)
    db.execute('INSERT INTO feature_reference_interpretation VALUES (?,?,?,?,?)',(ordinal,payload,nkey,ckey,reason))
    budget.stats['feature_reference_interpretation_bytes']+=size


def group_suffix(kind,key):
    return ':identity-group:'+kind+':'+hashlib.sha256(key.encode()).hexdigest()


def page_suffix(kind,key,page):
    return group_suffix(kind,key)+':page:'+str(page)


def page_value(prefix,kind,key,page,ordinals,next_page):
    return dict(group_source_ref=prefix+group_suffix(kind,key),
        member_source_refs=[prefix+':identity:'+str(i) for i in ordinals],
        next_page_ref=prefix+page_suffix(kind,key,next_page) if next_page is not None else None)


def _page_limit(budget):
    return min(budget.limits.max_context_bytes//2,budget.limits.max_batch_bytes,budget.limits.max_row_bytes)


def _save_page(db,kind,key,page,ordinals,next_page,budget):
    # 正式AD标识为64位摘要；sources仍核实际输出字节，夹具不冒充AD。
    value=page_value('feature:'+'0'*64,kind,key,page,ordinals,next_page)
    payload=typed(value).encode()
    if len(payload)>_page_limit(budget):raise ValueError('trend_feature_identity_page_budget')
    budget.charge(payload);budget.refs(len(ordinals)+2)
    db.execute('INSERT INTO feature_identity_pages VALUES (?,?,?,?,?)',
               (kind,key,page,typed(ordinals),next_page))
    budget.stats['identity_pages']+=1
    budget.stats['identity_member_refs']+=len(ordinals)


def build_index(db,budget):
    """两次索引顺序遍历；只保留当前组的常数摘要与一页成员。"""
    for kind,keycol,other in (('name','nkey','ckey'),('code','ckey','nkey')):
        budget.check()
        # 索引从首行写入即维护，避免一次CREATE INDEX引入未计量的外部排序临时文件。
        cursor=db.execute('SELECT '+keycol+',ordinal,'+other+',reason FROM '
            'feature_reference_interpretation INDEXED BY identity_'+kind+' WHERE '+keycol+' IS NOT NULL ORDER BY '+keycol+',ordinal')
        for key,items in groupby(cursor,lambda r:r[0]):
            first=None;multiple=False;bad=False;count=0;h=hashlib.sha256();page=0;pending=[]
            # 为next_page保留空间，追加成员仅做恒定字节计算，不反复编码整页。
            overhead=len(typed(page_value('feature:'+'0'*64,kind,key,page,[],page+1)).encode())
            bytes_used=overhead
            for _,ordinal,counterpart,reason in items:
                budget.check();budget.stats['identity_index_rows']+=1
                if reason is None:
                    if first is None:first=counterpart
                    elif first!=counterpart:multiple=True
                else:bad=True
                member_bytes=len(typed('feature:'+'0'*64+':identity:'+str(ordinal)).encode())+1
                if pending and (len(pending)>=min(256,budget.limits.max_batch_rows)
                                or bytes_used+member_bytes>_page_limit(budget)):
                    _save_page(db,kind,key,page,pending,page+1,budget)
                    page+=1;pending=[]
                    overhead=len(typed(page_value('feature:'+'0'*64,kind,key,page,[],page+1)).encode())
                    bytes_used=overhead
                if bytes_used+member_bytes>_page_limit(budget):raise ValueError('trend_feature_identity_page_budget')
                pending.append(ordinal);bytes_used+=member_bytes;count+=1
                h.update(bytes.fromhex(hashlib.sha256(typed(ordinal).encode()).hexdigest()))
            _save_page(db,kind,key,page,pending,None,budget)
            value=(kind,key,first,int(multiple),int(bad),count,h.hexdigest())
            budget.charge(typed(value).encode());budget.refs(3)
            db.execute('INSERT INTO feature_identity_groups VALUES (?,?,?,?,?,?,?)',value)
    budget.check()


def lookup(db,name,budget):
    if type(name) is not str:return None
    budget.step();budget.stats['identity_lookup_count']+=1
    # 唯一索引点查；不访问或解码任何原解释行，也不取成员页。
    row=db.execute('''SELECT n.first_key,n.multiple,n.bad,c.multiple,c.bad
        FROM feature_identity_groups n LEFT JOIN feature_identity_groups c
        ON c.kind='code' AND c.key=n.first_key WHERE n.kind='name' AND n.key=?''',(typed(name),)).fetchone()
    if row is None or row[0] is None or any(row[i] is None or row[i] for i in (1,2,3,4)):return None
    if len(typed(row).encode())>budget.limits.max_context_bytes:raise ValueError('trend_feature_identity_lookup_budget')
    return untyped(row[0])


def interpretations(db,budget):
    """原ordinal流及相同reason优先级；名称/代码摘要由索引连接，非逐窗解码。"""
    cursor=db.execute('''SELECT i.ordinal,i.payload,i.reason,n.bad,c.bad,n.multiple,c.multiple
        FROM feature_reference_interpretation i
        LEFT JOIN feature_identity_groups n ON n.kind='name' AND n.key=i.nkey
        LEFT JOIN feature_identity_groups c ON c.kind='code' AND c.key=i.ckey ORDER BY i.ordinal''')
    for ordinal,payload,reason,nb,cb,nm,cm in cursor:
        budget.check();value=untyped(payload);budget.stats['identity_source_decodes']+=1
        if reason is None:
            if None in (nb,cb,nm,cm):raise ValueError('trend_feature_identity_missing_group')
            reason='unknown_counterpart' if nb or cb else 'ambiguous_name' if nm else 'ambiguous_code' if cm else None
        yield ordinal,dict(value,state='usable' if reason is None else 'insufficient',reason=reason)


def evidence(db,prefix,budget):
    for kind,key,first,multiple,bad,count,digest,cm,cb in db.execute("SELECT g.*,c.multiple,c.bad FROM feature_identity_groups g LEFT JOIN feature_identity_groups c ON g.kind='name' AND c.kind='code' AND c.key=g.first_key ORDER BY g.kind,g.key"):
        budget.check()
        value=dict(kind=kind,value=untyped(key),first_counterpart=untyped(first) if first is not None else None,
            multiple=bool(multiple),bad=bool(bad),member_count=count,member_digest=digest,
            first_page_ref=prefix+page_suffix(kind,key,0),binding_source_ref=prefix+':identity-binding')
        usable=kind=='name' and first is not None and not multiple and not bad and cm==0 and cb==0
        value['usable_name_mapping']=usable
        if usable:value['code_group_ref']=prefix+group_suffix('code',first)
        if len(typed(value).encode())>budget.limits.max_context_bytes:raise ValueError('trend_feature_identity_group_budget')
        yield prefix+group_suffix(kind,key),value
    for kind,key,page,payload,next_page in db.execute('SELECT * FROM feature_identity_pages ORDER BY kind,key,page'):
        budget.check();value=page_value(prefix,kind,key,page,untyped(payload),next_page)
        if len(typed(value).encode())>_page_limit(budget):raise ValueError('trend_feature_identity_page_budget')
        yield prefix+page_suffix(kind,key,page),value

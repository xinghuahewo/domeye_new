"""Q3-C.1 固定结构合同；不授予业务查询资格。"""
from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
import time

import pyarrow as pa

from data_pipeline.history.database_import.freeze import Guard, Limits
from data_pipeline.history.database_import.codec import canonical

PROFILE_RULE = 'history-json-profile/v1'
FREEZE_RULE = 'history-collection-freeze/v1'
PROFILES = ('core-index/v1', 'general-read-model/v1')


@dataclass(frozen=True)
class CollectionLimits:
    chunk_bytes: int = 65536
    metadata_bytes: int = 4*1024**2
    line_bytes: int = 4*1024**2
    token_bytes: int = 1024**2
    depth: int = 64
    number_digits: int = 4096
    exponent: int = 100000
    document_nodes: int = 1000000
    document_bytes: int = 64*1024**2
    files: int = 1024
    edges: int = 4096
    nodes: int = 10000000
    file_bytes: int = 512*1024**2
    decoded_bytes: int = 2*1024**3
    temporary_bytes: int = 4*1024**3

    def __post_init__(self):
        if any(type(v) is not int or v < 1 for v in asdict(self).values()):
            raise ValueError('集合预算必须为正整数')


@dataclass(frozen=True)
class Root:
    profile: str
    path: str
    origin_uri: str
    source_version: str
    sha256: str


@dataclass(frozen=True)
class Binding:
    roots: tuple
    allowed_roots: tuple
    external_files: tuple = ()  # (原URI, 本地路径, SHA256)；不使用环境发现。
    closed_immutable: bool = True
    data_kind: str = 'fixture'


@dataclass(frozen=True)
class CollectionToken:
    collection_id: str
    manifest_sha256: str
    profile_sha256: str
    snapshot: int
    ready_sha256: str
    children: tuple


# 独立原生列；长JSON不作为单列权威文档。原字节由span定位原/解码实体。
DEFINITIONS = {
    'files': [('file_id','i'),('root_id','i'),('uri','s'),('role','s'),('profile','s'),('version','s'),
              ('raw_path','s'),('raw_sha','s'),('raw_bytes','i'),('decoded_path','s'),('decoded_sha','s'),('decoded_bytes','i'),('members','i'),('child_index','i')],
    'documents': [('document_id','i'),('file_id','i'),('document_ordinal','i'),('line','i'),('newline','s'),
                  ('byte_start','i'),('byte_end','i'),('node_count','i'),('table_name','s'),('row_ordinal','i'),('column_name','s'),('storage_class','s'),('entity_path','s')],
    'nodes': [('document_id','i'),('node_ordinal','i'),('parent_ordinal','i'),('member_ordinal','i'),('kind','s'),
              ('key','s'),('key_start','i'),('key_end','i'),('byte_start','i'),('byte_end','i'),('child_count','i'),
              ('text_value','s'),('bool_value','b'),('number_lexeme','s'),('sign','i'),('coefficient_digits','s'),('exponent10','i'),('negative_zero','b'),('int64_value','i'),('decimal128_value','d')],
    'edges': [('edge_ordinal','i'),('parent_file','i'),('document_id','i'),('node_ordinal','i'),('reference','s'),('role','s'),('relation_kind','s'),('resolution','s'),('target_file','i'),('expected_sha','s')],
    'identities': [('identity_ordinal','i'),('file_id','i'),('document_id','i'),('node_ordinal','i'),('name','s'),('value','s')],
    'availability': [('scope_ordinal','i'),('root_id','i'),('scope','s'),('state','s'),('document_id','i'),('node_ordinal','i')],
}
ORDER = {'files':'file_id','documents':'document_id','nodes':'document_id,node_ordinal','edges':'edge_ordinal','identities':'identity_ordinal','availability':'scope_ordinal'}
TYPES = {'i':pa.int64(),'s':pa.string(),'b':pa.bool_(),'d':pa.decimal128(38,18)}
SCHEMAS = {name:pa.schema([(col,TYPES[kind]) for col,kind in columns]) for name,columns in DEFINITIONS.items()}


def code_identity():
    from data_pipeline.history.database_import.reader import code_identity as old
    return {**{'q3/'+k:v for k,v in old().items()}, **{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(__file__).parent.glob('*.py'))}}


def profile_hash():
    return hashlib.sha256(canonical({'profiles':PROFILES,'rule':PROFILE_RULE,'schema':DEFINITIONS,'code':code_identity()})).hexdigest()


class Budget:
    def __init__(self, root, limits=Limits(), collection_limits=CollectionLimits(), check_binding=None):
        self.guard = Guard(root, limits)
        self.limits, self.collection_limits = limits, collection_limits
        self.frozen = (asdict(limits), asdict(collection_limits))
        self.check_binding = check_binding
        self.start = time.monotonic()
        self.counts = dict(raw_bytes=0, decoded_bytes=0, nodes=0, temporary_bytes=0, read_blocks=0,
                           hash_calls=0, hash_bytes=0, arrow_batches=0, typed_rows=0, typed_bytes=0, sql_calls=0)

    def check(self):
        if self.frozen != (asdict(self.limits),asdict(self.collection_limits)) or (self.check_binding and not self.check_binding()):
            raise ValueError('集合预算漂移')
        self.guard.check()

    def add(self, key, count, maximum=None):
        self.check()
        total = self.counts.get(key,0)+count
        if maximum is not None and total > maximum: raise ValueError('集合累计预算超限: '+key)
        self.counts[key] = total

    def hash(self, path):
        self.check(); self.add('hash_calls',1)
        h=hashlib.sha256()
        with open(path,'rb') as f:
            while True:
                self.check(); block=f.read(self.collection_limits.chunk_bytes)
                self.add('read_blocks',1)
                if not block: break
                self.add('hash_bytes',len(block)); h.update(block)
        return h.hexdigest()

    def report(self):
        import subprocess
        import os
        current=int(subprocess.check_output(['ps','-o','rss=','-p',str(os.getpid())],text=True).strip())*1024
        return {**self.counts, 'current_process_rss_bytes':current, 'elapsed_seconds':time.monotonic()-self.start,
                'memory_scope':'当前Python进程生命周期ru_maxrss；不含独立PostgreSQL进程',
                'process_lifetime_peak_rss_bytes':self.guard.peak_rss}

"""有限人工H2的固定来源、原生领域列与规则身份。"""
from dataclasses import dataclass
import hashlib
from pathlib import Path
import pyarrow as pa
from data_pipeline.history.event_collection.model import CollectionToken, code_identity as collection_code
from data_pipeline.history.event_index.model import code_identity as core_code
from data_pipeline.history.database_import.codec import canonical

RULE = 'history-rib-domain/v1'
@dataclass(frozen=True)
class RibToken:
    profile_id: str
    collection: CollectionToken
    rule_sha256: str
    snapshot: int
    ready_sha256: str

D = {
 'documents': 'document_id:i file_id:i root_id:i role:s entity_path:s byte_start:i byte_end:i',
 'scale_family': 'file_id:i family:s visible_prefixes:i rib_entries:i visible_origin_ases:i prefix_set_sha256:s observed_at:s',
 'origin_family': 'file_id:i family:s visible_prefixes:i rib_entries:i visible_origin_ases:i unattributed_entries:i prefix_set_sha256:s observed_at:s',
 'origin_members': 'file_id:i family:s member_ordinal:i asn:i',
 'peers': 'file_id:i peer_ordinal:i bgp_id:s ip:s asn:i',
 'origin_paths': 'file_id:i occurrence:i path_key:x as4_key:x raw_origin_asn:i attributed_origin_asn:i reason:s ipv4_count:i ipv6_count:i physical_record:i decoded_offset:i entry_index:i peer_index:i originated_time_epoch:i family:s',
 'path_endpoint_metrics': 'file_id:i family:s same:i different:i left_only:i right_only:i not_comparable:i comparable_pairs:i different_fraction:s interval_change_count:i session_continuity:s',
 'comparison_objects': 'file_id:i document_id:i line_ordinal:i object_ordinal:i afi:i safi:i prefix:s group_id:i status:s',
 'comparison_frames': 'file_id:i document_id:i side:i frame_position:i record:i offset:i subtype:i mrt_file:i',
 'comparison_refs': 'file_id:i document_id:i object_ordinal:i side:i ref_ordinal:i frame_position:i entry_index:i mrt_file:i record:i decoded_offset:i peer_index:i resolution:s',
 'comparison_reasons': 'file_id:i document_id:i object_ordinal:i reason_ordinal:i reason:s',
 'peer_groups': 'file_id:i group_id:i bgp_id:s ip:s asn:i',
 'peer_group_members': 'file_id:i group_id:i side:i member_ordinal:i peer_index:i',
 'mrt_frames': 'file_id:i entity_path:s record:i decoded_offset:i body_bytes:i epoch:i subtype:i afi:i safi:i prefix:s',
 'mrt_observations': 'file_id:i record:i entry_index:i decoded_offset:i epoch:i subtype:i afi:i safi:i prefix:s peer_index:i bgp_id:s ip:s asn:i originated_time_epoch:i path_id:i as_path:x as4_path:x canonical_path:s comparison_reasons:s raw_origin_asn:i attributed_origin_asn:i origin_reason:s',
}
DEFINITIONS = {name: [tuple(col.split(':')) for col in fields.split()] for name, fields in D.items()}
SCHEMAS = {name: pa.schema([(c, {'i':pa.int64(), 's':pa.string(), 'x':pa.binary()}[t]) for c,t in columns]) for name,columns in DEFINITIONS.items()}
TABLES = tuple(DEFINITIONS)
ORDER = {name: ','.join(c for c,t in cols if t=='i') for name,cols in DEFINITIONS.items()}
# 完整排序键均为原定位，不用可能相同的业务值折叠occurrence。
ORDER.update(scale_family='file_id,family',origin_family='file_id,family',origin_members='file_id,family,member_ordinal',
 path_endpoint_metrics='file_id,family',comparison_objects='file_id,line_ordinal,object_ordinal',
 comparison_frames='file_id,document_id,side,frame_position',comparison_refs='file_id,document_id,object_ordinal,side,ref_ordinal',
 comparison_reasons='file_id,document_id,object_ordinal,reason_ordinal',peers='file_id,peer_ordinal',
 peer_groups='file_id,group_id',peer_group_members='file_id,group_id,side,member_ordinal',
 origin_paths='file_id,occurrence',mrt_frames='file_id,record',mrt_observations='file_id,record,entry_index')

def code_identity():
    parent=Path(__file__).resolve().parents[2]
    paths=[*Path(__file__).parent.glob('*.py'),*(parent/p for p in ('bgp/snapshots/origin.py','bgp/snapshots/path_comparison.py','overview/scale.py','overview/paths.py'))]
    return {**{'core/'+k:v for k,v in core_code().items()},**{str(p.relative_to(parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}

def rule_sha():
    return hashlib.sha256(canonical({'rule':RULE,'schema':D,'code':code_identity()})).hexdigest()

def row_bytes(row):
    def encode(value):
        if isinstance(value, bytes): return {'blob_hex': value.hex()}
        if isinstance(value, dict): return {k:encode(v) for k,v in value.items()}
        if isinstance(value, (list, tuple)): return [encode(v) for v in value]
        return value
    return canonical(encode(row))


def freeze_identity():
    # 冻结角色仅依赖该适配器和不变C.1；查询修复不强迫重冻原MRT。
    path=Path(__file__).with_name('freeze.py')
    return {**{'collection/'+k:v for k,v in collection_code().items()}, 'history/rib_index/freeze.py':hashlib.sha256(path.read_bytes()).hexdigest()}


def valid_freeze_identity(identity):
    current = freeze_identity()
    # 8999969冻结器仅过早误拒合法child；此修复不更改原生角色、结构或准入范围。
    # 只兼容这一精确旧实现，全部C.1依赖仍须与当前一致；领域准入必须重新完整核验。
    legacy = {**current, 'history/rib_index/freeze.py': '7ce7c5fe729c73e5ed78ba307e5bba78698423d1fe13c5c3cfbcb23c71942b9d'}
    return identity == current or identity == legacy

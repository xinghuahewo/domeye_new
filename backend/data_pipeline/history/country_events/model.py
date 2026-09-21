"""H5有限General领域合同；原身份与新载体身份分开。"""
from dataclasses import dataclass
from pathlib import Path
import hashlib
import pyarrow as pa
from data_pipeline.history.event_collection.model import CollectionToken, DEFINITIONS as STRUCTURE
from data_pipeline.history.event_index.model import code_identity as core_code
from data_pipeline.history.database_import.codec import canonical
from decimal import Decimal

RULE = 'history-general-domain/v1'
@dataclass(frozen=True)
class GeneralToken:
    profile_id: str
    collection: CollectionToken
    rule_sha256: str
    snapshot: int
    ready_sha256: str

AS_FIELDS = 'rank asn as_name organization nature name_state organization_state nature_state event_classification fixed_prefix_count peak_partial_prefix_count peak_complete_prefix_count peak_invisible_direction_count path_downstream_asn_count concurrent_downstream_asn_count'.split()
REL_FIELDS = 'affected_asn downstream_asn downstream_as_name downstream_organization downstream_nature downstream_name_state downstream_organization_state downstream_nature_state observed_path_count associated_fixed_prefix_count independent_direction_count route_observation_count concurrent_state_point_count first_concurrent_state_point_utc last_concurrent_state_point_utc peak_concurrent_interrupted_prefix_count peak_concurrent_ipv4_address_count peak_concurrent_ipv6_slash48_count relationship_semantics'.split()
EVENT_FIELDS = 'event_read_model_id publication_id revision publication_state incident_id legacy_reference country_code cohort_id event_metric_id event_as_path_id window_start_utc window_end_utc data_through lifecycle_state is_final_in_data_range state_point_count affected_as_count path_downstream_relation_count path_sample_count'.split()
IDENTITIES = 'incident_id publication_id revision event_read_model_id cohort_id event_metric_id event_as_path_id legacy_reference country_code window_start_utc window_end_utc data_through lifecycle_state is_final_in_data_range'.split()
# 数值/缺值权威列在scalar_fields及points；筛选列只接受可精确int64的ASN/rank。
D = {
 'documents': ' '.join(c+':'+t for c,t in STRUCTURE['documents']),
 'general_stores': 'store_id:i file_id:i document_id:i dataset_id:s run_id:s implementation_id:s manifest_sha256:s content_sha256:s admission:s',
 'general_events': 'event_id:i store_id:i event_ordinal:i document_id:i node_ordinal:i incident_id:s publication_id:s revision:i event_read_model_id:s legacy_reference:s canonical_reference:s country_code:s cohort_id:s event_metric_id:s event_as_path_id:s lifecycle_state:s admission:s',
 'event_files': 'event_id:i role:s file_id:i document_id:i row_ordinal:i',
 'scalar_fields': 'event_id:i document_id:i node_ordinal:i owner:s owner_ordinal:i member_ordinal:i name:s presence:s kind:s text_value:s bool_value:i number_lexeme:s sign:i coefficient_digits:s exponent10:i negative_zero:i',
 'overview_metrics': 'event_id:i section:s metric_ordinal:i metric:s document_id:i node_ordinal:i value_node:i peak_time_node:i',
 'track_definitions': 'event_id:i track_ordinal:i track:s document_id:i node_ordinal:i definition_node:i presence:s',
 'series_points': 'event_id:i track_ordinal:i point_index:i document_id:i timestamp_node:i value_node:i timestamp_text:s timestamp_utc:s kind:s text_value:s bool_value:i number_lexeme:s sign:i coefficient_digits:s exponent10:i negative_zero:i presence:s',
 'affected_as': 'event_id:i row_ordinal:i document_id:i asn:i rank:i event_classification:s search:s',
 'path_relations': 'event_id:i relation_ordinal:i document_id:i affected_asn:i downstream_asn:i concurrent_state_point_count:i search:s',
 'path_samples': 'event_id:i relation_ordinal:i sample_ordinal:i document_id:i node_ordinal:i prefix:s address_family:s as_path_id:s as_path_canonical:s',
 'sample_peer_members': 'event_id:i relation_ordinal:i sample_ordinal:i member_ordinal:i document_id:i node_ordinal:i asn:i',
 'identity_references': 'event_id:i reference_ordinal:i document_id:i node_ordinal:i name:s actual:s expected:s resolution:s target_event:i target_store:i relation_kind:s',
}
DEFINITIONS={name:[tuple(c.split(':')) for c in fields.split()] for name,fields in D.items()}
SCHEMAS={name:pa.schema([(c,{'i':pa.int64(),'s':pa.string()}[t]) for c,t in fields]) for name,fields in DEFINITIONS.items()}
TABLES=tuple(D)
ORDER=dict(documents='document_id',general_stores='store_id',general_events='event_id',event_files='event_id,role,row_ordinal',scalar_fields='event_id,document_id,node_ordinal,owner,owner_ordinal,member_ordinal,name',overview_metrics='event_id,section,metric_ordinal',track_definitions='event_id,track_ordinal',series_points='event_id,track_ordinal,point_index',affected_as='event_id,row_ordinal',path_relations='event_id,relation_ordinal',path_samples='event_id,relation_ordinal,sample_ordinal',sample_peer_members='event_id,relation_ordinal,sample_ordinal,member_ordinal',identity_references='event_id,reference_ordinal')

def code_identity():
    parent=Path(__file__).resolve().parents[3]
    paths=[*Path(__file__).parent.glob('*.py'),parent/'services/country_outage_general_read_model.py']
    return {**{'core/'+k:v for k,v in core_code().items()},**{str(p.relative_to(parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}

def rule_sha():return hashlib.sha256(row_bytes({'rule':RULE,'schema':D,'code':code_identity()})).hexdigest()

def require(value,message):
    if not value:raise ValueError(message)


def row_bytes(value):
    def encode(v):
        if isinstance(v,bytes):return {'blob_hex':v.hex()}
        if isinstance(v,Decimal):return {'exact_decimal':str(v)}
        if isinstance(v,dict):return {k:encode(x) for k,x in v.items()}
        if isinstance(v,(list,tuple)):return [encode(x) for x in v]
        return v
    return canonical(encode(value))

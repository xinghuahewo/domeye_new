"""人工H1 profile公开固定合同；独立于C.1结构审计与旧Token。"""
from dataclasses import dataclass
import hashlib
from pathlib import Path

import pyarrow as pa

from data_pipeline.history.event_collection.model import CollectionToken, code_identity as collection_code
from data_pipeline.history.database_import.codec import canonical

RULE = 'history-core-domain/v1'


@dataclass(frozen=True)
class CoreToken:
    profile_id: str
    collection: CollectionToken
    root_id: int
    rule_sha256: str
    snapshot: int
    ready_sha256: str


DEFINITIONS = {
    'core_days': [('day', 's'), ('state', 's'), ('root_document', 'i'), ('document_id', 'i'), ('file_id', 'i'),
                  ('entity_path', 's'), ('byte_start', 'i'), ('byte_end', 'i'), ('input_version', 's'),
                  ('root_interpretation', 's'), ('input_interpretation', 's'), ('window_start', 's'),
                  ('window_end', 's'), ('record_count', 'i')],
    'core_diagnostics': [('day', 's'), ('reason_ordinal', 'i'), ('stage', 's'), ('kind', 's'),
                         ('code', 's'), ('count', 'i'), ('document_id', 'i')],
    'core_records': [('day', 's'), ('occurrence', 'i'), ('file_id', 'i'), ('source_ordinal', 'i'),
                     ('reference', 's'), ('kind', 's'), ('object', 's'), ('start_time', 's'), ('hour', 'i'),
                     ('family', 's'), ('level', 's'), ('severity', 'i'), ('search', 's'), ('content_version', 's'),
                     ('item_document', 'i'), ('item_path', 's'), ('item_start', 'i'), ('item_end', 'i'), ('item_storage', 's'),
                     ('payload_document', 'i'), ('payload_path', 's'), ('payload_start', 'i'), ('payload_end', 'i'), ('payload_storage', 's')],
    'core_scalars': [('occurrence', 'i'), ('scalar_ordinal', 'i'), ('document_id', 'i'), ('member_path', 's'),
                     ('scalar_kind', 's'), ('original_text', 's'), ('utc_time', 's'), ('precision', 's'),
                     ('sign', 'i'), ('coefficient_digits', 's'), ('exponent10', 'i'), ('negative_zero', 'i'),
                     ('integer_value', 'i'), ('utc_microseconds', 'i'), ('interpreted_timezone', 's')],
    'core_links': [('link_ordinal', 'i'), ('occurrence', 'i'), ('document_id', 'i'), ('reference', 's'),
                   ('role', 's'), ('resolution', 's'), ('target_file', 'i')],
}
ORDER = {'core_days': 'day', 'core_diagnostics': 'day,reason_ordinal', 'core_records': 'occurrence',
         'core_scalars': 'occurrence,scalar_ordinal', 'core_links': 'link_ordinal'}
SCHEMAS = {name: pa.schema([(c, pa.int64() if t == 'i' else pa.string()) for c, t in cols])
           for name, cols in DEFINITIONS.items()}
TABLES = tuple(DEFINITIONS)


def code_identity():
    parent = Path(__file__).resolve().parents[2]
    logic = ['common/event_records.py', 'overview/input.py', 'overview/index.py', 'overview/diagnostics.py']
    files = [*Path(__file__).parent.glob('*.py'), *(parent / p for p in logic), parent.parents[1] / 'config/data-profile.json']
    return {**{'collection/' + k: v for k, v in collection_code().items()},
            **{str(p.relative_to(parent.parents[1])): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}}


def rule_sha():
    return hashlib.sha256(canonical({'rule': RULE, 'schema': DEFINITIONS, 'code': code_identity()})).hexdigest()

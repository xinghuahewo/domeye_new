"""只经已固定 Country P1 捕获完整公开输入；不装载私表或重放科学来源。"""
import hashlib

from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as original, qualified_schema as m3_schema
from data_pipeline.analysis.country_events import result_admission as country
from data_pipeline.analysis.country_events.selection_contract import contract_value
from data_pipeline.bgp.archive.value_codec import fields, digest


def initialize(db):
    db.executescript('''CREATE TABLE country_input(sequence INTEGER PRIMARY KEY,table_name TEXT,
        incident TEXT,revision INTEGER,payload TEXT,bytes INTEGER);
        CREATE INDEX country_event ON country_input(incident,revision,sequence);
        CREATE TABLE country_events(incident TEXT PRIMARY KEY,revision INTEGER,sequence INTEGER,payload TEXT);
        CREATE INDEX country_event_sequence ON country_events(sequence);
        CREATE TABLE country_reads(view TEXT,table_name TEXT,request TEXT,receipt TEXT,
                                   PRIMARY KEY(view,table_name));''')


def save_source(db, wrapped, budget):
    fields(wrapped, {'table', 'row'})
    table, raw = wrapped['table'], wrapped['row']
    item = m3_schema.row_decode(table, raw)
    payload = original.encode(wrapped)
    budget.charge(payload.encode())
    event = (item.incident_id, item.revision) if type(item) is c2.C2Row else (None, None)
    db.execute('INSERT INTO country_input VALUES (?,?,?,?,?,?)',
               (raw['_sequence'], table, *event, payload, len(payload.encode())))


def save_event(db, envelope, budget):
    fields(envelope, {'raw', 'main', 'lifecycle', 'original_revisions', 'qualifications'})
    raw = envelope['raw']
    if raw['table'] != 'event_status': raise ValueError('trend_country_event_type')
    item = m3_schema.row_decode(raw['table'], raw['row'])
    if envelope['main'] is not None and original.encode(envelope['main']) != original.encode(raw):
        raise ValueError('trend_country_event_main_changed')
    payload = original.encode(envelope); budget.charge(payload.encode())
    db.execute('INSERT INTO country_events VALUES (?,?,?,?)',
               (item.incident_id, item.revision, raw['row']['_sequence'], payload))


def validate_events(db, budget):
    """公开事件信封的所有原引用必须等于同次公开捕获的原行。"""
    for incident, revision, seq, payload in db.execute('SELECT * FROM country_events'):
        envelope = original.decode(payload)
        for wrapped in (envelope['raw'], *envelope['original_revisions'], *envelope['qualifications']):
            budget.check()
            fields(wrapped, {'table', 'row'})
            row = m3_schema.row_decode(wrapped['table'], wrapped['row'])
            source = db.execute('SELECT payload FROM country_input WHERE sequence=?',
                                (wrapped['row']['_sequence'],)).fetchone()
            if (source is None or source[0] != original.encode(wrapped)
                    or row.incident_id != incident):
                raise ValueError('trend_country_selected_event_source')
        if envelope['raw']['row']['_sequence'] != seq:
            raise ValueError('trend_country_selected_event_sequence')
        for wrapped in envelope['original_revisions']:
            history = m3_schema.row_decode(wrapped['table'], wrapped['row'])
            if type(history.value) is not c2.InputEvidence or history.value.kind != 'event_revision':
                raise ValueError('trend_country_original_revision_type')
        for wrapped in envelope['qualifications']:
            q = m3_schema.row_decode(wrapped['table'], wrapped['row'])
            if (wrapped['table'] != 'country_qualification' or q.revision != revision
                    or q.value.dimension not in ('event_anchor', 'event_lifecycle')):
                raise ValueError('trend_country_selected_event_qualification')


def capture_country(db, admission, runtime, window_us, budget):
    """每个有限view耗尽、关闭并核回执后保存；绝不替调用者admit。

    21原表全前史+3类资格/覆盖+events共25个公开流。所有行及events中
    原revision/lifecycle原文保存，序号不按表或结果D重排。
    """
    country.verify_current(runtime, admission, guard=budget.check)
    binding = country.untyped(admission['owner_binding'])
    proof = contract_value(binding['proof'])
    read_binding = contract_value(binding['read_binding'])
    requests = [('raw', table) for table in original.TABLES]
    requests += [(name, None) for name in ('country_qualification', 'country_qualified_value', 'country_coverage', 'events')]
    for view, table in requests:
        budget.check()
        scope = dict(window_us=window_us, dimension=None, incident_id=None, revision=None,
                     table=table, after_sequence=-1, stop_sequence=None)
        request = dict(view=view, scope_typed=original.encode(scope), codec_version=country.CODEC,
                       batch_rows=min(budget.limits.max_batch_rows, runtime.limits.batch_rows),
                       batch_bytes=min(budget.limits.max_batch_bytes, runtime.limits.batch_bytes))
        count = 0; hasher = hashlib.sha256()
        with country.open_reader(runtime, admission, request, guard=budget.check) as session:
            for batch in session:
                fields(batch, {'rows_typed', 'codec_version', 'rows', 'bytes'})
                if (batch['codec_version'] != country.CODEC or type(batch['bytes']) is not int
                        or type(batch['rows']) is not int or not 0 < batch['rows'] <= request['batch_rows']
                        or batch['bytes'] != len(batch['rows_typed'].encode())
                        or batch['bytes'] > request['batch_bytes']):
                    raise ValueError('trend_country_batch')
                rows = original.decode(batch['rows_typed'])
                if type(rows) is not tuple or len(rows) != batch['rows']:
                    raise ValueError('trend_country_batch_rows')
                for wrapped in rows:
                    payload = original.encode(wrapped).encode()
                    hasher.update(len(payload).to_bytes(8, 'big')); hasher.update(payload); count += 1
                    if view == 'events': save_event(db, wrapped, budget)
                    else:
                        if wrapped['table'] != (table or view): raise ValueError('trend_country_wrong_view')
                        save_source(db, wrapped, budget)
        receipt = session.receipt
        fields(receipt, {'contract', 'admission_id', 'request_digest', 'rows', 'typed_digest', 'execution', 'coverage_ref'})
        if (receipt['contract'], receipt['admission_id'], receipt['request_digest'], receipt['rows'],
            receipt['typed_digest'], receipt['execution']) != ('component-publication-read/v1', admission['admission_id'],
                digest(request), count, hasher.hexdigest(), 'complete'):
            raise ValueError('trend_country_incomplete_public_read')
        coverage = original.decode(receipt['coverage_ref'])
        if (coverage['view'] != view or original.encode(coverage['scope']) != original.encode(scope)
                or coverage['result_id'] != read_binding.result_id
                or coverage['read_model_id'] != read_binding.read_model_id):
            raise ValueError('trend_country_receipt_scope')
        text = original.encode(receipt); budget.charge(text.encode())
        db.execute('INSERT INTO country_reads VALUES (?,?,?,?)',
                   (view, table or '', original.encode(request), text))
    count, first, last = db.execute('SELECT count(*),min(sequence),max(sequence) FROM country_input').fetchone()
    if count != proof.source_rows or count and (first, last) != (0, count - 1):
        raise ValueError('trend_country_complete_original_sequence')
    validate_events(db, budget)
    country.verify_current(runtime, admission, guard=budget.check)
    db.commit()
    return dict(owner_binding=binding, admission_id=admission['admission_id'], source_rows=count,
                public_reads=len(requests), events=db.execute('SELECT count(*) FROM country_events').fetchone()[0],
                window_us=window_us)


def decoded_rows(db, *, event=None, tables=None):
    clauses, args = [], []
    if event is not None:
        clauses += ['incident=?', 'revision=?']; args.extend(event)
    if tables is not None:
        if not tables or any(t not in m3_schema.TABLES for t in tables):
            raise ValueError('trend_country_finite_tables')
        clauses.append('table_name IN (' + ','.join('?' for _ in tables) + ')'); args.extend(tables)
    where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
    for sequence, payload in db.execute('SELECT sequence,payload FROM country_input' + where + ' ORDER BY sequence', args):
        wrapped = original.decode(payload)
        yield sequence, m3_schema.row_decode(wrapped['table'], wrapped['row'])

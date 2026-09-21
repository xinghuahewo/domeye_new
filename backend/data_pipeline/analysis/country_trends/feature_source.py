"""可选Feature活动只走已接受P1 windows/coverage，保留全部原资格。"""
from datetime import datetime, timezone
import hashlib

from data_pipeline.analysis.features import publication as feature
from data_pipeline.bgp.archive.value_codec import typed, untyped, digest, fields
from data_pipeline.analysis.country_events import qualified_schema as m3_schema, snapshot_schema as original
from data_pipeline.analysis.country_trends.contract import ActivityWindow
from data_pipeline.analysis.country_trends.feature_identity import capture_identity, window_identity


def _micros(value):
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None: raise ValueError('trend_feature_window_timezone')
    delta = dt.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days*86400 + delta.seconds)*1_000_000 + delta.microseconds


def capture_feature(db, admission, runtime, budget):
    db.executescript('''CREATE TABLE feature_input(view TEXT,ordinal INTEGER,payload TEXT,
                         PRIMARY KEY(view,ordinal));
        CREATE TABLE feature_reads(view TEXT PRIMARY KEY,request TEXT,receipt TEXT);''')
    for view in ('windows', 'coverage'):
        scope = dict(mode='all', window_role='all')
        request = dict(view=view, scope_typed=typed(scope), codec_version=feature.io.CODEC,
                       batch_rows=min(256, budget.limits.max_batch_rows), batch_bytes=budget.limits.max_batch_bytes)
        h = hashlib.sha256(); count = 0
        with feature.open_reader(runtime, admission, request, guard=budget.check) as session:
            for batch in session:
                fields(batch, {'rows_typed', 'codec_version', 'rows', 'bytes'})
                if (batch['codec_version'] != request['codec_version'] or type(batch['rows']) is not int
                        or not 0 < batch['rows'] <= request['batch_rows'] or type(batch['bytes']) is not int
                        or batch['bytes'] != len(batch['rows_typed'].encode()) or batch['bytes'] > request['batch_bytes']):
                    raise ValueError('trend_feature_public_batch')
                rows = untyped(batch['rows_typed'])
                if type(rows) is not list or len(rows) != batch['rows']: raise ValueError('trend_feature_public_rows')
                for item in rows:
                    text = typed(item); payload = text.encode(); budget.charge(payload)
                    h.update(bytes.fromhex(digest(text)))
                    db.execute('INSERT INTO feature_input VALUES (?,?,?)', (view, count, text)); count += 1
        receipt = session.receipt
        fields(receipt, {'contract', 'admission_id', 'request_digest', 'rows', 'typed_digest', 'execution', 'coverage_ref'})
        if (receipt['contract'], receipt['admission_id'], receipt['request_digest'], receipt['rows'], receipt['typed_digest'], receipt['execution']) != (
                'component-publication-read/v1', admission['admission_id'], digest(request), count, h.hexdigest(), 'complete'):
            raise ValueError('trend_feature_public_receipt')
        coverage = untyped(receipt['coverage_ref'])
        binding = untyped(admission['owner_binding'])
        if coverage != dict(admission_id=admission['admission_id'], view='coverage', scope=scope,
                            qualification_digest=binding['qualification_digest'], absence='Unknown'):
            raise ValueError('trend_feature_receipt_scope')
        db.execute('INSERT INTO feature_reads VALUES (?,?,?)', (view, typed(request), typed(receipt)))
    capture_identity(db, admission, runtime, budget)
    db.commit()


def feature_context(db, admission, selections, budget):
    """使用Feature同版Reference的双向唯一名称/代码关联；歧义不改绑事件。

    原完整windows/coverage另行逐行保存，未匹配选择不丢原值。
    """
    selected = {}
    if len(selections) > budget.limits.max_events or len(set(selections)) != len(selections):
        raise ValueError('trend_feature_selection_budget_or_duplicate')
    for event, mode in selections:
        if type(event) is not tuple or len(event) != 2 or mode not in ('ordinary', 'ir'):
            raise ValueError('trend_feature_selection')
        hit = db.execute('SELECT payload FROM country_events WHERE incident=? AND revision=?', event).fetchone()
        if hit is None: raise ValueError('trend_feature_selection_event')
        raw = original.decode(hit[0])['raw']
        status = m3_schema.row_decode(raw['table'], raw['row']).value
        selected[event, mode] = status.incident.country
    selected_by_country = {}
    for (event, mode), country in selected.items():
        selected_by_country.setdefault((country, mode), []).append(event)
    windows = []; size = 0
    for ordinal, payload in db.execute("SELECT ordinal,payload FROM feature_input WHERE view='windows' ORDER BY ordinal"):
        budget.check(); public = untyped(payload); raw = public['raw']
        identity = window_identity(db, admission, ordinal, public, budget)
        if identity is None: continue
        source, mapping = identity
        mode = raw['mode']
        for event in selected_by_country.get((mapping['code'], mode), ()):
            for metric, population in (('announ_num', 'accepted_announce_elements'), ('withdraw_num', 'accepted_withdraw_elements')):
                value = public['values'][metric]
                window = ActivityWindow(event, mode, metric, population, _micros(raw['start']), _micros(raw['end']),
                    value, 'complete' if value is not None else 'unknown', source,
                    raw['source_rank'], raw['source_id'], raw['window_role'])
                size += len(original.encode((event, mode, metric, raw, public['qualifications'])).encode())
                if size > budget.limits.max_context_bytes: raise ValueError('trend_feature_context_budget')
                windows.append(window)
    return tuple(windows)

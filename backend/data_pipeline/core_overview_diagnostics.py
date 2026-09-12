"""首页已知失败日期的离线证据编译与有界复读；不查询源库、不放行异常记录。"""
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from data_pipeline.core_overview_input import RecordTimeConflict, validate_record_time


def _compile_hijack_diagnostics(files, rows, receipt, selection_bytes, declaration):
    """核验已读32日原文中的双侧时间／等级失败；不由无失败推定消费准入。"""
    if files['query'] != (Path(__file__).parent / 'diagnostic_queries/hijack_selected_days.sql').read_bytes():
        raise ValueError('普通劫持预检SQL未核验')
    expected_days = sorted(set(re.findall(r"DATE '(\d{4}-\d{2}-\d{2})'", files['query'].decode())))
    if ([row['kind'] for row in rows[:5]] != ['tx_begin', 'columns', 'constraints', 'indexes', 'scope']
            or [row['kind'] for row in rows[-2:]] != ['tx_end', 'rollback_ack']
            or rows[-1] != {'kind': 'rollback_ack'}):
        raise ValueError('普通劫持原文回执不完整')
    begin, ending, scope = rows[0], rows[-2], rows[4]
    execution = receipt['execution']
    if (receipt['complete'] is not True or receipt['rollback_ack'] is not True or type(execution['exit_code']) is not int
            or execution['exit_code'] != 0 or execution['failure'] is not None or execution['child_reaped'] is not True
            or receipt['stderr_bytes'] != 0 or receipt['parameters'] != {}
            or receipt['source_instance'] != declaration['source']['instance']
            or receipt['tx_begin'] != begin or receipt['tx_end'] != ending
            or receipt['result_bytes'] != len(files['data']) or receipt['result_lines'] != len(rows)
            or receipt['result_sha256'] != _sha(files['data']) or receipt['sql_sha256'] != _sha(files['query'])
            or receipt['sql_bytes'] != len(files['query']) or receipt['application_name'] != ending['application_name']
            or begin['read_only'] != 'on' or ending['read_only'] != 'on'
            or begin['isolation'] != 'repeatable read' or ending['isolation'] != 'repeatable read'
            or begin['snapshot'] != ending['snapshot'] or begin['timezone'] != 'UTC'
            or begin['statement_timeout'] != '15s' or begin['lock_timeout'] != '2s'
            or begin['database'] != declaration['source']['instance'].rsplit('/', 1)[-1]
            or _receipt_time(ending['utc']) < _receipt_time(begin['utc'])
            or scope['source'] != declaration['source'] or scope['data_profile'] != declaration['data_profile']
            or scope['days'] != expected_days or 'hijack' not in declaration['kinds']):
        raise ValueError('普通劫持读取或日期范围绑定不一致')
    counts = [row for row in rows[5:-2] if row['kind'] == 'day_counts']
    originals = [row for row in rows[5:-2] if row['kind'] == 'source_record']
    direct = [row for row in rows[5:-2] if row['kind'] == 'day_detail_record']
    pairs = [row for row in rows[5:-2] if row['kind'] == 'pair_check']
    if ([row['day'] for row in counts] != expected_days
            or [row['kind'] for row in rows[5:-2]] != ['day_counts'] * len(counts)
                + ['source_record'] * len(originals) + ['day_detail_record'] * len(direct) + ['pair_check'] * len(pairs)
            or any(row['day'] not in expected_days for row in [*originals, *direct, *pairs])):
        raise ValueError('普通劫持日历或原文集合不完整')
    query_window = _window(expected_days[0], (datetime.fromisoformat(expected_days[-1]) + timedelta(days=1)).date().isoformat(), declaration)
    evidence = {'format': 'hijack-selected-days/v1', 'source_data_sha256': _sha(files['data']),
        'receipt_sha256': _sha(files['receipt']), 'query_sha256': _sha(files['query']),
        'selection_sha256': _sha(selection_bytes), 'query_window': query_window, 'queried_dates': expected_days,
        'read_at': begin['utc'], 'finished_at': ending['utc']}
    diagnostics = {}
    for count in counts:
        day = count['day']
        if (count['event_table'] != 'event_table_' + day[:7].replace('-', '')
                or count['detail_table'] != 'hijack_' + day[:7].replace('-', '')):
            raise ValueError('普通劫持日计数与对应月表不一致')
        selected = [row for row in originals if row['day'] == day]
        independent = [row for row in direct if row['day'] == day]
        comparisons = [row for row in pairs if row['day'] == day]
        references = [row['event']['detail_url'] for row in selected]
        if (len(set(references)) != len(selected) or sorted(row['detail_url'] for row in comparisons) != sorted(references)
                or any(type(row['candidate_count']) is not int or row['candidate_count'] != 1
                       or len(row['candidates']) != 1 for row in selected)
                or type(count['kind_mismatch']) is not int or count['kind_mismatch'] != 0
                or any(type(count[key]) is not int or count[key] != len(selected) for key in
                    ('event_rows', 'unique_refs', 'event_type_rows', 'refkind_rows', 'candidate_rows',
                     'unique_candidate_keys', 'detail_day_rows', 'detail_day_unique_keys'))):
            raise ValueError('普通劫持原记录、完整候选或独立计数不一致')
        expected_table = 'hijack_' + day[:7].replace('-', '')
        candidates = [[row['detail_table'], row['candidates'][0]] for row in selected]
        if sorted(json.dumps(row, sort_keys=True) for row in candidates) != sorted(
                json.dumps([row['detail_table'], row['detail']], sort_keys=True) for row in independent):
            raise ValueError('普通劫持候选与独立明细集合不一致')
        failures = {'time_fields_conflict': set(), 'level_conflict': set()}
        keys = set()
        for row in selected:
            event, detail = row['event'], row['candidates'][0]
            reference = event['detail_url']
            kind, start, target, number, source = reference.split('/')
            key = detail['source'], detail['prefix'], detail['hijack_eventid']
            if (key in keys or row['detail_table'] != expected_table or row['event_table'] != 'event_table_' + day[:7].replace('-', '')
                    or kind != 'hijack' or event['event_type'] != '前缀劫持'
                    or source != detail['source'] or source != event['source'] or source != declaration['source']['code']
                    or target.replace('-', '/') != detail['prefix'] or event['affected_prefix'] != detail['prefix']
                    or number != str(detail['hijack_eventid']) or start[:10] != day
                    or start.replace(' ', 'T') != event['s_time'] or event['s_time'] != detail['s_time']):
                raise ValueError('普通劫持预检含未支持的身份或开始时间冲突')
            keys.add(key)
            comparison = next(item for item in comparisons if item['detail_url'] == reference)
            time_bad = event['e_time'] != detail['e_time'] or event['duration'] != detail['duration']
            if (comparison['end_diff'] is not (event['e_time'] != detail['e_time'])
                    or comparison['duration_diff'] is not (event['duration'] != detail['duration'])
                    or comparison['level_diff'] is not (event['level'] != detail['hijack_level'])):
                raise ValueError('普通劫持双侧预检与原字段不一致')
            for values in (event, detail):
                try:
                    validate_record_time({'common': {'kind': 'hijack'}, 'raw_fields': values, 'data_profile': declaration['data_profile']})
                except RecordTimeConflict:
                    time_bad = True
            if time_bad:
                failures['time_fields_conflict'].add(reference)
            if event['level'] != detail['hijack_level']:
                failures['level_conflict'].add(reference)
        reasons = [{'code': code, 'kind': 'hijack', 'count': len(refs), 'evidence': evidence}
                   for code, refs in failures.items() if refs]
        if reasons:
            diagnostics[day] = _day_diagnostic(day, reasons, declaration)
    if not diagnostics:
        raise ValueError('普通劫持原文没有失败证据，不能替代完整消费准入')
    return diagnostics


def _sha(payload):
    return hashlib.sha256(payload).hexdigest()


def _read(path, limit=65536):
    if path.stat().st_size > limit:
        raise ValueError('诊断证据文件超限')
    payload = path.read_bytes()
    if len(payload) > limit:
        raise ValueError('诊断证据读取期间超限')
    return payload


def _receipt_time(value):
    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S' + ('.%f' if '.' in value else '') + '%z')


def _window(start, end, declaration):
    zone = ZoneInfo(declaration['data_profile']['timezone'])
    first = datetime.strptime(start, '%Y-%m-%d').replace(tzinfo=zone)
    last = datetime.strptime(end, '%Y-%m-%d').replace(tzinfo=zone)
    profile = declaration['data_profile']
    if (first.date().isoformat() != start or last.date().isoformat() != end
            or not datetime.fromisoformat(profile['window_start']) <= first < last <= datetime.fromisoformat(profile['window_end_exclusive'])):
        raise ValueError('诊断证据窗口未绑定项目数据档')
    return {'source': declaration['source']['code'], 'start': first.isoformat(), 'end_exclusive': last.isoformat()}


def _day_diagnostic(day, reasons, declaration):
    window = _window(day, (datetime.fromisoformat(day) + timedelta(days=1)).date().isoformat(), declaration)
    diagnostic = {'schema_version': 'core-overview-diagnostic/v1', 'stage': 'source_field_precheck',
        'source': declaration['source'], 'data_profile': declaration['data_profile'], 'window': window,
        'compiler_sha256': _sha(Path(__file__).read_bytes()), 'reasons': reasons}
    validate_diagnostic(diagnostic, declaration, window)
    return diagnostic


def validate_diagnostic(diagnostic, declaration, window):
    if diagnostic['schema_version'] == 'core-overview-diagnostic/v2':
        return _validate_day_failure(diagnostic, declaration, window)
    if (diagnostic['schema_version'] != 'core-overview-diagnostic/v1'
            or diagnostic['stage'] != 'source_field_precheck'
            or diagnostic['source'] != declaration['source'] or diagnostic['data_profile'] != declaration['data_profile']
            or diagnostic['window'] != window
            or not re.fullmatch('[0-9a-f]{64}', diagnostic['compiler_sha256'])):
        raise ValueError('诊断与目录来源、窗口或格式不一致')
    reasons = diagnostic['reasons']
    if not isinstance(reasons, list) or not 1 <= len(reasons) <= 6:
        raise ValueError('诊断缺少有限失败原因')
    seen = set()
    for reason in reasons:
        key = reason['kind'], reason['code']
        if (key in seen or reason['kind'] not in declaration['kinds']
                or reason['code'] not in {'level_conflict', 'invalid_time_order', 'time_fields_conflict'}
                or type(reason['count']) is not int or reason['count'] <= 0):
            raise ValueError('诊断原因、条数或唯一性无效')
        seen.add(key)
        evidence = reason['evidence']
        if evidence['format'] == 'hijack-selected-days/v1':
            dates = evidence['queried_dates']
            if (not isinstance(dates, list) or not 1 <= len(dates) <= 32 or sorted(set(dates)) != dates
                    or window['start'][:10] not in dates
                    or any(_window(day, (datetime.fromisoformat(day) + timedelta(days=1)).date().isoformat(), declaration)['start']
                           < evidence['query_window']['start'] for day in dates)
                    or _window(dates[0], (datetime.fromisoformat(dates[-1]) + timedelta(days=1)).date().isoformat(), declaration)
                       != evidence['query_window']):
                raise ValueError('普通劫持实际查询日与外包窗口不一致')
        if (evidence['format'] not in {'quality-by-day/v1', 'prefix-quality/v1', 'hijack-selected-days/v1'}
                or (reason['code'] == 'time_fields_conflict' and (reason['kind'] != 'hijack' or evidence['format'] != 'hijack-selected-days/v1'))
                or any(not re.fullmatch('[0-9a-f]{64}', evidence[key]) for key in
                       ('source_data_sha256', 'receipt_sha256', 'query_sha256', 'selection_sha256'))
                or _receipt_time(evidence['finished_at']) < _receipt_time(evidence['read_at'])
                or evidence['query_window']['source'] != window['source']
                or not datetime.fromisoformat(evidence['query_window']['start']) <= datetime.fromisoformat(window['start'])
                    < datetime.fromisoformat(window['end_exclusive']) <= datetime.fromisoformat(evidence['query_window']['end_exclusive'])):
            raise ValueError('诊断证据身份、读取顺序或查询窗口无效')


def _validate_day_failure(diagnostic, declaration, window):
    """复读离线编译的具体失败证据；不读取大源文件，也不宣告整日通过。"""
    reading = diagnostic['stage'] == 'source_read'
    if (diagnostic['stage'] not in {'source_read', 'source_field_validation'}
            or diagnostic['source'] != declaration['source'] or diagnostic['data_profile'] != declaration['data_profile']
            or diagnostic['window'] != window or not re.fullmatch('[0-9a-f]{64}', diagnostic['compiler_sha256'])):
        raise ValueError('完整日诊断来源、窗口或阶段不符')
    reasons = diagnostic['reasons']
    if not isinstance(reasons, list) or not 1 <= len(reasons) <= (1 if reading else 6):
        raise ValueError('完整日诊断原因数无效')
    allowed = {'source_identity_unresolved': {'as_outage'},
               'source_population_mismatch': {'country_outage', 'hijack'}, 'start_time_conflict': {'hijack'}}
    seen = set()
    for reason in reasons:
        key = reason['kind'], reason['code']
        if key in seen:
            raise ValueError('完整日诊断原因重复')
        seen.add(key)
        evidence = reason['evidence']
        if (evidence['query_window'] != window
                or any(not re.fullmatch('[0-9a-f]{64}', evidence[field]) for field in
                       ('source_data_sha256', 'query_sha256', 'selection_sha256'))):
            raise ValueError('完整日诊断摘要或查询窗口不符')
        _receipt_time(evidence['read_at'])
        if reading:
            if (key != ('all', 'source_read_timeout') or reason['count'] is not None
                    or evidence['format'] != 'failed-day-read/v1' or evidence['finished_at'] is not None
                    or evidence['receipt_sha256'] is not None or evidence['audit_sha256'] is not None
                    or not re.fullmatch('[0-9a-f]{64}', evidence['failure_sha256'])):
                raise ValueError('未读完整日不能声明记录数、成功回执或完成时点')
        elif (reason['kind'] not in declaration['kinds'] or reason['kind'] not in allowed.get(reason['code'], set())
                or type(reason['count']) is not int or reason['count'] <= 0
                or evidence['format'] != 'complete-day-audit/v1' or evidence['failure_sha256'] is not None
                or any(not re.fullmatch('[0-9a-f]{64}', evidence[field]) for field in ('receipt_sha256', 'audit_sha256'))
                or _receipt_time(evidence['finished_at']) < _receipt_time(evidence['read_at'])):
            raise ValueError('源字段校验失败的原因、条数或回执无效')


def compile_diagnostics(selection_path, declaration):
    """只接受已核验查询格式的原始预检回执；原因和条数必须从原结果提取。"""
    path = Path(selection_path)
    selection_bytes = _read(path)
    selection = json.loads(selection_bytes)
    if (selection['schema_version'] != 'core-overview-diagnostic-selection/v1'
            or selection['format'] not in {'quality-by-day/v1', 'prefix-quality/v1', 'hijack-selected-days/v1'}
            or selection['source'] != declaration['source'] or selection['data_profile'] != declaration['data_profile']
            or not isinstance(declaration['source']['instance'], str) or not declaration['source']['instance'].strip()):
        raise ValueError('诊断选择格式或来源不兼容')
    files = {}
    for key in ('data', 'receipt', 'query'):
        entry = selection['files'][key]
        target = path.parent / entry['file']
        if Path(entry['file']).name != entry['file'] or target.resolve().parent != path.parent.resolve():
            raise ValueError('诊断证据文件越界')
        files[key] = _read(target, 4 * 1024 * 1024 if key == 'data' else 65536)
        if _sha(files[key]) != entry['sha256']:
            raise ValueError('诊断证据摘要不一致')
    receipt = json.loads(files['receipt'])
    rows = [json.loads(line) for line in files['data'].splitlines()]
    if selection['format'] == 'hijack-selected-days/v1':
        return _compile_hijack_diagnostics(files, rows, receipt, selection_bytes, declaration)
    prefix = selection['format'] == 'prefix-quality/v1'
    expected_envelopes = ['context', 'prefix_quality', 'receipt', 'rollback_ack'] if prefix else ['context', 'quality_by_day', 'receipt']
    if [row['kind'] for row in rows] != expected_envelopes:
        raise ValueError('诊断原结果回执不完整')
    context, ending = rows[0], rows[-2] if prefix else rows[-1]
    query_file = 'prefix_quality.sql' if prefix else 'quality_by_day.sql'
    if (context != receipt['context'] or ending != receipt['receipt']
            or receipt['output_sha256' if prefix else 'input_sha256'] != _sha(files['data'])
            or receipt['query_sha256'] != _sha(files['query'])
            or (receipt['bytes']['stdout'] if prefix else receipt['bytes']) != len(files['data'])
            or receipt['source_instance'] != declaration['source']['instance']
            or files['query'] != (Path(__file__).parent / 'diagnostic_queries' / query_file).read_bytes()):
        raise ValueError('诊断必须绑定完整原始结果、回执及已核验SQL')
    if prefix and (receipt['state'] != 'success' or type(receipt['returncode']) is not int or receipt['returncode'] != 0
                   or receipt['error'] is not None or receipt['bytes']['stderr'] != 0 or receipt['rollback_ack'] != rows[-1]
                   or rows[-1]['status'] != 'ROLLBACK 后客户端到达确认行'):
        raise ValueError('前缀预检进程未成功闭合')
    if (context['read_only'] != ending['read_only'] or ending['read_only'] != 'on'
            or context['isolation'] != 'repeatable read' or context['source'] != declaration['source']['code']
            or context['database'] != declaration['source']['instance'].rsplit('/', 1)[-1]
            or context['business_timezone'] != declaration['data_profile']['timezone']
            or _receipt_time(ending['finished_at']) < _receipt_time(context['read_at'])):
        raise ValueError('诊断读取上下文不一致或不完整')
    query_window = _window(context['start'], context['end_exclusive'], declaration)
    month, kind = ('202603', 'prefix_outage') if prefix else (context['month'], context['anomaly_kind'])
    if kind not in declaration['kinds'] or month not in {'202602', '202603'}:
        raise ValueError('诊断查询类型或月份未支持')
    expected = {'month': month, 'anomaly_kind': kind, 'start': context['start'], 'end': context['end_exclusive'],
                'event_table': f'event_table_{month}', 'prefix_table': f'prefix_outage_{month}',
                'as_table': f'as_outage_{month}', 'leak_table': f'leak_event_{month}'}
    if prefix:
        expected = {'start': context['start'], 'end': context['end_exclusive'], 'explain': 'false'}
    if (receipt['parameters'] != expected or context['start'].replace('-', '')[:6] != month
            or (datetime.fromisoformat(context['end_exclusive']) - timedelta(days=1)).strftime('%Y%m') != month):
        raise ValueError('诊断SQL参数与原结果不一致')
    first = datetime.fromisoformat(query_window['start'])
    last = datetime.fromisoformat(query_window['end_exclusive'])
    expected_days = [(first + timedelta(days=n)).date().isoformat() for n in range((last-first).days)]
    if prefix:
        item = rows[1]['data']
        if (item['window_start'] != context['start'] or item['window_end_exclusive'] != context['end_exclusive']
                or any(type(item[key]) is not int or item[key] != item['records']
                       for key in ('unique_candidates', 'matched_candidate_rows'))
                or any(type(item[key]) is not int or item[key] != 0 for key in
                       ('null_refs', 'invalid_reference_ids', 'event_level_not_recognized', 'candidate_missing_start_rows',
                        'candidate_invalid_level_rows', 'detail_duration_arithmetic_conflicts'))):
            raise ValueError('前缀预检含未支持的字段矛盾或窗口不一致')
        daily = [{**item, 'day': context['start'], 'kind': kind}]
    else:
        daily = rows[1]['items']
    if [item['day'] for item in daily] != expected_days:
        raise ValueError('诊断原结果日历不完整或重复')
    evidence = {'format': selection['format'], 'source_data_sha256': _sha(files['data']),
                'receipt_sha256': _sha(files['receipt']), 'query_sha256': _sha(files['query']),
                'selection_sha256': _sha(selection_bytes), 'query_window': query_window,
                'read_at': context['read_at'], 'finished_at': ending['finished_at']}
    diagnostics = {}
    for item in daily:
        if (item['kind'] != kind or type(item['records']) is not int or item['records'] < 0
                or type(item['distinct_refs']) is not int or item['distinct_refs'] != item['records']
                or any(type(item[key]) is not int or item[key] != 0 for key in
                       ('missing_candidates', 'multiple_candidates', 'start_conflicts', 'reference_conflicts',
                        'end_conflicts', 'duration_conflicts', 'level_not_recognized'))):
            raise ValueError('原预检含未支持的关联或共同字段问题')
        reasons = []
        for field, code in (('level_conflicts', 'level_conflict'), ('invalid_time_order', 'invalid_time_order')):
            if type(item[field]) is not int or not 0 <= item[field] <= item['records']:
                raise ValueError('原预检失败计数无效')
            if item[field]:
                reasons.append({'code': code, 'kind': kind, 'count': item[field], 'evidence': evidence})
        if reasons:
            day = item['day']
            diagnostics[day] = _day_diagnostic(day, reasons, declaration)
    if not diagnostics:
        raise ValueError('该预检没有失败证据，不能替代完整消费准入')
    return diagnostics


def read_diagnostic(path, expected, declaration, window):
    payload = _read(path)
    if _sha(payload) != expected:
        raise ValueError('诊断文件摘要不一致')
    diagnostic = json.loads(payload)
    validate_diagnostic(diagnostic, declaration, window)
    revision = diagnostic['schema_version'].rsplit('/', 1)[-1]
    return {**diagnostic, 'version': f'overview_diagnostic_{revision}_' + _sha(payload)}

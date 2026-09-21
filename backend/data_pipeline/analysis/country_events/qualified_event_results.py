"""完整公开 D 结果链的国家选择：保留原修订，单列已接受生命周期证明。"""
from collections import defaultdict
from copy import deepcopy
import json
from data_pipeline.analysis.detection.result_window import identity_windows


def country_results(records, entries, results, coverage, binding, *, max_rows, guard, store=None):
    """消费已完整核验的四个公开视图，不访问 Detection state_entries。

    active_in_final_saved_state 是上游已接受视图的证明字段，国家只保留和
    交叉核对完整原修订/资格引用，不能自行重造该证明或将它当早期 anchor。
    调用方仍须持有实际四类 Admission，并核验各视图的完整公开回执。
    """
    if type(max_rows) is not int or max_rows <= 0:
        raise ValueError('M3 结果链预算无效')
    identity = binding['identity']
    if identity.get('result_window_rule') != 'detection-result-window/v1':
        raise ValueError('M3 结果链缺实际 D 窗口证明')
    windows = identity_windows(identity, binding['scope'])
    count = 0

    def charged(rows):
        nonlocal count
        for row in rows:
            guard()
            count += 1
            if store is None and count > max_rows:
                raise ValueError('resource_limit:M3_result_chain')
            yield row

    raw_by_sequence, chains = ({},defaultdict(list)) if store is None else (store.new_map(),store.new_groups())
    for row in charged(records):
        seq = row['sequence']
        if seq in raw_by_sequence:
            raise ValueError('M3 D 原修订序号重复')
        raw_by_sequence[seq] = row
        if row['record_kind'] == 'business_revision':
            chains[row['incident_id']].append(row)
    for chain in chains.values():
        guard()
        chain.sort(key=lambda r: r['sequence'])
    latest, by_id, source_coverage = ({},{},{}) if store is None else (store.new_map(),store.new_map(),store.new_map())
    for row in charged(entries):
        if row['entry_id'] in by_id:
            raise ValueError('M3 D 原资格身份重复')
        by_id[row['entry_id']] = row
        if row['kind'] == 'event_qualification':
            key = row['incident_id'], row['revision']
            if key not in latest or row['ordinal'] > latest[key]['ordinal']:
                latest[key] = row
        elif row['kind'] == 'source_coverage':
            source_coverage[row['ordinal']] = row
    selected, proof_by_incident, seen = (defaultdict(list),{},set()) if store is None else (store.new_groups(),store.new_map(),store.new_set())
    for result in charged(results):
        row, proof = result['raw'], result['lifecycle']
        seq, incident = row['sequence'], row['incident_id']
        if seq in seen or raw_by_sequence.get(seq) != row or row['record_kind'] != 'business_revision':
            raise ValueError('M3 D 结果原修订缺失、重复或改写')
        seen.add(seq)
        chain = chains[incident]
        if (proof['first_sequence'], proof['last_sequence'], proof['revisions']) != (
                chain[0]['sequence'], chain[-1]['sequence'], len(chain)):
            raise ValueError('M3 D 结果链截断历史修订')
        q = latest.get((incident, row['revision']))
        final_q = latest.get((incident, chain[-1]['revision']))
        if q is None or final_q is None:
            raise ValueError('M3 D 结果链缺原独立资格')
        expected = json.loads(q['payload_json'])
        expected['qualification_id'] = q['entry_id']
        if result['qualification'] != expected or proof['final_qualification_id'] != final_q['entry_id']:
            raise ValueError('M3 D 结果链原资格不符')
        first, last = json.loads(chain[0]['legacy_json']), json.loads(chain[-1]['legacy_json'])
        if (proof['original_start'], proof['original_end']) != (first.get('s_time'), last.get('e_time')):
            raise ValueError('M3 D 生命周期改写原起止')
        if (proof['result_window'] != identity['result_window']
                or proof['calculation_window'] != windows['calculation_window']
                or result['as_of_position'] != identity['qualification_as_of_position']
                or proof['earlier_history'] != 'Unknown'):
            raise ValueError('M3 D 生命周期窗口/原位置/早期边界不符')
        if proof['selection'] not in ('carry_in', 'started_in_result', 'possible_unknown'):
            raise ValueError('M3 D 结果选择不受支持')
        main = row if proof['selection'] != 'possible_unknown' and expected['coverage'] == 'complete' else None
        if result['main'] != main:
            raise ValueError('M3 D 结果主值越过原资格')
        if incident in proof_by_incident and proof_by_incident[incident] != proof:
            raise ValueError('M3 同一 D 原事件生命周期证明不一致')
        proof_by_incident[incident] = proof
        selected[incident].append(result)
    for incident, rows in selected.items():
        guard()
        left=rows if store is not None else sorted(rows,key=lambda r:r['raw']['sequence'])
        right=chains[incident]
        if len(left)!=len(right) or any(a['raw']['sequence']!=b['sequence'] for a,b in zip(left,right)):
            raise ValueError('M3 D 结果必须保留所选事件全部计算窗原修订')
    covered = set()
    for row in charged(coverage):
        ordinal = row['raw']['ordinal']
        if ordinal in covered or source_coverage.get(ordinal) != row['raw']:
            raise ValueError('M3 D 独立来源覆盖缺失、重复或改写')
        if (row['windows']['result_window'] != identity['result_window']
                or row['windows']['calculation_window'] != windows['calculation_window']
                or row['window_coverage'] != identity['window_coverage']
                or row['earlier_history'] != 'Unknown'):
            raise ValueError('M3 D 独立来源覆盖窗口不符')
        covered.add(ordinal)
    if covered != set(source_coverage):
        raise ValueError('M3 D 独立来源覆盖没有完整枚举')
    output=(deepcopy(row) for incident in sorted(selected)
                 for row in (selected[incident] if store is not None else sorted(selected[incident], key=lambda r: r['raw']['sequence']))
                 if row['raw']['subject_type'] == 'country' and row['raw']['event_kind'] == 'country_outage')
    if store is None:return tuple(output)
    key=('internal','country_results');store.add_view(key,output)
    for transient in (raw_by_sequence,chains,latest,by_id,source_coverage,selected,proof_by_incident,seen):transient.close()
    store.flush();return store[key]

"""缺口资格覆盖层；不修改科学状态、事件身份或解释受限载荷。"""
from dataclasses import asdict
from data_pipeline.analysis.detection.qualification_contract import PROFILE, QUALIFICATION_RULE, entry_id, target

DIMENSIONS = (
    'vp_origin_current', 'denominator_peak', 'origin_anchor',
    'prefix_leak_suppression', 'candidate_lifecycle', 'event_absence',
)


class Qualification:
    def __init__(self, binding, scope, component_run, sink):
        self.binding, self.scope = binding, scope
        self.component_run, self.sink = component_run, sink
        self.gaps, self.events = {}, {}
        self.position = (0, 0, 0, 0)
        self.source_id = None
        self.count = 0

    def write(self, kind, payload, *, incident_id=None, revision=None, gap_id=None):
        row = dict(ordinal=self.count, kind=kind, incident_id=incident_id,
                   revision=revision, source_id=self.source_id, gap_id=gap_id,
                   payload_json=None)
        payload = dict(payload, component='detection', component_run=self.component_run,
                       binding_ref=self.binding.binding_id, rule_version=QUALIFICATION_RULE)
        payload['target_ref'] = target(row,self.component_run)
        row['payload_json'] = payload
        row['entry_id'] = entry_id(self.component_run,row)
        self.sink(row)
        self.count += 1

    def boundary(self, boundary):
        self.position = boundary.position.sort_key
        if boundary.gap is None:
            return
        gap = boundary.gap
        if gap.gap_id in self.gaps:
            raise ValueError('重复Gap资格通知')
        self.gaps[gap.gap_id] = asdict(gap)
        self.write('scope_gap', dict(gap=asdict(gap),
                   effective_position=self.position,
                   dependency_scope=dict(legacy_vp=None if gap.scope.peer_asn_evidence is None
                                         else str(gap.scope.peer_asn_evidence),
                                         prefixes='all_including_unseen',
                                         aggregate_overlap='possible',
                                         direction='bidirectional',
                                         unmapped_rib='possible_overlap')),
                   gap_id=gap.gap_id)
        for (incident, revision), closed in self.events.items():
            if not closed:
                self.event(incident, revision, 'unknown')

    def event(self, incident, revision, coverage):
        self.write('event_qualification', dict(
            coverage=coverage, dimensions=DIMENSIONS,
            dimension_coverage={d:coverage for d in DIMENSIONS},
            completeness_scope='declared_cold_start_and_gap_dependencies_only',
            gap_refs=list(self.gaps), effective_position=self.position,
            window=[self.scope.window_start, self.scope.window_end],
            historical_recovery='not_proven',
        ), incident_id=incident, revision=revision)

    def observe_output(self, row):
        if row['kind'] != 'business_revision':
            return
        key = row['incident_id'], row['revision']
        # 旧完成修订的历史样本不被之后Gap追溯污染；开始/持续修订仍保留。
        self.events[key] = bool(row['legacy'].get('e_time'))
        self.event(*key, 'unknown' if self.gaps else 'complete')

    def source_complete(self, end):
        self.write('source_coverage', dict(
            coverage='unknown' if self.gaps else 'complete', dimensions=DIMENSIONS,
            dimension_coverage={d:'unknown' if self.gaps else 'complete' for d in DIMENSIONS},
            completeness_scope='declared_cold_start_and_gap_dependencies_only',
            gap_refs=list(self.gaps), effective_position=self.position,
            window=[self.scope.window_start, self.scope.window_end],
            receipt=end, selected_source=self.source_id,
        ))

    def state(self):
        return dict(gaps=self.gaps, event_revisions=[list(k) for k in self.events],
                    position=self.position, rule_version=QUALIFICATION_RULE,
                    history_cleared=False)

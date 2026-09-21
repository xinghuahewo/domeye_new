"""单事件有界纯输入索引；正式 Country P1 读取适配在上游冻结后接入。"""
from dataclasses import replace

from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as component_schema
from data_pipeline.analysis.country_events.compute import MetricPoint, Peak
from data_pipeline.analysis.country_trends.compute import metric_validate
from data_pipeline.analysis.country_trends.qualified_models import Target, metric_dimension
from data_pipeline.analysis.country_trends.qualified_calculation import Qualifications


class EventSources:
    """原21表行、原资格及主值分别索引，绝不改写原科学行。

    调用方须先按共享预算限定单事件大小。此纯对象无连接、登记或准入能力；
    mappings 保存来自公共 typed 解码的完整字段，不接受简写身份替代正式绑定。
    """
    def __init__(self, logical_run_id, input_binding_id, window_us, event,
                 originals, qualifications, qualified_values):
        self.logical_run_id, self.input_binding_id = logical_run_id, input_binding_id
        self.window_us, self.event = window_us, event
        self.originals, self.targets = {}, {}
        self.qualifications = Qualifications(qualifications)
        previous = -1
        for sequence, original in originals:
            if (type(sequence) is not int or sequence <= previous or type(original) is not c2.C2Row
                    or (original.incident_id, original.revision) != event):
                raise ValueError('trend_m3_original_sequence_or_event')
            previous = sequence
            v = original.value
            table = component_schema.BY_CLASS[type(v)]
            self.originals[table, sequence] = v
        statuses = [(key, v) for key, v in self.originals.items() if type(v) is c2.EventStatus]
        if len(statuses) != 1:
            raise ValueError('trend_m3_selected_status_count')
        self.population_ref, self.status = statuses[0]
        if (self.status.incident.incident_id, self.status.incident.revision) != event:
            raise ValueError('trend_m3_original_status_identity')
        for key, v in self.originals.items():
            self.targets[key] = Target(logical_run_id, input_binding_id, key, *event,
                getattr(v, 'cohort_id', self.status.cohort_id),
                getattr(getattr(v, 'route', None), 'object_id', getattr(v, 'prefix', None)),
                getattr(v, 'afi', None) if getattr(v, 'afi', None) in (1, 2) else None,
                getattr(v, 'metric', None), getattr(v, 'sample_us', None), window_us)
            if self.targets[key].cohort_id != self.status.cohort_id:
                raise ValueError('trend_m3_original_cohort')
        self.values = {}
        for value in qualified_values:
            key = value['raw_target_ref'], value['dimension']
            if key in self.values or key[0] not in self.targets:
                raise ValueError('trend_m3_qualified_target_duplicate_or_missing')
            self.values[key] = dict(value)
        for q in self.qualifications.records.values():
            target = self.targets.get(q['raw_target_ref'])
            if target is None:
                raise ValueError('trend_m3_qualification_original_missing')
            self.qualifications.check(target, (q['qualification_id'],), (q['dimension'],))

    def qualification(self, key, dimensions):
        refs = tuple(q['qualification_id'] for q in self.qualifications.records.values()
                     if q['raw_target_ref'] == key and q['dimension'] in dimensions)
        return self.qualifications.check(self.targets[key], refs, dimensions), refs

    def select(self, key, dimension, value, unit):
        return self.qualifications.select(self.targets[key], value,
                    self.values.get((key, dimension)), dimension=dimension,
                    population_ref=self.population_ref, unit=unit)

    def denominator(self):
        return self.select(self.population_ref, 'fixed_denominator',
                           self.status.direction_count, 'endpoint_direction_count')

    def metric(self, key):
        original = self.originals[key]
        if type(original) is not MetricPoint:
            raise ValueError('trend_m3_metric_original_type')
        metric_validate(original)
        selection = self.select(key, metric_dimension(original.metric), original.value, original.unit)
        reasons, refs = self.qualification(key, ('fixed_denominator',))
        # 点值可用不依赖无关的路径历史；比例另依赖此原样本的固定分母。
        if selection.denominator is not None and selection.denominator != original.denominator:
            raise ValueError('trend_m3_original_denominator_mismatch')
        return replace(selection, denominator=None if reasons else selection.denominator,
                       denominator_reasons=reasons,
                       qualification_refs=tuple(dict.fromkeys((*selection.qualification_refs, *refs))))

    def peak(self, name):
        matches = [(key, v) for key, v in self.originals.items() if type(v) is Peak and v.metric == name]
        if len(matches) > 1:
            raise ValueError('trend_m3_duplicate_original_peak')
        if not matches: return None
        key, raw = matches[0]
        proof = self.values.get((key, 'window_peak'))
        if proof is None: return self.select(key, 'window_peak', raw.value, raw.unit)
        refs = tuple(proof['qualification_refs']); own = []; support = []
        if len(set(refs)) != len(refs): raise ValueError('trend_m3_duplicate_qualification_reference')
        for ref in refs:
            qualification = self.qualifications.records.get(ref)
            if qualification is None: raise ValueError('trend_m3_dangling_qualification')
            (own if qualification['raw_target_ref'] == key else support).append(ref)
        if len(own) != 1 or self.qualifications.records[own[0]]['dimension'] != 'window_peak':
            raise ValueError('trend_m3_peak_own_qualification')
        # Country峰值原QV附带所选原样本的下界资格；它不属于Peak自身Target。
        if len(support) != int(proof['known_lower_bound'] is not None):
            raise ValueError('trend_m3_peak_lower_support_count')
        for ref in support:
            qualification = self.qualifications.records[ref]
            point_key = qualification['raw_target_ref']; point_raw = self.originals.get(point_key)
            if (type(point_raw) is not MetricPoint or name != 'visible_direction_count'
                    or point_raw.metric != name or qualification['dimension'] != 'point_presence'
                    or not self.window_us[0] < point_raw.sample_us <= self.window_us[1]):
                raise ValueError('trend_m3_peak_lower_support_target')
            point = self.metric(point_key)  # 按该原点自己的完整Target/QV/人口核验。
            point_proof = self.values.get((point_key, 'point_presence'))
            if (point_proof is None or tuple(point_proof['qualification_refs']) != (ref,)
                    or component_schema.encode(point.known_lower_bound) != component_schema.encode(proof['known_lower_bound'])
                    or point.population_ref != proof['population_ref'] or point_raw.unit != raw.unit
                    or not set(point.raw_basis_refs).issubset(proof['raw_basis_refs'])):
                raise ValueError('trend_m3_peak_lower_support_relation')
        selected = self.qualifications.select(self.targets[key], raw.value,
            dict(proof, qualification_refs=tuple(own)), dimension='window_peak',
            population_ref=self.population_ref, unit=raw.unit)
        # 仅分开消费条件；原QV及返回证据仍保留全部原引用，不把补证当精确峰值。
        return replace(selected, qualification_refs=refs)

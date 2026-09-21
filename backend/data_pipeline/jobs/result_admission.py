"""迁移调用原模块inspect/admit/current；不实现第二套资格规则或发布器。"""
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import resource
import sys
import time

from data_pipeline.jobs.stage_runner import save_new, save_final
from data_pipeline.jobs.input_plan import REAL_INPUT, ARTIFICIAL_INPUT


def candidate_mode(input_profile):
    """人工的是输入来源；两模式都验收实际冻结产物，不走fixture准入。"""
    if input_profile not in (REAL_INPUT, ARTIFICIAL_INPUT):
        raise ValueError("未知迁移输入模式")
    return dict(fixture_only=False, execution_profile="real-candidate/v1")


def inspect_m2(dsn, run_id, snapshot, sources, guard):
    """从公共Reader的实际返回组装完整绑定，随后仍由原Runtime.inspect核验。"""
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.ordered_reader import binding_from_reader
    from data_pipeline.bgp.record_types import metadata_json
    reader = ObservationReader(dsn, run_id, snapshot, sources, profile='observation', guard=guard)
    bound = binding_from_reader(reader)
    selection = reader.selection
    ranks = {s.source_id: i for i,s in enumerate(bound.sources)}
    return dict(owner='m2', run_id=run_id, snapshot=snapshot,
        ordered_source_ids=list(reader.sources), profile='observation', plan=selection.plan,
        seal=selection.seal, input_binding=metadata_json(bound), input_binding_id=bound.binding_id,
        selected_sources=[dict(source_id=s, upstream_rank=ranks[s], calculation_role=bound.sources[ranks[s]].role) for s in reader.sources],
        reference_sources=[dict(source_id=e['source_id'], checkpoint_ordinal=cp['ordinal'], interpretation=e['format'])
            for e,cp in zip(selection.plan['inputs'], selection.checkpoints) if e['role']=='reference'])


def admit_current(module, runtime, binding, output, guard):
    """保存实际返回的Admission后再做current；失败不改写已保存回执。"""
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    started_all = time.monotonic()
    metrics = dict(state='failed', started_at=datetime.now(timezone.utc).isoformat(),
        admit_seconds=None, current_seconds=None, query_seconds=None,
        physical_input_bytes=None, physical_output_bytes=None, processed_count=None, processed_unit=None,
        rss_scope='驱动进程启动以来RUSAGE_SELF高水位；不是独立准入阶段峰值，不含PG')
    try:
        started = time.monotonic()
        try:
            admission = module.admit(runtime, binding, guard=guard)
        finally:
            metrics['admit_seconds'] = time.monotonic()-started
        save_new(root/'admission.json', admission)
        started = time.monotonic()
        try:
            module.verify_current(runtime, admission, guard=guard)
        finally:
            metrics['current_seconds'] = time.monotonic()-started
        metrics['state'] = 'current_verified'
        return admission
    finally:
        metrics.update(ended_at=datetime.now(timezone.utc).isoformat(), wall_seconds=time.monotonic()-started_all,
            parent_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))
        save_final(root/'准入观测.json', metrics)


def m2_runtime(config, manifest, binding, *, input_profile=REAL_INPUT):
    from data_pipeline.bgp.archive.admission import Runtime
    options = dict(config)
    options['scratch_root'] = Path(options['scratch_root'])
    return Runtime(**options, **candidate_mode(input_profile),
                   input_manifest=manifest, expected_m2_binding=binding)


def admit_m2(config, manifest, seal, sources, output, guard, *, input_profile=REAL_INPUT):
    from data_pipeline.bgp.archive import admission as owner
    binding = inspect_m2(config['dsn'], seal['run_id'], seal['snapshot'], sources, guard)
    if binding['seal'] != seal or binding['plan']['manifest'] != manifest:
        raise ValueError('实际M2与固定manifest/封存回执不符')
    runtime = m2_runtime(config, manifest, binding, input_profile=input_profile)
    actual = owner.inspect_binding(runtime, seal['run_id'], seal['snapshot'], sources)
    if actual != binding:
        raise ValueError('公共inspect与原Reader完整绑定不符')
    admission = admit_current(owner, runtime, actual, output, guard)
    return runtime, admission


def admit_reference(runtime, m2_admission, source_id, output, guard):
    from data_pipeline.bgp.archive import admission as owner
    runtime.dependency_admissions = (m2_admission,)
    binding = owner.reference_binding(runtime, m2_admission, source_id)
    return admit_current(owner, runtime, binding, output, guard)


def admit_feature(config, produced, dependencies, output, guard, *, input_profile=REAL_INPUT):
    from data_pipeline.analysis.features import publication as owner
    from data_pipeline.analysis.features.qualified_read import inspect_binding
    options = dict(config)
    for key in ('scratch_root', 'output_root'):
        options[key] = Path(options[key])
    binding = inspect_binding(options['dsn'], produced['run_id'], produced['snapshot'],
                              guard=guard, max_rss_bytes=options['max_rss_bytes'])
    runtime = owner.Runtime(**options, **candidate_mode(input_profile),
        expected_feature_binding=binding, dependency_admissions=tuple(a for r,a in dependencies),
        dependency_runtimes={a['admission_id']:r for r,a in dependencies})
    return runtime, admit_current(owner, runtime, binding, output, guard)


def admit_canonical(config, manifest, produced, dependencies, output, guard, *, input_profile=REAL_INPUT):
    from data_pipeline.bgp.replay import snapshot_admission as owner
    from data_pipeline.bgp.replay.snapshot_store import ProjectionReader
    from data_pipeline.bgp.replay.snapshot_contract import ProjectionBinding
    options = dict(config)
    for key in ('scratch_root', 'output_root'):
        options[key] = Path(options[key])
    identity = ProjectionBinding(**produced)
    reader = ProjectionReader(options['dsn'], identity, guard=guard)
    binding = dict(binding=asdict(identity), descriptor=reader.descriptor())
    runtime = owner.Runtime(**options, **candidate_mode(input_profile),
        input_manifest=manifest, expected_canonical_binding=binding,
        dependency_admissions=tuple(a for r,a in dependencies),
        dependency_runtimes={a['admission_id']:r for r,a in dependencies})
    actual = owner.inspect_binding(runtime, identity, guard=guard)
    if actual != binding:
        raise ValueError('Canonical公共inspect与原descriptor不符')
    return runtime, admit_current(owner, runtime, actual, output, guard)


def admit_detection(config, ready, dependencies, output, guard, *, input_profile=REAL_INPUT):
    from data_pipeline.analysis.detection import publication as owner
    options = dict(config)
    for key in ('scratch_root', 'output_root'):
        options[key] = Path(options[key])
    binding = {k:ready[k] for k in ('run_id','snapshot','identity','scope')}
    runtime = owner.Runtime(**options, **candidate_mode(input_profile),
        expected_detection_binding=binding, dependency_admissions=tuple(a for r,a in dependencies),
        dependency_runtimes={a['admission_id']:r for r,a in dependencies})
    actual = owner.inspect_binding(runtime, binding['run_id'], binding['snapshot'])
    if actual != binding:
        raise ValueError('Detection公共inspect与原ready不符')
    return runtime, admit_current(owner, runtime, actual, output, guard)


def admit_resource(config, produced, dependencies, output, guard, *, input_profile=REAL_INPUT):
    from data_pipeline.analysis.resources import publication as owner
    from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
    options = dict(config)
    for key in ('scratch_root','output_root'):
        options[key] = Path(options[key])
    reader = ResourceObservationReader(options['dsn'], produced['run_id'], produced['snapshot'],
        produced['dataset_id'], guard=guard)
    binding = reader.inputs()
    runtime = owner.Runtime(**options, **candidate_mode(input_profile),
        expected_resource_binding=binding, upstream_runtime=dependencies[0][0],
        dependency_admissions=tuple(a for r,a in dependencies),
        dependency_runtimes={a['admission_id']:r for r,a in dependencies})
    actual = owner.inspect_binding(runtime, produced['run_id'], produced['snapshot'], produced['dataset_id'])
    if actual != binding:
        raise ValueError('Resource公共inspect与原Reader.inputs不符')
    return runtime, admit_current(owner, runtime, actual, output, guard)

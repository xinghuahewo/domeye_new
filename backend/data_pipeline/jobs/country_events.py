"""同驱动组合原 Country M3 入口；不另造科学结果或准入规则。"""
from dataclasses import asdict
from pathlib import Path
import sys
import uuid

from data_pipeline.analysis.country_events.snapshot_store import cleanup, close_stream
from data_pipeline.analysis.country_events.compute import Grid
from data_pipeline.analysis.country_events.route_inputs import M3Inputs
from data_pipeline.analysis.country_events.route_source_reader import M3Source
from data_pipeline.analysis.country_events.qualified_store import M3ComponentWriter
from data_pipeline.analysis.country_events import qualified_schema as m3_schema, snapshot_schema as component_schema
from data_pipeline.analysis.country_events.result_qualification import qualification_rows
from data_pipeline.analysis.country_events.qualified_index import prepare_qualified_country_index
from data_pipeline.analysis.country_events.selection_contract import QueryLimits, contract_json
from data_pipeline.analysis.country_events.qualified_reader import ResultLimits
from data_pipeline.analysis.country_events import result_admission as owner
from data_pipeline.jobs.result_admission import candidate_mode, admit_current
from data_pipeline.jobs.stage_runner import save_new


def produce_country(config, dependencies, output, guard, *, input_profile):
    """完整原输入捕获→原C2→24表C3→原C4→P1，关闭错误不覆盖主错。"""
    inputs = M3Inputs(tuple(a for r, a in dependencies),
        {a['admission_id']: r for r, a in dependencies}, guard=guard, **config['input_limits'])
    grid = Grid(**dict(config['grid'], samples_us=tuple(config['grid']['samples_us'])))
    logical_run_id = 'country-m3-' + uuid.uuid4().hex
    with M3Source.capture(inputs, **config['capture']) as source:
        writer = stream = added = None
        try:
            writer = M3ComponentWriter(config['dsn'], config['component_root'], source=source,
                logical_run_id=logical_run_id, window_us=tuple(config['window_us']),
                parameters=dict(grid=asdict(grid), c2_options=config['c2_limits']),
                audit_limits=config['audit_limits'], **config['component_limits'])
            stream = source.run_c2(grid, scratch_root=config['c2_scratch_root'], **config['c2_limits'])
            for row in stream:
                guard(); writer.append(row)
            finished, stream = stream, None
            close_stream(finished)
            writer.flush()
            def originals():
                for table, payload in writer.spool.execute('SELECT table_name,payload FROM rows ORDER BY sequence'):
                    guard()
                    yield m3_schema.row_decode(table, component_schema.decode(payload))
            added = qualification_rows(source, logical_run_id, originals(),
                guard=guard, **config['qualification_limits'])
            for row in added:
                guard(); writer.append(row)
            component = writer.seal()
            save_new(Path(output)/'原Country组件.json', asdict(component))
            read, proof = prepare_qualified_country_index(config['dsn'], component, source=source,
                output=config['query_root'], limits=QueryLimits(**config['query_limits']))
            binding = dict(read_binding=contract_json(read), proof=contract_json(proof))
            save_new(Path(output)/'原Country读模型.json', binding)
        except BaseException as error:
            if writer is not None:
                cleanup((lambda: writer.fail(error),), error)
            raise
        finally:
            cleanup((lambda: close_stream(stream),
                     lambda: added.close() if added is not None else None,
                     lambda: writer.close() if writer is not None else None), sys.exc_info()[1])
    options = dict(config['runtime'])
    options['limits'] = ResultLimits(**options['limits'])
    runtime = owner.Runtime(**options, dsn=config['dsn'], output_root=Path(config['query_root']),
        **candidate_mode(input_profile), expected_country_binding=binding,
        dependency_admissions=tuple(a for r, a in dependencies),
        dependency_runtimes={a['admission_id']: r for r, a in dependencies})
    return runtime, admit_current(owner, runtime, binding, Path(output)/'country-admit', guard)

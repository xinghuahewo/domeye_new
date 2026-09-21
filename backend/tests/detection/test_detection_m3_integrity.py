"""资格语义列全部绑定；同集合/计数的真实错指必须拒绝。"""
from copy import deepcopy
from collections import Counter
import json
import pytest
from data_pipeline.analysis.detection.qualification_contract import entry_id, validate_entry, validate_identity


def check_target_integrity(dsn,run,snapshot,bid):
    import psycopg2
    from data_pipeline.analysis.detection.store import connect_duckdb, literal, read_stored_rows
    from data_pipeline.analysis.detection.qualified_reader import read_coverage, read_binding
    original=list(read_coverage(dsn,run,snapshot,expected_binding_id=bid))
    identity=read_binding(dsn,run,snapshot)['identity']
    counts=Counter((r['incident_id'],r['revision']) for r in original if r['kind']=='event_qualification')
    row=next(r for r in sorted(original,key=lambda r:r['ordinal']!=18) if r['kind']=='event_qualification' and counts[r['incident_id'],r['revision']]>1)
    other=next(r for r in original if r['kind']=='event_qualification' and (r['incident_id'],r['revision'])!=(row['incident_id'],row['revision']))
    pg=psycopg2.connect(dsn);pg.autocommit=True
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres');db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
    try:
        for change in [dict(incident_id=other['incident_id'],revision=other['revision']),
                       dict(source_id=next(s for s in identity['selected_sources'] if s!=row['source_id'])),
                       dict(gap_id=next(r['gap_id'] for r in original if r['kind']=='scope_gap'))]:
            updated=dict(row,**change)
            from data_pipeline.analysis.detection.qualification_store import M3Store
            candidate=object.__new__(M3Store)
            candidate.guard=lambda:None
            candidate.closed_for_writes=False
            candidate.qcount=row['ordinal']
            candidate.run_id=run
            candidate.identity=identity
            with pytest.raises(ValueError,match='目标|身份'):
                candidate.emit_qualification(updated)
            altered=[updated if r['ordinal']==row['ordinal'] else r for r in original]
            assert {(r['incident_id'],r['revision']) for r in altered if r['kind']=='event_qualification'}==set(counts)
            assert updated['entry_id']==row['entry_id'] and updated['payload_json']==row['payload_json']
            sql=','.join(k+'=?' for k in change);pgsql=','.join(k+'=%s' for k in change)
            try:
                db.execute(f'UPDATE lake.det_{run}.m3_entries SET {sql} WHERE ordinal=?',[*change.values(),row['ordinal']])
                new=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
                with pg.cursor() as c:
                    c.execute(f'UPDATE detection.m3_entries SET {pgsql} WHERE run_id=%s AND ordinal=%s',(*change.values(),run,row['ordinal']))
                    c.execute('UPDATE detection.runs SET snapshot=%s WHERE run_id=%s',(new,run))
                with pytest.raises(ValueError,match='目标|身份'): list(read_coverage(dsn,run,new,expected_binding_id=bid))
            finally:
                db.execute(f'UPDATE lake.det_{run}.m3_entries SET {sql} WHERE ordinal=?',[*(row[k] for k in change),row['ordinal']])
                with pg.cursor() as c:
                    c.execute(f'UPDATE detection.m3_entries SET {pgsql} WHERE run_id=%s AND ordinal=%s',(*(row[k] for k in change),run,row['ordinal']))
                    c.execute('UPDATE detection.runs SET snapshot=%s WHERE run_id=%s',(snapshot,run))
        for key in ('store_schema_version','output_profile','qualification_rule','gap_rule','ordered_version'):
            for value in (None,'wrong/v999'):
                wrong=dict(identity)
                if value is None:wrong.pop(key)
                else:wrong[key]=value
                try:
                    with pg.cursor() as c:c.execute('UPDATE detection.runs SET identity=%s WHERE run_id=%s',(json.dumps(wrong),run))
                    with pytest.raises(ValueError):read_binding(dsn,run,snapshot)
                    with pytest.raises(ValueError):list(read_coverage(dsn,run,snapshot,expected_binding_id=bid))
                    for table in ('m3_entries','records','state_entries'):
                        with pytest.raises(ValueError):list(read_stored_rows(dsn,run,snapshot,table))
                finally:
                    with pg.cursor() as c:c.execute('UPDATE detection.runs SET identity=%s WHERE run_id=%s',(json.dumps(identity),run))
        # 即便重新算entry_id，也不能将不适用字段或Gap原ID与typed列分离。
        for original_row in original:
            validate_entry(original_row,run,identity)
        g=deepcopy(next(r for r in original if r['kind']=='scope_gap'))
        g['payload_json']=json.loads(g['payload_json']);g['gap_id']='wrong'
        g['payload_json']['target_ref']['gap_id']='wrong';g['entry_id']=entry_id(run,g)
        with pytest.raises(ValueError,match='Gap原ID'):validate_entry(g,run,identity)
    finally:db.close();pg.close()

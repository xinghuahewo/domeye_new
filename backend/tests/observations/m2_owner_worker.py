"""失锁后仍存活的旧invocation，不能补写三类选择状态。"""
import json,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from data_pipeline.bgp.archive.checkpoint import Owner, setup
c=json.loads(Path(sys.argv[1]).read_text());o=Owner(c['dsn'],c['run_id']);setup(o)
with o.transaction() as cur:
    cur.execute("INSERT INTO observation_m2.runs VALUES (%s,'p','{}','s','/unused','running',NULL,NULL)",(c['run_id'],))
Path(c['marker']).write_text(json.dumps({'pg_pid':o.pg.get_backend_pid()}))
while not Path(c['marker']+'.release').exists():time.sleep(.02)
try:
    with o.transaction() as cur:
        if c['operation']=='checkpoint':cur.execute("INSERT INTO observation_m2.checkpoints VALUES (%s,0,'{}')",(c['run_id'],))
        elif c['operation']=='seal':cur.execute("UPDATE observation_m2.runs SET state='observation_sealed' WHERE run_id=%s",(c['run_id'],))
        else:cur.execute("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",(c['run_id'],))
except Exception:
    Path(c['marker']+'.rejected').write_text('revoked')
else:raise AssertionError('失锁旧进程写入成功')
finally:o.close()

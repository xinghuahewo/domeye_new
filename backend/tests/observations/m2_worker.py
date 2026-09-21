"""人工进程验收入口，故障暂停由父测试进程用SIGKILL终止。"""
import json,os,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
c=json.loads(Path(sys.argv[1]).read_text())
def hook(label,ordinal,owner):
    if label==c.get('stop') and ordinal==c.get('ordinal'):
        Path(c['marker']).write_text(json.dumps(dict(label=label,ordinal=ordinal,pid=os.getpid(),pg_pid=owner.pg.get_backend_pid())))
        while not Path(c['marker']+'.release').exists():time.sleep(.02)
produce_checkpointed(c['manifest'],c['dsn'],c['output'],batch_rows=2,min_free_bytes=0,policy=c.get('policy','strict/v1'),catalog_data_path=c.get('catalog_data_path'),hook=hook)

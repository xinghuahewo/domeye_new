"""Q3-B增量：复用本任务实际Q3-A Token，不重新导入历史载体。"""
import hashlib
import json
import os
from pathlib import Path
import pytest
from data_pipeline.history.database_import import History, Token
from tests.historical.test_historical_import_bulk_budget import test_budget_change_stops_before_actual_read, test_fixed_budget_batching_and_qualification_cost

@pytest.fixture(scope='module')
def saved_history():
    location=os.environ.get('Q3_PRIVATE_ROOT')
    if not location: pytest.skip('需要显式本任务私有历史库')
    root=Path(location).resolve()
    prior=root/'acceptance-35e14f37925748b7aa7daaca0ee20071'/'交付回执.json'
    record=json.loads(prior.read_text())
    token=Token(**record['pg_token'])
    dsn=f'host={root / "socket"} port=28763 dbname=postgres'
    h=History(dsn,root);component=h.component(token)
    out=Path(os.environ['Q3B_INTEGRATION_EVIDENCE']);out.mkdir(parents=True,exist_ok=True)
    return root,dsn,token,component,out

@pytest.mark.parametrize('kind',['pg','sqlite'])
def test_original_q3a_token_full_bulk(saved_history,kind):
    root,dsn,_,_,out=saved_history
    prior=root/'acceptance-35e14f37925748b7aa7daaca0ee20071'
    record=json.loads((prior/'交付回执.json').read_text())
    token=Token(**record[kind+'_token']);h=History(dsn,root)
    component=h.component(token)
    hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/'history'/token.import_id).rglob('*') if p.is_file()}
    expected=[list(h.scan(token,i,limit=1000,purpose='audit')) for i in range(len(component['original']['tables']))]
    summaries=[]
    for cap in (1,None):
        with h.bulk(token,batch_rows=cap,purpose='audit') as reader: batches=list(reader)
        actual=[[row for b in batches if b['table_index']==i for row in b['rows']] for i in range(len(expected))]
        assert repr(actual)==repr(expected)
        assert reader.receipt['qualification']=='complete'
        assert [[r['occurrence']['ordinal'] for r in rows] for rows in actual]==[list(range(len(rows))) for rows in expected]
        summaries.append(reader.receipt)
    if kind=='pg':
        assert [r['values'] for r in expected[0]]==record['independent_pg_expected']
        assert expected[0][0]['typed']['numbers']['value']['lower_bounds']==[0,3]
    assert not (prior/'pg-freeze').exists() and not (prior/'sqlite-freeze').exists()
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in hashes.items())
    (out/('旧Q3A-'+kind+'.json')).write_text(json.dumps({'token':record[kind+'_token'],'table_rows':[len(rows) for rows in expected],'full_raw_typed_order_equal':True,'unchanged_files':hashes,'receipts':summaries},ensure_ascii=False,indent=2))

"""直接以旧pandas参数为oracle，保留词法/字节行的独立对照。"""
import csv
import hashlib
import pandas as pd
import pyarrow as pa
import pytest
from data_pipeline.analysis.features.reference import load_reference, USECOLS
from data_pipeline.bgp.input.reference_reader import rows

@pytest.mark.parametrize('asns',[['001','2','001'],['1.0','2.5','1.0'],['001','2_x','','001']])
def test_actual_pandas_inference_first_empty_and_underscore(tmp_path,asns):
    path=tmp_path/'ref.csv'
    with path.open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=USECOLS);writer.writeheader()
        for i,asn in enumerate(asns):writer.writerow({'asn':asn,'as_country':'IR' if i==0 else 'NA','as_country_cn':'伊朗' if i==0 else '', 'descr':'含逗号,与\n换行'})
        f.write('  \n\n')
    sha=hashlib.sha256(path.read_bytes()).hexdigest()
    class Reader:
        def reference_batches(self,source):
            assert source==sha
            for row in rows(path,sha):yield pa.RecordBatch.from_pylist([row])
    saved=[]
    ref=load_reference(Reader(),sha,path,sink=saved.append)
    oracle=pd.read_csv(path,keep_default_na=False,usecols=USECOLS).drop_duplicates(subset=['asn'],keep='first').set_index('asn')
    assert ref.country_names==dict(zip(oracle.index.astype(str),oracle.as_country_cn))
    assert ref.country_codes==dict(zip(oracle.index.astype(str),oracle.as_country))
    assert sum(bool(r['selected']) for r in saved)==len(oracle)
    assert any(r['csv_physical_end']>r['csv_physical_start'] for r in saved)
    assert any(r['csv_record_kind']=='whitespace_line' for r in saved)
    with pytest.raises(ValueError,match='原件'):load_reference(Reader(),'0'*64,path)

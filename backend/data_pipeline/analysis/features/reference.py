"""原CSV字节引用与旧pandas解释同时保存；不执行旧项目或来源表达式。"""
import hashlib
import json
from pathlib import Path
import pandas as pd

from data_pipeline.analysis.features.calculation import Reference
from data_pipeline.bgp.replay.route_replay import identity

USECOLS = ['asn','as_name','as_country','as_country_cn','type','org_name','org_name_cn','descr',
           'descr_cn','admin_info','import_as','export_as','is_ddos_provider','v4Peer','v6Peer','sibling_as']
REFERENCE_RULE = 'feature-pandas-legacy-39578fe/reference-rows-v2/v1'


def load_reference(reader, source_sha, raw_path, *, sink=lambda row: None, guard=lambda: None):
    path = Path(raw_path)
    before = path.stat()
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024**2), b''):
            digest.update(block); guard()
    if digest.hexdigest() != source_sha:
        raise ValueError('参考原件与已保存版本不一致')
    # 完整列上的旧读取参数和整列类型推断；不能改为dtype=str或分块推断。
    frame = pd.read_csv(path, keep_default_na=False, usecols=USECOLS)
    guard()
    selected = ~frame.duplicated(subset=['asn'], keep='first')
    legacy_keys = frame.set_index('asn').index.astype(str)
    names, codes, data_position, header = {}, {}, 0, None
    with path.open('rb') as stream:
        for batch in reader.reference_batches(source_sha):
            for stored in batch.to_pylist():
                if stored['rule'] != 'reference-rows/v2' or stored['location'] != 'csv':
                    raise ValueError('需要v2 CSV原词法与字节坐标')
                stream.seek(stored['raw_byte_offset'])
                remaining = stored['raw_byte_length']; raw_hash = hashlib.sha256()
                while remaining:
                    block = stream.read(min(1024**2, remaining))
                    if not block: raise ValueError('原参考字节截断')
                    remaining -= len(block); raw_hash.update(block); guard()
                if raw_hash.hexdigest() != stored['raw_record_sha256']:
                    raise ValueError('参考原行字节不匹配')
                lexical = json.loads(stored['raw_row'])
                row = {**stored, 'legacy_asn': None, 'legacy_asn_type': None,
                       'legacy_country': None, 'legacy_country_code': None, 'selected': None}
                if stored['csv_record_kind'] not in ('blank_line','whitespace_line'):
                    if header is None:
                        header = lexical
                    else:
                        if data_position >= len(frame): raise ValueError('词法行与旧pandas数据行对齐失败')
                        old = frame.iloc[data_position]
                        key = legacy_keys[data_position]
                        country, code = old['as_country_cn'], old['as_country']
                        country = country.item() if hasattr(country,'item') else country
                        code = code.item() if hasattr(code,'item') else code
                        keep = bool(selected.iloc[data_position])
                        row.update(legacy_asn=key, legacy_asn_type=str(frame['asn'].dtype),
                                   legacy_country=json.dumps(country, ensure_ascii=False, default=str),
                                   legacy_country_code=json.dumps(code, ensure_ascii=False, default=str), selected=keep)
                        if keep:
                            if key in names: raise ValueError('旧astype(str)后ASN键冲突，不能覆盖')
                            names[key], codes[key] = country, code
                        data_position += 1
                sink(row); guard()
    if data_position != len(frame): raise ValueError('词法与pandas行数不一致')
    after = path.stat()
    if (before.st_size,before.st_mtime_ns,before.st_ctime_ns,before.st_ino) != (after.st_size,after.st_mtime_ns,after.st_ctime_ns,after.st_ino):
        raise ValueError('解释期间参考原件变化')
    version = identity([source_sha, REFERENCE_RULE, pd.__version__, USECOLS, names, codes])
    # BIG_COUNTRY为原Feature配置的静态表，不从国家代码猜中文归属。
    big = dict(zip(('美国','巴西','中国','俄罗斯','印度','英国','印度尼西亚','德国','澳大利亚','波兰'),
                   ('US','BR','CN','RU','IN','GB','ID','DE','AU','PL')))
    return Reference(version, names, codes, big)

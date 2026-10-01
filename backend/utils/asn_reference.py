"""AS 身份参考的轻量只读入口；不加载前缀、域名或联系人资料。"""

from functools import lru_cache
import re
import threading

import pandas as pd

from config.config import AS_INFO_FILE
from utils import data_loader


REFERENCE_NOTE = 'AS 名称、组织、国家与类型为静态参考，历史适用性未知；参考未配置、缺失或不可读取时身份未知。'
_IDENTITY_FIELDS = ('as_name', 'as_country_cn', 'org_name', 'org_name_cn', 'type', 'type_cn')
_READ_LOCK = threading.Lock()


def _identity(row):
    return {key: str(row[key]).strip() for key in _IDENTITY_FIELDS
            if row.get(key) is not None and str(row[key]).strip()}


@lru_cache(maxsize=1)
def _read_identities(path):
    """每个配置路径在本进程中读取一次；部署替换参考后随进程重启刷新。"""
    if not path:
        return {}
    identities = {}
    try:
        with pd.read_csv(path, usecols=lambda name: name == 'asn' or name in _IDENTITY_FIELDS,
                         dtype=str, keep_default_na=False, chunksize=4096,
                         on_bad_lines='skip') as chunks:
            for chunk in chunks:
                if 'asn' not in chunk.columns:
                    return {}
                for row in chunk.to_dict(orient='records'):
                    asn = str(row['asn']).strip().upper()
                    asn = asn[2:] if asn.startswith('AS') else asn
                    if re.fullmatch(r'[0-9]{1,10}', asn) and 0 < int(asn) <= 4294967295:
                        identities.setdefault(str(int(asn)), _identity(row))
    except (OSError, UnicodeError, ValueError, pd.errors.ParserError):
        # 身份参考不是观测证据；不可读取时不影响 Feature/事件查询，也不保留半份索引。
        return {}
    return identities


def get_asn_identity(asn):
    """返回一个 AS 的身份副本；既有内存资料可复用，但绝不触发大型核心加载器。"""
    asn = str(asn)
    if asn in data_loader.as_info:
        return _identity(data_loader.as_info[asn])
    with _READ_LOCK:
        return dict(_read_identities(AS_INFO_FILE).get(asn, {}))

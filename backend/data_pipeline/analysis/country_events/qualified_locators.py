"""C4新增三表的内部定位索引；不提供绕过ComponentBinding/Proof的公开入口。"""
from collections import Counter

from data_pipeline.analysis.country_events.route_contract import DIMENSIONS
from data_pipeline.analysis.country_events import qualified_schema as m3_schema


DDL = '''
CREATE TABLE m3_locators(
    sequence INTEGER PRIMARY KEY, table_name TEXT NOT NULL, row_hash TEXT NOT NULL,
    logical_run TEXT NOT NULL, input_binding TEXT NOT NULL,
    window_start INTEGER NOT NULL, window_end INTEGER NOT NULL, dimension TEXT NOT NULL,
    incident TEXT, revision TEXT, cohort TEXT, entity TEXT, afi INTEGER, metric TEXT,
    content_id TEXT NOT NULL UNIQUE);
CREATE INDEX m3_result_scope ON m3_locators(table_name,logical_run,input_binding,window_start,window_end,dimension,sequence);
CREATE INDEX m3_event_scope ON m3_locators(incident,revision,table_name,dimension,sequence);
'''


def append_locator(db, sequence, item):
    """由C4完整C3流调用；仅存固定正文定位与索引，不复制资格正文。"""
    table, row = m3_schema.row_encode(sequence, item)
    if table not in m3_schema.NEW_TABLES:
        return
    value = item.value
    content_id = getattr(value, {'country_qualification': 'qualification_id',
                                 'country_coverage': 'coverage_id',
                                 'country_qualified_value': 'qualified_value_id'}[table])
    db.execute('INSERT INTO m3_locators VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
               (sequence, table, row['_row_hash'], value.logical_run_id, value.input_binding_id,
                *value.window_us, value.dimension, item.incident_id,
                str(item.revision) if item.revision is not None else None,
                getattr(value, 'cohort_id', None), getattr(value, 'entity_key', None),
                getattr(value, 'afi', None), getattr(value, 'metric', None), content_id))


class LocatorReader:
    """只有定位功能；实际C4须先验证固定组件、read_admissions与完整proof。

    本对象不签发ReadReceipt，不判断admitted_empty，不读取原证据或Detection状态。
    页间共用计数；lookahead也计入访问预算。
    """
    def __init__(self, db, logical_run_id, input_binding_id, *, max_rows, guard):
        self.db, self.logical_run_id, self.input_binding_id = db, logical_run_id, input_binding_id
        self.max_rows, self.guard = max_rows, guard
        self.stats = Counter()

    def page(self, table, window_us, dimension, *, after_sequence=-1, limit=256):
        self.guard()
        if (table not in m3_schema.NEW_TABLES or dimension not in DIMENSIONS
                or type(window_us) is not tuple or len(window_us) != 2
                or any(type(v) is not int for v in window_us) or window_us[0] >= window_us[1]
                or type(after_sequence) is not int or after_sequence < -1
                or type(limit) is not int or not 1 <= limit <= 10000):
            raise ValueError('M3 内部定位范围无效')
        if type(self.max_rows) is not int or self.max_rows <= 0:
            raise ValueError('resource_limit:M3_locator_rows')
        # 所有条件固定为实际已封存窗口的相等比较；不以窗口包含推断可用。
        cursor = self.db.execute('''SELECT sequence,table_name,row_hash,content_id FROM m3_locators
            WHERE table_name=? AND logical_run=? AND input_binding=? AND window_start=?
            AND window_end=? AND dimension=? AND sequence>? ORDER BY sequence LIMIT ?''',
            (table, self.logical_run_id, self.input_binding_id, *window_us, dimension, after_sequence, limit + 1))
        result = []
        try:
            for row in cursor:
                self.guard()
                if type(self.max_rows) is not int or self.max_rows <= 0:
                    raise ValueError('resource_limit:M3_locator_rows')
                self.stats['rows'] += 1
                if len(result) == limit:
                    return tuple(result), result[-1][0]
                result.append(tuple(row))
            return tuple(result), None
        finally:
            cursor.close()

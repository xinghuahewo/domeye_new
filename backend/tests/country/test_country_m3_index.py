"""临时SQLite内部定位测试；不冒充实际C4 proof/结果准入。"""
from dataclasses import replace
import sqlite3
import pytest

from tests.country.test_country_m3_schema import qualification, coverage
from data_pipeline.analysis.country_events.event_aggregation import C2Row
from data_pipeline.analysis.country_events.qualified_locators import DDL, append_locator, LocatorReader


def test_zero_event_coverage_locates_without_incident_and_exact_scope(tmp_path):
    with sqlite3.connect(tmp_path/'index.sqlite') as db:
        db.executescript(DDL)
        append_locator(db, 0, C2Row(None, None, coverage()))
        reader = LocatorReader(db, 'fixture-logical-run', 'fixture-binding', max_rows=10, guard=lambda: None)
        rows, after = reader.page('country_coverage', (100, 200), 'event_enumeration')
        assert len(rows) == 1 and rows[0][0] == 0 and after is None
        assert reader.page('country_coverage', (100, 201), 'event_enumeration') == ((), None)
        assert reader.page('country_coverage', (100, 200), 'event_anchor') == ((), None)
        other = LocatorReader(db, 'another-run', 'fixture-binding', max_rows=10, guard=lambda: None)
        assert other.page('country_coverage', (100, 200), 'event_enumeration') == ((), None)


def test_internal_pagination_preserves_raw_sequences_and_shared_budget(tmp_path):
    with sqlite3.connect(tmp_path/'index.sqlite') as db:
        db.executescript(DDL)
        for sequence in (2, 5, 9):
            q = qualification(effective_start_position=(2, sequence, 0, 0))
            append_locator(db, sequence, C2Row(None, None, q))
        reader = LocatorReader(db, 'fixture-logical-run', 'fixture-binding', max_rows=3, guard=lambda: None)
        rows, after = reader.page('country_qualification', (100, 200), 'event_enumeration', limit=1)
        assert [r[0] for r in rows] == [2] and after == 2 and reader.stats['rows'] == 2
        more,after=reader.page('country_qualification', (100, 200), 'event_enumeration', after_sequence=after, limit=1)
        assert [r[0] for r in more]==[5] and reader.stats['rows']==4
        complete = LocatorReader(db, 'fixture-logical-run', 'fixture-binding', max_rows=10, guard=lambda: None)
        assert [r[0] for r in complete.page('country_qualification', (100, 200), 'event_enumeration')[0]] == [2, 5, 9]

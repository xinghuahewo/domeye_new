"""原离线入口时间转换的隔离回归；不连接数据库或执行Resource生产。"""
import ast
from datetime import datetime, timezone
from pathlib import Path

import pytest


@pytest.mark.parametrize('position', [0, 1], ids=['sources', 'fixture-contexts'])
def test_frozen_entry_preserves_utc_z_and_offsets(position):
    source = Path(__file__).resolve().parents[3]/'scripts/pipeline/resource-frozen-run.py'
    tree = ast.parse(source.read_text())
    calls = sorted((node for node in ast.walk(tree) if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == 'fromisoformat'), key=lambda node: node.lineno)
    assert len(calls) == 2
    conversion = compile(ast.Expression(calls[position]), str(source), 'eval')
    for text, seconds in [('1970-01-01T00:01:40Z', 100),
                          ('1970-01-01T00:03:20Z', 200),
                          ('1970-01-01T00:05:00Z', 300),
                          ('1970-01-01T00:10:00Z', 600),
                          ('1970-01-01T00:01:40+00:00', 100),
                          ('1970-01-01T08:01:40+08:00', 100)]:
        context = {'snapshot_time': text}
        actual = eval(conversion, {'datetime': datetime, 's': {'context': context}, 'c': context})
        assert actual.tzinfo is not None
        assert actual.astimezone(timezone.utc) == datetime.fromtimestamp(seconds, timezone.utc)
        if text.endswith('+08:00'):
            assert actual.utcoffset().total_seconds() == 8*3600
    context = {'snapshot_time': 'invalid-time'}
    with pytest.raises(ValueError):
        eval(conversion, {'datetime': datetime, 's': {'context': context}, 'c': context})

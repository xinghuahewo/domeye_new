"""国家与 ASN 档案共用的已观测活动统计，不推定窗口完整性。"""


def _count(row, name):
    try:
        value = row[name]
        return None if value is None else int(value)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def _total(announce, withdraw):
    return None if announce is None or withdraw is None else announce + withdraw


def activity_summary(row):
    """只对明确存在的当前／前窗样本计算计数、比例和变化。"""
    sample_count = _count(row, 'sample_count') or 0
    previous_sample_count = _count(row, 'previous_sample_count') or 0
    announce = _count(row, 'announce') if sample_count else None
    withdraw = _count(row, 'withdraw') if sample_count else None
    previous_announce = _count(row, 'previous_announce') if previous_sample_count else None
    previous_withdraw = _count(row, 'previous_withdraw') if previous_sample_count else None
    total = _total(announce, withdraw)
    previous = _total(previous_announce, previous_withdraw)
    return {
        'announce': announce,
        'withdraw': withdraw,
        'update_total': total,
        'withdraw_rate': round(withdraw / total * 100, 1) if total else None,
        'previous_update_total': previous,
        'update_change_rate': (
            round((total - previous) / previous * 100, 1)
            if total is not None and previous else None
        ),
        'sample_count': sample_count,
        'previous_sample_count': previous_sample_count,
        'peak_updates': _count(row, 'peak_updates') if sample_count else None,
    }

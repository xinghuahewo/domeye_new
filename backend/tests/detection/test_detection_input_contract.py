"""通过公开入口验证独立复核发现的输入合同缺口。"""

from dataclasses import replace
import pytest
from tests.detection.test_detection_computation import engine, obs


@pytest.mark.parametrize("prefix", ["0.0.0.0/0", "::/0", "10.0.0.0/24"])
def test_invalid_action_fails_before_business_filter_and_stops(prefix):
    e = engine()
    before = e.export_state()
    rows = e.consume(obs(1, action="BOGUS", p=prefix))
    assert [r["kind"] for r in rows] == ["failed"]
    assert rows[0]["legacy"]["stage"] == "input"
    assert e.export_state()["status"] == "failed"
    assert (
        e.export_state()["compatibility_projection"]
        == before["compatibility_projection"]
    )
    assert e.export_state()["modules"] == before["modules"]
    with pytest.raises(ValueError):
        e.consume(obs(2))
    assert e.finish_file()[-1]["legacy"]["status"] == "failed"


def boundary():
    from tests.detection.test_detection_computation import KINDS
    from data_pipeline.analysis.detection import FileBoundary

    return FileBoundary(
        "fixture-file",
        "fixture-v1",
        "2026-02-28T00:00:00Z",
        {k: k + "_202602" for k in KINDS},
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("file_id", ""),
        ("file_id", "  "),
        ("source_version", ""),
        ("observed_at", "garbage"),
        ("observed_at", "2026-02-28T00:00:00"),
        ("observed_at", "2026-02-30T00:00:00Z"),
        ("observed_at", None),
        ("observed_at", "2026-01-31T23:59:59Z"),
        ("observed_at", "2027-04-01T00:00:00Z"),
    ],
)
def test_invalid_file_boundary_rejected_without_committing_state(field, value):
    e = engine()
    e.finish_file()
    before = e.export_state()
    with pytest.raises(ValueError):
        e.begin_file(replace(boundary(), **{field: value}))
    assert e.export_state() == before
    with pytest.raises(ValueError):
        e.consume(obs(1))
    with pytest.raises(ValueError):
        e.finish_file()
    # 拒绝的边界未提交；修正后可以明确打开有效文件。
    e.begin_file(boundary())
    assert not any(r["kind"] == "failed" for r in e.consume(obs(2)))
    assert e.finish_file()[-1]["legacy"]["status"] == "computed"


@pytest.mark.parametrize(
    "action,prefix,path,reason",
    [
        ("A", "0.0.0.0/0", "100 1", "default_route"),
        ("A", "::/0", "100 1", "default_route"),
        ("W", "0.0.0.0/0", "", "default_route"),
        ("W", "::/0", "", "default_route"),
        ("A", "10.0.0.0/24", "100 {1}", "announcement_as_set"),
        ("STATE", "not-a-prefix", "", "detection_state"),
    ],
)
def test_legal_business_filters_still_allow_next_observation(
    action, prefix, path, reason
):
    e = engine()
    rows = e.consume(obs(1, action=action, p=prefix, path=path))
    assert [r["kind"] for r in rows] == ["filtered"]
    assert rows[0]["legacy"]["reason"] == reason
    assert not any(r["kind"] == "failed" for r in e.consume(obs(2)))
    assert e.finish_file()[-1]["legacy"]["status"] == "computed"


@pytest.mark.parametrize("prefix", ["0.0.0.0/0", "::/0"])
def test_required_observation_identity_cannot_be_hidden_by_default_filter(prefix):
    e = engine()
    rows = e.consume(replace(obs(1, p=prefix), peer_ref=""))
    assert [r["kind"] for r in rows] == ["failed"]
    with pytest.raises(ValueError):
        e.consume(obs(2))


@pytest.mark.parametrize(
    "at",
    [
        "2026-02-01T00:00:00Z",
        "2026-02-28T08:00:00+08:00",
        "2027-03-31T23:59:59Z",
    ],
)
def test_valid_file_time_timezone_and_half_open_window(at):
    e = engine()
    e.finish_file()
    e.begin_file(replace(boundary(), observed_at=at))
    assert not any(r["kind"] == "failed" for r in e.consume(obs(1, at=at)))
    assert e.finish_file()[-1]["legacy"]["status"] == "computed"

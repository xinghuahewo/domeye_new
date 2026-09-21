"""旧角色未锚定时，规范分类不把随机旧过滤结果升级为确定结论。"""

CLASSIFICATION_RULE = "detection-moas-classification/v1"
DECISION_LOCATION = "来源 BGPHijack.py:200；兼容计算，外部副作用已移除。"


def interpret_classification(row):
    result = dict(
        classification_state="not_applicable",
        classified_hijack=None,
        classification_reason="not_a_moas_hijack_decision",
        classification_rule_version=CLASSIFICATION_RULE,
    )
    payload = row["legacy"]
    if row["kind"] == "rule_decision" and payload.get("rule") == DECISION_LOCATION:
        _, pair, origin = payload["inputs"]["args"]
        legacy_result = payload["result"][0] == 1
    else:
        if row.get("event_kind") not in ("moas", "hijack"):
            if row["kind"] not in ("hijack_start", "hijack_end"):
                return result
            payload = payload.get("moas_event_dict", {})
        pair = (payload.get("moas_as1"), payload.get("moas_as2"))
        origin, legacy_result = payload.get("ori_as"), payload.get("is_hijack")
    if not origin or origin not in pair:
        return {
            **result,
            "classification_state": "ambiguous",
            "classification_reason": "legacy_role_assignment_unanchored_may_change_filter_outcome",
        }
    return {
        **result,
        "classification_state": "legacy_rule_evaluated",
        "classified_hijack": legacy_result,
        "classification_reason": "anchored_legacy_rule_not_proof_of_real_attack",
    }

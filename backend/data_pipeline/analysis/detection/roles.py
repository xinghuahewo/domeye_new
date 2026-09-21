"""规范角色的最小证据门禁；原算法和legacy字段不变。"""

ROLE_RULE = "detection-moas-role/v2"


def interpret_roles(kind, record):
    result = dict(
        asn_roles="not_applicable",
        attacker_asn=None,
        victim_asn=None,
        role_reason="not_a_moas_hijack_role",
        role_rule_version=ROLE_RULE,
    )
    if kind == "sub_hijack":
        return {
            **result,
            "asn_roles": "legacy_set_valued_roles",
            "role_reason": "parent_origins_and_child_origins_retained_in_detail_no_single_asn_selection",
        }
    if kind not in ("moas", "hijack"):
        return result
    pair = (record.get("moas_as1"), record.get("moas_as2"))
    if not record.get("ori_as") or record["ori_as"] not in pair:
        return {
            **result,
            "asn_roles": "ambiguous",
            "role_reason": "original_as_missing_or_outside_moas_pair",
        }
    attacker, victim = record.get("hijacker_as"), record.get("hijacked_as")
    if not all(
        isinstance(value, str) and value.isdigit() for value in (attacker, victim)
    ):
        return {
            **result,
            "asn_roles": "unknown",
            "role_reason": "legacy_role_is_not_single_asn",
        }
    return {
        **result,
        "asn_roles": "legacy_heuristic_candidate",
        "attacker_asn": int(attacker),
        "victim_asn": int(victim),
        "role_reason": "original_as_in_pair_not_proof_of_attack_or_responsibility",
    }

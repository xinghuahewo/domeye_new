"""旧 39578fe 计算迁入；私有实现，不读取数据库、环境或原始文件。"""

from data_pipeline.analysis.detection._results import rule
import datetime
import itertools
from data_pipeline.analysis.detection._reference_helpers import get_as_admin, get_as_country_cn, get_as_descr, get_as_info, get_as_name, get_as_org_name


def as_relationship(relationships, as1, as2):
    """按原顺序查询六种证据；缺关系仍返回 -2，不推断反向关系。"""
    if relationships.get(as1) is not None:
        if relationships[as1].get("peers") is not None:
            if as2 in relationships[as1]["peers"]:
                return 0
    if relationships.get(as2) is not None:
        if relationships[as2].get("peers") is not None:
            if as1 in relationships[as2]["peers"]:
                return 0
    if relationships.get(as1) is not None:
        if relationships[as1].get("provider") is not None:
            if as2 in relationships[as1]["provider"]:
                return 1
    if relationships.get(as2) is not None:
        if relationships[as2].get("customer") is not None:
            if as1 in relationships[as2]["customer"]:
                return 1
    if relationships.get(as1) is not None:
        if relationships[as1].get("customer") is not None:
            if as2 in relationships[as1]["customer"]:
                return -1
    if relationships.get(as2) is not None:
        if relationships[as2].get("provider") is not None:
            if as1 in relationships[as2]["provider"]:
                return -1
    return -2


class BGPLeak:
    def __init__(self, output, bgp_info, bgp_rib):
        """来源 BGPLeak.py:65；兼容计算，外部副作用已移除。"""
        self.output = output
        self.bgp_info = bgp_info
        self.bgp_rib = bgp_rib
        self.leak_phenomenon_table = None
        self.leak_event_table = None
        self.event_table = None
        self.prefix_event = dict()
        self.prefix_path_dict = dict()
        self.phenomenon_dict = dict()
        self.phenomenon_init_id = dict()
        self.event_init_id = dict()

    def init_leak(self):
        """来源 BGPLeak.py:97；兼容计算，外部副作用已移除。"""

    def get_table(self):
        """来源 BGPLeak.py:100；兼容计算，外部副作用已移除。"""
        return (self.leak_phenomenon_table, self.leak_event_table, self.event_table)

    def set_table(self, leak_phenomenon_table, leak_event_table, event_table):
        """来源 BGPLeak.py:103；兼容计算，外部副作用已移除。"""
        self.leak_phenomenon_table = leak_phenomenon_table
        self.leak_event_table = leak_event_table
        self.event_table = event_table

    @rule("来源 BGPLeak.py:108；兼容计算，外部副作用已移除。")
    def get_as_rel(self, as1, as2):
        """来源 BGPLeak.py:108；兼容计算，外部副作用已移除。"""
        lookup = getattr(self.bgp_info, "relationship_lookup", None)
        if lookup is not None:
            return lookup(as1, as2)
        return as_relationship(self.bgp_info.as_rel_dict, as1, as2)

    @rule("来源 BGPLeak.py:141；兼容计算，外部副作用已移除。")
    def leak_level(self, prefix, asn):
        """来源 BGPLeak.py:141；兼容计算，外部副作用已移除。"""
        descr_info = "这仅仅是一个可能的路由泄漏事件"
        level = "low"
        if self.bgp_info.prefix_info.get(prefix) is not None:
            num = self.bgp_info.prefix_info[prefix]["domain_num"]
            auth_num = self.bgp_info.prefix_info[prefix]["domain_auth_num"]
            if num > 0 or auth_num > 0:
                descr_info = ""
                if num > 30:
                    descr_info = "在前缀中有" + str(num) + " 个普通网站。"
                    level = "high"
                else:
                    descr_info = "在前缀中有" + str(num) + " 个普通网站。"
                    level = "middle"
                if auth_num > 15:
                    descr_info = (
                        descr_info
                        + "在前缀中有"
                        + str(auth_num)
                        + " 个权威解析服务器。"
                    )
                    level = "high"
                elif level != "high":
                    descr_info = (
                        descr_info
                        + "在前缀中有"
                        + str(auth_num)
                        + " 个权威解析服务器。"
                    )
                    level = "middle"
            domain = list()
            if self.bgp_info.prefix_info[prefix]["domain"] not in [None, ""]:
                domain.extend(
                    self.output.parse_domains(
                        self.bgp_info.prefix_info[prefix]["domain"], "BGPLeak.py:168"
                    )
                )
            if self.bgp_info.prefix_info[prefix]["domain_auth"] not in [None, ""]:
                domain.extend(
                    self.output.parse_domains(
                        self.bgp_info.prefix_info[prefix]["domain_auth"],
                        "BGPLeak.py:170",
                    )
                )
            for d in domain:
                if self.bgp_info.important_domain_dict.get(d) is not None:
                    level = "high"
                    try:
                        descr_info = (
                            descr_info
                            + "在前缀中有"
                            + self.bgp_info.important_domain_dict.get("name", "重要")
                            + "的网站 。"
                        )
                    except:
                        descr_info = descr_info + "在前缀中有重要网站"
        try:
            if self.bgp_info.important_as_dict.get(int(asn)) is not None:
                level = "high"
                if descr_info != "":
                    descr_info = (
                        descr_info
                        + " 并且 "
                        + asn
                        + " 是 Cloud|IDC|CDN 或者 顶级内容提供商 (top content provider)。"
                    )
        except:
            pass
        return (level, descr_info)

    @rule("来源 BGPLeak.py:190；兼容计算，外部副作用已移除。")
    def is_leak_event(self, leak_by, leak_to):
        """来源 BGPLeak.py:190；兼容计算，外部副作用已移除。"""
        if leak_by == leak_to:
            return (0, "by equal to")
        leak_by_org = get_as_org_name(self.bgp_info.as_info, leak_by)
        leak_to_org = get_as_org_name(self.bgp_info.as_info, leak_to)
        if leak_to_org:
            if "个人" in leak_to_org:
                return (0, "personal network")
        if leak_by_org:
            if "个人" in leak_by_org:
                return (0, "personal network")
        if leak_by_org == "未知组织" or leak_to_org == "未知组织":
            return (0, "unknown org")
        if leak_by_org == leak_to_org:
            return (0, "org equal")
        as_set = [leak_to, leak_by]
        for asn in as_set:
            if "{" in asn:
                return (0, "as set")
            asn_num = int(asn)
            if asn_num in range(64511, 65536) or asn_num > 4294967295:
                return (0, "private as")
        (leak_by_str, leak_to_str) = ("AS" + leak_by, "AS" + leak_to)
        if leak_by in self.bgp_info.as_info.keys():
            leak_by_import = self.bgp_info.as_info[leak_by]["import_as"]
            leak_by_export = self.bgp_info.as_info[leak_by]["export_as"]
            if leak_to_str in leak_by_import:
                return (0, leak_by + " import filter " + leak_to)
            if leak_to_str in leak_by_export:
                return (0, leak_by + " export filter " + leak_to)
        if leak_to in self.bgp_info.as_info.keys():
            leak_to_import = self.bgp_info.as_info[leak_to]["import_as"]
            leak_to_export = self.bgp_info.as_info[leak_to]["export_as"]
            if leak_by_str in leak_to_import:
                return (0, leak_to + " import filter " + leak_by)
            if leak_by_str in leak_to_export:
                return (0, leak_to + " export filter " + leak_by)
        return (1, "possible leak")

    def leak_detect(self, t, flag, prefix, vp, as_path):
        """来源 BGPLeak.py:238；兼容计算，外部副作用已移除。"""
        if flag == "A":
            if prefix not in self.prefix_event.keys():
                self.prefix_event[prefix] = dict()
                self.prefix_event[prefix]["last_leak_time"] = ""
                if prefix in self.phenomenon_init_id:
                    self.prefix_event[prefix]["phenomenon_id"] = (
                        self.phenomenon_init_id[prefix]
                    )
                else:
                    self.prefix_event[prefix]["phenomenon_id"] = 0
                if prefix in self.event_init_id:
                    self.prefix_event[prefix]["event_id"] = self.event_init_id[prefix]
                else:
                    self.prefix_event[prefix]["event_id"] = 0
            self.check_leak(t, vp, prefix, as_path)

    def check_leak(self, t, vp, prefix, vp_path):
        """来源 BGPLeak.py:264；兼容计算，外部副作用已移除。"""
        if "{" in vp_path:
            return
        vp_path_fields_o = vp_path.split(" ")
        vp_path_fields = [k for (k, g) in itertools.groupby(vp_path_fields_o)]
        if prefix in self.prefix_path_dict and self.prefix_path_dict[prefix] is True:
            return
        else:
            ori_asn = vp_path_fields[-1]
            length = len(vp_path_fields)
            if length > 3:
                for i in range(length - 1, -1, -1):
                    if i - 2 == -1:
                        break
                    if (
                        self.get_as_rel(vp_path_fields[i], vp_path_fields[i - 1]) == -1
                        and self.get_as_rel(
                            vp_path_fields[i - 2], vp_path_fields[i - 1]
                        )
                        == -1
                    ):
                        if (
                            self.output.triplet(
                                self.bgp_info.triplet_info,
                                vp_path_fields[i],
                                vp_path_fields[i - 1],
                                vp_path_fields[i - 2],
                            )
                            >= 0.2
                        ):
                            continue
                        if self.prefix_event[prefix]["last_leak_time"] != "":
                            last_leak_time = self.prefix_event[prefix]["last_leak_time"]
                            last_leak_time = datetime.datetime.strptime(
                                last_leak_time, "%Y-%m-%d %H:%M:%S"
                            )
                            start_time = datetime.datetime.strptime(
                                t, "%Y-%m-%d %H:%M:%S"
                            )
                            if last_leak_time.month != start_time.month:
                                self.prefix_event[prefix]["phenomenon_id"] = 0
                                self.prefix_event[prefix]["event_id"] = 0
                        self.prefix_event[prefix]["last_leak_time"] = t
                        (level, level_info) = self.leak_level(prefix, ori_asn)
                        self.prefix_event[prefix]["phenomenon_id"] += 1
                        if level == "high" or level == "middle":
                            self.prefix_event[prefix]["event_id"] += 1
                        phenomenon_id = self.prefix_event[prefix]["phenomenon_id"]
                        self.phenomenon_dict.setdefault(prefix, dict()).setdefault(
                            phenomenon_id, dict()
                        )
                        self.phenomenon_dict[prefix][phenomenon_id].update(
                            leak_phenomenon_table=self.leak_phenomenon_table,
                            leak_event_table=self.leak_event_table,
                            event_table=self.event_table,
                            legacy_event_id=self.prefix_event[prefix]["event_id"]
                            if level in ("middle", "high")
                            else None,
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["s_time"] = (
                            datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                        )
                        leak_by = vp_path_fields[i - 1]
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_by"] = leak_by
                        self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_by_country"
                        ] = get_as_country_cn(self.bgp_info.as_info, leak_by)
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_by_name"] = (
                            get_as_name(self.bgp_info.as_info, leak_by)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_by_org"] = (
                            get_as_org_name(self.bgp_info.as_info, leak_by)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_by_descr"] = (
                            get_as_descr(self.bgp_info.as_info, leak_by)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_by_admin"] = (
                            get_as_admin(self.bgp_info.as_info, leak_by)
                        )
                        leak_to = vp_path_fields[i - 2]
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_to"] = leak_to
                        self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_to_country"
                        ] = get_as_country_cn(self.bgp_info.as_info, leak_to)
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_to_name"] = (
                            get_as_name(self.bgp_info.as_info, leak_to)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_to_org"] = (
                            get_as_org_name(self.bgp_info.as_info, leak_to)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_to_descr"] = (
                            get_as_descr(self.bgp_info.as_info, leak_to)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_to_admin"] = (
                            get_as_admin(self.bgp_info.as_info, leak_to)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_vp"] = vp
                        self.phenomenon_dict[prefix][phenomenon_id]["as_path"] = vp_path
                        self.phenomenon_dict[prefix][phenomenon_id]["prefix_ori_as"] = (
                            ori_asn
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["ori_as_org"] = (
                            get_as_org_name(self.bgp_info.as_info, ori_asn)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id][
                            "ori_as_country"
                        ] = get_as_country_cn(self.bgp_info.as_info, ori_asn)
                        self.phenomenon_dict[prefix][phenomenon_id]["ori_as_name"] = (
                            get_as_name(self.bgp_info.as_info, ori_asn)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["ori_as_descr"] = (
                            get_as_descr(self.bgp_info.as_info, ori_asn)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["ori_as_admin"] = (
                            get_as_admin(self.bgp_info.as_info, ori_asn)
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["leak_level"] = (
                            level
                        )
                        self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_level_info"
                        ] = level_info
                        leak_by = self.phenomenon_dict[prefix][phenomenon_id]["leak_by"]
                        leak_by_name = self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_by_name"
                        ]
                        leak_by_org = self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_by_org"
                        ]
                        leak_by_country = self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_by_country"
                        ]
                        leak_to = self.phenomenon_dict[prefix][phenomenon_id]["leak_to"]
                        leak_to_name = self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_to_name"
                        ]
                        leak_to_org = self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_to_org"
                        ]
                        leak_to_country = self.phenomenon_dict[prefix][phenomenon_id][
                            "leak_to_country"
                        ]
                        s_time = self.phenomenon_dict[prefix][phenomenon_id]["s_time"]
                        prefix_ori_as = self.phenomenon_dict[prefix][phenomenon_id][
                            "prefix_ori_as"
                        ]
                        ori_as_org = self.phenomenon_dict[prefix][phenomenon_id][
                            "ori_as_org"
                        ]
                        ori_as_name = self.phenomenon_dict[prefix][phenomenon_id][
                            "ori_as_name"
                        ]
                        ori_as_country = self.phenomenon_dict[prefix][phenomenon_id][
                            "ori_as_country"
                        ]
                        event_info = "北京时间 {} , 归属于 {} 的前缀 {} 被 {} 泄漏给 {}。".format(
                            s_time,
                            get_as_info(prefix_ori_as, ori_as_name, ori_as_country),
                            prefix,
                            get_as_info(leak_by, leak_by_name, leak_by_country),
                            get_as_info(leak_to, leak_to_name, leak_to_country),
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["event_info"] = (
                            event_info
                        )
                        self.prefix_path_dict[prefix] = True
                        self.phenomenon_dict[prefix][phenomenon_id]["is_leak"] = None
                        self.phenomenon_dict[prefix][phenomenon_id]["filter_reason"] = (
                            None
                        )
                        self.phenomenon_dict[prefix][phenomenon_id]["rule_state"] = (
                            "not_evaluated_level_gate"
                        )
                        if level == "middle" or level == "high":
                            (is_leak, filter_reason) = self.is_leak_event(
                                leak_by=leak_by, leak_to=leak_to
                            )
                            self.phenomenon_dict[prefix][phenomenon_id].update(
                                is_leak=bool(is_leak),
                                filter_reason=filter_reason,
                                rule_state="evaluated",
                            )
                            if is_leak:
                                event_id = self.prefix_event[prefix]["event_id"]
                                self.output.capture(
                                    "leak_event_record",
                                    phenomenon_dict=self.phenomenon_dict,
                                    source=self.output.source,
                                    prefix=prefix,
                                    phenomenon_id=phenomenon_id,
                                    event_id=event_id,
                                    leak_event_table=self.leak_event_table,
                                    is_leak=is_leak,
                                    filter_reason=filter_reason,
                                )
                                detail_url = "{}/{}/{}/{}/{}".format(
                                    "leak",
                                    s_time,
                                    prefix.replace("/", "-"),
                                    event_id,
                                    self.output.source,
                                )
                                org_name = leak_by_org
                                if leak_by_country in ["中国"] or leak_to_country in [
                                    "中国"
                                ]:
                                    state = "judge"
                                    is_domestic = True
                                else:
                                    state = "abroad"
                                    is_domestic = False
                                attacker_as = (
                                    "AS{}\n({})".format(leak_by, leak_by_name)
                                    if leak_by_name
                                    else "AS{}".format(leak_by)
                                )
                                attacked_as = (
                                    "AS{}\n({})".format(prefix_ori_as, ori_as_name)
                                    if ori_as_name
                                    else "AS{}".format(prefix_ori_as)
                                )
                                attacker_org = leak_by_org
                                attacked_org = ori_as_org
                                attacker_country = leak_by_country
                                attacked_country = ori_as_country
                                self.output.capture(
                                    "event_start",
                                    source=self.output.source,
                                    event_type="路由泄漏",
                                    level=self.phenomenon_dict[prefix][phenomenon_id][
                                        "leak_level"
                                    ],
                                    s_time=s_time,
                                    e_time=None,
                                    duration=None,
                                    attacker_as=attacker_as,
                                    attacked_as=attacked_as,
                                    affected_prefix=prefix,
                                    event_info=event_info,
                                    detail_url=detail_url,
                                    attacker_org=attacker_org,
                                    attacked_org=attacked_org,
                                    attacker_country=attacker_country,
                                    attacked_country=attacked_country,
                                    state=state,
                                    is_domestic=is_domestic,
                                    table=self.event_table,
                                )
                        if (
                            prefix in self.prefix_path_dict
                            and phenomenon_id in self.phenomenon_dict[prefix]
                        ):
                            self.output.retire(
                                "leak",
                                prefix,
                                phenomenon_id,
                                self.phenomenon_dict[prefix][phenomenon_id],
                            )
                            del self.phenomenon_dict[prefix][phenomenon_id]

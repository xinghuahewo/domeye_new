"""旧 39578fe 计算迁入；私有实现，不读取数据库、环境或原始文件。"""

from data_pipeline.analysis.detection._results import rule
from data_pipeline.common.prefix_networks import network_facts
import datetime
from data_pipeline.analysis.detection._reference_helpers import get_as_admin, get_as_country_cn, get_as_descr, get_as_info, get_as_name, get_as_org_name, get_as_org_name_en, get_duration


class BGPSubHijack:
    def __init__(self, output, bgp_info, bgp_rib):
        """来源 BGPSubHijack.py:63；兼容计算，外部副作用已移除。"""
        self.bgp_info = bgp_info
        self.bgp_rib = bgp_rib
        self.prefix_event = {}
        self.sub_hijack_dict = dict()
        self.output = output
        self.sub_hijack_table = None
        self.event_table = None
        self.moas_init_id = dict()
        self.sub_hijack_init_id = dict()

    def __init_state(self):
        """来源 BGPSubHijack.py:100；兼容计算，外部副作用已移除。"""
        for prefix in self.prefix_event.keys():
            (
                is_sub_hijack,
                parent,
                pre,
                par_origin_set,
                origin_set,
                is_true,
                filter_reason,
            ) = self.get_state(prefix)
            if is_sub_hijack is not None and is_sub_hijack is True:
                self.prefix_event[prefix]["is_sub_hijack"] = True
            elif is_sub_hijack is not None and is_sub_hijack is False:
                self.prefix_event[prefix]["is_sub_hijack"] = False

    def init_sub_hijack(self, prefix_dict):
        """来源 BGPSubHijack.py:111；兼容计算，外部副作用已移除。"""
        for prefix, vp_paths in prefix_dict.items():
            self.prefix_event.setdefault(prefix, dict())
            self.prefix_event[prefix]["last_sub_hijack_time"] = ""
            self.prefix_event[prefix]["sub_hijack_id"] = (
                self.sub_hijack_init_id[prefix]
                if prefix in self.sub_hijack_init_id
                else 0
            )
        self.__init_state()

    def get_table(self):
        """来源 BGPSubHijack.py:124；兼容计算，外部副作用已移除。"""
        return (self.sub_hijack_table, self.event_table)

    def set_table(self, sub_hijack_table, event_table):
        """来源 BGPSubHijack.py:127；兼容计算，外部副作用已移除。"""
        self.sub_hijack_table = sub_hijack_table
        self.event_table = event_table

    @rule("来源 BGPSubHijack.py:131；兼容计算，外部副作用已移除。")
    def is_sub_hijack_event(self, ori_as, par_origin_set, prefix, parent):
        """来源 BGPSubHijack.py:131；兼容计算，外部副作用已移除。"""
        if len(par_origin_set) > 1:
            return (False, "parent origin as num more than 1")
        if len(par_origin_set) < 1:
            return (False, "parent origin as num less than 1")
        ori_as_org = get_as_org_name(self.bgp_info.as_info, ori_as)
        if ori_as_org:
            if "个人" in ori_as_org:
                return (False, "personal network")
            if ori_as_org == "未知组织":
                return (False, "unknown org")
        for par_ori_as in par_origin_set:
            par_ori_as_org = get_as_org_name(self.bgp_info.as_info, par_ori_as)
            if par_ori_as_org:
                if par_ori_as_org == "未知组织":
                    return (False, "unknown org")
                if "个人" in par_ori_as_org:
                    return (False, "personal network")
        origin_set = set()
        origin_set.add(ori_as)
        if prefix in self.bgp_info.prefix_info.keys():
            if self.bgp_info.prefix_info[prefix]["route"]:
                set1 = set(self.bgp_info.prefix_info[prefix]["route"].split("|"))
                origin_set = origin_set.union(set1)
        if parent in self.bgp_info.prefix_info.keys():
            if self.bgp_info.prefix_info[parent]["route"]:
                set2 = set(self.bgp_info.prefix_info[parent]["route"].split("|"))
                par_origin_set = par_origin_set.union(set2)
        for ori_as in origin_set:
            for par_asn in par_origin_set:
                peers = set()
                siblings = set()
                hijack_as_set = [ori_as, par_asn]
                for asn in hijack_as_set:
                    if "{" in asn:
                        return (False, "as_set")
                    if "_" in asn:
                        return (False, "private as")
                    asn_num = int(asn)
                    if asn_num in range(64511, 65536) or asn_num > 4294967295:
                        return (False, "private as")
                (ori_as_str, par_asn_str) = ("AS" + ori_as, "AS" + par_asn)
                if ori_as in self.bgp_info.as_info.keys():
                    ori_import = self.bgp_info.as_info[ori_as]["import_as"]
                    ori_export = self.bgp_info.as_info[ori_as]["export_as"]
                    if par_asn_str in ori_import:
                        return (False, ori_as + " import filter " + par_asn)
                    if par_asn_str in ori_export:
                        return (False, ori_as + " export filter " + par_asn)
                if par_asn in self.bgp_info.as_info.keys():
                    par_import = self.bgp_info.as_info[par_asn]["import_as"]
                    par_export = self.bgp_info.as_info[par_asn]["export_as"]
                    if ori_as_str in par_import:
                        return (False, par_asn + " import filter " + ori_as)
                    if ori_as_str in par_export:
                        return (False, par_asn + " export filter " + ori_as)
                if self.bgp_info.as_rel_dict.get(ori_as) is not None:
                    if self.bgp_info.as_rel_dict[ori_as].get("provider") is not None:
                        provider = set(self.bgp_info.as_rel_dict[ori_as]["provider"])
                        if par_asn in provider:
                            return (False, "as rel provider-customer")
                if self.bgp_info.as_rel_dict.get(ori_as) is not None:
                    if self.bgp_info.as_rel_dict[ori_as].get("customer") is not None:
                        customer = set(self.bgp_info.as_rel_dict[ori_as]["customer"])
                        if par_asn in customer:
                            return (False, "as rel provider-customer")
                if self.bgp_info.as_rel_dict.get(par_asn) is not None:
                    if self.bgp_info.as_rel_dict[par_asn].get("provider") is not None:
                        provider = set(self.bgp_info.as_rel_dict[par_asn]["provider"])
                        if ori_as in provider:
                            return (False, "as rel provider-customer")
                if self.bgp_info.as_rel_dict.get(par_asn) is not None:
                    if self.bgp_info.as_rel_dict[par_asn].get("customer") is not None:
                        customer = set(self.bgp_info.as_rel_dict[par_asn]["customer"])
                        if ori_as in customer:
                            return (False, "as rel provider-customer")
                if self.bgp_info.as_rel_dict.get(ori_as) is not None:
                    if self.bgp_info.as_rel_dict[ori_as].get("peers") is not None:
                        peers = set(self.bgp_info.as_rel_dict[ori_as]["peers"])
                        if par_asn in peers:
                            return (False, "as rel peer")
                if self.bgp_info.as_rel_dict.get(par_asn) is not None:
                    if self.bgp_info.as_rel_dict[par_asn].get("peers") is not None:
                        peers = set(self.bgp_info.as_rel_dict[par_asn]["peers"])
                        if ori_as in peers:
                            return (False, "as rel peer")
                if self.bgp_info.as_rel_dict.get(ori_as) is not None:
                    if self.bgp_info.as_rel_dict[ori_as].get("sibling") is not None:
                        sibling = set(self.bgp_info.as_rel_dict[ori_as]["sibling"])
                        if par_asn in sibling:
                            return (False, "as rel same org")
                if self.bgp_info.as_rel_dict.get(par_asn) is not None:
                    if self.bgp_info.as_rel_dict[par_asn].get("sibling") is not None:
                        sibling = set(self.bgp_info.as_rel_dict[par_asn]["sibling"])
                        if ori_as in sibling:
                            return (False, "as rel same org")
                ori_org_name_en = get_as_org_name_en(self.bgp_info.as_info, par_asn)
                pre_org_name_en = get_as_org_name_en(self.bgp_info.as_info, ori_as)
                if pre_org_name_en and ori_org_name_en:
                    if pre_org_name_en == ori_org_name_en:
                        return (False, "as rel same org")
                for asn in hijack_as_set:
                    asn_int = int(asn)
                    if asn in self.bgp_info.as_info.keys():
                        if self.bgp_info.as_info[asn]["is_ddos_provider"] == True:
                            return (False, asn + " is AntiDDoS provider")
                        if "v4Peer" in self.bgp_info.as_info[asn].keys():
                            temp_peer = set(self.bgp_info.as_info[asn]["v4Peer"])
                            temp_peer.discard(asn_int)
                            if temp_peer is not None:
                                peers = peers | temp_peer
                        if "v6Peer" in self.bgp_info.as_info[asn].keys():
                            temp_peer = set(self.bgp_info.as_info[asn]["v6Peer"])
                            temp_peer.discard(asn_int)
                            if temp_peer is not None:
                                peers = peers | temp_peer
                        if "sibling_as" in self.bgp_info.as_info[asn].keys():
                            temp_siblings = set(
                                self.bgp_info.as_info[asn]["sibling_as"]
                            )
                            temp_siblings.discard(asn_int)
                            if temp_siblings is not None:
                                siblings = siblings | temp_siblings
                for asn in hijack_as_set:
                    asn_int = int(asn)
                    if peers is not None:
                        if asn_int in peers:
                            return (False, "peer")
                    if siblings is not None:
                        if asn_int in siblings:
                            return (False, "siblings")
        return (True, "possible hijack")

    @rule("来源 BGPSubHijack.py:300；兼容计算，外部副作用已移除。")
    def sub_hijack_level(self, prefix, origin_set):
        """来源 BGPSubHijack.py:300；兼容计算，外部副作用已移除。"""
        level = "low"
        descr_info = "这是一个普通的子前缀劫持事件。"
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
                try:
                    domain.extend(
                        self.output.parse_domains(
                            self.bgp_info.prefix_info[prefix]["domain"],
                            "BGPSubHijack.py:335",
                        )
                    )
                except Exception:
                    pass
            if self.bgp_info.prefix_info[prefix]["domain_auth"] not in [None, ""]:
                try:
                    domain.extend(
                        self.output.parse_domains(
                            self.bgp_info.prefix_info[prefix]["domain_auth"],
                            "BGPSubHijack.py:340",
                        )
                    )
                except Exception:
                    pass
            for d in domain:
                if self.bgp_info.important_domain_dict.get(d) is not None:
                    level = "high"
                    descr_info = (
                        descr_info
                        + "在前缀中有"
                        + self.bgp_info.important_domain_dict.get("name", "重要")
                        + "的网站 。"
                    )
        for asn in origin_set:
            try:
                if self.bgp_info.important_as_dict.get(int(asn)) is not None:
                    level = "high"
                    if descr_info != "":
                        descr_info = (
                            descr_info
                            + " 并且 "
                            + asn
                            + " 是 Cloud|IDC|CDN 或者 顶级内容提供商(top content provider)。"
                        )
            except:
                continue
        return (level, descr_info)

    @rule("来源 BGPSubHijack.py:360；兼容计算，外部副作用已移除。")
    def get_state(self, prefix):
        """来源 BGPSubHijack.py:360；兼容计算，外部副作用已移除。"""
        try:
            prefix_net = network_facts(prefix)
        except Exception:
            return (None, "", "", set(), set(), False, "invalid prefix")
        version = prefix_net.version
        hijacker_as_set = set()
        (is_sub_hijack, filter_reason) = (False, "")
        if version == 4:
            if not self.bgp_rib.ipv4_tree.has_key(prefix):
                return (False, "", "", set(), set(), False, "")
            origin_set = self.bgp_rib.ipv4_tree.get(prefix)
            parent = self.bgp_rib.ipv4_tree.parent(prefix)
            if parent is None or parent == "0.0.0.0/0":
                return (False, "", "", set(), set(), False, "")
            try:
                parent_net = network_facts(parent)
            except Exception:
                return (None, "", "", set(), set(), False, "invalid parent prefix")
            par_origin_set = self.bgp_rib.ipv4_tree.get(parent)
            if prefix_net.prefixlen - parent_net.prefixlen > 8:
                return (
                    False,
                    parent,
                    prefix,
                    par_origin_set,
                    set(),
                    False,
                    "prefix length difference is too large",
                )
            for asn in origin_set:
                if asn not in par_origin_set:
                    (is_sub_hijack, filter_reason) = self.is_sub_hijack_event(
                        asn, par_origin_set, prefix, parent
                    )
                    if is_sub_hijack is True:
                        hijacker_as_set.add(asn)
            if hijacker_as_set:
                return (
                    True,
                    parent,
                    prefix,
                    par_origin_set,
                    hijacker_as_set,
                    True,
                    filter_reason,
                )
            return (False, parent, prefix, par_origin_set, set(), False, filter_reason)
        if version == 6:
            if not self.bgp_rib.ipv6_tree.has_key(prefix):
                return (False, "", "", set(), set(), False, "")
            origin_set = self.bgp_rib.ipv6_tree.get(prefix)
            parent = self.bgp_rib.ipv6_tree.parent(prefix)
            if parent is None or parent == "::/0":
                return (False, "", "", set(), set(), False, "")
            try:
                parent_net = network_facts(parent)
            except Exception:
                return (None, "", "", set(), set(), False, "invalid parent prefix")
            par_origin_set = self.bgp_rib.ipv6_tree.get(parent)
            if prefix_net.prefixlen - parent_net.prefixlen > 3:
                return (
                    False,
                    parent,
                    prefix,
                    par_origin_set,
                    set(),
                    False,
                    "prefix length difference is too large",
                )
            for asn in origin_set:
                if asn not in par_origin_set:
                    (is_sub_hijack, filter_reason) = self.is_sub_hijack_event(
                        asn, par_origin_set, prefix, parent
                    )
                    if is_sub_hijack is True:
                        hijacker_as_set.add(asn)
            if hijacker_as_set:
                return (
                    True,
                    parent,
                    prefix,
                    par_origin_set,
                    hijacker_as_set,
                    True,
                    filter_reason,
                )
            return (False, parent, prefix, par_origin_set, set(), False, filter_reason)
        return (None, "", "", set(), set(), False, "unknown ip version")

    def sub_hijack_detect(self, t, prefix):
        """来源 BGPSubHijack.py:439；兼容计算，外部副作用已移除。"""
        if prefix not in self.prefix_event:
            self.prefix_event.setdefault(prefix, dict())
            self.prefix_event[prefix]["sub_hijack_id"] = (
                self.sub_hijack_init_id[prefix]
                if prefix in self.sub_hijack_init_id
                else 0
            )
            self.prefix_event[prefix]["is_sub_hijack"] = False
            self.prefix_event[prefix]["last_sub_hijack_time"] = ""
        (
            is_sub_hijack,
            parent,
            pre,
            par_origin_set,
            hijacker_as_set,
            is_true,
            filter_reason,
        ) = self.get_state(prefix)
        if is_sub_hijack:
            if (
                prefix in self.prefix_event
                and self.prefix_event[prefix]["is_sub_hijack"] is False
            ):
                self.prefix_event[prefix]["is_sub_hijack"] = True
                if self.prefix_event[prefix]["last_sub_hijack_time"] != "":
                    last_sub_hijack_time = self.prefix_event[prefix][
                        "last_sub_hijack_time"
                    ]
                    last_sub_hijack_time = datetime.datetime.strptime(
                        last_sub_hijack_time, "%Y-%m-%d %H:%M:%S"
                    )
                    start_time = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                    if last_sub_hijack_time.month != start_time.month:
                        self.prefix_event[prefix]["sub_hijack_id"] = 0
                self.prefix_event[prefix]["sub_hijack_id"] += 1
                sub_hijack_id = self.prefix_event[prefix]["sub_hijack_id"]
                self.sub_hijack_dict.setdefault(prefix, dict()).setdefault(
                    sub_hijack_id, dict()
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["is_sub_hijack"] = is_true
                self.sub_hijack_dict[prefix][sub_hijack_id]["filter_reason"] = (
                    filter_reason
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["sub_hijack_table"] = (
                    self.sub_hijack_table
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["event_table"] = (
                    self.event_table
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["s_time"] = t
                self.sub_hijack_dict[prefix][sub_hijack_id]["e_time"] = None
                self.sub_hijack_dict[prefix][sub_hijack_id]["duration"] = None
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacked_prefix"] = parent
                self.sub_hijack_dict[prefix][sub_hijack_id]["parent_origins"] = (
                    par_origin_set.copy()
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["child_origins"] = (
                    hijacker_as_set.copy()
                )
                hijacked_as_list = list(par_origin_set)
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacked_as"] = (
                    str(hijacked_as_list) if len(hijacked_as_list) > 0 else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacked_as_name"] = (
                    get_as_name(self.bgp_info.as_info, hijacked_as_list[0])
                    if len(hijacked_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacked_as_org"] = (
                    get_as_org_name(self.bgp_info.as_info, hijacked_as_list[0])
                    if len(hijacked_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacked_as_country"] = (
                    get_as_country_cn(self.bgp_info.as_info, hijacked_as_list[0])
                    if len(hijacked_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacked_as_descr"] = (
                    get_as_descr(self.bgp_info.as_info, hijacked_as_list[0])
                    if len(hijacked_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacked_as_admin"] = (
                    get_as_admin(self.bgp_info.as_info, hijacked_as_list[0])
                    if len(hijacked_as_list) > 0
                    else None
                )
                hijacker_as_list = list(hijacker_as_set)
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacker_as"] = (
                    str(hijacker_as_list) if len(hijacker_as_list) > 0 else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacker_as_name"] = (
                    get_as_name(self.bgp_info.as_info, hijacker_as_list[0])
                    if len(hijacker_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacker_as_org"] = (
                    get_as_org_name(self.bgp_info.as_info, hijacker_as_list[0])
                    if len(hijacker_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacker_as_country"] = (
                    get_as_country_cn(self.bgp_info.as_info, hijacker_as_list[0])
                    if len(hijacker_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacker_as_descr"] = (
                    get_as_descr(self.bgp_info.as_info, hijacker_as_list[0])
                    if len(hijacker_as_list) > 0
                    else None
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["hijacker_as_admin"] = (
                    get_as_admin(self.bgp_info.as_info, hijacker_as_list[0])
                    if len(hijacker_as_list) > 0
                    else None
                )
                (sub_hijack_level, level_info) = self.sub_hijack_level(
                    parent, hijacked_as_list
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["sub_hijack_level"] = (
                    sub_hijack_level
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["level_info"] = level_info
                s_time = self.sub_hijack_dict[prefix][sub_hijack_id]["s_time"]
                hijacked_as = hijacked_as_list[0]
                hijacked_as_name = self.sub_hijack_dict[prefix][sub_hijack_id][
                    "hijacked_as_name"
                ]
                hijacked_as_org = self.sub_hijack_dict[prefix][sub_hijack_id][
                    "hijacked_as_org"
                ]
                hijacked_as_country = self.sub_hijack_dict[prefix][sub_hijack_id][
                    "hijacked_as_country"
                ]
                hijacker_as = hijacker_as_list[0]
                hijacker_as_name = self.sub_hijack_dict[prefix][sub_hijack_id][
                    "hijacker_as_name"
                ]
                hijacker_as_org = self.sub_hijack_dict[prefix][sub_hijack_id][
                    "hijacker_as_org"
                ]
                hijacker_as_country = self.sub_hijack_dict[prefix][sub_hijack_id][
                    "hijacker_as_country"
                ]
                hijacked_prefix = self.sub_hijack_dict[prefix][sub_hijack_id][
                    "hijacked_prefix"
                ]
                event_info = (
                    "北京时间 {} , 归属于 {} 的前缀 {} 被 {} 的子前缀 {} 劫持。".format(
                        s_time,
                        get_as_info(hijacked_as, hijacked_as_name, hijacked_as_country),
                        hijacked_prefix,
                        get_as_info(hijacker_as, hijacker_as_name, hijacker_as_country),
                        prefix,
                    )
                )
                self.sub_hijack_dict[prefix][sub_hijack_id]["event_info"] = event_info
                self.output.capture(
                    "sub_hijack_start",
                    sub_hijack_dict=self.sub_hijack_dict,
                    source=self.output.source,
                    prefix=prefix,
                    sub_hijack_id=sub_hijack_id,
                    table=self.sub_hijack_table,
                )
                detail_url = "{}/{}/{}/{}/{}".format(
                    "sub_hijack",
                    s_time,
                    prefix.replace("/", "-"),
                    sub_hijack_id,
                    self.output.source,
                )
                if hijacked_as_country in ["中国"] or hijacker_as_country in ["中国"]:
                    state = "judge"
                    is_domestic = True
                else:
                    state = "abroad"
                    is_domestic = False
                attacker_as = (
                    "AS{}\n({})".format(hijacker_as, hijacker_as_name)
                    if hijacker_as_name
                    else "AS{}".format(hijacker_as)
                )
                attacked_as = (
                    "AS{}\n({})".format(hijacked_as, hijacked_as_name)
                    if hijacked_as_name
                    else "AS{}".format(hijacked_as)
                )
                attacker_org = hijacker_as_org
                attacked_org = hijacked_as_org
                attacker_country = hijacker_as_country
                attacked_country = hijacked_as_country
                affected_prefix = "父前缀：{}\n子前缀：{}".format(
                    hijacked_prefix, prefix
                )
                self.output.capture(
                    "event_start",
                    source=self.output.source,
                    event_type="子前缀劫持",
                    level=self.sub_hijack_dict[prefix][sub_hijack_id][
                        "sub_hijack_level"
                    ],
                    s_time=self.sub_hijack_dict[prefix][sub_hijack_id]["s_time"],
                    e_time=self.sub_hijack_dict[prefix][sub_hijack_id]["e_time"],
                    duration=self.sub_hijack_dict[prefix][sub_hijack_id]["duration"],
                    attacker_as=attacker_as,
                    attacked_as=attacked_as,
                    affected_prefix=affected_prefix,
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
        else:
            if (
                prefix in self.prefix_event
                and self.prefix_event[prefix]["is_sub_hijack"] is True
            ):
                sub_hijack_id = self.prefix_event[prefix]["sub_hijack_id"]
                if (
                    prefix in self.sub_hijack_dict
                    and sub_hijack_id in self.sub_hijack_dict[prefix]
                ):
                    self.sub_hijack_dict[prefix][sub_hijack_id]["e_time"] = t
                    self.sub_hijack_dict[prefix][sub_hijack_id]["duration"] = (
                        get_duration(
                            self.sub_hijack_dict[prefix][sub_hijack_id]["s_time"], t
                        )
                    )
                    sub_hijack_table = self.sub_hijack_dict[prefix][sub_hijack_id][
                        "sub_hijack_table"
                    ]
                    if bool(sub_hijack_table):
                        self.output.capture(
                            "sub_hijack_end",
                            sub_hijack_dict=self.sub_hijack_dict,
                            source=self.output.source,
                            prefix=prefix,
                            sub_hijack_id=sub_hijack_id,
                            table=sub_hijack_table,
                        )
                    event_table = self.sub_hijack_dict[prefix][sub_hijack_id][
                        "event_table"
                    ]
                    if bool(event_table):
                        s_time = self.sub_hijack_dict[prefix][sub_hijack_id]["s_time"]
                        detail_url = "{}/{}/{}/{}/{}".format(
                            "sub_hijack",
                            s_time,
                            prefix.replace("/", "-"),
                            sub_hijack_id,
                            self.output.source,
                        )
                        self.output.capture(
                            "event_end",
                            detail_url=detail_url,
                            e_time=self.sub_hijack_dict[prefix][sub_hijack_id][
                                "e_time"
                            ],
                            duration=self.sub_hijack_dict[prefix][sub_hijack_id][
                                "duration"
                            ],
                            table=event_table,
                        )
                    self.prefix_event[prefix]["last_sub_hijack_time"] = (
                        self.sub_hijack_dict[prefix][sub_hijack_id]["s_time"]
                    )
                    if (
                        prefix in self.sub_hijack_dict
                        and sub_hijack_id in self.sub_hijack_dict[prefix]
                    ):
                        self.output.retire(
                            "sub_hijack",
                            prefix,
                            sub_hijack_id,
                            self.sub_hijack_dict[prefix][sub_hijack_id],
                        )
                        del self.sub_hijack_dict[prefix][sub_hijack_id]
            self.prefix_event[prefix]["is_sub_hijack"] = False

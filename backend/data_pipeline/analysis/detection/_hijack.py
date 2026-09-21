"""旧 39578fe 计算迁入；私有实现，不读取数据库、环境或原始文件。"""

from data_pipeline.analysis.detection._results import rule
import copy
import datetime
from data_pipeline.analysis.detection._reference_helpers import get_as_admin, get_as_country_cn, get_as_descr, get_as_info, get_as_name, get_as_org_name, get_as_org_name_en, get_duration


class BGPHijack:
    def __init__(self, output, bgp_info, bgp_rib):
        """来源 BGPHijack.py:72；兼容计算，外部副作用已移除。"""
        self.bgp_info = bgp_info
        self.bgp_rib = bgp_rib
        self.prefix_event = {}
        self.moas_event_dict = {}
        self.output = output
        self.moas_table = None
        self.hijack_table = None
        self.event_table = None
        self.moas_init_id = dict()
        self.hijack_init_id = dict()

    def init_hijack(self, prefix_dict):
        """来源 BGPHijack.py:113；兼容计算，外部副作用已移除。"""
        for prefix, vp_paths in prefix_dict.items():
            if prefix not in self.prefix_event.keys():
                self.prefix_event[prefix] = {}
            if prefix in self.moas_init_id.keys():
                self.prefix_event[prefix]["moas_eventid"] = self.moas_init_id[prefix]
            else:
                self.prefix_event[prefix]["moas_eventid"] = 0
            if prefix in self.hijack_init_id.keys():
                self.prefix_event[prefix]["hijack_eventid"] = self.hijack_init_id[
                    prefix
                ]
            else:
                self.prefix_event[prefix]["hijack_eventid"] = 0
            self.prefix_event[prefix]["last_moas_time"] = ""
            total_moas = 0
            moas_set = self.bgp_rib.prefix_as.get(prefix, set())
            moas_num = len(moas_set)
            if moas_num > 1:
                self.prefix_event[prefix]["is_moas_now"] = True
                self.prefix_event[prefix]["original_as"] = ""
                self.prefix_event[prefix]["pre_vp_paths"] = None
                total_moas += 1
            elif moas_num == 1:
                self.prefix_event[prefix]["is_moas_now"] = False
                self.prefix_event[prefix]["original_as"] = list(moas_set)[0]
                self.__set_pre_vp_paths(prefix, vp_paths, self.bgp_rib.t)

    def __set_pre_vp_paths(self, prefix, vp_paths, t):
        """来源 BGPHijack.py:154；兼容计算，外部副作用已移除。"""
        self.prefix_event[prefix]["pre_vp_paths"] = dict()
        self.prefix_event[prefix]["pre_vp_paths"][t] = copy.copy(
            list(vp_paths.values())
        )

    def __set_eve_vp_paths(self, prefix, moas_eventid, vp_paths, t):
        """来源 BGPHijack.py:166；兼容计算，外部副作用已移除。"""
        self.moas_event_dict[prefix][moas_eventid]["eve_vp_paths"] = dict()
        self.moas_event_dict[prefix][moas_eventid]["eve_vp_paths"][t] = copy.copy(
            list(vp_paths.values())
        )

    def set_table(self, moas_table, hijack_table, event_table):
        """来源 BGPHijack.py:179；兼容计算，外部副作用已移除。"""
        self.moas_table = moas_table
        self.hijack_table = hijack_table
        self.event_table = event_table

    def get_table(self):
        """来源 BGPHijack.py:184；兼容计算，外部副作用已移除。"""
        return (self.moas_table, self.hijack_table, self.event_table)

    def __check_private_as(self, asn):
        """来源 BGPHijack.py:187；兼容计算，外部副作用已移除。"""
        if "{" in asn:
            return False
        if "_" in asn:
            return True
        try:
            asn_int = int(asn)
            return 64512 <= asn_int <= 65535 or 4200000000 <= asn_int <= 4294967294
        except ValueError:
            return False

    @rule("来源 BGPHijack.py:200；兼容计算，外部副作用已移除。")
    def is_hijack_event(self, prefix, moas_set, ori_as):
        """来源 BGPHijack.py:200；兼容计算，外部副作用已移除。"""
        if len(moas_set) > 2:
            return (0, "moas num more than 2")
        for asn in moas_set:
            if asn is None:
                return (0, "invalid asn")
            asn = str(asn).strip()
            if not asn or (not asn.isdigit() and "{" not in asn and ("_" not in asn)):
                return (0, "invalid asn")
            if "{" in asn:
                return (0, "as_set")
            if self.__check_private_as(asn):
                return (0, "private as")
        moas_list = list(moas_set)
        (moas_as1, moas_as2) = (moas_list[0], moas_list[1])
        if ori_as == moas_as1:
            hijacked_as = moas_as1
            hijacker_as = moas_as2
        else:
            hijacked_as = moas_as2
            hijacker_as = moas_as1
        hijacked_as_org = get_as_org_name(self.bgp_info.as_info, hijacked_as)
        hijacker_as_org = get_as_org_name(self.bgp_info.as_info, hijacker_as)
        if hijacked_as_org:
            if "个人" in hijacked_as_org or "未知" in hijacked_as_org:
                return (0, "personal network or unknown org")
        if hijacker_as_org:
            if "个人" in hijacker_as_org or "未知" in hijacker_as_org:
                return (0, "personal network or unknown org")
        hijacked_set = set()
        hijacked_set.add(hijacked_as)
        if prefix in self.bgp_info.prefix_info.keys():
            if self.bgp_info.prefix_info[prefix]["route"]:
                set1 = set(self.bgp_info.prefix_info[prefix]["route"].split("|"))
                hijacked_set = hijacked_set.union(set1)
        for hijacked_as in hijacked_set:
            peers = set()
            siblings = set()
            (hijacked_as_str, hijacker_as_str) = (
                "AS" + hijacked_as,
                "AS" + hijacker_as,
            )
            if hijacked_as in self.bgp_info.as_info.keys():
                hijacked_import = self.bgp_info.as_info[hijacked_as]["import_as"]
                hijacked_export = self.bgp_info.as_info[hijacked_as]["export_as"]
                if hijacker_as_str in hijacked_import:
                    return (0, hijacked_as + " import filter " + hijacker_as)
                if hijacker_as_str in hijacked_export:
                    return (0, hijacked_as + " export filter " + hijacker_as)
            if hijacker_as in self.bgp_info.as_info.keys():
                hijacker_import = self.bgp_info.as_info[hijacker_as]["import_as"]
                hijacker_export = self.bgp_info.as_info[hijacker_as]["export_as"]
                if hijacked_as_str in hijacker_import:
                    return (0, hijacker_as + " import filter " + hijacked_as)
                if hijacked_as_str in hijacker_export:
                    return (0, hijacker_as + " export filter " + hijacked_as)
            if self.bgp_info.as_rel_dict.get(hijacked_as) is not None:
                if self.bgp_info.as_rel_dict[hijacked_as].get("provider") is not None:
                    provider = set(self.bgp_info.as_rel_dict[hijacked_as]["provider"])
                    if hijacker_as in provider:
                        return (0, "as rel provider-customer")
            if self.bgp_info.as_rel_dict.get(hijacked_as) is not None:
                if self.bgp_info.as_rel_dict[hijacked_as].get("customer") is not None:
                    customer = set(self.bgp_info.as_rel_dict[hijacked_as]["customer"])
                    if hijacker_as in customer:
                        return (0, "as rel provider-customer")
            if self.bgp_info.as_rel_dict.get(hijacker_as) is not None:
                if self.bgp_info.as_rel_dict[hijacker_as].get("provider") is not None:
                    provider = set(self.bgp_info.as_rel_dict[hijacker_as]["provider"])
                    if hijacked_as in provider:
                        return (0, "as rel provider-customer")
            if self.bgp_info.as_rel_dict.get(hijacker_as) is not None:
                if self.bgp_info.as_rel_dict[hijacker_as].get("customer") is not None:
                    customer = set(self.bgp_info.as_rel_dict[hijacker_as]["customer"])
                    if hijacked_as in customer:
                        return (0, "as rel provider-customer")
            if self.bgp_info.as_rel_dict.get(hijacked_as) is not None:
                if self.bgp_info.as_rel_dict[hijacked_as].get("peers") is not None:
                    peers = set(self.bgp_info.as_rel_dict[hijacked_as]["peers"])
                    if hijacker_as in peers:
                        return (0, "as rel peer")
            if self.bgp_info.as_rel_dict.get(hijacker_as) is not None:
                if self.bgp_info.as_rel_dict[hijacker_as].get("peers") is not None:
                    peers = set(self.bgp_info.as_rel_dict[hijacker_as]["peers"])
                    if hijacked_as in peers:
                        return (0, "as rel peer")
            if self.bgp_info.as_rel_dict.get(hijacked_as) is not None:
                if self.bgp_info.as_rel_dict[hijacked_as].get("sibling") is not None:
                    sibling = set(self.bgp_info.as_rel_dict[hijacked_as]["sibling"])
                    if hijacker_as in sibling:
                        return (0, "as rel same org")
            if self.bgp_info.as_rel_dict.get(hijacker_as) is not None:
                if self.bgp_info.as_rel_dict[hijacker_as].get("sibling") is not None:
                    sibling = set(self.bgp_info.as_rel_dict[hijacker_as]["sibling"])
                    if hijacked_as in sibling:
                        return (0, "as rel same org")
            hijacker_org_name_en = get_as_org_name_en(
                self.bgp_info.as_info, hijacker_as
            )
            hijacked_org_name_en = get_as_org_name_en(
                self.bgp_info.as_info, hijacked_as
            )
            if hijacker_org_name_en and hijacked_org_name_en:
                if hijacker_org_name_en == hijacked_org_name_en:
                    return (0, "as rel same org")
            if (
                self.bgp_info.as_prefix_dict.get(hijacker_as) is not None
                and self.bgp_info.as_prefix_dict.get(hijacked_as) is not None
            ):
                if (
                    self.bgp_info.as_prefix_dict[hijacker_as].get(prefix) is not None
                    and self.bgp_info.as_prefix_dict[hijacked_as].get(prefix)
                    is not None
                ):
                    return (0, prefix + "both in moasset")
            as_set = [hijacker_as, hijacked_as]
            for asn in as_set:
                asn_int = int(asn)
                if asn in self.bgp_info.as_info.keys():
                    if self.bgp_info.as_info[asn]["is_ddos_provider"] == True:
                        return (0, asn + " is AntiDDoS provider")
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
                        temp_siblings = set(self.bgp_info.as_info[asn]["sibling_as"])
                        temp_siblings.discard(asn_int)
                        if temp_siblings is not None:
                            siblings = siblings | temp_siblings
            for asn in as_set:
                asn_int = int(asn)
                if peers is not None:
                    if asn_int in peers:
                        return (0, "peer")
                if siblings is not None:
                    if asn_int in siblings:
                        return (0, "siblings")
        return (1, "possible hijack")

    @rule("来源 BGPHijack.py:373；兼容计算，外部副作用已移除。")
    def hijack_level(self, prefix, moas_set):
        """来源 BGPHijack.py:373；兼容计算，外部副作用已移除。"""
        descr_info = "这仅仅是一个可能的前缀劫持事件。"
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
                        self.bgp_info.prefix_info[prefix]["domain"], "BGPHijack.py:406"
                    )
                )
            if self.bgp_info.prefix_info[prefix]["domain_auth"] not in [None, ""]:
                domain.extend(
                    self.output.parse_domains(
                        self.bgp_info.prefix_info[prefix]["domain_auth"],
                        "BGPHijack.py:408",
                    )
                )
            for d in domain:
                if self.bgp_info.important_domain_dict.get(d) is not None:
                    level = "high"
                    descr_info = (
                        descr_info
                        + "在前缀中有"
                        + self.bgp_info.important_domain_dict.get("name", "重要")
                        + "的网站 。"
                    )
        for asn in moas_set:
            try:
                if self.bgp_info.important_as_dict.get(int(asn)) is not None:
                    level = "high"
                    if descr_info != "":
                        descr_info = (
                            descr_info
                            + " 并且 "
                            + asn
                            + " 是 Cloud|IDC|CDN 或者顶级内容提供商(top content provider)."
                        )
            except:
                continue
        return (level, descr_info)

    def get_hijacker(self, prefix, moas_eventid):
        """来源 BGPHijack.py:425；兼容计算，外部副作用已移除。"""
        ori_as = self.moas_event_dict[prefix][moas_eventid]["ori_as"]
        moas_as1 = self.moas_event_dict[prefix][moas_eventid]["moas_as1"]
        moas_as2 = self.moas_event_dict[prefix][moas_eventid]["moas_as2"]
        if ori_as == moas_as1:
            hijacked_as = moas_as1
            hijacker_as = moas_as2
        else:
            hijacked_as = moas_as2
            hijacker_as = moas_as1
        return (hijacked_as, hijacker_as)

    def get_event_info(self, prefix, moas_eventid):
        """来源 BGPHijack.py:444；兼容计算，外部副作用已移除。"""
        event_info = ""
        s_time = self.moas_event_dict[prefix][moas_eventid]["s_time"]
        hijacked_as = self.moas_event_dict[prefix][moas_eventid]["hijacked_as"]
        hijacked_as_name = self.moas_event_dict[prefix][moas_eventid][
            "hijacked_as_name"
        ]
        hijacked_as_org = self.moas_event_dict[prefix][moas_eventid]["hijacked_as_org"]
        hijacked_as_country = self.moas_event_dict[prefix][moas_eventid][
            "hijacked_as_country"
        ]
        hijacker_as = self.moas_event_dict[prefix][moas_eventid]["hijacker_as"]
        hijacker_as_name = self.moas_event_dict[prefix][moas_eventid][
            "hijacker_as_name"
        ]
        hijacker_as_org = self.moas_event_dict[prefix][moas_eventid]["hijacker_as_org"]
        hijacker_as_country = self.moas_event_dict[prefix][moas_eventid][
            "hijacker_as_country"
        ]
        str_ = "在北京时间 {} , 归属于 {} 的前缀 {} 被 {} 劫持。".format(
            s_time,
            get_as_info(hijacked_as, hijacked_as_name, hijacked_as_country),
            prefix,
            get_as_info(hijacker_as, hijacker_as_name, hijacker_as_country),
        )
        event_info = event_info + str_
        return event_info

    def hijack_detect(self, t, prefix, old_origin_set, origin_set, new_vp_paths):
        """来源 BGPHijack.py:471；兼容计算，外部副作用已移除。"""
        old_origin_num = len(old_origin_set)
        origin_num = len(origin_set)
        if not (origin_num > 1 or old_origin_num > 1):
            return
        if old_origin_num == origin_num:
            return
        if origin_num > 1 and old_origin_num > 1:
            return
        if old_origin_num == 1 and origin_num > 1:
            if origin_num > 2:
                return
            if prefix not in self.prefix_event.keys():
                self.prefix_event[prefix] = {
                    "moas_eventid": 0,
                    "hijack_eventid": 0,
                    "last_moas_time": "",
                    "original_as": "",
                    "pre_vp_paths": None,
                    "is_moas_now": False,
                }
            self.prefix_event[prefix]["is_moas_now"] = True
            if self.prefix_event[prefix]["last_moas_time"] != "":
                last_moas_time = self.prefix_event[prefix]["last_moas_time"]
                last_moas_time = datetime.datetime.strptime(
                    last_moas_time, "%Y-%m-%d %H:%M:%S"
                )
                start_time = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                if last_moas_time.month != start_time.month:
                    self.prefix_event[prefix]["moas_eventid"] = 0
                    self.prefix_event[prefix]["hijack_eventid"] = 0
            ori_as = self.prefix_event[prefix]["original_as"]
            self.prefix_event[prefix]["moas_eventid"] += 1
            moas_eventid = self.prefix_event[prefix]["moas_eventid"]
            self.moas_event_dict.setdefault(prefix, dict()).setdefault(
                moas_eventid, dict()
            )
            (is_hijack, filter_reason) = self.is_hijack_event(
                prefix, origin_set, ori_as
            )
            if is_hijack == 1:
                self.prefix_event[prefix]["hijack_eventid"] += 1
                (level, level_info) = self.hijack_level(prefix, origin_set)
                self.moas_event_dict[prefix][moas_eventid]["level"] = level
                self.moas_event_dict[prefix][moas_eventid]["level_info"] = level_info
                self.__set_eve_vp_paths(prefix, moas_eventid, new_vp_paths, t)
                self.moas_event_dict[prefix][moas_eventid]["pre_vp_paths"] = (
                    self.prefix_event[prefix]["pre_vp_paths"]
                )
                self.moas_event_dict[prefix][moas_eventid]["hijack_table"] = (
                    self.hijack_table
                )
                self.moas_event_dict[prefix][moas_eventid]["event_table"] = (
                    self.event_table
                )
            else:
                self.moas_event_dict[prefix][moas_eventid]["level"] = None
                self.moas_event_dict[prefix][moas_eventid]["level_info"] = None
                self.moas_event_dict[prefix][moas_eventid]["eve_vp_paths"] = None
                self.moas_event_dict[prefix][moas_eventid]["pre_vp_paths"] = None
                self.moas_event_dict[prefix][moas_eventid]["hijack_table"] = None
                self.moas_event_dict[prefix][moas_eventid]["event_table"] = None
            self.moas_event_dict[prefix][moas_eventid]["moas_table"] = self.moas_table
            self.moas_event_dict[prefix][moas_eventid]["moas_set"] = origin_set.copy()
            self.moas_event_dict[prefix][moas_eventid]["legacy_hijack_id"] = (
                self.prefix_event[prefix]["hijack_eventid"] if is_hijack == 1 else None
            )
            moas_list = list(origin_set)
            self.moas_event_dict[prefix][moas_eventid]["ori_as"] = ori_as
            self.moas_event_dict[prefix][moas_eventid]["moas_as1"] = moas_list[0]
            self.moas_event_dict[prefix][moas_eventid]["moas_as2"] = moas_list[1]
            (hijacked_as, hijacker_as) = self.get_hijacker(prefix, moas_eventid)
            self.moas_event_dict[prefix][moas_eventid]["hijacked_as"] = hijacked_as
            self.moas_event_dict[prefix][moas_eventid]["hijacked_as_name"] = (
                get_as_name(self.bgp_info.as_info, hijacked_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacked_as_org"] = (
                get_as_org_name(self.bgp_info.as_info, hijacked_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacked_as_country"] = (
                get_as_country_cn(self.bgp_info.as_info, hijacked_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacked_as_descr"] = (
                get_as_descr(self.bgp_info.as_info, hijacked_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacked_as_admin"] = (
                get_as_admin(self.bgp_info.as_info, hijacked_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacker_as"] = hijacker_as
            self.moas_event_dict[prefix][moas_eventid]["hijacker_as_name"] = (
                get_as_name(self.bgp_info.as_info, hijacker_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacker_as_org"] = (
                get_as_org_name(self.bgp_info.as_info, hijacker_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacker_as_country"] = (
                get_as_country_cn(self.bgp_info.as_info, hijacker_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacker_as_descr"] = (
                get_as_descr(self.bgp_info.as_info, hijacker_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["hijacker_as_admin"] = (
                get_as_admin(self.bgp_info.as_info, hijacker_as)
            )
            self.moas_event_dict[prefix][moas_eventid]["s_time"] = t
            self.moas_event_dict[prefix][moas_eventid]["e_time"] = None
            self.moas_event_dict[prefix][moas_eventid]["duration"] = None
            self.moas_event_dict[prefix][moas_eventid]["next_vp_paths"] = None
            self.moas_event_dict[prefix][moas_eventid]["end_as"] = None
            self.moas_event_dict[prefix][moas_eventid]["is_hijack"] = (
                True if is_hijack == 1 else False
            )
            self.moas_event_dict[prefix][moas_eventid]["filter_reason"] = filter_reason
            self.moas_event_dict[prefix][moas_eventid]["event_info"] = (
                self.get_event_info(prefix, moas_eventid)
            )
            if is_hijack == 1:
                self.output.capture(
                    "hijack_start",
                    moas_event_dict=self.moas_event_dict,
                    source=self.output.source,
                    prefix=prefix,
                    moas_id=moas_eventid,
                    hijack_id=self.prefix_event[prefix]["hijack_eventid"],
                    table=self.hijack_table,
                )
                hijacked_as = self.moas_event_dict[prefix][moas_eventid]["hijacked_as"]
                hijacked_as_name = self.moas_event_dict[prefix][moas_eventid][
                    "hijacked_as_name"
                ]
                hijacked_as_org = self.moas_event_dict[prefix][moas_eventid][
                    "hijacked_as_org"
                ]
                hijacked_as_country = self.moas_event_dict[prefix][moas_eventid][
                    "hijacked_as_country"
                ]
                hijacker_as = self.moas_event_dict[prefix][moas_eventid]["hijacker_as"]
                hijacker_as_name = self.moas_event_dict[prefix][moas_eventid][
                    "hijacker_as_name"
                ]
                hijacker_as_org = self.moas_event_dict[prefix][moas_eventid][
                    "hijacker_as_org"
                ]
                hijacker_as_country = self.moas_event_dict[prefix][moas_eventid][
                    "hijacker_as_country"
                ]
                event_info = "归属于 {} 的前缀 {} 被 {} 劫持".format(
                    get_as_info(hijacked_as, hijacked_as_name, hijacked_as_country),
                    prefix,
                    get_as_info(hijacker_as, hijacker_as_name, hijacker_as_country),
                )
                s_time = self.moas_event_dict[prefix][moas_eventid]["s_time"]
                hijack_eventid = self.prefix_event[prefix]["hijack_eventid"]
                detail_url = "{}/{}/{}/{}/{}".format(
                    "hijack",
                    s_time,
                    prefix.replace("/", "-"),
                    hijack_eventid,
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
                self.output.capture(
                    "event_start",
                    source=self.output.source,
                    event_type="前缀劫持",
                    level=self.moas_event_dict[prefix][moas_eventid]["level"],
                    s_time=self.moas_event_dict[prefix][moas_eventid]["s_time"],
                    e_time=self.moas_event_dict[prefix][moas_eventid]["e_time"],
                    duration=self.moas_event_dict[prefix][moas_eventid]["duration"],
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
            return
        if old_origin_num > 1 and origin_num == 1:
            if old_origin_num != 2:
                return
            if prefix not in self.prefix_event.keys():
                self.prefix_event[prefix] = {}
            if self.prefix_event[prefix].get("is_moas_now", False):
                self.__set_pre_vp_paths(prefix, new_vp_paths, t)
                moas_eventid = self.prefix_event[prefix]["moas_eventid"]
                if (
                    prefix in self.moas_event_dict
                    and moas_eventid in self.moas_event_dict[prefix]
                ):
                    moas_eventid = self.prefix_event[prefix]["moas_eventid"]
                    self.moas_event_dict[prefix][moas_eventid]["e_time"] = t
                    self.moas_event_dict[prefix][moas_eventid]["duration"] = (
                        get_duration(
                            self.moas_event_dict[prefix][moas_eventid]["s_time"], t
                        )
                    )
                    self.moas_event_dict[prefix][moas_eventid]["next_vp_paths"] = (
                        self.prefix_event[prefix]["pre_vp_paths"]
                    )
                    self.moas_event_dict[prefix][moas_eventid]["end_as"] = list(
                        origin_set
                    )[0]
                    self.moas_event_dict[prefix][moas_eventid]["event_info"] = (
                        self.get_event_info(prefix, moas_eventid)
                    )
                    hijack_table = self.moas_event_dict[prefix][moas_eventid][
                        "hijack_table"
                    ]
                    if bool(hijack_table):
                        if self.moas_event_dict[prefix][moas_eventid]["is_hijack"]:
                            self.output.capture(
                                "hijack_end",
                                moas_event_dict=self.moas_event_dict,
                                source=self.output.source,
                                prefix=prefix,
                                moas_id=moas_eventid,
                                hijack_id=self.prefix_event[prefix]["hijack_eventid"],
                                table=hijack_table,
                            )
                    event_table = self.moas_event_dict[prefix][moas_eventid][
                        "event_table"
                    ]
                    if bool(event_table):
                        if self.moas_event_dict[prefix][moas_eventid]["is_hijack"]:
                            s_time = self.moas_event_dict[prefix][moas_eventid][
                                "s_time"
                            ]
                            hijack_eventid = self.prefix_event[prefix]["hijack_eventid"]
                            detail_url = "{}/{}/{}/{}/{}".format(
                                "hijack",
                                s_time,
                                prefix.replace("/", "-"),
                                hijack_eventid,
                                self.output.source,
                            )
                            self.output.capture(
                                "event_end",
                                detail_url=detail_url,
                                e_time=self.moas_event_dict[prefix][moas_eventid][
                                    "e_time"
                                ],
                                duration=self.moas_event_dict[prefix][moas_eventid][
                                    "duration"
                                ],
                                table=event_table,
                            )
                    self.prefix_event[prefix]["last_moas_time"] = self.moas_event_dict[
                        prefix
                    ][moas_eventid]["s_time"]
                    if (
                        prefix in self.moas_event_dict
                        and moas_eventid in self.moas_event_dict[prefix]
                    ):
                        self.output.retire(
                            "moas",
                            prefix,
                            moas_eventid,
                            self.moas_event_dict[prefix][moas_eventid],
                        )
                        del self.moas_event_dict[prefix][moas_eventid]
            self.prefix_event[prefix]["is_moas_now"] = False
            self.prefix_event[prefix]["original_as"] = (
                "" if origin_num == 0 else list(origin_set)[0]
            )
            return

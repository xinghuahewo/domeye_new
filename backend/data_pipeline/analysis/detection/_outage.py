"""旧 39578fe 计算迁入；私有实现，不读取数据库、环境或原始文件。"""

from data_pipeline.analysis.detection._results import rule
import copy
import datetime
from data_pipeline.analysis.detection._reference_helpers import get_as_admin, get_as_country, get_as_country_cn, get_as_descr, get_as_info, get_as_name, get_as_org_name, get_as_type, get_country_chinese_name, get_duration, get_private_as_city
from data_pipeline.analysis.detection._blacklist import prefix_blacklist
from data_pipeline.analysis.detection.country_outage import build_live_observation, new_runtime_state, reduce_live_observation, legacy_peak_projection


class BGPOutage:
    def __init__(self, output, bgp_info, bgp_rib):
        """来源 BGPOutage.py:1149；兼容计算，外部副作用已移除。"""
        self.output = output
        self.bgp_info = bgp_info
        self.bgp_rib = bgp_rib
        self.prefix_vp = {}
        self.as_prefix = {}
        self.country_as = {}
        self.prefix_init_id = {}
        self.as_init_id = {}
        self.country_init_id = {}
        self.prefix_outage_event = {}
        self.as_outage_event = {}
        self.country_outage_event = {}
        self.country_outage_v2_runtime = {}
        self.prefix_table = ""
        self.as_table = ""
        self.country_table = ""
        self.event_table = ""

    def __check_private_as(self, asn):
        """来源 BGPOutage.py:1185；兼容计算，外部副作用已移除。"""
        if "{" in asn:
            return False
        if "_" in asn:
            return True
        try:
            asn_int = int(asn)
            return 64512 <= asn_int <= 65535 or 4200000000 <= asn_int <= 4294967294
        except ValueError:
            return False

    def __get_origin_asn(self, as_path):
        """来源 BGPOutage.py:1235；兼容计算，外部副作用已移除。"""
        as_path_fields = as_path.split(" ")
        for index in range(len(as_path_fields) - 1, -1, -1):
            asn = as_path_fields[index]
            if self.__check_private_as(asn):
                continue
            else:
                return asn
        return as_path_fields[-1]

    def init_outage(self, prefix_dict):
        """来源 BGPOutage.py:1275；兼容计算，外部副作用已移除。"""
        for prefix, vp_paths in prefix_dict.items():
            if prefix not in self.prefix_vp.keys():
                self.prefix_vp[prefix] = {}
            if prefix not in self.prefix_vp[prefix].keys():
                self.prefix_vp[prefix] = {
                    "reachable_vp_set": set(),
                    "unreachable_vp_set": set(),
                    "prefix_outage_id": self.prefix_init_id.get(prefix, 0),
                    "is_outage_now": False,
                    "last_start_time": "",
                }
            self.__set_pre_pre_vp_paths(prefix, self.bgp_rib.t, vp_paths)
            for vp in vp_paths.keys():
                self.prefix_vp[prefix]["reachable_vp_set"].add(vp)
            for asn in self.bgp_rib.prefix_as.get(prefix, set()):
                if asn not in self.as_prefix.keys():
                    self.as_prefix[asn] = {
                        "normal_prefix_set": set(),
                        "outage_prefix_set": set(),
                        "as_outage_id": self.as_init_id.get(asn, 0),
                        "is_outage_now": False,
                        "is_private_as": self.__check_private_as(asn),
                        "private_as_city": get_private_as_city(
                            self.bgp_info.private_as_dict, asn
                        )
                        if self.__check_private_as(asn)
                        else "",
                        "last_start_time": "",
                    }
                self.as_prefix[asn]["normal_prefix_set"].add(prefix)
                country = get_as_country(self.bgp_info.as_info, asn)
                if country is not None and country not in self.country_as.keys():
                    self.country_as[country] = {
                        "normal_as_set": set(),
                        "outage_as_set": set(),
                        "country_outage_id": self.country_init_id.get(country, 0),
                        "is_outage_now": False,
                        "last_start_time": "",
                    }
                if country is not None:
                    self.country_as[country]["normal_as_set"].add(asn)

    def set_table(self, prefix_table, as_table, country_table, event_table):
        """来源 BGPOutage.py:1334；兼容计算，外部副作用已移除。"""
        self.prefix_table = prefix_table
        self.as_table = as_table
        self.country_table = country_table
        self.event_table = event_table

    def get_table(self):
        """来源 BGPOutage.py:1340；兼容计算，外部副作用已移除。"""
        return (self.prefix_table, self.as_table, self.country_table, self.event_table)

    def outage_detect(
        self,
        t,
        flag,
        prefix,
        vp,
        as_path,
        old_origin_set,
        old_vp_paths,
        origin_set,
        new_vp_paths,
    ):
        """来源 BGPOutage.py:1343；兼容计算，外部副作用已移除。"""
        if flag == "W":
            if prefix in self.prefix_vp.keys():
                if vp in self.prefix_vp[prefix]["reachable_vp_set"]:
                    self.prefix_vp[prefix]["reachable_vp_set"].remove(vp)
                    self.prefix_vp[prefix]["unreachable_vp_set"].add(vp)
            (is_ourage, is_recover) = self.__check_prefix_outage(
                prefix=prefix,
                t=t,
                old_origin_set=old_origin_set,
                old_vp_paths=old_vp_paths,
                new_vp_paths=new_vp_paths,
            )
            if is_ourage:
                for origin in old_origin_set:
                    country = get_as_country(self.bgp_info.as_info, origin)
                    self.__check_as_outage(country=country, origin=origin, t=t)
                    if country is not None:
                        self.__check_country_outage(country=country, t=t)
        elif flag == "A":
            origin = self.__get_origin_asn(as_path)
            if origin not in self.as_prefix:
                self.as_prefix[origin] = dict()
                if (
                    "_" in origin
                    and len(origin.split("_")) == 2
                    and self.__check_private_as(origin.split("_")[1])
                ):
                    self.as_prefix[origin]["is_private_as"] = True
                    self.as_prefix[origin]["private_as_city"] = get_private_as_city(
                        self.bgp_info.private_as_dict, origin
                    )
                else:
                    self.as_prefix[origin]["is_private_as"] = False
                    self.as_prefix[origin]["private_as_city"] = ""
                self.as_prefix[origin]["normal_prefix_set"] = set()
                self.as_prefix[origin]["outage_prefix_set"] = set()
                if origin in self.as_init_id:
                    self.as_prefix[origin]["as_outage_id"] = self.as_init_id[origin]
                else:
                    self.as_prefix[origin]["as_outage_id"] = 0
                self.as_prefix[origin]["is_outage_now"] = False
                self.as_prefix[origin]["last_start_time"] = ""
            if prefix not in self.as_prefix[origin]["outage_prefix_set"]:
                self.as_prefix[origin]["normal_prefix_set"].add(prefix)
            if prefix not in self.prefix_vp.keys():
                self.prefix_vp[prefix] = dict()
                self.prefix_vp[prefix]["reachable_vp_set"] = set()
                self.prefix_vp[prefix]["unreachable_vp_set"] = set()
                if prefix in self.prefix_init_id:
                    self.prefix_vp[prefix]["prefix_outage_id"] = self.prefix_init_id[
                        prefix
                    ]
                else:
                    self.prefix_vp[prefix]["prefix_outage_id"] = 0
                self.__set_pre_pre_vp_paths(prefix, t, {vp: as_path})
                self.prefix_vp[prefix]["is_outage_now"] = False
                self.prefix_vp[prefix]["last_start_time"] = ""
            self.prefix_vp[prefix]["reachable_vp_set"].add(vp)
            if vp in self.prefix_vp[prefix]["unreachable_vp_set"]:
                self.prefix_vp[prefix]["unreachable_vp_set"].remove(vp)
            country = get_as_country(self.bgp_info.as_info, origin)
            if country is not None and country not in self.country_as:
                self.country_as[country] = dict()
                self.country_as[country]["normal_as_set"] = set()
                self.country_as[country]["outage_as_set"] = set()
                if country in self.country_init_id:
                    self.country_as[country]["country_outage_id"] = (
                        self.country_init_id[country]
                    )
                else:
                    self.country_as[country]["country_outage_id"] = 0
                self.country_as[country]["is_outage_now"] = False
                self.country_as[country]["last_start_time"] = ""
            if country is not None and self.as_prefix[origin]["is_outage_now"] is False:
                self.country_as[country]["normal_as_set"].add(origin)
            (is_ourage, is_recover) = self.__check_prefix_outage(
                prefix=prefix,
                t=t,
                old_origin_set=origin,
                old_vp_paths=old_vp_paths,
                new_vp_paths=new_vp_paths,
            )
            if is_recover:
                self.__check_as_outage(country=country, origin=origin, t=t)
                if country is not None:
                    self.__check_country_outage(country=country, t=t)
        if prefix in self.prefix_vp.keys():
            if self.prefix_vp[prefix]["is_outage_now"] is False:
                if len(new_vp_paths) >= len(old_vp_paths):
                    self.__set_pre_pre_vp_paths(prefix, t, new_vp_paths)

    def update_outage_event_table(self):
        """来源 BGPOutage.py:1442；兼容计算，外部副作用已移除。"""
        for AS in self.as_outage_event:
            if len(list(self.as_outage_event[AS].keys())) > 0:
                as_outage_id = list(self.as_outage_event[AS].keys())[0]
                if self.as_outage_event[AS][as_outage_id]["if_change"] is True:
                    table = self.as_outage_event[AS][as_outage_id]["table"]
                    if bool(table):
                        self.output.capture(
                            "as_outage_update",
                            as_outage_event=self.as_outage_event,
                            source=self.output.source,
                            origin=AS,
                            as_outage_id=as_outage_id,
                            table=table,
                        )
                        self.as_outage_event[AS][as_outage_id]["if_change"] = False
                    event_table = self.as_outage_event[AS][as_outage_id]["event_table"]
                    if bool(event_table):
                        s_time1 = self.as_outage_event[AS][as_outage_id]["s_time"]
                        detail_url = "{}/{}/{}/{}/{}".format(
                            "as_outage", s_time1, AS, as_outage_id, self.output.source
                        )
                        as_name = self.as_outage_event[AS][as_outage_id]["as_name"]
                        org_name = self.as_outage_event[AS][as_outage_id]["org_name"]
                        country_chinese_name = self.as_outage_event[AS][as_outage_id][
                            "country"
                        ]
                        s_time = self.as_outage_event[AS][as_outage_id]["s_time"]
                        e_time = self.as_outage_event[AS][as_outage_id]["e_time"]
                        max_outage_prefix_num = self.as_outage_event[AS][as_outage_id][
                            "max_outage_prefix_num"
                        ]
                        max_outage_prefix_ratio = "%.2f%%" % (
                            self.as_outage_event[AS][as_outage_id][
                                "max_outage_prefix_ratio"
                            ]
                            * 100
                        )
                        event_info = "归属于 {} 的 {} 个前缀发生路由回撤中断，中断前缀数量占比 {}".format(
                            get_as_info(AS, as_name, country_chinese_name),
                            max_outage_prefix_num,
                            max_outage_prefix_ratio,
                        )
                        affected_prefix = "{} 等 {} 个前缀发生路由回撤中断，中断前缀数量占比 {}".format(
                            self.as_outage_event[AS][as_outage_id]["outage_prefixes"][
                                0
                            ],
                            max_outage_prefix_num,
                            max_outage_prefix_ratio,
                        )
                        self.output.capture(
                            "event_as_outage_update",
                            detail_url=detail_url,
                            event_info=event_info,
                            affected_prefix=affected_prefix,
                            level=self.as_outage_event[AS][as_outage_id][
                                "outage_level"
                            ],
                            table=event_table,
                        )
        for country in self.country_outage_event:
            if len(list(self.country_outage_event[country].keys())) > 0:
                country_outage_id = list(self.country_outage_event[country].keys())[0]
                if (
                    self.country_outage_event[country][country_outage_id]["if_change"]
                    is True
                ):
                    table = self.country_outage_event[country][country_outage_id][
                        "table"
                    ]
                    if bool(table):
                        self.output.capture(
                            "country_outage_update",
                            country_outage_event=self.country_outage_event,
                            source=self.output.source,
                            country=country,
                            country_outage_id=country_outage_id,
                            table=table,
                        )
                        self.country_outage_event[country][country_outage_id][
                            "if_change"
                        ] = False
                    event_table = self.country_outage_event[country][country_outage_id][
                        "event_table"
                    ]
                    if bool(event_table):
                        country_chinese_name = self.country_outage_event[country][
                            country_outage_id
                        ]["country_chinese_name"]
                        max_outage_as_num = self.country_outage_event[country][
                            country_outage_id
                        ]["max_outage_as_num"]
                        s_time = self.country_outage_event[country][country_outage_id][
                            "s_time"
                        ]
                        max_outage_as_ratio = "%.2f%%" % (
                            self.country_outage_event[country][country_outage_id][
                                "max_outage_as_ratio"
                            ]
                            * 100
                        )
                        event_info = "{} 发生大规模路由回撤中断，中断AS数量 {} 个，占整个国家AS数量的 {}".format(
                            country_chinese_name, max_outage_as_num, max_outage_as_ratio
                        )
                        attacked_as = "{} 等 {} 个AS发生路由回撤中断，占整个国家AS数量的 {}".format(
                            self.country_outage_event[country][country_outage_id][
                                "outage_ases"
                            ][0],
                            max_outage_as_num,
                            max_outage_as_ratio,
                        )
                        detail_url = "{}/{}/{}/{}/{}".format(
                            "country_outage",
                            s_time,
                            country,
                            country_outage_id,
                            self.output.source,
                        )
                        self.output.capture(
                            "event_country_outage_update",
                            detail_url=detail_url,
                            event_info=event_info,
                            attacker_as=attacked_as,
                            attacked_as=attacked_as,
                            level=self.country_outage_event[country][country_outage_id][
                                "outage_level"
                            ],
                            table=event_table,
                        )

    def __set_pre_pre_vp_paths(self, prefix, t, vp_paths):
        """来源 BGPOutage.py:1538；兼容计算，外部副作用已移除。"""
        self.prefix_vp[prefix]["pre_vp_paths"] = dict()
        self.prefix_vp[prefix]["pre_vp_paths"][t] = copy.copy(list(vp_paths.values()))

    def __set_pre_eve_vp_paths(self, prefix, prefix_outage_id, vp_paths, t):
        """来源 BGPOutage.py:1551；兼容计算，外部副作用已移除。"""
        self.prefix_outage_event[prefix][prefix_outage_id]["eve_vp_paths"] = dict()
        self.prefix_outage_event[prefix][prefix_outage_id]["eve_vp_paths"][t] = (
            copy.copy(list(vp_paths.values()))
        )

    def __set_as_pre_vp_paths(self, asn, prefix, as_outage_id):
        """来源 BGPOutage.py:1566；兼容计算，外部副作用已移除。"""
        self.as_outage_event[asn][as_outage_id]["pre_vp_paths"] = copy.copy(
            self.prefix_vp[prefix]["pre_vp_paths"]
        )

    def __set_as_eve_vp_paths(self, asn, prefix, as_outage_id, t):
        """来源 BGPOutage.py:1578；兼容计算，外部副作用已移除。"""
        prefix_outage_id = self.prefix_vp[prefix]["prefix_outage_id"]
        self.as_outage_event[asn][as_outage_id]["eve_vp_paths"] = dict()
        if (
            len(
                list(
                    self.prefix_outage_event[prefix][prefix_outage_id][
                        "eve_vp_paths"
                    ].values()
                )
            )
            > 0
        ):
            self.as_outage_event[asn][as_outage_id]["eve_vp_paths"][t] = copy.copy(
                list(
                    self.prefix_outage_event[prefix][prefix_outage_id][
                        "eve_vp_paths"
                    ].values()
                )[0]
            )
        else:
            self.as_outage_event[asn][as_outage_id]["eve_vp_paths"][t] = list()

    def __set_as_outage_pre(self, origin, as_outage_id):
        """来源 BGPOutage.py:1587；兼容计算，外部副作用已移除。"""
        self.as_outage_event[origin][as_outage_id]["outage_prefixes"] = list(
            self.as_prefix[origin]["outage_prefix_set"]
        )

    def __set_country_outage_as(self, country, country_outage_id):
        """来源 BGPOutage.py:1598；兼容计算，外部副作用已移除。"""
        self.country_outage_event[country][country_outage_id]["outage_ases"] = list(
            self.country_as[country]["outage_as_set"]
        )

    def __check_prefix_outage(
        self, prefix, t, old_origin_set, old_vp_paths, new_vp_paths
    ):
        """来源 BGPOutage.py:1627；兼容计算，外部副作用已移除。"""
        if isinstance(old_origin_set, str):
            old_origin_set = {old_origin_set}
        if prefix not in prefix_blacklist:
            if len(old_origin_set) != 0:
                origin = sorted(old_origin_set)[0]
            else:
                if len(new_vp_paths) == 0:
                    return (False, False)
                origin = self.__get_origin_asn(list(new_vp_paths.values())[0])
            org_name = get_as_org_name(self.bgp_info.as_info, origin)
            if org_name:
                if "个人" in org_name or "未知" in org_name:
                    return (False, False)
            if prefix in self.prefix_vp.keys():
                unreachable_vp_num = len(self.prefix_vp[prefix]["unreachable_vp_set"])
                reachable_vp_num = len(self.prefix_vp[prefix]["reachable_vp_set"])
                total = unreachable_vp_num + reachable_vp_num
                if total < 3:
                    return (False, False)
                if self.prefix_vp[prefix]["is_outage_now"] == False:
                    if (
                        unreachable_vp_num
                        >= (unreachable_vp_num + reachable_vp_num)
                        * self.output.thresholds["PREFIX_OUTAGE_THRESHOLD"]
                    ):
                        self.prefix_vp[prefix]["is_outage_now"] = True
                        if self.prefix_vp[prefix]["last_start_time"] != "":
                            last_start_time = self.prefix_vp[prefix]["last_start_time"]
                            last_start_time = datetime.datetime.strptime(
                                last_start_time, "%Y-%m-%d %H:%M:%S"
                            )
                            start_time = datetime.datetime.strptime(
                                t, "%Y-%m-%d %H:%M:%S"
                            )
                            if start_time.month != last_start_time.month:
                                self.prefix_vp[prefix]["prefix_outage_id"] = 0
                        if prefix in self.as_prefix[origin]["normal_prefix_set"]:
                            self.as_prefix[origin]["normal_prefix_set"].remove(prefix)
                        self.as_prefix[origin]["outage_prefix_set"].add(prefix)
                        self.prefix_vp[prefix]["prefix_outage_id"] += 1
                        prefix_outage_id = self.prefix_vp[prefix]["prefix_outage_id"]
                        self.prefix_outage_event.setdefault(prefix, dict()).setdefault(
                            prefix_outage_id, dict()
                        )
                        self.prefix_outage_event[prefix][prefix_outage_id]["table"] = (
                            self.prefix_table
                        )
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "event_table"
                        ] = self.event_table
                        self.prefix_outage_event[prefix][prefix_outage_id]["asn"] = (
                            origin
                        )
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "is_private_as"
                        ] = self.as_prefix[origin]["is_private_as"]
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "private_as_city"
                        ] = self.as_prefix[origin]["private_as_city"]
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "country"
                        ] = get_as_country_cn(self.bgp_info.as_info, origin)
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "as_name"
                        ] = get_as_name(self.bgp_info.as_info, origin)
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "org_name"
                        ] = get_as_org_name(self.bgp_info.as_info, origin)
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "as_type"
                        ] = get_as_type(self.bgp_info.as_info, origin)
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "as_descr"
                        ] = get_as_descr(self.bgp_info.as_info, origin)
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "as_admin"
                        ] = get_as_admin(self.bgp_info.as_info, origin)
                        self.prefix_outage_event[prefix][prefix_outage_id]["s_time"] = t
                        self.prefix_outage_event[prefix][prefix_outage_id]["e_time"] = (
                            None
                        )
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "duration"
                        ] = None
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "next_vp_paths"
                        ] = dict()
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "pre_vp_paths"
                        ] = self.prefix_vp[prefix]["pre_vp_paths"]
                        self.__set_pre_eve_vp_paths(
                            prefix, prefix_outage_id, new_vp_paths, t
                        )
                        (outage_level, outage_level_descr) = self.__prefix_outage_level(
                            prefix=prefix
                        )
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "outage_level"
                        ] = outage_level
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "outage_level_descr"
                        ] = outage_level_descr
                        asn = self.prefix_outage_event[prefix][prefix_outage_id]["asn"]
                        as_name = self.prefix_outage_event[prefix][prefix_outage_id][
                            "as_name"
                        ]
                        org_name = self.prefix_outage_event[prefix][prefix_outage_id][
                            "org_name"
                        ]
                        country_chinese_name = self.prefix_outage_event[prefix][
                            prefix_outage_id
                        ]["country"]
                        s_time = self.prefix_outage_event[prefix][prefix_outage_id][
                            "s_time"
                        ]
                        e_time = self.prefix_outage_event[prefix][prefix_outage_id][
                            "e_time"
                        ]
                        event_info = "北京时间 {}, 归属于 {} 的前缀 {} 发生路由回撤中断。".format(
                            s_time,
                            get_as_info(asn, as_name, country_chinese_name),
                            prefix,
                        )
                        self.prefix_outage_event[prefix][prefix_outage_id][
                            "event_info"
                        ] = event_info
                        self.output.capture(
                            "prefix_outage_start",
                            prefix_outage_event=self.prefix_outage_event,
                            source=self.output.source,
                            origin=origin,
                            prefix=prefix,
                            prefix_outage_id=prefix_outage_id,
                            table=self.prefix_table,
                        )
                        detail_url = "{}/{}/{}/{}/{}".format(
                            "prefix_outage",
                            s_time,
                            prefix.replace("/", "-"),
                            prefix_outage_id,
                            self.output.source,
                        )
                        if country_chinese_name in ["中国"]:
                            state = "judge"
                            is_domestic = True
                        else:
                            state = "abroad"
                            is_domestic = False
                        attacked_as = (
                            "AS{}\n({})".format(asn, as_name)
                            if as_name
                            else "AS{}".format(asn)
                        )
                        self.output.capture(
                            "event_start",
                            source=self.output.source,
                            event_type="前缀中断",
                            level=self.prefix_outage_event[prefix][prefix_outage_id][
                                "outage_level"
                            ],
                            s_time=s_time,
                            e_time=e_time,
                            duration=self.prefix_outage_event[prefix][prefix_outage_id][
                                "duration"
                            ],
                            attacker_as=attacked_as,
                            attacked_as=attacked_as,
                            affected_prefix=prefix,
                            event_info=event_info,
                            detail_url=detail_url,
                            attacker_org=org_name,
                            attacked_org=org_name,
                            attacker_country=country_chinese_name,
                            attacked_country=country_chinese_name,
                            state=state,
                            is_domestic=is_domestic,
                            table=self.event_table,
                        )
                        return (True, False)
                elif (
                    reachable_vp_num
                    >= (unreachable_vp_num + reachable_vp_num)
                    * self.output.thresholds["PREFIX_RESTORE_THRESHOLD"]
                ):
                    self.prefix_vp[prefix]["is_outage_now"] = False
                    self.__set_pre_pre_vp_paths(prefix, t, new_vp_paths)
                    prefix_outage_id = self.prefix_vp[prefix]["prefix_outage_id"]
                    origin = self.prefix_outage_event[prefix][prefix_outage_id]["asn"]
                    if prefix in self.as_prefix[origin]["outage_prefix_set"]:
                        self.as_prefix[origin]["outage_prefix_set"].remove(prefix)
                    self.as_prefix[origin]["normal_prefix_set"].add(prefix)
                    s_time = self.prefix_outage_event[prefix][prefix_outage_id][
                        "s_time"
                    ]
                    self.prefix_outage_event[prefix][prefix_outage_id]["e_time"] = t
                    self.prefix_outage_event[prefix][prefix_outage_id]["duration"] = (
                        get_duration(s_time, t)
                    )
                    self.prefix_outage_event[prefix][prefix_outage_id][
                        "next_vp_paths"
                    ] = self.prefix_vp[prefix]["pre_vp_paths"]
                    table = self.prefix_outage_event[prefix][prefix_outage_id]["table"]
                    if bool(table):
                        self.output.capture(
                            "prefix_outage_end",
                            prefix_outage_event=self.prefix_outage_event,
                            source=self.output.source,
                            origin=origin,
                            prefix=prefix,
                            prefix_outage_id=prefix_outage_id,
                            table=table,
                        )
                    event_table = self.prefix_outage_event[prefix][prefix_outage_id][
                        "event_table"
                    ]
                    if bool(event_table):
                        s_time1 = self.prefix_outage_event[prefix][prefix_outage_id][
                            "s_time"
                        ]
                        detail_url = "{}/{}/{}/{}/{}".format(
                            "prefix_outage",
                            s_time1,
                            prefix.replace("/", "-"),
                            prefix_outage_id,
                            self.output.source,
                        )
                        self.output.capture(
                            "event_end",
                            detail_url=detail_url,
                            e_time=self.prefix_outage_event[prefix][prefix_outage_id][
                                "e_time"
                            ],
                            duration=self.prefix_outage_event[prefix][prefix_outage_id][
                                "duration"
                            ],
                            table=event_table,
                        )
                    self.prefix_vp[prefix]["last_start_time"] = s_time
                    self.__set_pre_pre_vp_paths(prefix, t, new_vp_paths)
                    self.output.retire(
                        "prefix_outage",
                        prefix,
                        prefix_outage_id,
                        self.prefix_outage_event[prefix][prefix_outage_id],
                    )
                    del self.prefix_outage_event[prefix][prefix_outage_id]
                    return (False, True)
        return (False, False)

    def __check_as_outage(self, country, origin, t):
        """来源 BGPOutage.py:1844；兼容计算，外部副作用已移除。"""
        org_name = get_as_org_name(self.bgp_info.as_info, origin)
        if org_name:
            if "个人" in org_name or "未知" in org_name:
                return
        if origin in self.as_prefix.keys():
            outage_prefix_num = len(self.as_prefix[origin]["outage_prefix_set"])
            normal_prefix_num = len(self.as_prefix[origin]["normal_prefix_set"])
            if self.as_prefix[origin]["is_outage_now"] == False:
                if (
                    outage_prefix_num
                    > (outage_prefix_num + normal_prefix_num)
                    * self.output.thresholds["AS_OUTAGE_THRESHOLD"]
                ):
                    self.as_prefix[origin]["is_outage_now"] = True
                    if self.as_prefix[origin]["last_start_time"] != "":
                        last_start_time = self.as_prefix[origin]["last_start_time"]
                        last_start_time = datetime.datetime.strptime(
                            last_start_time, "%Y-%m-%d %H:%M:%S"
                        )
                        start_time = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                        if start_time.month != last_start_time.month:
                            self.as_prefix[origin]["as_outage_id"] = 0
                    if country is not None and country in self.country_as.keys():
                        if origin in self.country_as[country]["normal_as_set"]:
                            self.country_as[country]["normal_as_set"].remove(origin)
                        self.country_as[country]["outage_as_set"].add(origin)
                    self.as_prefix[origin]["as_outage_id"] += 1
                    as_outage_id = self.as_prefix[origin]["as_outage_id"]
                    self.as_outage_event.setdefault(origin, dict()).setdefault(
                        as_outage_id, dict()
                    )
                    self.as_outage_event[origin][as_outage_id]["is_private_as"] = (
                        self.as_prefix[origin]["is_private_as"]
                    )
                    self.as_outage_event[origin][as_outage_id]["private_as_city"] = (
                        self.as_prefix[origin]["private_as_city"]
                    )
                    self.as_outage_event[origin][as_outage_id]["country"] = (
                        get_as_country_cn(self.bgp_info.as_info, origin)
                    )
                    self.as_outage_event[origin][as_outage_id]["as_name"] = get_as_name(
                        self.bgp_info.as_info, origin
                    )
                    self.as_outage_event[origin][as_outage_id]["org_name"] = (
                        get_as_org_name(self.bgp_info.as_info, origin)
                    )
                    self.as_outage_event[origin][as_outage_id]["as_type"] = get_as_type(
                        self.bgp_info.as_info, origin
                    )
                    self.as_outage_event[origin][as_outage_id]["as_descr"] = (
                        get_as_descr(self.bgp_info.as_info, origin)
                    )
                    self.as_outage_event[origin][as_outage_id]["as_admin"] = (
                        get_as_admin(self.bgp_info.as_info, origin)
                    )
                    self.as_outage_event[origin][as_outage_id]["s_time"] = t
                    self.as_outage_event[origin][as_outage_id]["e_time"] = None
                    self.as_outage_event[origin][as_outage_id]["duration"] = None
                    self.as_outage_event[origin][as_outage_id]["next_vp_paths"] = dict()
                    self.as_outage_event[origin][as_outage_id]["total_prefix_num"] = (
                        outage_prefix_num + normal_prefix_num
                    )
                    self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_num"
                    ] = outage_prefix_num
                    self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_ratio"
                    ] = outage_prefix_num / (outage_prefix_num + normal_prefix_num)
                    prefix = None
                    for p in self.as_prefix[origin]["outage_prefix_set"]:
                        prefix = p
                        break
                    self.as_outage_event[origin][as_outage_id][
                        "legacy_selected_prefix"
                    ] = prefix
                    self.__set_as_pre_vp_paths(origin, prefix, as_outage_id)
                    self.__set_as_eve_vp_paths(origin, prefix, as_outage_id, t)
                    self.__set_as_outage_pre(origin, as_outage_id)
                    self.as_outage_event[origin][as_outage_id]["outage_level"] = ""
                    (outage_level, outage_level_descr) = self.__as_outage_level(
                        origin=origin, as_outage_id=as_outage_id
                    )
                    self.as_outage_event[origin][as_outage_id]["outage_level"] = (
                        outage_level
                    )
                    self.as_outage_event[origin][as_outage_id]["outage_level_descr"] = (
                        outage_level_descr
                    )
                    self.as_outage_event[origin][as_outage_id]["table"] = self.as_table
                    self.as_outage_event[origin][as_outage_id]["event_table"] = (
                        self.event_table
                    )
                    self.as_outage_event[origin][as_outage_id]["if_change"] = False
                    asn = origin
                    as_name = self.as_outage_event[origin][as_outage_id]["as_name"]
                    org_name = self.as_outage_event[origin][as_outage_id]["org_name"]
                    country_chinese_name = self.as_outage_event[origin][as_outage_id][
                        "country"
                    ]
                    s_time = self.as_outage_event[origin][as_outage_id]["s_time"]
                    e_time = self.as_outage_event[origin][as_outage_id]["e_time"]
                    max_outage_prefix_num = self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_num"
                    ]
                    max_outage_prefix_ratio = "%.2f%%" % (
                        self.as_outage_event[origin][as_outage_id][
                            "max_outage_prefix_ratio"
                        ]
                        * 100
                    )
                    event_info = "北京时间 {} , 归属于 {} 的 {} 个前缀发生路由回撤中断，中断前缀数量占比 {}。".format(
                        s_time,
                        get_as_info(asn, as_name, country_chinese_name),
                        max_outage_prefix_num,
                        max_outage_prefix_ratio,
                    )
                    self.as_outage_event[origin][as_outage_id]["event_info"] = (
                        event_info
                    )
                    self.output.capture(
                        "as_outage_start",
                        as_outage_event=self.as_outage_event,
                        source=self.output.source,
                        origin=origin,
                        as_outage_id=as_outage_id,
                        table=self.as_table,
                    )
                    affected_prefix = (
                        "{} 等 {} 个前缀发生路由回撤中断，中断前缀数量占比 {}".format(
                            self.as_outage_event[origin][as_outage_id][
                                "outage_prefixes"
                            ][0],
                            max_outage_prefix_num,
                            max_outage_prefix_ratio,
                        )
                    )
                    detail_url = "{}/{}/{}/{}/{}".format(
                        "as_outage", s_time, asn, as_outage_id, self.output.source
                    )
                    if country_chinese_name in ["中国"]:
                        state = "judge"
                        is_domestic = True
                    else:
                        state = "abroad"
                        is_domestic = False
                    attacked_as = (
                        "AS{}\n({})".format(asn, as_name)
                        if as_name
                        else "AS{}".format(asn)
                    )
                    self.output.capture(
                        "event_start",
                        source=self.output.source,
                        event_type="AS中断",
                        level=self.as_outage_event[origin][as_outage_id][
                            "outage_level"
                        ],
                        s_time=s_time,
                        e_time=e_time,
                        duration=self.as_outage_event[origin][as_outage_id]["duration"],
                        attacker_as=attacked_as,
                        attacked_as=attacked_as,
                        affected_prefix=affected_prefix,
                        event_info=event_info,
                        detail_url=detail_url,
                        attacker_org=org_name,
                        attacked_org=org_name,
                        attacker_country=country_chinese_name,
                        attacked_country=country_chinese_name,
                        state=state,
                        is_domestic=is_domestic,
                        table=self.event_table,
                    )
            elif (
                normal_prefix_num
                > (outage_prefix_num + normal_prefix_num)
                * self.output.thresholds["AS_RESTORE_THRESHOLD"]
            ):
                self.as_prefix[origin]["is_outage_now"] = False
                if country is not None and country in self.country_as.keys():
                    if origin in self.country_as[country]["outage_as_set"]:
                        self.country_as[country]["outage_as_set"].remove(origin)
                    self.country_as[country]["normal_as_set"].add(origin)
                as_outage_id = self.as_prefix[origin]["as_outage_id"]
                s_time = self.as_outage_event[origin][as_outage_id]["s_time"]
                self.as_outage_event[origin][as_outage_id]["e_time"] = t
                self.as_outage_event[origin][as_outage_id]["duration"] = get_duration(
                    s_time, t
                )
                table = self.as_outage_event[origin][as_outage_id]["table"]
                if bool(table):
                    self.output.capture(
                        "as_outage_end",
                        as_outage_event=self.as_outage_event,
                        source=self.output.source,
                        origin=origin,
                        as_outage_id=as_outage_id,
                        table=table,
                    )
                event_table = self.as_outage_event[origin][as_outage_id]["event_table"]
                if bool(event_table):
                    s_time1 = self.as_outage_event[origin][as_outage_id]["s_time"]
                    asn = origin
                    detail_url = "{}/{}/{}/{}/{}".format(
                        "as_outage", s_time1, asn, as_outage_id, self.output.source
                    )
                    self.output.capture(
                        "event_end",
                        detail_url=detail_url,
                        e_time=self.as_outage_event[origin][as_outage_id]["e_time"],
                        duration=self.as_outage_event[origin][as_outage_id]["duration"],
                        table=event_table,
                    )
                self.as_prefix[origin]["last_start_time"] = s_time
                self.output.retire(
                    "as_outage",
                    origin,
                    as_outage_id,
                    self.as_outage_event[origin][as_outage_id],
                )
                del self.as_outage_event[origin][as_outage_id]
            else:
                as_outage_id = self.as_prefix[origin]["as_outage_id"]
                if (
                    outage_prefix_num
                    > self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_num"
                    ]
                ):
                    self.as_outage_event[origin][as_outage_id]["outage_prefixes"] = (
                        list(self.as_prefix[origin]["outage_prefix_set"])
                    )
                    self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_num"
                    ] = outage_prefix_num
                    self.as_outage_event[origin][as_outage_id]["if_change"] = True
                outage_prefix_ratio = outage_prefix_num / (
                    outage_prefix_num + normal_prefix_num
                )
                if (
                    outage_prefix_ratio
                    > self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_ratio"
                    ]
                ):
                    self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_ratio"
                    ] = outage_prefix_ratio
                    self.as_outage_event[origin][as_outage_id]["if_change"] = True
                total_prefix_num = outage_prefix_num + normal_prefix_num
                if (
                    total_prefix_num
                    > self.as_outage_event[origin][as_outage_id]["total_prefix_num"]
                ):
                    self.as_outage_event[origin][as_outage_id]["total_prefix_num"] = (
                        total_prefix_num
                    )
                    self.as_outage_event[origin][as_outage_id]["if_change"] = True
                if self.as_outage_event[origin][as_outage_id]["if_change"] is True:
                    (outage_level, outage_level_descr) = self.__as_outage_level(
                        origin=origin, as_outage_id=as_outage_id
                    )
                    self.as_outage_event[origin][as_outage_id]["outage_level"] = (
                        outage_level
                    )
                    self.as_outage_event[origin][as_outage_id]["outage_level_descr"] = (
                        outage_level_descr
                    )
                    asn = origin
                    as_name = self.as_outage_event[origin][as_outage_id]["as_name"]
                    org_name = self.as_outage_event[origin][as_outage_id]["org_name"]
                    country_chinese_name = self.as_outage_event[origin][as_outage_id][
                        "country"
                    ]
                    max_outage_prefix_num = self.as_outage_event[origin][as_outage_id][
                        "max_outage_prefix_num"
                    ]
                    max_outage_prefix_ratio = "%.2f%%" % (
                        self.as_outage_event[origin][as_outage_id][
                            "max_outage_prefix_ratio"
                        ]
                        * 100
                    )
                    event_info = "北京时间 {} , 归属于 {} 的 {} 个前缀发生路由回撤中断，中断前缀数量占比 {}。".format(
                        t,
                        get_as_info(asn, as_name, country_chinese_name),
                        max_outage_prefix_num,
                        max_outage_prefix_ratio,
                    )
                    self.as_outage_event[origin][as_outage_id]["event_info"] = (
                        event_info
                    )

    def __check_country_outage(self, country, t):
        """来源 BGPOutage.py:2244；兼容计算，外部副作用已移除。"""
        if country not in self.country_as:
            return
        outage_asns = sorted(
            (int(value) for value in self.country_as[country]["outage_as_set"])
        )
        normal_asns = sorted(
            (int(value) for value in self.country_as[country]["normal_as_set"])
        )
        current_population = sorted(set(outage_asns) | set(normal_asns))
        if not current_population:
            return
        prior_runtime = self.country_outage_v2_runtime.get(country)
        if prior_runtime is None:
            prior_runtime = new_runtime_state(
                source=self.output.source,
                country_code=country,
                collector_id="legacy_live",
                baseline_asns=current_population,
            )
        observation = build_live_observation(
            source=self.output.source,
            country_code=country,
            observed_at_local=t,
            outage_asns=outage_asns,
            normal_asns=normal_asns,
            baseline_asns=prior_runtime["baseline_asns"],
            collector_id="legacy_live",
        )
        reduced = reduce_live_observation(
            prior_runtime,
            observation,
            damaged_ratio_threshold=self.output.thresholds["COUNTRY_OUTAGE_THRESHOLD"],
            detection_confirm_slots=2,
            recovery_confirm_slots=6,
        )
        self.output.append(
            "country_reduction",
            {
                "observation": observation,
                "reduced": reduced,
                "policy": "39578fe-call-count-second-initial-peak-sticky-recovered/v1",
                "cadence": "not_validated",
                "live_prefix_vp": "unavailable",
            },
        )
        next_runtime = reduced["state"]
        incident = next_runtime.get("incident")
        if incident is None:
            self.country_outage_v2_runtime[country] = next_runtime
            return
        lifecycle_action = reduced["lifecycle_action"]
        if lifecycle_action == "started":
            if self.country_as[country]["last_start_time"] != "":
                last_start_time = datetime.datetime.strptime(
                    self.country_as[country]["last_start_time"], "%Y-%m-%d %H:%M:%S"
                )
                start_time = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                if start_time.month != last_start_time.month:
                    self.country_as[country]["country_outage_id"] = 0
            country_outage_id = self.country_as[country]["country_outage_id"] + 1
        else:
            country_outage_id = self.country_as[country]["country_outage_id"]
            if country_outage_id <= 0:
                raise RuntimeError("结构化 Incident 缺少旧 outage_id 投影")
        country_chinese_name = get_country_chinese_name(self.bgp_info.country, country)
        peak_observation = next_runtime["peak_observation"]
        provisional = {
            "total_as_num": peak_observation["cohort"]["baseline_asn_count"],
            "max_outage_as_num": peak_observation["asn_state"]["affected_asn_count"],
            "max_outage_as_ratio": peak_observation["asn_state"]["affected_asn_ratio"],
            "outage_level": "",
        }
        prior_event = self.country_outage_event.get(country, {}).get(country_outage_id)
        if prior_event is not None:
            provisional["outage_level"] = prior_event.get("outage_level", "")
        self.country_outage_event.setdefault(country, {})[country_outage_id] = (
            provisional
        )
        (outage_level, outage_level_descr) = self.__country_outage_level(
            country, country_outage_id
        )
        projection = legacy_peak_projection(
            incident=incident,
            peak_observation=peak_observation,
            country_chinese_name=country_chinese_name,
            outage_level=outage_level,
            outage_level_descr=outage_level_descr,
            outage_id=country_outage_id,
        )
        projection.update(
            {
                "if_change": lifecycle_action == "peak_updated",
                "table": self.country_table,
                "event_table": self.event_table,
                "structured_v2": True,
            }
        )
        self.output.capture(
            "persist_country_outage_v2",
            incident=incident,
            episodes=[next_runtime["episode"]],
            observations=reduced["persist_observations"],
            legacy_table=self.country_table,
            legacy_projection=projection,
            legacy_source=self.output.source,
            legacy_country=country,
            legacy_outage_id=country_outage_id,
        )
        self.country_outage_v2_runtime[country] = next_runtime
        self.country_outage_event[country][country_outage_id] = projection
        self.country_as[country]["country_outage_id"] = country_outage_id
        self.country_as[country]["is_outage_now"] = (
            incident["recovery_state"] != "fully_recovered"
        )
        if lifecycle_action == "started":
            event_info = projection["event_info"]
            s_time = projection["s_time"]
            detail_url = "{}/{}/{}/{}/{}".format(
                "country_outage", s_time, country, country_outage_id, self.output.source
            )
            is_domestic = country_chinese_name in ["中国"]
            event_state = "judge" if is_domestic else "abroad"
            outage_members = projection["outage_ases"]
            attacked_as = (
                "{} 等 {} 个AS发生路由状态异常，占固定 cohort 的 {:.2%}".format(
                    outage_members[0],
                    projection["max_outage_as_num"],
                    projection["max_outage_as_ratio"],
                )
            )
            self.output.capture(
                "event_start",
                source=self.output.source,
                event_type="国家中断",
                level=projection["outage_level"],
                s_time=s_time,
                e_time=None,
                duration=None,
                attacker_as=attacked_as,
                affected_prefix=None,
                event_info=event_info,
                detail_url=detail_url,
                attacker_org=None,
                attacked_org=None,
                attacker_country=country_chinese_name,
                attacked_country=country_chinese_name,
                state=event_state,
                is_domestic=is_domestic,
                table=self.event_table,
            )
            self.output.capture(
                "send_outage_alert",
                event_type="国家中断",
                event_info=event_info,
                detail_url=detail_url,
                level=projection["outage_level"],
                source=self.output.source,
            )
        if lifecycle_action == "fully_recovered":
            detail_url = "{}/{}/{}/{}/{}".format(
                "country_outage",
                projection["s_time"],
                country,
                country_outage_id,
                self.output.source,
            )
            if bool(self.event_table):
                self.output.capture(
                    "event_end",
                    detail_url=detail_url,
                    e_time=projection["e_time"],
                    duration=projection["duration"],
                    table=self.event_table,
                )
            self.country_as[country]["last_start_time"] = projection["s_time"]
            self.output.retire(
                "country_outage",
                country,
                country_outage_id,
                self.country_outage_event[country][country_outage_id],
            )
            del self.country_outage_event[country][country_outage_id]

    @rule("来源 BGPOutage.py:2460；兼容计算，外部副作用已移除。")
    def __prefix_outage_level(self, prefix):
        """来源 BGPOutage.py:2460；兼容计算，外部副作用已移除。"""
        descr_info = "仅仅是一个可能的中断或者轻微的中断。"
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
                        self.bgp_info.prefix_info[prefix]["domain"], "BGPOutage.py:2492"
                    )
                )
            if self.bgp_info.prefix_info[prefix]["domain_auth"] not in [None, ""]:
                domain.extend(
                    self.output.parse_domains(
                        self.bgp_info.prefix_info[prefix]["domain_auth"],
                        "BGPOutage.py:2494",
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
        return (level, descr_info)

    @rule("来源 BGPOutage.py:2501；兼容计算，外部副作用已移除。")
    def __as_outage_level(self, origin, as_outage_id):
        """来源 BGPOutage.py:2501；兼容计算，外部副作用已移除。"""
        descr_info = "仅仅是一个可能的中断或者轻微的中断。"
        level = "low"
        if (
            origin in self.bgp_info.important_as_dict
            and self.as_outage_event[origin][as_outage_id]["total_prefix_num"] > 10
        ):
            level = "high"
            descr_info = "AS{} 是一个重要AS, 并且AS的中断前缀数量超过10。".format(
                origin
            )
            if (
                self.as_outage_event[origin][as_outage_id]["outage_level"] == ""
                or self.as_outage_event[origin][as_outage_id]["outage_level"] == "low"
            ):
                self.as_outage_event[origin][as_outage_id]["outage_level"] = level
                self.as_outage_event[origin][as_outage_id]["outage_level_descr"] = (
                    descr_info
                )
        elif (
            self.as_outage_event[origin][as_outage_id]["total_prefix_num"] > 500
            and self.as_outage_event[origin][as_outage_id]["max_outage_prefix_ratio"]
            > 0.3
        ):
            level = "middle"
            descr_info = "中断前缀的比例超过30%。"
            if (
                self.as_outage_event[origin][as_outage_id]["outage_level"] == ""
                or self.as_outage_event[origin][as_outage_id]["outage_level"] == "low"
            ):
                self.as_outage_event[origin][as_outage_id]["outage_level"] = level
                self.as_outage_event[origin][as_outage_id]["outage_level_descr"] = (
                    descr_info
                )
        return (level, descr_info)

    @rule("来源 BGPOutage.py:2531；兼容计算，外部副作用已移除。")
    def __country_outage_level(self, country, country_outage_id):
        """来源 BGPOutage.py:2531；兼容计算，外部副作用已移除。"""
        total_as_num = self.country_outage_event[country][country_outage_id][
            "total_as_num"
        ]
        max_outage_as_num = self.country_outage_event[country][country_outage_id][
            "max_outage_as_num"
        ]
        max_outage_as_ratio = self.country_outage_event[country][country_outage_id][
            "max_outage_as_ratio"
        ]
        if (
            max_outage_as_ratio >= 0.075
            and total_as_num >= 100
            and (max_outage_as_num >= 5)
        ):
            level = "high"
            descr_info = "该国中断的AS数量大于或等于该国AS总数的7.5%。"
            if (
                self.country_outage_event[country][country_outage_id]["outage_level"]
                != "high"
            ):
                self.country_outage_event[country][country_outage_id][
                    "outage_level"
                ] = level
                self.country_outage_event[country][country_outage_id][
                    "outage_level_descr"
                ] = descr_info
        elif 0.05 <= max_outage_as_ratio < 0.075:
            level = "middle"
            descr_info = (
                "该国中断的AS数量大于或等于该国AS总数的5%，且小于该国AS总数量的7.5%。"
            )
        else:
            level = "low"
            descr_info = "仅仅是一个可能的中断或者轻微的中断。"
        return (level, descr_info)

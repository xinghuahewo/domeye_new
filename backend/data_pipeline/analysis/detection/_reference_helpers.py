"""固定旧版本静态字段解释；仅接收显式内存资料。"""

import datetime


def get_as_country(as_info: dict, asn: str):
    """
    Returns the country to which the autonomous system number asn belongs
    If there is no such asn in the as_info, return None
    :param as_info: Autonomous System Information
    :param asn: Autonomous system number
    :return: A country name or None
    """
    if "_" in asn:
        public_as = asn.split("_")[0]
        return (
            as_info[public_as].get("as_country")
            if public_as in as_info and as_info[public_as].get("as_country") != ""
            else ""
        )
    else:
        return (
            as_info[asn].get("as_country")
            if asn in as_info and as_info[asn].get("as_country") != ""
            else ""
        )


def get_as_country_cn(as_info: dict, asn: str):
    """
    Returns the country to which the autonomous system number asn belongs
    If there is no such asn in the as_info, return None
    :param as_info: Autonomous System Information
    :param asn: Autonomous system number
    :return: A country name or None
    """
    if "_" in asn:
        public_as = asn.split("_")[0]
        return (
            as_info[public_as].get("as_country_cn")
            if public_as in as_info and as_info[public_as].get("as_country_cn") != ""
            else ""
        )
    else:
        return (
            as_info[asn].get("as_country_cn")
            if asn in as_info and as_info[asn].get("as_country_cn") != ""
            else ""
        )


def get_as_name(as_info: dict, asn: str):
    """
    Returns the name of the autonomous system numbered asn
    If there is no such asn in the as_info, return None
    :param as_info: Autonomous System Information
    :param asn: Autonomous system number
    :return: An Autonomous System name or None
    """
    if "_" in asn:
        public_as = asn.split("_")[0]
        return (
            as_info[public_as].get("as_name")
            if public_as in as_info and as_info[public_as].get("as_name") != ""
            else ""
        )
    else:
        return (
            as_info[asn].get("as_name")
            if asn in as_info and as_info[asn].get("as_name") != ""
            else ""
        )


def get_as_org_name(as_info: dict, asn: str):
    """
    Returns the name of the organization to which the autonomous system with number asn belongs
    If there is no such asn in the as_info, return None
    :param as_info: Autonomous System Information
    :param asn: Autonomous system number
    :return: A business name or None
    """
    if "_" in asn:
        public_as = asn.split("_")[0]
        if public_as in as_info and as_info[public_as].get("org_name_cn") not in [
            "",
            None,
        ]:
            return as_info[public_as].get("org_name_cn")
        if public_as in as_info and as_info[public_as].get("org_name") not in [
            "",
            None,
        ]:
            return as_info[public_as].get("org_name")
        return ""
    else:
        if asn in as_info and as_info[asn].get("org_name_cn") not in ["", None]:
            return as_info[asn].get("org_name_cn")
        if asn in as_info and as_info[asn].get("org_name") not in ["", None]:
            return as_info[asn].get("org_name")
        return ""


def get_as_org_name_en(as_info: dict, asn: str):
    """
    Returns the name of the organization to which the autonomous system with number asn belongs
    If there is no such asn in the as_info, return None
    :param as_info: Autonomous System Information
    :param asn: Autonomous system number
    :return: A business name or None
    """
    if "_" in asn:
        public_as = asn.split("_")[0]
        if public_as in as_info and as_info[public_as].get("org_name") not in [
            "",
            None,
        ]:
            return as_info[public_as].get("org_name")
        return ""
    else:
        if asn in as_info and as_info[asn].get("org_name") not in ["", None]:
            return as_info[asn].get("org_name")
        return ""


def get_as_type(as_info: dict, asn: str):
    if "_" in asn:
        public_as = asn.split("_")[0]
        if public_as in as_info and as_info[public_as].get("type_cn") not in ["", None]:
            return as_info[public_as].get("type_cn")
        if public_as in as_info and as_info[public_as].get("type") not in ["", None]:
            return as_info[public_as].get("type")
        return ""
    else:
        if asn in as_info and as_info[asn].get("type_cn") not in ["", None]:
            return as_info[asn].get("type_cn")
        if asn in as_info and as_info[asn].get("type") not in ["", None]:
            return as_info[asn].get("type")
        return ""


def get_as_descr(as_info: dict, asn: str):
    if "_" in asn:
        public_as = asn.split("_")[0]
        if public_as in as_info and as_info[public_as].get("descr_cn") != "":
            return as_info[public_as].get("descr_cn")
        if public_as in as_info and as_info[public_as].get("descr") != "":
            return as_info[public_as].get("descr")
        return ""
    else:
        if asn in as_info and as_info[asn].get("descr_cn") != None:
            return as_info[asn].get("descr_cn")
        if asn in as_info and as_info[asn].get("descr") != "":
            return as_info[asn].get("descr")
        return ""


def get_as_admin(as_info: dict, asn: str):
    if "_" in asn:
        public_as = asn.split("_")[0]
        return (
            as_info[public_as].get("admin_info")
            if public_as in as_info and as_info[public_as].get("admin_info") != ""
            else ""
        )
    else:
        return (
            as_info[asn].get("admin_info")
            if asn in as_info and as_info[asn].get("admin_info") != ""
            else ""
        )


def get_leak_triplet(triplet_info: dict, first_as: str, second_as: str, third_as: str):
    """
    获取leak triplet稳定度信息
    """
    if (
        first_as in triplet_info
        and second_as in triplet_info[first_as]
        and (third_as in triplet_info[first_as][second_as])
    ):
        return triplet_info[first_as][second_as][third_as]["stability"]
    else:
        return 0


def get_private_as_city(private_as_dict: dict, asn: str):
    """
    返回私有AS的分布城市
    :param private_as_dict: 私有AS信息字典
    :param asn: 格式为"公有AS_私有AS"
    :return: 私有AS的分布城市
    """
    if "_" in asn:
        public_as = asn.split("_")[0]
        private_as = asn.split("_")[1]
        try:
            city = private_as_dict[public_as][private_as]["city"]
        except:
            city = "not found"
        if city in ["", "NULL", "None"]:
            city = "not found"
        return city
    else:
        return None


def get_country_chinese_name(country_info_dict: dict, two_letter_code: str):
    """
    返回国家的中文简称
    :param country_info_dict: 国家信息字典
    :param two_letter_code: 国家两字母简称
    :return: 国家的中文简称
    """
    if two_letter_code is None:
        return None
    if two_letter_code in country_info_dict:
        if country_info_dict[two_letter_code].get("chinese_short_name") != "":
            return country_info_dict[two_letter_code].get("chinese_short_name")
    if two_letter_code == "EU":
        return "欧盟"
    return two_letter_code


def get_duration(start_time: str, end_time: str) -> str:
    """
    Return time difference
    :param start_time: start_time, shaped like 2022-04-26 15:14:37
    :param end_time: end_time, shaped like 2022-05-04 00:19:24
    :return: time difference
    """
    s_time_struct = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    e_time_struct = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    total_seconds = (e_time_struct - s_time_struct).total_seconds()
    (m, s) = divmod(total_seconds, 60)
    (h, m) = divmod(m, 60)
    (d, h) = divmod(h, 24)
    (d, h, m, s) = (int(d), int(h), int(m), int(s))
    return "{} days {} hours {} minutes {} seconds".format(d, h, m, s)


def get_as_info(asn, as_name, as_country):
    as_info = "AS {}".format(asn)
    if as_name is None and as_country is None:
        return as_info
    as_info += "("
    if as_name is not None:
        as_info += "{} ".format(as_name)
    if as_country is not None:
        if as_name is not None:
            as_info += ", {} ".format(as_country)
        else:
            as_info += "{} ".format(as_country)
    as_info += ")"
    return as_info

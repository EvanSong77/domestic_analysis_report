# -*- coding: utf-8 -*-
# @Time    : 2025/11/10 14:23
# @Author  : EvanSong
from typing import Dict, Counter

from src.utils import log_utils
from src.utils.db_utils import MySQLDataConnector

logger = log_utils.logger


def _format_detail_data_to_string(level: str, diagnosis_type: str, local_vars: dict) -> str:
    """将处理后的detail数据格式化为字符串

    Args:
        level: 维度级别 (TOTAL/PROVINCE/OFFICE)
        diagnosis_type: 诊断类型 (ORG/CHAN/IND/PROD)
        local_vars: 本地变量字典，包含所有处理后的变量

    Returns:
        格式化后的数据字符串
    """
    result_lines = []

    # ====================== TOTAL 维度 ======================
    if level == "TOTAL":
        if diagnosis_type == "ORG":
            negative_gpm_count = local_vars.get('negative_gpm_count', 0)
            influence_top5_datas = local_vars.get('influence_top5_datas', [])
            max_influences = local_vars.get('max_influences', [])
            low_gpm_datas = local_vars.get('low_gpm_datas', [])
            low_max_influences = local_vars.get('low_max_influences', [])

            # ===== 负毛利项目部分 =====
            if influence_top5_datas:
                # 统计省份出现次数
                from collections import Counter
                province_counts = Counter([item['ASSESS_CENTER_NAME_LV5'] for item in influence_top5_datas])
                most_common_province, count_in_province = province_counts.most_common(1)[0] if province_counts else ("", 0)

                # 直接从 max_influences 提取业务员和客户信息(已去重)
                salesmen = [f"{item[0]}({item[1]})" for item in max_influences[:2]]
                customers = list(set([f"{item[2]}({item[3]})" for item in max_influences[:2]]))

                # 构建第一行
                result_lines.append(
                    f"5、项目负毛利异常{negative_gpm_count}个，以下所示TOP5："
                    f"其中{most_common_province}{count_in_province}个，"
                    f"业务员{'、'.join(salesmen)}，"
                    f"客户{'、'.join(customers)}，对整体影响较大；"
                )

                # 添加详细项目信息(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['ASSESS_CENTER_NAME_LV5']}-"
                        f"{data['COMPETITION_NAME']}({data['CRM_CONTRACT_NUMBER']})-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})-"
                        f"{data['GPM']:.1%}；"
                    )
            else:
                result_lines.append(f"5、项目负毛利异常{negative_gpm_count}个")

            # ===== 低毛利项目部分 =====
            if low_gpm_datas:
                # 统计省份出现次数
                from collections import Counter
                province_counts = Counter([item['ASSESS_CENTER_NAME_LV5'] for item in low_gpm_datas])
                most_common_province, count_in_province = province_counts.most_common(1)[0] if province_counts else ("", 0)

                # 直接从 low_max_influences 提取业务员和客户信息(已去重)
                salesmen = [f"{item[0]}({item[1]})" for item in low_max_influences[:2]]
                customers = list(set([f"{item[2]}({item[3]})" for item in low_max_influences[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、国内>100万低毛利项目TOP5，以下列示TOP5："
                    f"其中{most_common_province}{count_in_province}个，"
                    f"业务员{'、'.join(salesmen)}，"
                    f"客户{'、'.join(customers)}，对整体影响较大；"
                )

                # 添加详细项目信息(前3个)
                for data in low_gpm_datas[:3]:
                    result_lines.append(
                        f"       {data['ASSESS_CENTER_NAME_LV5']}-"
                        f"{data['COMPETITION_NAME']}({data['CRM_CONTRACT_NUMBER']})-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})-"
                        f"{data['GPM']:.1%}；"
                    )

        elif diagnosis_type == "CHAN":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计省份出现次数
                from collections import Counter
                province_counts = Counter([item['ASSESS_CENTER_NAME_LV5'] for item in influence_top5_datas])
                most_common_province, count_in_province = province_counts.most_common(1)[0] if province_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['ORDER_CUSTOMER_NAME']}({item['ORDER_CUSTOMER_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、主航道占比<50% or 网线硬盘指定外购占比>10%客户及业务员TOP10，"
                    f"以下列示TOP5：其中{most_common_province}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细数据(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['ASSESS_CENTER_NAME_LV5']}-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"主航道占比{data['INCOME_RATE']:.1%}、"
                        f"网线硬盘指定外购占比{data.get('SPECIAL_RATE', 0):.1%}；"
                    )

        elif diagnosis_type == "IND":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计省份出现次数
                from collections import Counter
                province_counts = Counter([item['INDUSTRY_LV1_NAME'] for item in influence_top5_datas])
                most_common_province, count_in_province = province_counts.most_common(1)[0] if province_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['GROUP_CUST_NAME']}({item['GROUP_CUST_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"7、各行业毛利异常TOP5客户(GROUP)，以下列示TOP5："
                    f"其中{most_common_province}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细行业信息(前3个)
                for data in influence_top5_datas[:3]:
                    income_rate_str = f"{data.get('INCOME_RATE'):.1%}" if data.get('INCOME_RATE') is not None else "为空"
                    result_lines.append(
                        f"       {data['INDUSTRY_LV1_NAME']}-"
                        f"{data['GROUP_CUST_NAME']}({data['GROUP_CUST_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"收入{data['INCOME_TOTAL']:.2f}万、"
                        f"毛利{data['GPM']:.1%}、"
                        f"主航道占比{income_rate_str}；"
                    )

        elif diagnosis_type == "PROD":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计省份出现次数
                from collections import Counter
                province_counts = Counter([item['PRODUCT_LINE_LV2_INLAND_REPORT'] for item in influence_top5_datas])
                most_common_province, count_in_province = province_counts.most_common(1)[0] if province_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['GROUP_CUST_NAME']}({item['GROUP_CUST_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、各产品线毛利异常TOP5客户(GROUP)，以下列示TOP5："
                    f"其中{most_common_province}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细产品线信息(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['PRODUCT_LINE_LV2_INLAND_REPORT']}-"
                        f"{data['GROUP_CUST_NAME']}({data['GROUP_CUST_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"收入{data['INCOME_TOTAL']:.2f}万、"
                        f"毛利{data['GPM']:.1%}；"
                    )

    # ====================== PROVINCE 维度 ======================
    elif level == "PROVINCE":
        province_name = local_vars.get('params', {}).get('provinceName', '该省')

        if diagnosis_type == "ORG":
            negative_gpm_count = local_vars.get('negative_gpm_count', 0)
            influence_top5_datas = local_vars.get('influence_top5_datas', [])
            max_influences = local_vars.get('max_influences', [])
            low_gpm_datas = local_vars.get('low_gpm_datas', [])
            low_max_influences = local_vars.get('low_max_influences', [])

            # ===== 负毛利项目部分 =====
            if influence_top5_datas:
                # 统计二级办出现次数
                from collections import Counter
                office_counts = Counter([item['ASSESS_CENTER_NAME_LV6'] for item in influence_top5_datas])
                most_common_office, count_in_office = office_counts.most_common(1)[0] if office_counts else ("", 0)

                # 直接从 max_influences 提取业务员和客户信息
                salesmen = [f"{item[0]}({item[1]})" for item in max_influences[:2]]
                customers = list(set([f"{item[2]}({item[3]})" for item in max_influences[:2]]))

                # 构建第一行
                result_lines.append(
                    f"5、项目负毛利异常{negative_gpm_count}个，"
                    f"其中{most_common_office}{count_in_office}个，"
                    f"业务员{'、'.join(salesmen)}，"
                    f"客户{'、'.join(customers)}，对整体影响较大；"
                )

                # 添加详细项目信息(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['ASSESS_CENTER_NAME_LV6']}-"
                        f"{data['COMPETITION_NAME']}({data['CRM_CONTRACT_NUMBER']})-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})-"
                        f"{data['GPM']:.1%}；"
                    )
            else:
                result_lines.append(f"5、项目负毛利异常{negative_gpm_count}个")

            # ===== 低毛利项目部分 =====
            if low_gpm_datas:
                # 统计二级办出现次数
                from collections import Counter
                office_counts = Counter([item['ASSESS_CENTER_NAME_LV6'] for item in low_gpm_datas])
                most_common_office, count_in_office = office_counts.most_common(1)[0] if office_counts else ("", 0)

                # 直接从 low_max_influences 提取业务员和客户信息
                salesmen = [f"{item[0]}({item[1]})" for item in low_max_influences[:2]]
                customers = list(set([f"{item[2]}({item[3]})" for item in low_max_influences[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、{province_name}>30万低毛利项目TOP5，"
                    f"其中{most_common_office}{count_in_office}个，"
                    f"业务员{'、'.join(salesmen)}，"
                    f"客户{'、'.join(customers)}，对整体影响较大；"
                )

                # 添加详细项目信息(前3个)
                for data in low_gpm_datas[:3]:
                    result_lines.append(
                        f"       {data['ASSESS_CENTER_NAME_LV6']}-"
                        f"{data['COMPETITION_NAME']}({data['CRM_CONTRACT_NUMBER']})-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})-"
                        f"{data['GPM']:.1%}；"
                    )

        elif diagnosis_type == "CHAN":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计二级办出现次数
                from collections import Counter
                office_counts = Counter([item['ASSESS_CENTER_NAME_LV6'] for item in influence_top5_datas])
                most_common_office, count_in_office = office_counts.most_common(1)[0] if office_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['ORDER_CUSTOMER_NAME']}({item['ORDER_CUSTOMER_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、主航道占比<50% or 网线硬盘指定外购占比>10%客户及业务员TOP5，"
                    f"以下列示TOP5：其中{most_common_office}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细数据(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['ASSESS_CENTER_NAME_LV6']}-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"主航道占比{data['INCOME_RATE']:.1%}、"
                        f"网线硬盘指定外购占比{data.get('SPECIAL_RATE', 0):.1%}；"
                    )

        elif diagnosis_type == "IND":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计二级办出现次数
                from collections import Counter
                office_counts = Counter([item['INDUSTRY_LV1_NAME'] for item in influence_top5_datas])
                most_common_office, count_in_office = office_counts.most_common(1)[0] if office_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['GROUP_CUST_NAME']}({item['GROUP_CUST_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"7、各行业毛利异常TOP5客户(GROUP)，以下列示TOP5："
                    f"其中{most_common_office}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细行业信息(前3个)
                for data in influence_top5_datas[:3]:
                    income_rate_str = f"{data.get('INCOME_RATE'):.1%}" if data.get('INCOME_RATE') is not None else "为空"
                    result_lines.append(
                        f"       {data['INDUSTRY_LV1_NAME']}-"
                        f"{data['GROUP_CUST_NAME']}({data['GROUP_CUST_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"收入{data['INCOME_TOTAL']:.2f}万、"
                        f"毛利{data['GPM']:.1%}、"
                        f"主航道占比{income_rate_str}；"
                    )

        elif diagnosis_type == "PROD":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计二级办出现次数
                from collections import Counter
                office_counts = Counter([item['PRODUCT_LINE_LV2_INLAND_REPORT'] for item in influence_top5_datas])
                most_common_office, count_in_office = office_counts.most_common(1)[0] if office_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['GROUP_CUST_NAME']}({item['GROUP_CUST_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、各产品线毛利异常TOP5客户(GROUP)，以下列示TOP5："
                    f"其中{most_common_office}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细产品线信息(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['PRODUCT_LINE_LV2_INLAND_REPORT']}-"
                        f"{data['GROUP_CUST_NAME']}({data['GROUP_CUST_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"收入{data['INCOME_TOTAL']:.2f}万、"
                        f"毛利{data['GPM']:.1%}；"
                    )

    # ====================== OFFICE 维度 ======================
    elif level == "OFFICE":
        office_name = local_vars.get('params', {}).get('officeLv2Name', '该二级办')

        if diagnosis_type == "ORG":
            negative_gpm_count = local_vars.get('negative_gpm_count', 0)
            influence_top5_datas = local_vars.get('influence_top5_datas', [])
            max_influences = local_vars.get('max_influences', [])
            low_gpm_datas = local_vars.get('low_gpm_datas', [])
            low_max_influences = local_vars.get('low_max_influences', [])

            # ===== 负毛利项目部分 =====
            if influence_top5_datas:
                # 直接从 max_influences 提取业务员和客户信息
                salesmen = [f"{item[0]}({item[1]})" for item in max_influences[:2]]
                customers = list(set([f"{item[2]}({item[3]})" for item in max_influences[:2]]))

                # 构建第一行
                result_lines.append(
                    f"4、项目负毛利异常{negative_gpm_count}个，"
                    f"其中业务员{'、'.join(salesmen)}，"
                    f"客户{'、'.join(customers)}，对整体影响较大；"
                )

                # 添加详细项目信息(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"          {data['SALESMAN_NAME']}({data['SALESMAN_ID']})-"
                        f"{data['COMPETITION_NAME']}({data['CRM_CONTRACT_NUMBER']})-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['GPM']:.1%}；"
                    )
            else:
                result_lines.append(f"4、项目负毛利异常{negative_gpm_count}个")

            # ===== 低毛利项目部分 =====
            if low_gpm_datas:
                # 直接从 low_max_influences 提取业务员和客户信息
                salesmen = [f"{item[0]}({item[1]})" for item in low_max_influences[:2]]
                customers = list(set([f"{item[2]}({item[3]})" for item in low_max_influences[:2]]))

                # 构建第一行
                result_lines.append(
                    f"5、{office_name}>10万低毛利项目TOP5，"
                    f"其中业务员{'、'.join(salesmen)}，"
                    f"客户{'、'.join(customers)}，对整体影响较大；"
                )

                # 添加详细项目信息(前3个)
                for data in low_gpm_datas[:3]:
                    result_lines.append(
                        f"          {data['SALESMAN_NAME']}({data['SALESMAN_ID']})-"
                        f"{data['COMPETITION_NAME']}({data['CRM_CONTRACT_NUMBER']})-"
                        f"{data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['GPM']:.1%}；"
                    )

        elif diagnosis_type == "CHAN":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 提取客户信息
                customers = list(set([f"{item['ORDER_CUSTOMER_NAME']}({item['ORDER_CUSTOMER_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、主航道占比<50% or 网线硬盘指定外购占比>10%客户及业务员TOP3，"
                    f"以下列示TOP5：其中客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细数据(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['ORDER_CUSTOMER_NAME']}({data['ORDER_CUSTOMER_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"主航道占比{data['INCOME_RATE']:.1%}、"
                        f"网线硬盘指定外购占比{data.get('SPECIAL_RATE', 0):.1%}；"
                    )

        elif diagnosis_type == "IND":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计省份出现次数
                from collections import Counter
                province_counts = Counter([item['INDUSTRY_LV1_NAME'] for item in influence_top5_datas])
                most_common_province, count_in_province = province_counts.most_common(1)[0] if province_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['GROUP_CUST_NAME']}({item['GROUP_CUST_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"6、各行业毛利异常TOP5客户(GROUP)，以下列示TOP5："
                    f"其中{most_common_province}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细行业信息(前3个)
                for data in influence_top5_datas[:3]:
                    income_rate_str = f"{data.get('INCOME_RATE'):.1%}" if data.get('INCOME_RATE') is not None else "为空"
                    result_lines.append(
                        f"       {data['INDUSTRY_LV1_NAME']}-"
                        f"{data['GROUP_CUST_NAME']}({data['GROUP_CUST_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"收入{data['INCOME_TOTAL']:.2f}万、"
                        f"毛利{data['GPM']:.1%}、"
                        f"主航道占比{income_rate_str}；"
                    )

        elif diagnosis_type == "PROD":
            influence_top5_datas = local_vars.get('influence_top5_datas', [])

            if influence_top5_datas:
                # 统计省份出现次数
                from collections import Counter
                province_counts = Counter([item['PRODUCT_LINE_LV2_INLAND_REPORT'] for item in influence_top5_datas])
                most_common_province, count_in_province = province_counts.most_common(1)[0] if province_counts else ("", 0)

                # 提取客户信息
                customers = list(set([f"{item['GROUP_CUST_NAME']}({item['GROUP_CUST_CRM']})" for item in influence_top5_datas[:2]]))

                # 构建第一行
                result_lines.append(
                    f"5、各产品线毛利异常TOP5客户(GROUP)，以下列示TOP5："
                    f"其中{most_common_province}客户{customers[0] if customers else ''}，"
                    f"影响较大；"
                )

                # 添加详细产品线信息(前3个)
                for data in influence_top5_datas[:3]:
                    result_lines.append(
                        f"       {data['PRODUCT_LINE_LV2_INLAND_REPORT']}-"
                        f"{data['GROUP_CUST_NAME']}({data['GROUP_CUST_CRM']})-"
                        f"{data['SALESMAN_NAME']}({data['SALESMAN_ID']})："
                        f"收入{data['INCOME_TOTAL']:.2f}万、"
                        f"毛利{data['GPM']:.1%}；"
                    )

    # 如果没有数据，返回空字符串
    if not result_lines:
        return ""

    return "\n".join(result_lines)


def get_where(sql: str, params: Dict):
    base_sql = sql
    if "WHERE" not in sql.upper():
        base_sql += " WHERE 1=1"

    conditions = []
    con_params = {}

    # 1. 期间维度 (必选)
    if params.get('period'):
        conditions.append("PERIOD = %(period)s")
        con_params["period"] = params['period']

    # 2. 省区维度
    if params.get('provinceName'):
        conditions.append("ASSESS_CENTER_NAME_LV5 = %(province)s")
        con_params["province"] = params['provinceName']

    # 3. 二级办维度
    if params.get('officeLv2Name'):
        conditions.append("ASSESS_CENTER_NAME_LV6 = %(office_lv2)s")
        con_params["office_lv2"] = params['officeLv2Name']

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    # 确定层级
    if not params.get('provinceName') and not params.get('officeLv2Name'):
        level = "TOTAL"
    elif params.get('provinceName') and not params.get('officeLv2Name'):
        level = "PROVINCE"
    else:
        level = "OFFICE"

    base_sql += " AND ORG_LEVEL = %(level)s"
    con_params['level'] = level

    return base_sql, con_params


async def deal_detail_datas_before_model(query_result: Dict) -> str:
    """调用模型前对detail数据进行处理

    Args:
        query_result: 查询结果，包含数据和参数

    Returns:
        处理后的数据字符串
    """
    db_connector = MySQLDataConnector(db_type="data_db")
    # 连接数据库
    db_connector.connect_database()
    datas = query_result['list_data']
    params = query_result['params']

    # 确定维度级别
    if not params.get('provinceName') and not params.get('officeLv2Name'):
        level = "TOTAL"  # 全国维度
    elif params.get('provinceName') and not params.get('officeLv2Name'):
        level = "PROVINCE"  # 省份维度
    else:
        level = "OFFICE"  # 二级办维度

    diagnosis_type = params.get('diagnosisType', '')
    if level == "TOTAL":
        if diagnosis_type == "ORG":
            # 整体影响top5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND GPM_UNACHIEVE = 1 AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

            # 负毛利max influence
            max_influences = []
            if influence_top5_datas:
                # 项目负毛利异常数量
                negative_gpm_count = int(influence_top5_datas[0].get('NEGATIVE_GPM_NUMBER', 0))
                names = [item['ASSESS_CENTER_NAME_LV5'] for item in influence_top5_datas]
                count = Counter(names)
                most_common_name = count.most_common(1)[0][0]
                filtered_data = [item for item in influence_top5_datas if item['ASSESS_CENTER_NAME_LV5'] == most_common_name]
                for f_data in filtered_data:
                    max_influences.append([f"{f_data['SALESMAN_NAME']}", f"{f_data['SALESMAN_ID']}", f"{f_data['ORDER_CUSTOMER_NAME']}", f"{f_data['ORDER_CUSTOMER_CRM']}"])

            # 低毛利 TOP5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND GPM_UNACHIEVE = 0 AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            low_gpm_datas = db_connector.query_results

            # 低毛利max influence
            low_max_influences = []
            if low_gpm_datas:
                low_names = [item['ASSESS_CENTER_NAME_LV5'] for item in low_gpm_datas]
                low_count = Counter(low_names)
                low_most_common_name = low_count.most_common(1)[0][0]
                filtered_data = [item for item in low_gpm_datas if item['ASSESS_CENTER_NAME_LV5'] == low_most_common_name]
                for f_data in filtered_data:
                    low_max_influences.append([f"{f_data['SALESMAN_NAME']}", f"{f_data['SALESMAN_ID']}", f"{f_data['ORDER_CUSTOMER_NAME']}", f"{f_data['ORDER_CUSTOMER_CRM']}"])

        elif diagnosis_type == "CHAN":
            # 主航道占比<50% or 网线硬盘指定外购占比>10% 客户及业务员top5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

        elif diagnosis_type == "IND":
            # 整体影响top5 - 修复：添加ASSESS_CENTER_NAME_LV5字段
            base_sql, con_params = get_where(
                "SELECT ASSESS_CENTER_NAME_LV5,INDUSTRY_LV1_NAME,GROUP_CUST_NAME,GROUP_CUST_CRM,SALESMAN_NAME,SALESMAN_ID,INCOME_TOTAL,GPM,INCOME_RATE,GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS",
                params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

        elif diagnosis_type == "PROD":
            # 整体影响top5 - 修复：添加ASSESS_CENTER_NAME_LV5字段
            base_sql, con_params = get_where(
                "SELECT ASSESS_CENTER_NAME_LV5,PRODUCT_LINE_LV2_INLAND_REPORT,GROUP_CUST_NAME,GROUP_CUST_CRM,SALESMAN_NAME,SALESMAN_ID,INCOME_TOTAL,GPM,GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS",
                params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

    elif level == "PROVINCE":
        if diagnosis_type == "ORG":
            # 整体影响top5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND GPM_UNACHIEVE = 1 AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

            # max influence
            max_influences = []
            if influence_top5_datas:
                # 项目负毛利异常数量
                negative_gpm_count = int(influence_top5_datas[0].get('NEGATIVE_GPM_NUMBER', 0))
                names = [item['ASSESS_CENTER_NAME_LV6'] for item in influence_top5_datas]
                count = Counter(names)
                most_common_name = count.most_common(1)[0][0]
                filtered_data = [item for item in influence_top5_datas if item['ASSESS_CENTER_NAME_LV6'] == most_common_name]
                for f_data in filtered_data:
                    max_influences.append([f"{f_data['SALESMAN_NAME']}", f"{f_data['SALESMAN_ID']}", f"{f_data['ORDER_CUSTOMER_NAME']}", f"{f_data['ORDER_CUSTOMER_CRM']}"])

            # 低毛利 TOP5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND GPM_UNACHIEVE = 0 AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            low_gpm_datas = db_connector.query_results

            # 低毛利max influence
            low_max_influences = []
            if low_gpm_datas:
                low_names = [item['ASSESS_CENTER_NAME_LV6'] for item in low_gpm_datas]
                low_count = Counter(low_names)
                low_most_common_name = low_count.most_common(1)[0][0]
                filtered_data = [item for item in low_gpm_datas if item['ASSESS_CENTER_NAME_LV6'] == low_most_common_name]
                for f_data in filtered_data:
                    low_max_influences.append([f"{f_data['SALESMAN_NAME']}", f"{f_data['SALESMAN_ID']}", f"{f_data['ORDER_CUSTOMER_NAME']}", f"{f_data['ORDER_CUSTOMER_CRM']}"])

        elif diagnosis_type == "CHAN":
            # 主航道占比<50% or 网线硬盘指定外购占比>10% 客户及业务员top5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

        elif diagnosis_type == "IND":
            # 整体影响top5 - 修复：添加ASSESS_CENTER_NAME_LV6字段
            base_sql, con_params = get_where(
                "SELECT ASSESS_CENTER_NAME_LV6,INDUSTRY_LV1_NAME,GROUP_CUST_NAME,GROUP_CUST_CRM,SALESMAN_NAME,SALESMAN_ID,INCOME_TOTAL,GPM,INCOME_RATE,GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS",
                params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

        elif diagnosis_type == "PROD":
            # 整体影响top5 - 修复：添加ASSESS_CENTER_NAME_LV6字段
            base_sql, con_params = get_where(
                "SELECT ASSESS_CENTER_NAME_LV6,PRODUCT_LINE_LV2_INLAND_REPORT,GROUP_CUST_NAME,GROUP_CUST_CRM,SALESMAN_NAME,SALESMAN_ID,INCOME_TOTAL,GPM,GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS",
                params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

    elif level == "OFFICE":
        if diagnosis_type == "ORG":
            # 整体影响top5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND GPM_UNACHIEVE = 1 AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

            # max influence - 修复：统一结构，避免嵌套列表
            max_influences = []
            if influence_top5_datas:
                # 项目负毛利异常数量
                negative_gpm_count = int(influence_top5_datas[0].get('NEGATIVE_GPM_NUMBER', 0))
                max_sale_names = [item['SALESMAN_NAME'] for item in influence_top5_datas]
                count = Counter(max_sale_names)
                most_common_name = count.most_common(1)[0][0]
                filtered_data = [item for item in influence_top5_datas if item['SALESMAN_NAME'] == most_common_name]
                for f_data in filtered_data:
                    max_influences.append([f"{f_data['SALESMAN_NAME']}", f"{f_data['SALESMAN_ID']}", f"{f_data['ORDER_CUSTOMER_NAME']}", f"{f_data['ORDER_CUSTOMER_CRM']}"])

            # 低毛利 TOP5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND GPM_UNACHIEVE = 0 AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            low_gpm_datas = db_connector.query_results

            # 低毛利max influence - 修复：统一结构
            low_max_influences = []
            if low_gpm_datas:
                low_names = [item['SALESMAN_NAME'] for item in low_gpm_datas]
                low_count = Counter(low_names)
                low_most_common_name = low_count.most_common(1)[0][0]
                filtered_data = [item for item in low_gpm_datas if item['SALESMAN_NAME'] == low_most_common_name]
                for f_data in filtered_data:
                    low_max_influences.append([f"{f_data['SALESMAN_NAME']}", f"{f_data['SALESMAN_ID']}", f"{f_data['ORDER_CUSTOMER_NAME']}", f"{f_data['ORDER_CUSTOMER_CRM']}"])

        elif diagnosis_type == "CHAN":
            # 主航道占比<50% or 网线硬盘指定外购占比>10% 客户及业务员top5
            base_sql, con_params = get_where("SELECT * FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS", params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

        elif diagnosis_type == "IND":
            # 整体影响top5
            base_sql, con_params = get_where(
                "SELECT INDUSTRY_LV1_NAME,GROUP_CUST_NAME,GROUP_CUST_CRM,SALESMAN_NAME,SALESMAN_ID,INCOME_TOTAL,GPM,INCOME_RATE,GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS",
                params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

        elif diagnosis_type == "PROD":
            # 整体影响top5
            base_sql, con_params = get_where(
                "SELECT PRODUCT_LINE_LV2_INLAND_REPORT,GROUP_CUST_NAME,GROUP_CUST_CRM,SALESMAN_NAME,SALESMAN_ID,INCOME_TOTAL,GPM,GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 FROM DM_F_AI_GROSS_ANALYZE_LIST_DORIS",
                params)
            base_sql += f" AND MODULE_TYPE = '{diagnosis_type}' AND SORT_ID <= 20 ORDER BY GPM - CAST(REPLACE(GPM_BASE_LINE,'%%','') AS DOUBLE)/100 ASC limit 5"
            logger.info(f"查询明细 {level}-{diagnosis_type}: {base_sql}, params: {con_params}")
            db_connector.execute_query(base_sql, con_params)
            influence_top5_datas = db_connector.query_results

    else:
        # 其他维度暂不处理
        pass

    # 将处理结果转换为字符串格式
    return _format_detail_data_to_string(level, diagnosis_type, locals())
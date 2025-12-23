# -*- coding: utf-8 -*-
# @Time    : 2025/11/10 13:51
# @Author  : EvanSong
from typing import Dict


def _format_base_data_to_string(level: str, diagnosis_type: str, local_vars: dict) -> str:
    """将处理后的base数据格式化为字符串

    Args:
        level: 维度级别 (TOTAL/PROVINCE/OFFICE)
        diagnosis_type: 诊断类型 (ORG/CHAN/IND/PROD)
        local_vars: 本地变量字典，包含所有处理后的变量

    Returns:
        格式化后的数据字符串
    """
    result_lines = []

    # 根据维度和类型构建数据描述
    if level == "TOTAL":
        if diagnosis_type == "ORG":
            result_lines.append(f"未达成毛利预测数量: {local_vars.get('gpm_unachieve_count', 0)}")
            result_lines.append(f"达成毛利预测数量: {local_vars.get('gpm_achieve_count', 0)}")
            if local_vars.get('gpm_unachieve_pro'):
                result_lines.append("未达成毛利预测省份: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_unachieve_pro'].items()]))
            if local_vars.get('gpm_continue_pro'):
                result_lines.append("连续2个月毛利下降省份: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_pro'].items()]))
            if local_vars.get('gpm_mom_pro'):
                result_lines.append("毛利环比下降>1%省份: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_mom_pro'].items()]))
            if local_vars.get('acc_incomes_high_rate_pro'):
                result_lines.append("网线&硬盘累计占比>20%省份: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_incomes_high_rate_pro'].items()]))

        elif diagnosis_type == "CHAN":
            result_lines.append(f"主航道收入占比下降数量: {local_vars.get('incomes_rate_decline_count', 0)}")
            result_lines.append(f"主航道毛利下降数量: {local_vars.get('gpm_mom_decline_count', 0)}")
            if local_vars.get('incomes_rate_continue_pro'):
                result_lines.append(
                    "连续两个月主航道占比下降省份: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['incomes_rate_continue_pro'].items()]))
            if local_vars.get('incomes_yoy_decline_pro'):
                result_lines.append("主航道收入同比下降省份: " + ", ".join([f"{k}({v})" for k, v in local_vars['incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_incomes_rate_mom_decline_pro'):
                result_lines.append("主航道累计收入占比环比下降省份: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_incomes_rate_mom_decline_pro'].items()]))
            if local_vars.get('incomes_rate_low_top_pro'):
                result_lines.append("收入占比<60%的TOP5省份: " + ", ".join([f"{k}({v})" for k, v in local_vars['incomes_rate_low_top_pro'].items()]))
            if local_vars.get('gpm_low_pro'):
                result_lines.append("主航道毛利率<50%省份: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_low_pro'].items()]))

        elif diagnosis_type == "IND":
            result_lines.append(f"行业未达成毛利基线数量: {local_vars.get('gpm_unachieve_count', 0)}")
            result_lines.append(f"行业高于毛利基线1个点以上数量: {local_vars.get('gpm_achieve_count', 0)}")
            if local_vars.get('gpm_unachieve_pro'):
                result_lines.append("毛利预算未达标行业: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_unachieve_pro'].items()]))
            if local_vars.get('dou_incomes_yoy_decline_pro'):
                result_lines.append("当月&累计收入同比下降行业: " + ", ".join([f"{k}({v[0]}&{v[1]})" for k, v in local_vars['dou_incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_gpm_yoy_decline_pro'):
                result_lines.append("累计毛利率同比下降行业: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_gpm_yoy_decline_pro'].items()]))
            if local_vars.get('gpm_continue_decline_pro'):
                result_lines.append(
                    "连续两个月毛利下降行业: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_decline_pro'].items()]))
            if local_vars.get('incomes_rate_continue_decline_pro'):
                result_lines.append("连续2个月主航道占比下降行业: " + ", ".join(
                    [f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['incomes_rate_continue_decline_pro'].items()]))
            if local_vars.get('incomes_rate_lower_ave_pro'):
                _, first_value = next(iter(local_vars['incomes_rate_lower_ave_pro'].items()))
                avg = first_value[-1]
                result_lines.append(f"主航道占比低于国内均值({avg})行业: " + "、 ".join([f"{k}({v[0]})" for k, v in local_vars['incomes_rate_lower_ave_pro'].items()]))

        elif diagnosis_type == "PROD":
            result_lines.append(f"产品线未达成毛利基线数量: {local_vars.get('gpm_unachieve_count', 0)}")
            result_lines.append(f"产品线高于毛利基线1个点以上数量: {local_vars.get('gpm_achieve_count', 0)}")
            if local_vars.get('gpm_unachieve_pro'):
                result_lines.append("毛利预算未达标产品线: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_unachieve_pro'].items()]))
            if local_vars.get('dou_incomes_yoy_decline_pro'):
                result_lines.append(
                    "当月&累计收入同比下降产品线: " + ", ".join([f"{k}({v[0]}&{v[1]})" for k, v in local_vars['dou_incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_gpm_yoy_decline_pro'):
                result_lines.append("累计毛利率同比下降产品线: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_gpm_yoy_decline_pro'].items()]))
            if local_vars.get('gpm_continue_decline_pro'):
                result_lines.append(
                    "连续2个月环比毛利下降产品线: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_decline_pro'].items()]))
            if local_vars.get('discount_continue_decline_pro'):
                result_lines.append(
                    "连续2个月产品折扣下降产品线: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['discount_continue_decline_pro'].items()]))

    elif level == "PROVINCE":
        if diagnosis_type == "ORG":
            result_lines.append(f"未达成毛利预测数量: {local_vars.get('gpm_unachieve_count', 0)}")
            result_lines.append(f"达成毛利预测数量: {local_vars.get('gpm_achieve_count', 0)}")
            if local_vars.get('gpm_unachieve_pro'):
                result_lines.append("未达成毛利预测二级办: " + ", ".join(
                    [f"{k}({v})" for k, v in local_vars['gpm_unachieve_pro'].items()]))
            if local_vars.get('gpm_continue_pro'):
                result_lines.append("连续两个月毛利下降二级办: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_pro'].items()]))
            if local_vars.get('gpm_mom_pro'):
                result_lines.append("毛利环比下降>1%二级办: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_mom_pro'].items()]))
            if local_vars.get('acc_incomes_high_rate_pro'):
                result_lines.append("网线&硬盘累计占比>20%二级办: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_incomes_high_rate_pro'].items()]))

        elif diagnosis_type == "CHAN":
            result_lines.append(f"主航道收入占比下降数量: {local_vars.get('incomes_rate_decline_count', 0)}")
            result_lines.append(f"主航道毛利下降数量: {local_vars.get('gpm_mom_decline_count', 0)}")
            if local_vars.get('incomes_rate_continue_pro'):
                result_lines.append(
                    "连续两个月主航道占比下降二级办: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['incomes_rate_continue_pro'].items()]))
            if local_vars.get('incomes_yoy_decline_pro'):
                result_lines.append("主航道收同比下降二级办: " + ", ".join([f"{k}({v})" for k, v in local_vars['incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_incomes_rate_mom_decline_pro'):
                result_lines.append(
                    "主航道累计占比环比下降二级办: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_incomes_rate_mom_decline_pro'].items()]))
            if local_vars.get('incomes_rate_low_top_pro'):
                result_lines.append("收入占比<60%的TOP5二级办: " + ", ".join([f"{k}({v})" for k, v in local_vars['incomes_rate_low_top_pro'].items()]))
            if local_vars.get('gpm_low_pro'):
                result_lines.append("主航道毛利率<50%二级办: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_low_pro'].items()]))

        elif diagnosis_type == "IND":
            result_lines.append(f"行业未达成毛利基线数量: {local_vars.get('gpm_unachieve_count', 0)}")
            result_lines.append(f"行业高于毛利基线1个点以上数量: {local_vars.get('gpm_achieve_count', 0)}")
            if local_vars.get('gpm_unachieve_pro'):
                result_lines.append("毛利预算未达标行业: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_unachieve_pro'].items()]))
            if local_vars.get('dou_incomes_yoy_decline_pro'):
                result_lines.append("当月&累计收入同比下降行业: " + ", ".join([f"{k}({v[0]}&{v[1]})" for k, v in local_vars['dou_incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_gpm_yoy_decline_pro'):
                result_lines.append("累计毛利同比下降行业: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_gpm_yoy_decline_pro'].items()]))
            if local_vars.get('gpm_continue_decline_pro'):
                result_lines.append(
                    "连续两个月环比毛利下降行业: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_decline_pro'].items()]))
            if local_vars.get('incomes_rate_continue_decline_pro'):
                result_lines.append("连续2个月主航道占比下降行业: " + ", ".join(
                    [f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['incomes_rate_continue_decline_pro'].items()]))
            if local_vars.get('incomes_rate_lower_ave_pro'):
                _, first_value = next(iter(local_vars['incomes_rate_lower_ave_pro'].items()))
                avg = first_value[-1]
                result_lines.append(f"主航道占比低于国内均值({avg})行业: " + ", ".join([f"{k}({v})" for k, v in local_vars['incomes_rate_lower_ave_pro'].items()]))

        elif diagnosis_type == "PROD":
            result_lines.append(f"产品线未达成毛利基线数量: {local_vars.get('gpm_unachieve_count', 0)}")
            result_lines.append(f"产品线高于毛利基线1个点以上数量: {local_vars.get('gpm_achieve_count', 0)}")
            if local_vars.get('gpm_unachieve_pro'):
                result_lines.append("毛利预算未达标产品线: " + ", ".join([f"{k}({v})" for k, v in local_vars['gpm_unachieve_pro'].items()]))
            if local_vars.get('dou_incomes_yoy_decline_pro'):
                result_lines.append(
                    "当月&累计收入同比下降产品线: " + ", ".join([f"{k}({v[0]}&{v[1]})" for k, v in local_vars['dou_incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_gpm_yoy_decline_pro'):
                result_lines.append("累计毛利同比下降产品线: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_gpm_yoy_decline_pro'].items()]))
            if local_vars.get('gpm_continue_decline_pro'):
                result_lines.append(
                    "连续两个月毛利下降产品线: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_decline_pro'].items()]))
            if local_vars.get('discount_continue_decline_pro'):
                result_lines.append(
                    "连续两个月产品折扣下降产品线: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['discount_continue_decline_pro'].items()]))

    elif level == "OFFICE":
        if diagnosis_type == "ORG":
            if local_vars.get('gpm_continue_pro'):
                result_lines.append("连续两个月毛利下降业务员: " + ", ".join([f"{k}({v[-1]})({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_pro'].items()]))
            if local_vars.get('gpm_mom_pro'):
                result_lines.append("毛利环比下降>1%业务员: " + ", ".join([f"{k}({v[-1]})({v[0]})" for k, v in local_vars['gpm_mom_pro'].items()]))
            if local_vars.get('acc_incomes_high_rate_pro'):
                result_lines.append("网盘&硬盘累计占比>20%业务员: " + ", ".join([f"{k}({v[-1]})({v[0]})" for k, v in local_vars['acc_incomes_high_rate_pro'].items()]))

        elif diagnosis_type == "CHAN":
            result_lines.append(f"主航道收入占比下降数量: {local_vars.get('incomes_rate_decline_count', 0)}")
            result_lines.append(f"主航道毛利下降数量: {local_vars.get('gpm_mom_decline_count', 0)}")
            if local_vars.get('incomes_rate_continue_pro'):
                result_lines.append(
                    "连续两个月主航道占比下降业务员: " + ", ".join([f"{k}({v[-1]})({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['incomes_rate_continue_pro'].items()]))
            if local_vars.get('incomes_yoy_decline_pro'):
                result_lines.append("主航道收入同比下降业务员: " + ", ".join([f"{k}({v[-1]})({v[0]})" for k, v in local_vars['incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_incomes_rate_mom_decline_pro'):
                result_lines.append(
                    "主航道累计占比环比下降业务员: " + ", ".join([f"{k}({v[-1]})({v[0]})" for k, v in local_vars['acc_incomes_rate_mom_decline_pro'].items()]))
            if local_vars.get('incomes_rate_low_top_pro'):
                result_lines.append("主航道占比<60%的TOP5业务员: " + ", ".join([f"{k}({v[-1]})({v[0]})" for k, v in local_vars['incomes_rate_low_top_pro'].items()]))
            if local_vars.get('gpm_low_pro'):
                result_lines.append("主航道毛利率<50%业务员: " + ", ".join([f"{k}({v[-1]})({v[0]})" for k, v in local_vars['gpm_low_pro'].items()]))

        elif diagnosis_type == "IND":
            if local_vars.get('dou_incomes_yoy_decline_pro'):
                result_lines.append("当月&累计收入同比下降行业: " + ", ".join([f"{k}({v[0]}&{v[1]})" for k, v in local_vars['dou_incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_gpm_yoy_decline_pro'):
                result_lines.append("累计毛利同比下降行业: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_gpm_yoy_decline_pro'].items()]))
            if local_vars.get('gpm_continue_decline_pro'):
                result_lines.append(
                    "连续两个月毛利下降行业: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_decline_pro'].items()]))
            if local_vars.get('incomes_rate_continue_decline_pro'):
                result_lines.append("连续两个月主航道占比下降行业: " + ", ".join(
                    [f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['incomes_rate_continue_decline_pro'].items()]))
            if local_vars.get('incomes_rate_lower_ave_pro'):
                _, first_value = next(iter(local_vars['incomes_rate_lower_ave_pro'].items()))
                avg = first_value[-1]
                result_lines.append(f"主航道占比低于二级办均值({avg})行业: " + ", ".join([f"{k}({v})" for k, v in local_vars['incomes_rate_lower_ave_pro'].items()]))

        elif diagnosis_type == "PROD":
            if local_vars.get('dou_incomes_yoy_decline_pro'):
                result_lines.append(
                    "当月&累计收入同比下降产品线: " + ", ".join([f"{k}({v[0]}&{v[1]})" for k, v in local_vars['dou_incomes_yoy_decline_pro'].items()]))
            if local_vars.get('acc_gpm_yoy_decline_pro'):
                result_lines.append("累计毛利同比下降产品线: " + ", ".join([f"{k}({v})" for k, v in local_vars['acc_gpm_yoy_decline_pro'].items()]))
            if local_vars.get('gpm_continue_decline_pro'):
                result_lines.append(
                    "连续两个月毛利环比下降产品线: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['gpm_continue_decline_pro'].items()]))
            if local_vars.get('discount_continue_decline_pro'):
                result_lines.append(
                    "连续两个月产品折扣下降产品线: " + ", ".join([f"{k}({v[0]}-{v[1]}-{v[2]})" for k, v in local_vars['discount_continue_decline_pro'].items()]))

    # 如果没有数据，返回空字符串
    if len(result_lines) <= 1:  # 只有标题行
        return ""

    return "\n".join(result_lines)


async def deal_base_datas_before_model(query_result: Dict) -> str:
    """调用模型前对base数据进行处理

    Args:
        query_result: 查询结果，包含数据和参数

    Returns:
        处理后的数据字符串
    """
    datas = query_result['base_data']
    params = query_result['params']
    # 确定维度级别
    if not params.get('provinceName') and not params.get('officeLv2Name'):
        level = "TOTAL"  # 全国维度
    elif params.get('provinceName') and not params.get('officeLv2Name'):
        level = "PROVINCE"  # 省份维度
    else:
        level = "OFFICE"  # 二级办维度

    # 获取模块维度
    diagnosis_type = params.get('diagnosisType', '')
    if level == "TOTAL":
        if diagnosis_type == "ORG":
            # 未达成毛利预测数量 达成毛利预测数量
            gpm_unachieve_count, gpm_achieve_count = 0, 0
            # 未达成毛利预测省份信息 连续两个月环比毛利下降省份信息 毛利下降一个点以上省份信息 网盘&硬盘高于20%
            gpm_unachieve_pro, gpm_continue_pro, gpm_mom_pro, acc_incomes_high_rate_pro = {}, {}, {}, {}
            for data in datas:
                if data['gpm_unachieve'.upper()] == 1.0:
                    gpm_unachieve_count += 1
                    gpm_unachieve_pro[data['assess_center_name_lv5'.upper()]] = f"{data['gpm_gap'.upper()]:.1%}"

                if data['gpm_achieve'.upper()] == 1.0:
                    gpm_achieve_count += 1

                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_pro[data['assess_center_name_lv5'.upper()]] = [
                        f"{data['gpm_before_last_month'.upper()]:.1%}", f"{data['gpm_last_month'.upper()]:.1%}", f"{data['gpm'.upper()]:.1%}"]

                if data['gpm_mom_decline'.upper()] == 1.0:
                    gpm_mom_pro[data['assess_center_name_lv5'.upper()]] = f"{data['gpm_mom'.upper()]:.1%}"

                if data['acc_incomes_high_rate'.upper()] == 1.0:
                    acc_incomes_high_rate_pro[data['assess_center_name_lv5'.upper()]] = f"{data['acc_incomes_rate'.upper()]:.1%}"

        elif diagnosis_type == "CHAN":
            # 主航道收入占比下降数量 主航道毛利下降数量（环比）
            incomes_rate_decline_count, gpm_mom_decline_count = 0, 0
            # 连续两个月主航道占比下降省份信息
            incomes_rate_continue_pro = {}
            # 主航道收入当月同比下降省份信息
            incomes_yoy_decline_pro = {}
            # 主航道累计收入占比环比下降省份信息
            acc_incomes_rate_mom_decline_pro = {}
            # 收入占比<60%的TOP5省份信息
            incomes_rate_low_top_pro = {}
            # 主航道毛利率<50%省份信息
            gpm_low_pro = {}

            for data in datas:
                if data['incomes_rate_decline'.upper()] == 1.0:
                    incomes_rate_decline_count += 1

                if data['gpm_mom_decline'.upper()] == 1.0:
                    gpm_mom_decline_count += 1

                if data['incomes_rate_continue_decline'.upper()] == 1.0:
                    incomes_rate_continue_pro[data['assess_center_name_lv5'.upper()]] = [f"{data['incomes_rate_before_last_month'.upper()]:.1%}",
                                                                                         f"{data['incomes_rate_last_month'.upper()]:.1%}",
                                                                                         f"{data['incomes_rate'.upper()]:.1%}"]

                if data['incomes_yoy_decline'.upper()] == 1.0:
                    incomes_yoy_decline_pro[
                        data['assess_center_name_lv5'.upper()]] = f"{data['incomes_yoy'.upper()]:.1%}"

                if data['acc_incomes_rate_mom_decline'.upper()] == 1.0:
                    acc_incomes_rate_mom_decline_pro[data['assess_center_name_lv5'.upper()]] = f"{data['acc_incomes_rate_mom'.upper()]:.1%}"

                if data['incomes_rate_low_top'.upper()] == 1.0:
                    incomes_rate_low_top_pro[data['assess_center_name_lv5'.upper()]] = f"{data['incomes_rate'.upper()]:.1%}"

                if data['gpm_low'.upper()] == 1.0:
                    gpm_low_pro[data['assess_center_name_lv5'.upper()]] = f"{data['gpm'.upper()]:.1%}"

        elif diagnosis_type == "IND":
            # 行业未达成毛利基线数量  行业高于毛利基线1个点以上数量
            gpm_unachieve_count, gpm_achieve_count = 0, 0
            # 毛利预算未达标行业信息
            gpm_unachieve_pro = {}
            # 当月&累计收入同比下滑行业
            dou_incomes_yoy_decline_pro = {}
            # 累计毛利率同比下滑行业
            acc_gpm_yoy_decline_pro = {}
            # 连续个月环比毛利下降行业
            gpm_continue_decline_pro = {}
            # 主航道占比连续2个月环比下降行业
            incomes_rate_continue_decline_pro = {}
            # 主航道占比低于国内均值（37%）行业
            incomes_rate_lower_ave_pro = {}
            for data in datas:
                if data['gpm_unachieve'.upper()] == 1.0:
                    gpm_unachieve_count += 1
                    gpm_unachieve_pro[data['industry_lv1_name'.upper()]] = f"{data['gpm_gap'.upper()]:.1%}"

                if data['gpm_achieve'.upper()] == 1.0:
                    gpm_achieve_count += 1

                if data['dou_incomes_yoy_decline'.upper()] == 1.0:
                    dou_incomes_yoy_decline_pro[data['industry_lv1_name'.upper()]] = [f"{data['incomes_yoy'.upper()]:.1%}", f"{data['acc_incomes_yoy'.upper()]:.1%}"]

                if data['acc_gpm_yoy_decline'.upper()] == 1.0:
                    acc_gpm_yoy_decline_pro[data['industry_lv1_name'.upper()]] = f"{data['acc_gpm_yoy'.upper()]:.1%}"

                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_decline_pro[data['industry_lv1_name'.upper()]] = [f"{data['gpm_before_last_month'.upper()]:.1%}",
                                                                                   f"{data['gpm_last_month'.upper()]:.1%}",
                                                                                   f"{data['gpm'.upper()]:.1%}"]

                if data['incomes_rate_continue_decline'.upper()] == 1.0:
                    incomes_rate_continue_decline_pro[data['industry_lv1_name'.upper()]] = [f"{data['incomes_rate_before_last_month'.upper()]:.1%}",
                                                                                            f"{data['incomes_rate_last_month'.upper()]:.1%}",
                                                                                            f"{data['incomes_rate'.upper()]:.1%}"]

                if data['incomes_rate_lower_ave'.upper()] == 1.0:
                    incomes_rate_lower_ave_pro[data['industry_lv1_name'.upper()]] = [f"{data['incomes_rate'.upper()]:.1%}", f"{data['incomes_rate_average'.upper()]:.1%}"]

        elif diagnosis_type == "PROD":
            # 产品线未达成毛利基线数量 产品线高于毛利基线1个点以上数量
            gpm_unachieve_count, gpm_achieve_count = 0, 0
            # 毛利预算未达标产品线
            gpm_unachieve_pro = {}
            # 当月&累计收入同比下滑产品线
            dou_incomes_yoy_decline_pro = {}
            # 累计毛利率同比下滑产品线
            acc_gpm_yoy_decline_pro = {}
            # 连续2个月环比毛利下降产品线
            gpm_continue_decline_pro = {}
            # 产品折扣连续2个月环比下降
            discount_continue_decline_pro = {}
            for data in datas:
                if data['gpm_unachieve'.upper()] == 1.0:
                    gpm_unachieve_count += 1
                    gpm_unachieve_pro[data['product_line_lv2_inland_report'.upper()]] = f"{data['gpm_gap'.upper()]:.1%}"

                if data['gpm_achieve'.upper()] == 1.0:
                    gpm_achieve_count += 1

                if data['dou_incomes_yoy_decline'.upper()] == 1.0:
                    dou_incomes_yoy_decline_pro[data['product_line_lv2_inland_report'.upper()]] = [f"{data['incomes_yoy'.upper()]:.1%}",
                                                                                                   f"{data['acc_incomes_yoy'.upper()]:.1%}"]

                if data['acc_gpm_yoy_decline'.upper()] == 1.0:
                    acc_gpm_yoy_decline_pro[
                        data['product_line_lv2_inland_report'.upper()]] = f"{data['acc_gpm_yoy'.upper()]:.1%}"

                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_decline_pro[
                        data['product_line_lv2_inland_report'.upper()]] = [f"{data['gpm_before_last_month'.upper()]:.1%}", f"{data['gpm_last_month'.upper()]:.1%}",
                                                                           f"{data['gpm'.upper()]:.1%}"]

                if data['discount_continue_decline'.upper()] == 1.0:
                    discount_continue_decline_pro[
                        data['product_line_lv2_inland_report'.upper()]] = [f"{data['discount_before_last_month'.upper()]:.1%}",
                                                                           f"{data['discount_last_month'.upper()]:.1%}",
                                                                           f"{data['discount'.upper()]:.1%}"]


    elif level == "PROVINCE":
        if diagnosis_type == "ORG":
            # 未达成毛利预测数量 达成毛利预测数量
            gpm_unachieve_count, gpm_achieve_count = 0, 0
            # 未达成毛利预测二级办信息 连续两个月环比毛利下降二级办信息 毛利下降一个点以上二级办信息 网盘&硬盘高于20%
            gpm_unachieve_pro, gpm_continue_pro, gpm_mom_pro, acc_incomes_high_rate_pro = {}, {}, {}, {}
            for data in datas:
                if data['gpm_unachieve'.upper()] == 1.0:
                    gpm_unachieve_count += 1
                    gpm_unachieve_pro[data['assess_center_name_lv6'.upper()]] = f"{data['gpm_gap'.upper()]:.1%}"
                if data['gpm_achieve'.upper()] == 1.0:
                    gpm_achieve_count += 1
                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_pro[data['assess_center_name_lv6'.upper()]] = [f"{data['gpm_before_last_month'.upper()]:.1%}",
                                                                                f"{data['gpm_last_month'.upper()]:.1%}",
                                                                                f"{data['gpm'.upper()]:.1%}"]
                if data['gpm_mom_decline'.upper()] == 1.0:
                    gpm_mom_pro[data['assess_center_name_lv6'.upper()]] = f"{data['gpm_mom'.upper()]:.1%}"
                if data['acc_incomes_high_rate'.upper()] == 1.0:
                    acc_incomes_high_rate_pro[data['assess_center_name_lv6'.upper()]] = f"{data['acc_incomes_rate'.upper()]:.1%}"

        elif diagnosis_type == "CHAN":
            # 主航道收入占比下降数量 主航道毛利下降数量（环比）
            incomes_rate_decline_count, gpm_mom_decline_count = 0, 0
            # 连续两个月主航道占比下降二级办信息
            incomes_rate_continue_pro = {}
            # 主航道收入当月同比下降二级办信息
            incomes_yoy_decline_pro = {}
            # 主航道累计收入占比环比下降二级办信息
            acc_incomes_rate_mom_decline_pro = {}
            # 收入占比<60%的TOP5二级办信息
            incomes_rate_low_top_pro = {}
            # 主航道毛利率<50%二级办信息
            gpm_low_pro = {}

            for data in datas:
                if data['incomes_rate_decline'.upper()] == 1.0:
                    incomes_rate_decline_count += 1

                if data['gpm_mom_decline'.upper()] == 1.0:
                    gpm_mom_decline_count += 1

                if data['incomes_rate_continue_decline'.upper()] == 1.0:
                    incomes_rate_continue_pro[data['assess_center_name_lv6'.upper()]] = [f"{data['incomes_rate_before_last_month'.upper()]:.1%}",
                                                                                         f"{data['incomes_rate_last_month'.upper()]:.1%}",
                                                                                         f"{data['incomes_rate'.upper()]:.1%}"]

                if data['incomes_yoy_decline'.upper()] == 1.0:
                    incomes_yoy_decline_pro[
                        data['assess_center_name_lv6'.upper()]] = f"{data['incomes_yoy'.upper()]:.1%}"

                if data['acc_incomes_rate_mom_decline'.upper()] == 1.0:
                    acc_incomes_rate_mom_decline_pro[
                        data['assess_center_name_lv6'.upper()]] = f"{data['acc_incomes_rate_mom'.upper()]:.1%}"

                if data['incomes_rate_low_top'.upper()] == 1.0:
                    incomes_rate_low_top_pro[
                        data['assess_center_name_lv6'.upper()]] = f"{data['incomes_rate'.upper()]:.1%}"

                if data['gpm_low'.upper()] == 1.0:
                    gpm_low_pro[data['assess_center_name_lv6'.upper()]] = f"{data['gpm'.upper()]:.1%}"

        elif diagnosis_type == "IND":
            # 行业未达成毛利基线数量  行业高于毛利基线1个点以上数量
            gpm_unachieve_count, gpm_achieve_count = 0, 0
            # 毛利预算未达标行业信息
            gpm_unachieve_pro = {}
            # 当月&累计收入同比下滑行业
            dou_incomes_yoy_decline_pro = {}
            # 累计毛利率同比下滑行业
            acc_gpm_yoy_decline_pro = {}
            # 连续2个月环比毛利下降行业
            gpm_continue_decline_pro = {}
            # 主航道占比连续2个月环比下降行业
            incomes_rate_continue_decline_pro = {}
            # 主航道占比低于国内均值行业
            incomes_rate_lower_ave_pro = {}
            for data in datas:
                if data['gpm_unachieve'.upper()] == 1.0:
                    gpm_unachieve_count += 1
                    gpm_unachieve_pro[data['industry_lv1_name'.upper()]] = f"{data['gpm_gap'.upper()]:.1%}"

                if data['gpm_achieve'.upper()] == 1.0:
                    gpm_achieve_count += 1

                if data['dou_incomes_yoy_decline'.upper()] == 1.0:
                    dou_incomes_yoy_decline_pro[data['industry_lv1_name'.upper()]] = [
                        f"{data['incomes_yoy'.upper()]:.1%}", f"{data['acc_incomes_yoy'.upper()]:.1%}"]

                if data['acc_gpm_yoy_decline'.upper()] == 1.0:
                    acc_gpm_yoy_decline_pro[
                        data['industry_lv1_name'.upper()]] = f"{data['acc_gpm_yoy'.upper()]:.1%}"

                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_decline_pro[data['industry_lv1_name'.upper()]] = [
                        f"{data['gpm_before_last_month'.upper()]:.1%}", f"{data['gpm_last_month'.upper()]:.1%}", f"{data['gpm'.upper()]:.1%}"]

                if data['incomes_rate_continue_decline'.upper()] == 1.0:
                    incomes_rate_continue_decline_pro[data['industry_lv1_name'.upper()]] = [f"{data['incomes_rate_before_last_month'.upper()]:.1%}",
                                                                                            f"{data['incomes_rate_last_month'.upper()]:.1%}",
                                                                                            f"{data['incomes_rate'.upper()]:.1%}"]

                if data['incomes_rate_lower_ave'.upper()] == 1.0:
                    incomes_rate_lower_ave_pro[
                        data['industry_lv1_name'.upper()]] = [f"{data['incomes_rate'.upper()]:.1%}", f"{data['incomes_rate_average'.upper()]:.1%}"]

        elif diagnosis_type == "PROD":
            # 产品线未达成毛利基线数量 产品线高于毛利基线1个点以上数量
            gpm_unachieve_count, gpm_achieve_count = 0, 0
            # 毛利预算未达标产品线
            gpm_unachieve_pro = {}
            # 当月&累计收入同比下滑产品线
            dou_incomes_yoy_decline_pro = {}
            # 累计毛利率同比下滑产品线
            acc_gpm_yoy_decline_pro = {}
            # 连续2个月环比毛利下降产品线
            gpm_continue_decline_pro = {}
            # 产品折扣连续2个月环比下降
            discount_continue_decline_pro = {}
            for data in datas:
                if data['gpm_unachieve'.upper()] == 1.0:
                    gpm_unachieve_count += 1
                    gpm_unachieve_pro[data['product_line_lv2_inland_report'.upper()]] = f"{data['gpm_gap'.upper()]:.1%}"

                if data['gpm_achieve'.upper()] == 1.0:
                    gpm_achieve_count += 1

                if data['dou_incomes_yoy_decline'.upper()] == 1.0:
                    dou_incomes_yoy_decline_pro[data['product_line_lv2_inland_report'.upper()]] = [f"{data['incomes_yoy'.upper()]:.1%}",
                                                                                                   f"{data['acc_incomes_yoy'.upper()]:.1%}"]

                if data['acc_gpm_yoy_decline'.upper()] == 1.0:
                    acc_gpm_yoy_decline_pro[data['product_line_lv2_inland_report'.upper()]] = f"{data['acc_gpm_yoy'.upper()]:.1%}"

                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_decline_pro[data['product_line_lv2_inland_report'.upper()]] = [f"{data['gpm_before_last_month'.upper()]:.1%}",
                                                                                                f"{data['gpm_last_month'.upper()]:.1%}",
                                                                                                f"{data['gpm'.upper()]:.1%}"]

                if data['discount_continue_decline'.upper()] == 1.0:
                    discount_continue_decline_pro[data['product_line_lv2_inland_report'.upper()]] = [f"{data['discount_before_last_month'.upper()]:.1%}",
                                                                                                     f"{data['discount_last_month'.upper()]:.1%}",
                                                                                                     f"{data['discount'.upper()]:.1%}"]

    elif level == "OFFICE":
        if diagnosis_type == "ORG":
            # 连续两个月环比毛利下降业务员信息 毛利下降一个点以上二级办信息 网盘&硬盘高于20%
            gpm_continue_pro, gpm_mom_pro, acc_incomes_high_rate_pro = {}, {}, {}
            for data in datas:
                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_pro[data['salesman_name'.upper()]] = [f"{data['gpm_before_last_month'.upper()]:.1%}", f"{data['gpm_last_month'.upper()]:.1%}",
                                                                       f"{data['gpm'.upper()]:.1%}", data['salesman_id'.upper()]]
                if data['gpm_mom_decline'.upper()] == 1.0:
                    gpm_mom_pro[data['salesman_name'.upper()]] = [f"{data['gpm_mom'.upper()]:.1%}", data['salesman_id'.upper()]]
                if data['acc_incomes_high_rate'.upper()] == 1.0:
                    acc_incomes_high_rate_pro[data['salesman_name'.upper()]] = [f"{data['acc_incomes_rate'.upper()]:.1%}", data['salesman_id'.upper()]]

        elif diagnosis_type == "CHAN":
            # 主航道收入占比下降数量 主航道毛利下降数量（环比）
            incomes_rate_decline_count, gpm_mom_decline_count = 0, 0
            # 连续两个月主航道占比下降业务员信息
            incomes_rate_continue_pro = {}
            # 主航道收入当月同比下降业务员信息
            incomes_yoy_decline_pro = {}
            # 主航道累计收入占比环比下降业务员信息
            acc_incomes_rate_mom_decline_pro = {}
            # 收入占比<60%的TOP5业务员信息
            incomes_rate_low_top_pro = {}
            # 主航道毛利率<50%业务员信息
            gpm_low_pro = {}

            for data in datas:
                if data['incomes_rate_decline'.upper()] == 1.0:
                    incomes_rate_decline_count += 1

                if data['gpm_mom_decline'.upper()] == 1.0:
                    gpm_mom_decline_count += 1

                if data['incomes_rate_continue_decline'.upper()] == 1.0:
                    incomes_rate_continue_pro[data['salesman_name'.upper()]] = [f"{data['incomes_rate_before_last_month'.upper()]:.1%}",
                                                                                f"{data['incomes_rate_last_month'.upper()]:.1%}",
                                                                                f"{data['incomes_rate'.upper()]:.1%}", data['salesman_id'.upper()]]

                if data['incomes_yoy_decline'.upper()] == 1.0:
                    incomes_yoy_decline_pro[
                        data['salesman_name'.upper()]] = [f"{data['incomes_yoy'.upper()]:.1%}", data['salesman_id'.upper()]]

                if data['acc_incomes_rate_mom_decline'.upper()] == 1.0:
                    acc_incomes_rate_mom_decline_pro[
                        data['salesman_name'.upper()]] = [f"{data['acc_incomes_rate_mom'.upper()]:.1%}", data['salesman_id'.upper()]]

                if data['incomes_rate_low_top'.upper()] == 1.0:
                    incomes_rate_low_top_pro[
                        data['salesman_name'.upper()]] = [f"{data['incomes_rate'.upper()]:.1%}", data['salesman_id'.upper()]]

                if data['gpm_low'.upper()] == 1.0:
                    gpm_low_pro[data['salesman_name'.upper()]] = [f"{data['gpm'.upper()]:.1%}", data['salesman_id'.upper()]]

        elif diagnosis_type == "IND":
            # 当月&累计收入同比下滑行业
            dou_incomes_yoy_decline_pro = {}
            # 累计毛利率同比下滑行业
            acc_gpm_yoy_decline_pro = {}
            # 连续2个月环比毛利下降行业
            gpm_continue_decline_pro = {}
            # 主航道占比连续2个月环比下降行业
            incomes_rate_continue_decline_pro = {}
            # 主航道占比低于国内均值行业
            incomes_rate_lower_ave_pro = {}
            for data in datas:
                if data['dou_incomes_yoy_decline'.upper()] == 1.0:
                    dou_incomes_yoy_decline_pro[data['industry_lv1_name'.upper()]] = [f"{data['incomes_yoy'.upper()]:.1%}", f"{data['acc_incomes_yoy'.upper()]:.1%}"]

                if data['acc_gpm_yoy_decline'.upper()] == 1.0:
                    acc_gpm_yoy_decline_pro[data['industry_lv1_name'.upper()]] = f"{data['acc_gpm_yoy'.upper()]:.1%}"

                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_decline_pro[data['industry_lv1_name'.upper()]] = [f"{data['gpm_before_last_month'.upper()]:.1%}",
                                                                                   f"{data['gpm_last_month'.upper()]:.1%}",
                                                                                   f"{data['gpm'.upper()]:.1%}"]

                if data['incomes_rate_continue_decline'.upper()] == 1.0:
                    incomes_rate_continue_decline_pro[data['industry_lv1_name'.upper()]] = [f"{data['incomes_rate_before_last_month'.upper()]:.1%}",
                                                                                            f"{data['incomes_rate_last_month'.upper()]:.1%}",
                                                                                            f"{data['incomes_rate'.upper()]:.1%}"]

                if data['incomes_rate_lower_ave'.upper()] == 1.0:
                    incomes_rate_lower_ave_pro[data['industry_lv1_name'.upper()]] = [f"{data['incomes_rate'.upper()]:.1%}", f"{data['incomes_rate_average'.upper()]:.1%}"]

        elif diagnosis_type == "PROD":
            # 当月&累计收入同比下滑产品线
            dou_incomes_yoy_decline_pro = {}
            # 累计毛利率同比下滑产品线
            acc_gpm_yoy_decline_pro = {}
            # 连续2个月环比毛利下降产品线
            gpm_continue_decline_pro = {}
            # 产品折扣连续2个月环比下降
            discount_continue_decline_pro = {}
            for data in datas:
                if data['dou_incomes_yoy_decline'.upper()] == 1.0:
                    dou_incomes_yoy_decline_pro[data['product_line_lv2_inland_report'.upper()]] = [f"{data['incomes_yoy'.upper()]:.1%}",
                                                                                                   f"{data['acc_incomes_yoy'.upper()]:.1%}"]

                if data['acc_gpm_yoy_decline'.upper()] == 1.0:
                    acc_gpm_yoy_decline_pro[data['product_line_lv2_inland_report'.upper()]] = f"{data['acc_gpm_yoy'.upper()]:.1%}"

                if data['gpm_continue_decline'.upper()] == 1.0:
                    gpm_continue_decline_pro[
                        data['product_line_lv2_inland_report'.upper()]] = [f"{data['gpm_before_last_month'.upper()]:.1%}", f"{data['gpm_last_month'.upper()]:.1%}",
                                                                           f"{data['gpm'.upper()]:.1%}"]

                if data['discount_continue_decline'.upper()] == 1.0:
                    discount_continue_decline_pro[data['product_line_lv2_inland_report'.upper()]] = [f"{data['discount_before_last_month'.upper()]:.1%}",
                                                                                                     f"{data['discount_last_month'.upper()]:.1%}",
                                                                                                     f"{data['discount'.upper()]:.1%}"]

    else:
        # 其他维度暂不处理
        pass

    # 将处理结果转换为字符串格式
    return _format_base_data_to_string(level, diagnosis_type, locals())

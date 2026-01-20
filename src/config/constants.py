# -*- coding: utf-8 -*-
# @Time    : 2025/11/27 10:08
# @Author  : EvanSong


MODULE_DIMENSIONS = ["ORG", "CHAN", "IND", "PROD"]

ALL_KEYWORD = "ALL"

SPECIAL_PROVINCES_SQL = """SELECT PROVINCE_NAME FROM (SELECT PROVINCE_NAME, OFFICE_LV2_NAME, SUM(CASE WHEN OFFICE_WAR_AREA = '其他' THEN 0 ELSE 1 END) OVER(PARTITION BY PROVINCE_NAME) AS NUM FROM DM_D_FINANCE_BA_GPM_REGION_FBA_DORIS WHERE REGION_LEVEL = 'OFFICE' AND IFNULL(province_level, '') <> '其他') T WHERE T.NUM = 0"""

# user提示词
USER_PROMPT_DICTS = {
    "ORG": {
        "CURRENT": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  2. 注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<project_abnormal>对应的明细数据不存在，那么需要返回无，比如：没有“工程异常项目”，那么就返回<project_abnormal><summary>产品异常合同：无</summary></br></project_abnormal>。
        4. 如果<product_abnormal>对应的明细数据不存在，那么需要返回无，比如：没有“产品异常合同”，那么就返回<product_abnormal><summary>产品异常合同：无</summary></br></product_abnormal></current>。 \n5. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
        "CUMULATIVE": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  2. 注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<project_abnormal>对应的明细数据不存在，那么需要返回无，比如：没有“工程异常项目”，那么就返回<project_abnormal><summary>产品异常合同：无</summary></br></project_abnormal>。
        4. 如果<product_abnormal>对应的明细数据不存在，那么需要返回无，比如：没有“产品异常合同”，那么就返回<product_abnormal><summary>产品异常合同：无</summary></br></product_abnormal></accumulate>。\n5. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
    },
    "CHAN": {
        "CURRENT": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  2. 注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<customer_abnormal>对应的明细数据不存在，那么需要返回无，比如：没有“异常客户”，那么就返回<customer_abnormal><summary>异常客户：无</summary></br></customer_abnormal></current>。\n4. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
        "CUMULATIVE": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  2. 注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<customer_abnormal>对应的明细数据不存在，那么需要返回无，比如：没有“异常客户”，那么就返回<customer_abnormal><summary>异常客户：无</summary></br></customer_abnormal></accumulate>。\n4. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
    },
    "IND": {
        "CURRENT": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  2. 注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<industry_customer>对应的明细数据不存在，那么需要返回无，比如：没有“异常客户”，那么就返回<industry_customer><summary>行业异常客户：无</summary></br></industry_customer></current>。\n4. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
        "CUMULATIVE": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  2. 注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<industry_customer>对应的明细数据不存在，那么需要返回无，比如：没有“异常客户”，那么就返回<industry_customer><summary>行业异常客户：无</summary></br></industry_customer></accumulate>。\n4. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
    },
    "PROD": {
        "CURRENT": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  2. 注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<product_customer>对应的明细数据不存在，那么需要返回无，比如：没有“异常客户”，那么就返回<product_customer><summary>产品线异常客户：无</summary></br></product_customer></current>。\n4. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
        "CUMULATIVE": """1. 如果对应的数据不存在，那么需要返回无，**不要加标签（underline）**，比如：没有“毛利同比下降>1%xx”，那么就返回 毛利同比下降>1%xx：无。</br>  注意所有标签都要有它对应的闭合标签！比如<underline>，用来表述某条数据时它需要与</underline>一起使用
        3. 如果<product_customer>对应的明细数据不存在，那么需要返回无，比如：没有“异常客户”，那么就返回<product_customer><summary>产品线异常客户：无</summary></br></product_customer></accumulate>。\n4. 给定的<data>中有多少条数据就需要按照模板格式返回多少条数据""",
    }
}

# 内容类型配置 - 按维度分开配置，支持灵活扩展
# 格式：{"维度标识": {"内容类型标识": {"db_type": "数据库对应类型"}}}
CONTENT_TYPE_CONFIG = {
    "ORG": {
        "CURRENT": {"db_type": "CUR"},
        "CUMULATIVE": {"db_type": "ACC"},
    },
    "CHAN": {
        "CURRENT": {"db_type": "CUR"},
        "CUMULATIVE": {"db_type": "ACC"},
    },
    "IND": {
        "CURRENT": {"db_type": "CUR"},
        "CUMULATIVE": {"db_type": "ACC"},
    },
    "PROD": {
        "CURRENT": {"db_type": "CUR"},
        "CUMULATIVE": {"db_type": "ACC"},
    }
}

# 动态生成所有内容类型列表，无需手动维护
CONTENT_TYPES = []
for dimension_config in CONTENT_TYPE_CONFIG.values():
    CONTENT_TYPES.extend(list(dimension_config.keys()))
# 去重，确保每个内容类型只出现一次
CONTENT_TYPES = list(set(CONTENT_TYPES))

# 保留原有的CONTENT_WHERE_DICTS映射，确保向后兼容
CONTENT_WHERE_DICTS = {}
for dimension_config in CONTENT_TYPE_CONFIG.values():
    for content_type, config in dimension_config.items():
        CONTENT_WHERE_DICTS[content_type] = config["db_type"]

NONE_VALUE_KEY = ["CHANNEL_RATE", "INCOME_TOTAL", "DISCOUNT_TOTAL"]

# -*- coding: utf-8 -*-
# @Time    : 2025/12/25 10:37
# @Author  : EvanSong
from typing import Dict, Any, List

from src.config.constants import CONTENT_WHERE_DICTS, NONE_VALUE_KEY, CONTENT_TYPE_CONFIG
from src.utils import log_utils
from src.utils.async_utils import async_executor

logger = log_utils.logger


class DataTemplateProcessor:
    """新的数据处理器 - 支持两种内容类型(CURRENT, CUMULATIVE)和独立SQL（异步版本）"""

    def __init__(self, config: Dict[str, Any], special_provinces: List[str], db_connector=None):
        self.config = config
        self.db_connector = db_connector
        self.special_provinces = special_provinces

    def get_where(self, sql: str, params: Dict, data_type: str):
        """获取 WHERE 条件（同步方法）"""
        base_sql = ""
        if "WHERE" not in sql.upper():
            base_sql += " WHERE 1=1"
        else:
            base_sql += " 1=1 "
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
        level = self._determine_level(params)

        base_sql += " AND ORG_LEVEL = %(level)s"
        con_params['level'] = level

        # MODULE_TYPE
        diagnosis_type = params.get('diagnosisType', '')
        base_sql += " AND MODULE_TYPE = %(module_type)s"
        con_params['module_type'] = diagnosis_type

        # DATA TYPE
        base_sql += " AND DATA_TYPE = %(data_type)s"
        con_params['data_type'] = data_type

        # DISTRIBUTION_TYPE
        if params.get('distributionType'):
            base_sql += " AND DISTRIBUTION_TYPE = %(distribution_type)s"
            con_params['distribution_type'] = params['distributionType']

        # IT_INCLUDE_TYPE
        if params.get('itIncludeType'):
            base_sql += " AND IT_INCLUDE_TYPE = %(IT_include_type)s"
            con_params['IT_include_type'] = params['itIncludeType']

        logger.debug(f"查询 {level}-{diagnosis_type} params: {con_params}")
        return sql.format(base_sql), con_params

    async def _execute_queries(self, sql: str, params: Dict, data_type: str):
        """
        异步执行SQL查询

        ⭐ 改进：使用异步执行器，不阻塞事件循环
        """
        base_sql, con_params = self.get_where(sql, params, data_type)

        # ⭐ 关键：使用异步执行器在线程池中运行同步数据库操作
        def sync_execute():
            self.db_connector.execute_query(base_sql, con_params)
            return self.db_connector.query_results

        # 在线程池中执行同步操作（不阻塞事件循环）
        query_results = await async_executor.run_in_thread(
            sync_execute,
            timeout=None
        )

        return query_results

    def process(self, params: Dict) -> Dict[str, Dict]:
        """
        处理数据 - 返回两种内容类型的结果，每个template有独立的SQL

        ⚠️ 注意：这个方法是同步的，调用者应该使用 asyncio.create_task() 或 run_in_thread()
        """
        # 确定级别和诊断类型
        level = self._determine_level(params)
        diagnosis_type = params.get('diagnosisType', '')

        # 处理特殊省份(如：北京、上海、天津)
        if (diagnosis_type == 'ORG' or diagnosis_type == 'CHAN') and level == "PROVINCE" and params.get('provinceName') in self.special_provinces:
            logger.info(f"特殊省份：{params.get('provinceName')}")
            level_config = self.config.get("OFFICE", {})
        else:
            # 获取对应的配置
            level_config = self.config.get(level, {})

        type_config = level_config.get(diagnosis_type, {})

        if not type_config:
            return {}

        # 将参数存储到配置中，供字段提取使用
        self.config['params'] = params

        # 处理当前维度对应的内容类型
        results = {}

        # 获取当前诊断类型（维度）
        diagnosis_type = params.get('diagnosisType', '') or "ORG"

        # 获取当前维度对应的内容类型配置
        dimension_content_types = list(CONTENT_TYPE_CONFIG.get(diagnosis_type, {}).keys())

        for content_type in dimension_content_types:
            rules = type_config.get(content_type, [])
            if not rules:
                continue

            # 为每个template生成结果
            content_results = []
            for rule in rules:
                sql = rule.get('sql', '')
                # 提取对应类型的数据
                use_data_type = rule.get('use_data_type', '')

                # ⚠️ 同步调用，需要在外部使用 await 或线程池
                content_data = self._execute_queries_sync(
                    sql, params, use_data_type if use_data_type else CONTENT_WHERE_DICTS[content_type]
                )

                # 处理单个template
                line = self._execute_rule(rule, content_data)
                if line:
                    content_results.append(line)

            if content_results:
                results[content_type] = content_results

        return results

    def _execute_queries_sync(self, sql: str, params: Dict, data_type: str):
        """同步版本（保留向后兼容）"""
        base_sql, con_params = self.get_where(sql, params, data_type)
        self.db_connector.execute_query(base_sql, con_params)
        return self.db_connector.query_results

    @staticmethod
    def _determine_level(params: Dict) -> str:
        """确定维度级别"""
        if not params.get('provinceName') and not params.get('officeLv2Name'):
            return "TOTAL"
        elif params.get('provinceName') and not params.get('officeLv2Name'):
            return "PROVINCE"
        else:
            return "OFFICE"

    def _execute_rule(self, rule: Dict, datas: List[Dict]) -> str:
        """执行单条规则"""
        template = rule.get('template', '')
        if not template:
            return ""

        # 提取所有需要的字段
        fields = {}
        for field_name, field_spec in rule.get('fields', {}).items():
            value = self._extract_field(field_spec, datas)
            if value is not None:
                fields[field_name] = value
            else:
                if not rule.get('condition'):
                    fields[field_name] = "无"
                else:
                    fields[field_name] = None

        # 如果定义了条件，检查是否满足
        if rule.get('condition'):
            if not DataTemplateProcessor._eval_condition(rule['condition'], fields):
                return rule.get('non_message')

        # 格式化模板
        try:
            return template.format(**fields)
        except KeyError:
            return ""

    def _extract_field(self, field_spec: Dict, datas: List[Dict]) -> Any:
        """提取字段值"""
        operation = field_spec.get('operation')
        source_key = field_spec.get('source_key', '').upper()
        format_type = field_spec.get('format', '')
        name_key = field_spec.get('name_key', '').upper()
        value_key = field_spec.get('value_key', '').upper()
        id_key = field_spec.get('id_key', '').upper()
        id_value = field_spec.get('id_value', '').upper()
        value_keys = [k.upper() for k in field_spec.get('value_keys', [])]
        item_format = field_spec.get('item_format', '')
        separator = field_spec.get('separator', ', ')
        limit = field_spec.get('limit')

        if operation == 'count':
            # 计数操作：统计满足条件（source_key=1.0）的记录数
            count = sum(1 for data in datas if data.get(source_key) == '1')
            return count
        elif operation == 'get_period':
            # 获取当前调用的时间维度
            return self.config['params'].get('period')
        elif operation == 'group':
            # 如 省份(毛利差额)  name-value型
            result = {}
            for data in datas:
                name = data.get(name_key, 'Unknown')
                value = data.get(value_key, 0)
                result[name] = DataTemplateProcessor._format_value(value, format_type)

            if not result:
                return None

            items = []
            if limit and len(items) > limit:
                items = items[:limit]

            for name, value in result.items():
                item = item_format.format(name=name, value=value)
                items.append(item)

            return separator.join(items)

        elif operation == 'group_multi_value':
            # 如 省份(上上月毛利-上月毛利-当月毛利)  name-value-value-value型
            result = []
            for data in datas:
                name = data.get(name_key, 'Unknown')
                if not format_type:
                    values = []
                    for vk in value_keys:
                        if not data.get(vk) and vk in NONE_VALUE_KEY:
                            values.append(0)
                        else:
                            values.append(data.get(vk, 0))
                else:
                    values = [DataTemplateProcessor._format_value(data.get(vk, 0), format_type) for vk in value_keys]
                result.append({'name': name, 'values': values})

            if not result:
                return None

            items = []
            for item_data in result:
                name = item_data['name']
                values = item_data['values']
                # 同时保留 name 和 names 列表
                format_dict = {'name': name, 'names': [name]}
                for i, v in enumerate(values):
                    format_dict[f'v{i}'] = v
                item = item_format.format(**format_dict)
                items.append(item)

            if limit and len(items) > limit:
                items = items[:limit]

            return separator.join(items)

        elif operation == 'extract_avg':
            # 抽取出平均值字段值
            if datas:
                last_data = datas[-1]
                avg_value = last_data.get(name_key, 0)
                return DataTemplateProcessor._format_value(avg_value, format_type)
            return None

        elif operation == 'extract_most_common':
            """提取出现次数最多的名称"""
            from collections import Counter
            names = [data.get(name_key.upper(), '') for data in datas]
            if names:
                count = Counter(names)
                most_common_name, _ = count.most_common(1)[0]
                return most_common_name
            return ""

        elif operation == 'extract_first_value':
            """提取第一条数据中指定字段的值"""
            if datas:
                first_data = datas[0]
                return first_data.get(source_key, '')
            return ""

        elif operation == 'extract_most_common_with_count':
            """提取出现次数最多的名称及其数量"""
            from collections import Counter
            names = [data.get(name_key.upper(), '') for data in datas]
            if names:
                count = Counter(names)
                most_common_name, count_val = count.most_common(1)[0]
                return f"{most_common_name}{count_val}个"
            return ""

        elif operation == 'extract_top_influencers':
            """提取影响最大的业务员或客户信息"""
            from collections import Counter

            names = [data.get(name_key.upper(), '') for data in datas]
            influencers = {}
            if names:
                count = Counter(names)
                most_common_name, count_val = count.most_common(1)[0]

                filtered_data = [data for data in datas if data.get(name_key) == most_common_name]

                if limit and len(filtered_data) > limit:
                    filtered_data = filtered_data[:limit]
                for data in filtered_data:
                    key, value = data.get(id_key.upper(), ''), data.get(id_value.upper(), '')
                    if key not in influencers:
                        influencers[key] = value

            if not influencers:
                return None

            items = []
            for key, value in influencers.items():
                item = item_format.format(key=key, value=value)
                items.append(item)

            return separator.join(items)

        return None

    @staticmethod
    def _format_value(value: Any, format_type: str) -> str:
        """对源数据值进行格式化处理"""
        if value is None:
            return ""

        if format_type == 'percentage':
            if isinstance(value, (int, float)):
                return f"{value:.1%}"
        elif format_type == 'int':
            return str(int(value))
        elif format_type == 'float':
            return f"{value:.2f}"

        return str(value)

    @staticmethod
    def _eval_condition(condition: str, fields: Dict) -> bool:
        """评估条件"""
        try:
            return eval(condition, {'fields': fields})
        except Exception:
            return True

# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 14:32
# @Author  : EvanSong

from typing import Dict, List

from src.config.constants import ALL_KEYWORD, MODULE_DIMENSIONS
from src.models.diagnosis_request import DiagnosisRequest
from src.utils import log_utils
from src.utils.db_utils import MySQLDataConnector

logger = log_utils.logger


class DataQueryService:
    """数据查询服务 - 从数据表获取诊断分析所需数据"""

    def __init__(self):
        self.db_connector = MySQLDataConnector(db_type="data_db")

    def _get_dimension_values(self, dimension: str, filters: Dict = None) -> List[str]:
        """获取指定维度的所有值

        Args:
            dimension: 维度字段名 (province, office_lv2等)
            filters: 上级过滤条件

        Returns:
            维度值列表
        """
        db_config = self.db_connector.settings.get_database_config("data_db")

        # 构建查询SQL
        sql = f"SELECT DISTINCT {dimension} FROM {db_config.table_name} WHERE 1=1"
        params = {}

        if filters:
            for key, value in filters.items():
                sql += f" AND {key} = %({key})s"
                params[key] = value

        sql += f" AND {dimension} IS NOT NULL ORDER BY {dimension}"

        query_result = self.db_connector.execute_query(sql, params)

        if query_result['status'] != 'success':
            logger.error(f"获取维度值失败: {query_result['message']}")
            return []

        return [row[dimension] for row in self.db_connector.query_results]

    def _is_all_or_empty(self, value: str) -> bool:
        """判断值是否为ALL或空"""
        return not value or value.upper() == ALL_KEYWORD

    @staticmethod
    def _create_result_dict(period: str, diagnosis_type: str,
                            province_name: str = None, office_lv2_name: str = None) -> Dict:
        """创建结果字典"""
        return {
            'period': period,
            'diagnosisType': diagnosis_type,
            'provinceName': province_name,
            'officeLv2Name': office_lv2_name
        }

    def _get_provinces(self, period: str) -> List[str]:
        """获取所有省份"""
        provinces = self._get_dimension_values("ASSESS_CENTER_NAME_LV5", {"PERIOD": period})
        logger.info(f"查询到 {len(provinces)} 个省份: {provinces}")
        return provinces

    def _get_offices(self, period: str, province: str) -> List[str]:
        """获取指定省份的所有二级办"""
        offices = self._get_dimension_values(
            "ASSESS_CENTER_NAME_LV6",
            {"PERIOD": period, "ASSESS_CENTER_NAME_LV5": province}
        )
        logger.info(f"省份[{province}]查询到 {len(offices)} 个二级办")
        return offices

    def _expand_by_modules_provinces_offices(self, period: str, modules: List[str],
                                             provinces: List[str]) -> List[Dict]:
        """展开：模块 × 省份 × 二级办"""
        results = []
        for module in modules:
            for province in provinces:
                offices = self._get_offices(period, province)
                for office in offices:
                    results.append(self._create_result_dict(period, module, province, office))
        return results

    def _expand_by_modules_provinces(self, period: str, modules: List[str],
                                     provinces: List[str], office_lv2_name: str = None) -> List[Dict]:
        """展开：模块 × 省份"""
        results = []
        for module in modules:
            for province in provinces:
                if province:
                    results.append(self._create_result_dict(period, module, province, office_lv2_name))
        return results

    def _expand_by_modules_offices(self, period: str, modules: List[str],
                                   province_name: str, offices: List[str]) -> List[Dict]:
        """展开：模块 × 二级办"""
        results = []
        for module in modules:
            for office in offices:
                if office:
                    results.append(self._create_result_dict(period, module, province_name, office))
        return results

    def _expand_by_provinces_offices(self, period: str, diagnosis_type: str,
                                     provinces: List[str]) -> List[Dict]:
        """展开：省份 × 二级办"""
        results = []
        for province in provinces:
            offices = self._get_offices(period, province)
            for office in offices:
                results.append(self._create_result_dict(period, diagnosis_type, province, office))
        return results

    def _expand_by_provinces(self, period: str, diagnosis_type: str,
                             provinces: List[str], office_lv2_name: str = None) -> List[Dict]:
        """展开：省份"""
        results = []
        for province in provinces:
            if province:
                results.append(self._create_result_dict(period, diagnosis_type, province, office_lv2_name))
        return results

    def _expand_by_offices(self, period: str, diagnosis_type: str,
                           province_name: str, offices: List[str]) -> List[Dict]:
        """展开：二级办"""
        results = []
        for office in offices:
            if office:
                results.append(self._create_result_dict(period, diagnosis_type, province_name, office))
        return results

    def _expand_by_modules(self, period: str, modules: List[str],
                           province_name: str = None, office_lv2_name: str = None) -> List[Dict]:
        """展开：模块"""
        return [self._create_result_dict(period, module, province_name, office_lv2_name)
                for module in modules]

    def _execute_split_queries(self, request: DiagnosisRequest) -> List[Dict]:
        """执行分组查询（处理ALL参数）

        Args:
            request: 诊断请求参数

        Returns:
            查询结果列表，每个结果包含数据和实际参数
        """
        period = request.period
        diagnosis_type = request.diagnosisType
        province_name = request.provinceName
        office_lv2_name = request.officeLv2Name

        # 判断各维度是否为ALL或空
        is_module_all = self._is_all_or_empty(diagnosis_type)
        is_province_all = province_name and province_name.upper() == ALL_KEYWORD
        is_office_all = office_lv2_name and office_lv2_name.upper() == ALL_KEYWORD

        # 获取模块列表
        modules = MODULE_DIMENSIONS if is_module_all else [diagnosis_type]
        if is_module_all:
            logger.info(f"模块维度为ALL或空，将查询 {len(modules)} 个模块: {modules}")

        # 场景1: 模块=ALL/空 + 省份=ALL + 二级办=ALL
        if is_module_all and is_province_all and is_office_all:
            provinces = self._get_provinces(period)
            return self._expand_by_modules_provinces_offices(period, modules, provinces)

        # 场景2: 模块=ALL/空 + 省份=ALL + 二级办=空/具体值
        if is_module_all and is_province_all:
            provinces = self._get_provinces(period)
            return self._expand_by_modules_provinces(period, modules, provinces, office_lv2_name)

        # 场景3: 模块=ALL/空 + 省份=具体值 + 二级办=ALL
        if is_module_all and is_office_all:
            if not province_name:
                raise ValueError("二级办为ALL时，省区参数不能为空")
            offices = self._get_offices(period, province_name)
            return self._expand_by_modules_offices(period, modules, province_name, offices)

        # 场景4: 模块=ALL/空 + 省份和二级办都是具体值/空
        if is_module_all:
            return self._expand_by_modules(period, modules, province_name, office_lv2_name)

        # 场景5: 模块=具体值 + 省份=ALL + 二级办=ALL
        if is_province_all and is_office_all:
            provinces = self._get_provinces(period)
            return self._expand_by_provinces_offices(period, diagnosis_type, provinces)

        # 场景6: 模块=具体值 + 省份=ALL + 二级办=空/具体值
        if is_province_all:
            provinces = self._get_provinces(period)
            return self._expand_by_provinces(period, diagnosis_type, provinces, office_lv2_name)

        # 场景7: 模块=具体值 + 省份=具体值 + 二级办=ALL
        if is_office_all:
            if not province_name:
                raise ValueError("二级办为ALL时，省区参数不能为空")
            offices = self._get_offices(period, province_name)
            return self._expand_by_offices(period, diagnosis_type, province_name, offices)

        # 场景8: 所有参数都是具体值
        return [self._create_result_dict(period, diagnosis_type, province_name, office_lv2_name)]

    def get_diagnosis_data(self, request: DiagnosisRequest) -> Dict:
        """根据请求参数获取诊断数据

        Args:
            request: 诊断请求参数

        Returns:
            包含查询结果和统计数据的字典
        """
        # 连接数据库
        conn_result = self.db_connector.connect_database()
        logger.info(conn_result['message'])
        if conn_result['status'] != 'success':
            return {
                'status': 'error',
                'message': f"数据库连接失败: {conn_result['message']}"
            }

        try:
            # 执行查询（支持ALL参数分组查询）
            query_results = self._execute_split_queries(request)
            logger.info(f"查询完成，共获取 {len(query_results)} 组数据")

            return {
                'status': 'success',
                'message': '数据查询成功',
                'data': query_results,
                'record_count': len(query_results),
            }

        except ValueError as e:
            return {
                'status': 'error',
                'message': f"参数验证失败: {str(e)}"
            }
        except Exception as e:
            logger.exception("数据处理失败")
            return {
                'status': 'error',
                'message': f"数据处理失败: {str(e)}"
            }
        finally:
            self.db_connector.close_connection()

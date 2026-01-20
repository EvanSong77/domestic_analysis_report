# -*- coding: utf-8 -*-
# @Time    : 2025/12/23
# @Author  : EvanSong

import asyncio
from typing import Dict, List

from src.config.constants import ALL_KEYWORD, MODULE_DIMENSIONS
from src.models.diagnosis_request import DiagnosisRequest
from src.utils import log_utils
from src.utils.async_db_utils import get_async_db_connector

logger = log_utils.logger


class AsyncDataQueryService:
    """异步数据查询服务 - 从数据表获取诊断分析所需数据"""

    def __init__(self):
        self.db_connector = None

    async def _initialize_connector(self):
        """初始化异步数据库连接器"""
        if not self.db_connector:
            self.db_connector = await get_async_db_connector("data_db")

    async def _get_dimension_values(self, dimension: str, filters: Dict = None,
                                    timeout: float = 30.0) -> List[str]:
        """异步获取指定维度的所有值
        
        Args:
            dimension: 维度字段名 (province, office_lv2等)
            filters: 上级过滤条件
            timeout: 查询超时时间（秒）
            
        Returns:
            维度值列表
        """
        await self._initialize_connector()

        db_config = self.db_connector.settings.get_database_config("data_db")
        # 构建查询SQL
        sql = f"SELECT DISTINCT {dimension} FROM {db_config.table_name} WHERE 1=1"
        params = {}

        if filters:
            for key, value in filters.items():
                sql += f" AND {key} = %({key})s"
                params[key] = value

        sql += f" AND {dimension} IS NOT NULL ORDER BY {dimension}"

        query_result = await self.db_connector.execute_query(sql, params, timeout)

        if query_result['status'] != 'success':
            logger.error(f"获取维度值失败: {query_result['message']}")
            return []

        return [row[dimension] for row in query_result['data']]

    def _is_all_or_empty(self, value: str) -> bool:
        """判断值是否为ALL或空"""
        return not value or value.upper() == ALL_KEYWORD

    @staticmethod
    def _create_result_dict(period: str, diagnosis_type: str,
                            province_name: str = None, office_lv2_name: str = None,
                            distribution_type: str = '', it_include_type: str = '') -> Dict:
        """创建结果字典"""
        return {
            'period': period,
            'diagnosisType': diagnosis_type,
            'provinceName': province_name,
            'officeLv2Name': office_lv2_name,
            'distributionType': distribution_type,
            'itIncludeType': it_include_type
        }

    async def _get_provinces(self, period: str, timeout: float = 30.0) -> List[str]:
        """异步获取所有省份"""
        provinces = await self._get_dimension_values("ASSESS_CENTER_NAME_LV5", {"PERIOD": period}, timeout)
        logger.info(f"查询到 {len(provinces)} 个省份")
        return provinces

    async def _get_offices(self, period: str, province: str, timeout: float = 30.0) -> List[str]:
        """异步获取指定省份的所有二级办"""
        offices = await self._get_dimension_values(
            "ASSESS_CENTER_NAME_LV6",
            {"PERIOD": period, "ASSESS_CENTER_NAME_LV5": province},
            timeout
        )
        logger.info(f"省份[{province}]查询到 {len(offices)} 个二级办")
        return offices

    async def _expand_by_modules_provinces_offices(self, period: str, modules: List[str],
                                                   provinces: List[str], distribution_type: str,
                                                   it_include_type: str,
                                                   timeout: float = 30.0) -> List[Dict]:
        """异步展开：模块 × 省份 × 二级办"""
        results = []
        tasks = []

        # 为每个省份创建获取二级办的异步任务
        for province in provinces:
            task = self._get_offices(period, province, timeout)
            tasks.append((province, task))

        # 并发执行所有任务
        for province, task in tasks:
            offices = await task
            for module in modules:
                for office in offices:
                    results.append(self._create_result_dict(period, module, province, office, distribution_type, it_include_type))

        return results

    async def _expand_by_modules_provinces(self, period: str, modules: List[str],
                                           provinces: List[str], office_lv2_name: str = None,
                                           distribution_type: str = '', it_include_type: str = '') -> List[Dict]:
        """展开：模块 × 省份"""
        results = []
        for module in modules:
            for province in provinces:
                if province:
                    results.append(self._create_result_dict(period, module, province, office_lv2_name, distribution_type, it_include_type))
        return results

    async def _expand_by_modules_offices(self, period: str, modules: List[str],
                                         province_name: str, offices: List[str],
                                         distribution_type: str = '', it_include_type: str = '') -> List[Dict]:
        """展开：模块 × 二级办"""
        results = []
        for module in modules:
            for office in offices:
                if office:
                    results.append(self._create_result_dict(period, module, province_name, office, distribution_type, it_include_type))
        return results

    async def _expand_by_provinces_offices(self, period: str, diagnosis_type: str,
                                           provinces: List[str], distribution_type: str = '',
                                           it_include_type: str = '', timeout: float = 30.0) -> List[Dict]:
        """异步展开：省份 × 二级办"""
        results = []
        tasks = []

        # 为每个省份创建获取二级办的异步任务
        for province in provinces:
            task = self._get_offices(period, province, timeout)
            tasks.append((province, task))

        # 并发执行所有任务
        for province, task in tasks:
            offices = await task
            for office in offices:
                results.append(self._create_result_dict(period, diagnosis_type, province, office, distribution_type, it_include_type))

        return results

    async def _expand_by_provinces(self, period: str, diagnosis_type: str,
                                   provinces: List[str], office_lv2_name: str = None,
                                   distribution_type: str = '', it_include_type: str = '') -> List[Dict]:
        """展开：省份"""
        results = []
        for province in provinces:
            if province:
                results.append(self._create_result_dict(period, diagnosis_type, province, office_lv2_name, distribution_type, it_include_type))
        return results

    async def _expand_by_offices(self, period: str, diagnosis_type: str,
                                 province_name: str, offices: List[str],
                                 distribution_type: str = '', it_include_type: str = '') -> List[Dict]:
        """展开：二级办"""
        results = []
        for office in offices:
            if office:
                results.append(self._create_result_dict(period, diagnosis_type, province_name, office, distribution_type, it_include_type))
        return results

    async def _expand_by_modules(self, period: str, modules: List[str],
                                 province_name: str = None, office_lv2_name: str = None,
                                 distribution_type: str = '', it_include_type: str = '') -> List[Dict]:
        """展开：模块"""
        return [self._create_result_dict(period, module, province_name, office_lv2_name, distribution_type, it_include_type)
                for module in modules]

    async def _execute_split_queries(self, request: DiagnosisRequest, timeout: float = 60.0) -> List[Dict]:
        """异步执行分组查询（处理ALL参数）
        
        Args:
            request: 诊断请求参数
            timeout: 总超时时间（秒）
            
        Returns:
            查询结果列表，每个结果包含数据和实际参数
        """
        period = request.period
        diagnosis_type = request.diagnosisType
        province_name = request.provinceName
        office_lv2_name = request.officeLv2Name
        distribution_type = request.distributionType
        it_include_type = request.itIncludeType

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
            provinces = await self._get_provinces(period, timeout)
            return await self._expand_by_modules_provinces_offices(period, modules, provinces, distribution_type, it_include_type, timeout)

        # 场景2: 模块=ALL/空 + 省份=ALL + 二级办=空/具体值
        if is_module_all and is_province_all:
            provinces = await self._get_provinces(period, timeout)
            return await self._expand_by_modules_provinces(period, modules, provinces, office_lv2_name, distribution_type, it_include_type)

        # 场景3: 模块=ALL/空 + 省份=具体值 + 二级办=ALL
        if is_module_all and is_office_all:
            if not province_name:
                raise ValueError("二级办为ALL时，省区参数不能为空")
            offices = await self._get_offices(period, province_name, timeout)
            return await self._expand_by_modules_offices(period, modules, province_name, offices, distribution_type, it_include_type)

        # 场景4: 模块=ALL/空 + 省份和二级办都是具体值/空
        if is_module_all:
            return await self._expand_by_modules(period, modules, province_name, office_lv2_name, distribution_type, it_include_type)

        # 场景5: 模块=具体值 + 省份=ALL + 二级办=ALL
        if is_province_all and is_office_all:
            provinces = await self._get_provinces(period, timeout)
            return await self._expand_by_provinces_offices(period, diagnosis_type, provinces, distribution_type, it_include_type, timeout)

        # 场景6: 模块=具体值 + 省份=ALL + 二级办=空/具体值
        if is_province_all:
            provinces = await self._get_provinces(period, timeout)
            return await self._expand_by_provinces(period, diagnosis_type, provinces, office_lv2_name, distribution_type, it_include_type)

        # 场景7: 模块=具体值 + 省份=具体值 + 二级办=ALL
        if is_office_all:
            if not province_name:
                raise ValueError("二级办为ALL时，省区参数不能为空")
            offices = await self._get_offices(period, province_name, timeout)
            return await self._expand_by_offices(period, diagnosis_type, province_name, offices, distribution_type, it_include_type)

        # 场景8: 所有参数都是具体值
        return [self._create_result_dict(period, diagnosis_type, province_name, office_lv2_name, distribution_type, it_include_type)]

    async def get_diagnosis_data(self, request: DiagnosisRequest, timeout: float = 60.0) -> Dict:
        """异步根据请求参数获取诊断数据
        
        Args:
            request: 诊断请求参数
            timeout: 总超时时间（秒）
            
        Returns:
            包含查询结果和统计数据的字典
        """
        try:
            # 使用超时控制整个查询过程
            async with asyncio.timeout(timeout):
                # 执行查询（支持ALL参数分组查询）
                query_results = await self._execute_split_queries(request, timeout)
                logger.info(f"查询完成，共获取 {len(query_results)} 组数据")

                return {
                    'status': 'success',
                    'message': '数据查询成功',
                    'data': query_results,
                    'record_count': len(query_results),
                }

        except asyncio.TimeoutError:
            logger.error(f"数据查询超时 - 请求ID: {getattr(request, 'reqId', 'unknown')}, 超时时间: {timeout}秒")
            return {
                'status': 'error',
                'message': f'数据查询超时（{timeout}秒）'
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

    async def close(self):
        """关闭数据库连接"""
        if self.db_connector:
            await self.db_connector.close()

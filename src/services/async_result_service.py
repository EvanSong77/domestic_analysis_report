# -*- coding: utf-8 -*-
# @Time    : 2025/12/23
# @Author  : EvanSong

import json
import asyncio
from typing import Dict

from src.utils.async_db_utils import get_async_db_connector
from src.config.config import get_settings
from src.utils import log_utils

logger = log_utils.logger


class AsyncResultService:
    """异步结果保存服务 - 将生成的报告保存到中间结果表"""
    
    def __init__(self):
        self.settings = get_settings()
        self.db_config = self.settings.get_database_config("intermediate_db")
        self.db_connector = None
    
    async def _initialize_connector(self):
        """初始化异步数据库连接器"""
        if not self.db_connector:
            self.db_connector = await get_async_db_connector("intermediate_db")
    
    async def save_diagnosis_result(self, req_id: str, req_param: Dict, 
                                   resp_result: Dict, timeout: float = 30.0) -> Dict:
        """异步保存诊断结果到中间表
        
        Args:
            req_id: 请求ID
            req_param: 请求参数
            resp_result: 响应结果
            timeout: 操作超时时间（秒）
            
        Returns:
            保存结果字典
        """
        await self._initialize_connector()
        
        try:
            # 使用超时控制整个保存过程
            async with asyncio.timeout(timeout):
                # 使用配置中的插入SQL
                sql = self.db_config.insert_sql
                
                # 格式化响应结果为指定格式 - 支持单个和多个报告
                if 'report_results' in resp_result:
                    # 多个报告的情况 - 使用实际的参数值
                    formatted_result = []
                    for i, report_result_item in enumerate(resp_result['report_results']):
                        # 获取实际的参数值
                        actual_params = resp_result.get('actual_params', [{}] * len(resp_result['report_results']))
                        params = actual_params[i] if i < len(actual_params) else {}
                        
                        formatted_result.append({
                            "period": params.get('period', req_param.get("period", "")),
                            "diagnosisType": params.get('diagnosisType', req_param.get("diagnosisType", "")),
                            "provinceName": params.get('provinceName', req_param.get("provinceName", "")),
                            "officeLv2Name": params.get('officeLv2Name', req_param.get("officeLv2Name", "")),
                            "distributionType": params.get('distributionType', req_param.get("distributionType", "")),
                            "itIncludeType": params.get('itIncludeType', req_param.get("itIncludeType", "")),
                            "diagnosisResult": report_result_item.get('report_content', '')
                        })
                else:
                    # 单个报告的情况
                    formatted_result = [{
                        "period": req_param.get("period", ""),
                        "diagnosisType": req_param.get("diagnosisType", ""),
                        "provinceName": req_param.get("provinceName", ""),
                        "officeLv2Name": req_param.get("officeLv2Name", ""),
                        "distributionType": req_param.get("distributionType", ""),
                        "itIncludeType": req_param.get("itIncludeType", ""),
                        "diagnosisResult": resp_result.get('report_content', '')
                    }]
                
                params = {
                    'req_id': req_id,
                    'req_param': json.dumps(req_param, ensure_ascii=False),
                    'resp_result': json.dumps(formatted_result, ensure_ascii=False)
                }
                
                # 异步执行插入
                result = await self.db_connector.execute_update(sql, params, timeout)
                
                if result['status'] != 'success':
                    logger.error(f"结果保存失败 - 请求ID: {req_id}, 错误: {result['message']}")
                    return {
                        'status': 'error',
                        'message': f"结果保存失败: {result['message']}"
                    }
                
                logger.info(f"异步结果保存成功 - 请求ID: {req_id}, 插入ID: {result.get('lastrowid')}")
                
                return {
                    'status': 'success',
                    'message': '结果保存成功',
                    'data': {
                        'inserted_id': result.get('lastrowid'),
                        'req_id': req_id,
                        'affected_rows': result.get('affected_rows', 0)
                    }
                }
                
        except asyncio.TimeoutError:
            logger.error(f"结果保存超时 - 请求ID: {req_id}, 超时时间: {timeout}秒")
            return {
                'status': 'error',
                'message': f'结果保存超时（{timeout}秒）'
            }
        except Exception as e:
            logger.error(f"结果保存异常 - 请求ID: {req_id}, 错误: {str(e)}")
            return {
                'status': 'error',
                'message': f"结果保存失败: {str(e)}"
            }
    
    async def get_diagnosis_result(self, req_id: str, timeout: float = 10.0) -> Dict:
        """异步根据请求ID获取诊断结果
        
        Args:
            req_id: 请求ID
            timeout: 查询超时时间（秒）
            
        Returns:
            诊断结果字典
        """
        await self._initialize_connector()
        
        try:
            # 使用超时控制查询过程
            async with asyncio.timeout(timeout):
                # 使用配置中的查询SQL
                sql = self.db_config.select_sql
                
                params = {'req_id': req_id}
                
                # 异步执行查询
                query_result = await self.db_connector.execute_query(sql, params, timeout)
                
                if query_result['status'] != 'success' or not query_result.get('data'):
                    return {
                        'status': 'error',
                        'message': '未找到对应的诊断结果'
                    }
                
                result = query_result['data'][0]
                
                # 解析JSON字段
                try:
                    req_param = json.loads(result['req_param']) if result['req_param'] else {}
                    resp_result = json.loads(result['resp_result']) if result['resp_result'] else {}
                except json.JSONDecodeError:
                    req_param = {}
                    resp_result = {}
                
                return {
                    'status': 'success',
                    'message': '查询成功',
                    'data': {
                        'id': result['id'],
                        'req_id': result['req_id'],
                        'req_param': req_param,
                        'resp_result': resp_result,
                        'create_time': result['create_time'].isoformat() if result['create_time'] else None,
                        'update_time': result['update_time'].isoformat() if result['update_time'] else None
                    }
                }
                
        except asyncio.TimeoutError:
            logger.error(f"结果查询超时 - 请求ID: {req_id}, 超时时间: {timeout}秒")
            return {
                'status': 'error',
                'message': f'结果查询超时（{timeout}秒）'
            }
        except Exception as e:
            logger.error(f"结果查询异常 - 请求ID: {req_id}, 错误: {str(e)}")
            return {
                'status': 'error',
                'message': f"查询失败: {str(e)}"
            }
    
    async def close(self):
        """关闭数据库连接"""
        if self.db_connector:
            await self.db_connector.close()
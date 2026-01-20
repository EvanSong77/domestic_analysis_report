# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 14:39
# @Author  : EvanSong

import hashlib
import json
import time
from typing import Dict, Optional
from src.utils.db_utils import MySQLDataConnector
from src.config.config import get_settings


class ResultService:
    """结果保存服务 - 将生成的报告保存到中间结果表"""
    
    def __init__(self):
        self.db_connector = MySQLDataConnector(db_type="intermediate_db")
        self.settings = get_settings()
        self.db_config = self.settings.get_database_config("intermediate_db")
    
    def save_diagnosis_result(self, req_id: str, req_param: Dict, resp_result: Dict) -> Dict:
        """保存诊断结果到中间表
        
        Args:
            req_id: 请求ID
            req_param: 请求参数
            resp_result: 响应结果
            
        Returns:
            保存结果字典
        """
        # 连接数据库
        conn_result = self.db_connector.connect_database()
        if conn_result['status'] != 'success':
            return {
                'status': 'error',
                'message': f"数据库连接失败: {conn_result['message']}"
            }
        
        try:
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
                        "diagnosisResult": report_result_item.get('report_content', '')
                    })
            else:
                # 单个报告的情况
                formatted_result = [{
                    "period": req_param.get("period", ""),
                    "diagnosisType": req_param.get("diagnosisType", ""),
                    "provinceName": req_param.get("provinceName", ""),
                    "officeLv2Name": req_param.get("officeLv2Name", ""),
                    "diagnosisResult": resp_result.get('report_content', '')
                }]
            
            params = {
                'req_id': req_id,
                'req_param': json.dumps(req_param, ensure_ascii=False),
                'resp_result': json.dumps(formatted_result, ensure_ascii=False)
            }
            
            # 执行插入
            cursor = self.db_connector.connection.cursor()
            cursor.execute(sql, params)
            self.db_connector.connection.commit()
            
            # 获取插入的ID
            inserted_id = cursor.lastrowid
            cursor.close()
            
            return {
                'status': 'success',
                'message': '结果保存成功',
                'data': {
                    'inserted_id': inserted_id,
                    'req_id': req_id
                }
            }
            
        except Exception as e:
            # 回滚事务
            if self.db_connector.connection:
                self.db_connector.connection.rollback()
            
            return {
                'status': 'error',
                'message': f"结果保存失败: {str(e)}"
            }
        finally:
            self.db_connector.close_connection()
    
    def get_diagnosis_result(self, req_id: str) -> Dict:
        """根据请求ID获取诊断结果
        
        Args:
            req_id: 请求ID
            
        Returns:
            诊断结果字典
        """
        # 连接数据库
        conn_result = self.db_connector.connect_database()
        if conn_result['status'] != 'success':
            return {
                'status': 'error',
                'message': f"数据库连接失败: {conn_result['message']}"
            }
        
        try:
            # 使用配置中的查询SQL
            sql = self.db_config.select_sql
            
            params = {'req_id': req_id}
            
            # 执行查询
            query_result = self.db_connector.execute_query(sql, params)
            
            if query_result['status'] != 'success' or not self.db_connector.query_results:
                return {
                    'status': 'error',
                    'message': '未找到对应的诊断结果'
                }
            
            result = self.db_connector.query_results[0]
            
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
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f"查询失败: {str(e)}"
            }
        finally:
            self.db_connector.close_connection()

# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 9:44
# @Author  : EvanSong
import time
import warnings
from typing import Dict, Optional, Any

import pymysql
from pymysql.cursors import DictCursor

from src.config.config import get_settings
from src.utils.log_utils import logger

warnings.filterwarnings('ignore')


class MySQLDataConnector:
    """MySQL数据库连接器 - 支持中间表和数据表两个数据库"""

    def __init__(self, db_type: str = "data_db", env: str = None):
        self.connection = None
        self.query_results = None
        self.settings = get_settings()
        self.env = env or self.settings.environment
        self.db_type = db_type  # "intermediate_db" 或 "data_db"

    def connect_database(self, db_type: str = None, env: str = None) -> Dict[str, Any]:
        """连接MySQL数据库

        Args:
            db_type: 数据库类型 ("intermediate_db" 或 "data_db"), 默认使用初始化时的类型
            env: 环境名称 (test, uat, prod)，默认使用当前环境

        Returns:
            连接状态字典
        """
        db_type = db_type or self.db_type
        env = env or self.env

        # 获取数据库配置
        try:
            db_config = self.settings.get_database_config(db_type, env)
        except ValueError as e:
            logger.error(f'数据库配置错误 - 类型: {db_type}, 环境: {env}, 错误: {e}')
            return {'status': 'error', 'message': f'数据库配置错误: {e}'}

        # 连接测试
        start_time = time.time()
        try:
            self.connection = pymysql.connect(
                host=db_config.host,
                port=db_config.port,
                user=db_config.username,
                password=db_config.password,
                database=db_config.database,
                connect_timeout=db_config.timeout
            )

            # 测试连接
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()

            connect_time = round(time.time() - start_time, 2)
            
            logger.info(f'数据库连接成功 - 类型: {db_type}, 环境: {env}, 主机: {db_config.host}, 数据库: {db_config.database}, 连接时间: {connect_time}秒')

            return {
                'status': 'success',
                'message': f'{db_type}数据库连接成功',
                'connection_info': {
                    'db_type': db_type,
                    'env': env,
                    'host': db_config.host,
                    'port': db_config.port,
                    'database': db_config.database,
                    'username': db_config.username,
                    'connect_time': f'{connect_time}秒'
                }
            }

        except Exception as e:
            connect_time = round(time.time() - start_time, 2)
            logger.error(f'数据库连接失败 - 类型: {db_type}, 环境: {env}, 错误: {str(e)}, 连接时间: {connect_time}秒')
            return {
                'status': 'error',
                'message': f'{db_type}数据库连接失败: {str(e)}',
                'connect_time': connect_time
            }

    def execute_query(self, sql: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        执行SQL查询

        Args:
            sql: SQL查询语句
            params: 查询参数

        Returns:
            查询结果字典
        """
        if not self.connection:
            logger.error('查询执行失败 - 未连接数据库，请先调用connect_database方法')
            return {'status': 'error', 'message': '请先连接数据库'}

        start_time = time.time()
        try:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(sql, params or {})

            results = cursor.fetchall()
            query_time = round(time.time() - start_time, 2)
            
            logger.debug(f'查询执行成功 - SQL: {sql}, 记录数: {len(results)}, 查询时间: {query_time}秒')

            self.query_results = results

            return {
                'status': 'success',
                'message': f'查询成功，返回 {len(results)} 条记录',
                'query_time': f'{query_time}秒',
                'record_count': len(results),
                'sql': sql
            }

        except Exception as e:
            query_time = round(time.time() - start_time, 2)
            logger.error(f'查询执行失败 - SQL: {sql}, 错误: {str(e)}, 查询时间: {query_time}秒')
            return {
                'status': 'error',
                'message': f'查询执行失败: {str(e)}',
                'query_time': query_time
            }

    def close_connection(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None

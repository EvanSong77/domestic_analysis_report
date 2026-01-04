# -*- coding: utf-8 -*-
# @Time    : 2025/12/23
# @Author  : EvanSong

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Optional, Any

import aiomysql

from src.config.config import get_settings
from src.utils.log_utils import logger


class AsyncMySQLDataConnector:
    """异步MySQL数据库连接器 - 支持中间表和数据表两个数据库"""

    def __init__(self, db_type: str = "data_db", env: str = None):
        self.pool = None
        self.settings = get_settings()
        self.env = env or self.settings.environment
        self.db_type = db_type  # "intermediate_db" 或 "data_db"
        self._connection_params = None
        self._pool_initialized = False  # 标记池是否已初始化

    async def initialize_pool(self, min_size: int = 1, max_size: int = 10) -> bool:
        """初始化连接池（⭐ 改进：支持重复初始化）"""
        try:
            # ⭐ 改进：如果已初始化且池有效，直接返回
            if self._pool_initialized and self.pool and not self.pool.closed:
                logger.debug(f"连接池已初始化 - 类型: {self.db_type}, 环境: {self.env}")
                return True

            # 如果旧池存在且未关闭，先关闭
            if self.pool and not self.pool.closed:
                try:
                    self.pool.close()
                    await self.pool.wait_closed()
                    logger.info(f"已关闭旧连接池 - 类型: {self.db_type}")
                except Exception as e:
                    logger.warning(f"关闭旧连接池失败: {e}")

            db_config = self.settings.get_database_config(self.db_type, self.env)

            self._connection_params = {
                'host': db_config.host,
                'port': db_config.port,
                'user': db_config.username,
                'password': db_config.password,
                'db': db_config.database,
                'minsize': min_size,
                'maxsize': max_size,
                'autocommit': True,
                'cursorclass': aiomysql.DictCursor,
                'connect_timeout': db_config.timeout
            }

            self.pool = await aiomysql.create_pool(**self._connection_params)
            self._pool_initialized = True

            logger.info(f"异步数据库连接池初始化成功 - 类型: {self.db_type}, 环境: {self.env}")
            return True

        except Exception as e:
            logger.error(f"异步数据库连接池初始化失败 - 类型: {self.db_type}, 错误: {e}")
            self._pool_initialized = False
            return False

    async def _ensure_pool(self) -> bool:
        """确保连接池已初始化且有效"""
        if not self._pool_initialized or not self.pool or self.pool.closed:
            logger.warning(f"连接池无效，重新初始化 - 类型: {self.db_type}")
            return await self.initialize_pool()
        return True

    @asynccontextmanager
    async def get_connection(self):
        """获取数据库连接上下文管理器（⭐ 改进：自动重新初始化池）"""
        # 确保池有效
        if not await self._ensure_pool():
            raise RuntimeError(f"无法初始化连接池 - 类型: {self.db_type}")

        conn = None
        try:
            conn = await asyncio.wait_for(self.pool.acquire(), timeout=5.0)
            yield conn
        except asyncio.TimeoutError:
            logger.error(f"获取数据库连接超时 - 类型: {self.db_type}")
            raise
        except Exception as e:
            logger.error(f"获取数据库连接异常 - 类型: {self.db_type}, 错误: {e}")
            raise
        finally:
            if conn:
                try:
                    self.pool.release(conn)
                except Exception as e:
                    logger.debug(f"释放连接异常: {e}")

    @asynccontextmanager
    async def get_cursor(self):
        """获取游标上下文管理器"""
        async with self.get_connection() as conn:
            cursor = None
            try:
                cursor = await conn.cursor()
                yield cursor
            finally:
                if cursor:
                    await cursor.close()

    async def execute_query(self, sql: str, params: Optional[Dict] = None,
                            timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        异步执行SQL查询

        Args:
            sql: SQL查询语句
            params: 查询参数
            timeout: 查询超时时间（秒）

        Returns:
            查询结果字典
        """
        start_time = time.time()

        try:
            # 设置超时
            if timeout:
                async with asyncio.timeout(timeout):
                    return await self._execute_query_internal(sql, params, start_time)
            else:
                return await self._execute_query_internal(sql, params, start_time)

        except asyncio.TimeoutError:
            query_time = round(time.time() - start_time, 2)
            logger.error(f"数据库查询超时 - SQL: {sql[:100]}..., 超时时间: {timeout}秒, 已运行: {query_time}秒")
            return {
                'status': 'error',
                'message': f'数据库查询超时（{timeout}秒）',
                'query_time': query_time
            }
        except Exception as e:
            query_time = round(time.time() - start_time, 2)
            logger.error(f"数据库查询异常 - SQL: {sql[:100]}..., 错误: {str(e)}, 耗时: {query_time}秒")
            return {
                'status': 'error',
                'message': f'数据库查询异常: {str(e)}',
                'query_time': query_time
            }

    async def _execute_query_internal(self, sql: str, params: Optional[Dict], start_time: float) -> Dict[str, Any]:
        """内部查询执行逻辑"""
        async with self.get_cursor() as cursor:
            try:
                await cursor.execute(sql, params or {})
                results = await cursor.fetchall()

                query_time = round(time.time() - start_time, 2)

                logger.debug(f"数据库查询成功 - 行数: {len(results)}, 耗时: {query_time}秒")

                return {
                    'status': 'success',
                    'message': '查询成功',
                    'data': results,
                    'row_count': len(results),
                    'query_time': query_time
                }

            except Exception as e:
                query_time = round(time.time() - start_time, 2)
                logger.error(f"SQL执行失败 - SQL: {sql[:100]}..., 错误: {str(e)}, 耗时: {query_time}秒")
                raise

    async def execute_update(self, sql: str, params: Optional[Dict] = None,
                             timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        异步执行更新操作（INSERT/UPDATE/DELETE）

        Args:
            sql: SQL语句
            params: 参数
            timeout: 超时时间

        Returns:
            更新结果字典
        """
        start_time = time.time()

        try:
            if timeout:
                async with asyncio.timeout(timeout):
                    return await self._execute_update_internal(sql, params, start_time)
            else:
                return await self._execute_update_internal(sql, params, start_time)

        except asyncio.TimeoutError:
            query_time = round(time.time() - start_time, 2)
            logger.error(f"数据库更新超时 - SQL: {sql[:100]}..., 超时时间: {timeout}秒")
            return {
                'status': 'error',
                'message': f'数据库更新超时（{timeout}秒）',
                'query_time': query_time
            }
        except Exception as e:
            query_time = round(time.time() - start_time, 2)
            logger.error(f"数据库更新异常 - SQL: {sql[:100]}..., 错误: {str(e)}")
            return {
                'status': 'error',
                'message': f'数据库更新异常: {str(e)}',
                'query_time': query_time
            }

    async def _execute_update_internal(self, sql: str, params: Optional[Dict], start_time: float) -> Dict[str, Any]:
        """内部更新执行逻辑"""
        async with self.get_connection() as conn:
            cursor = None
            try:
                cursor = await conn.cursor()
                await cursor.execute(sql, params or {})
                affected_rows = cursor.rowcount
                lastrowid = cursor.lastrowid

                await conn.commit()

                query_time = round(time.time() - start_time, 2)

                logger.debug(f"数据库更新成功 - 影响行数: {affected_rows}, 耗时: {query_time}秒")

                return {
                    'status': 'success',
                    'message': '更新成功',
                    'affected_rows': affected_rows,
                    'lastrowid': lastrowid,
                    'query_time': query_time
                }

            except Exception as e:
                await conn.rollback()
                query_time = round(time.time() - start_time, 2)
                logger.error(f"SQL更新失败 - SQL: {sql[:100]}..., 错误: {str(e)}")
                raise
            finally:
                if cursor:
                    await cursor.close()

    async def close(self):
        """关闭连接池"""
        if self.pool and not self.pool.closed:
            try:
                self.pool.close()
                await self.pool.wait_closed()
                self._pool_initialized = False
                logger.info(f"异步数据库连接池已关闭 - 类型: {self.db_type}")
            except Exception as e:
                logger.error(f"关闭连接池异常: {e}")

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.initialize_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()


# 全局连接池实例（按数据库类型）
_async_pools = {}
_pool_lock = asyncio.Lock()


async def get_async_db_connector(db_type: str = "data_db", env: str = None) -> AsyncMySQLDataConnector:
    """获取异步数据库连接器（单例模式 + 线程安全）"""
    key = f"{db_type}:{env or 'default'}"

    # 使用锁确保线程安全
    async with _pool_lock:
        if key not in _async_pools:
            connector = AsyncMySQLDataConnector(db_type, env)
            await connector.initialize_pool()
            _async_pools[key] = connector

        # ⭐ 改进：确保池有效
        connector = _async_pools[key]
        if not await connector._ensure_pool():
            logger.warning(f"连接池无效，重新初始化 - key: {key}")
            await connector.initialize_pool()

    return _async_pools[key]


async def close_all_async_pools():
    """关闭所有异步连接池"""
    for key, connector in list(_async_pools.items()):
        try:
            await connector.close()
        except Exception as e:
            logger.error(f"关闭连接池失败 - key: {key}, 错误: {e}")

    _async_pools.clear()
    logger.info("所有异步数据库连接池已关闭")
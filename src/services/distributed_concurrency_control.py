# -*- coding: utf-8 -*-
# @Time    : 2025/11/18 13:48
# @Author  : EvanSong

import asyncio
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

# 跨平台文件锁支持
if sys.platform == 'win32':
    pass
else:
    import fcntl

from src.utils import log_utils

logger = log_utils.logger


@dataclass
class InstanceMetadata:
    """实例元数据类"""
    hostname: str
    pid: int
    owner_id: str
    request_path: str
    acquire_time: float
    instance_id: str
    last_acquire_time: Optional[float] = None


@dataclass
class InstanceInfo:
    """实例信息类"""
    concurrent_count: int
    last_heartbeat: float
    meta: Dict


class DistributedConcurrencyControl:
    """
    分布式并发控制服务 - 多实例协调控制

    优化特性：
    1. 更好的错误处理和恢复机制
    2. 改进的文件锁策略（减少竞争）
    3. 心跳保活机制防止泄漏
    4. 详细的日志和监控指标
    5. 自动清理过期实例
    6. 原子性操作确保一致性

    ⭐ 修复：
    - 改进了 release_slot 的容错性
    - 避免多次释放同一个槽位
    - 改进了日志输出的准确性
    """

    def __init__(self, config=None, max_total_concurrent: int = None, lock_file_path: str = None):
        """
        初始化分布式并发控制

        Args:
            config: 配置对象，优先使用配置
            max_total_concurrent: 所有实例允许的最大并发数
            lock_file_path: 锁文件路径，需要所有实例可访问的共享路径
        """
        if config:
            self.max_total_concurrent = config.max_total_concurrent
            self.lock_file_path = config.lock_file_path
            self.acquire_timeout = config.acquire_timeout
            self.stale_instance_timeout = config.stale_instance_timeout
        else:
            self.max_total_concurrent = max_total_concurrent or 2
            self.lock_file_path = lock_file_path or "logs/concurrency_lock.json"
            self.acquire_timeout = 30
            self.stale_instance_timeout = 300

        # 确保锁文件目录存在
        lock_dir = os.path.dirname(self.lock_file_path)
        if lock_dir and not os.path.exists(lock_dir):
            os.makedirs(lock_dir, exist_ok=True)

        # 文件锁对象
        self.lock_file = None
        self._lock_retry_count = 0
        self._max_lock_retries = 3

        # 初始化锁文件
        self._initialize_lock_file()

        logger.info(
            f"分布式并发控制初始化完成 - 最大并发数: {self.max_total_concurrent}, "
            f"锁文件: {self.lock_file_path}"
        )

    def _initialize_lock_file(self):
        """初始化锁文件（重启时重新创建）"""
        try:
            # 检查文件是否存在，如果存在则删除重新创建
            if os.path.exists(self.lock_file_path):
                os.remove(self.lock_file_path)
                logger.info(f"删除旧锁文件: {self.lock_file_path}")

            # 创建新的锁文件
            with open(self.lock_file_path, 'w') as f:
                json.dump({
                    'current_concurrent': 0,
                    'max_concurrent': self.max_total_concurrent,
                    'last_updated': time.time(),
                    'active_instances': {},
                    'created_at': datetime.now().isoformat(),
                    'version': 1
                }, f, indent=2)
            logger.info(f"锁文件已重新创建: {self.lock_file_path}")
        except Exception as e:
            logger.error(f"初始化锁文件失败: {e}")

    def _acquire_file_lock(self, timeout: float = 5.0) -> bool:
        """
        获取文件锁（带重试机制）

        Args:
            timeout: 获取锁的超时时间（秒）

        Returns:
            bool: 是否成功获取锁
        """
        start_time = time.time()
        retry_count = 0

        while time.time() - start_time < timeout:
            try:
                # 确保文件目录和文件存在
                lock_dir = os.path.dirname(self.lock_file_path)
                if lock_dir and not os.path.exists(lock_dir):
                    os.makedirs(lock_dir, exist_ok=True)

                if not os.path.exists(self.lock_file_path):
                    with open(self.lock_file_path, 'w') as f:
                        json.dump({
                            'current_concurrent': 0,
                            'max_concurrent': self.max_total_concurrent,
                            'last_updated': time.time(),
                            'active_instances': {}
                        }, f, indent=2)

                if sys.platform == 'win32':
                    # Windows：直接打开文件进行读写
                    try:
                        self.lock_file = open(self.lock_file_path, 'r+')
                        return True
                    except (IOError, OSError) as e:
                        if retry_count < self._max_lock_retries:
                            retry_count += 1
                            logger.debug(f"Windows文件锁获取失败（重试{retry_count}）: {e}")
                        time.sleep(0.05 * (2 ** retry_count))  # 指数退避
                        continue
                else:
                    # Linux/Unix：使用文件锁
                    self.lock_file = open(self.lock_file_path, 'r+')
                    try:
                        fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        return True
                    except (IOError, BlockingIOError) as e:
                        if retry_count < self._max_lock_retries:
                            retry_count += 1
                        self.lock_file.close()
                        self.lock_file = None
                        time.sleep(0.05 * (2 ** retry_count))  # 指数退避
                        continue

            except Exception as e:
                logger.error(f"获取文件锁异常（重试{retry_count}）: {e}")
                if self.lock_file:
                    try:
                        self.lock_file.close()
                    except:
                        pass
                    self.lock_file = None
                break

        logger.warning(f"文件锁获取超时（{timeout}秒内）")
        return False

    def _release_file_lock(self):
        """释放文件锁"""
        try:
            if self.lock_file:
                if sys.platform != 'win32':
                    # Linux/Unix 释放文件锁
                    try:
                        fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                    except Exception as e:
                        logger.debug(f"解除文件锁失败: {e}")
                self.lock_file.close()
                self.lock_file = None
        except Exception as e:
            logger.error(f"释放文件锁失败: {e}")

    def _read_lock_data(self) -> dict:
        """读取锁文件数据（带容错）"""
        try:
            if not self.lock_file:
                return self._get_default_lock_data()

            self.lock_file.seek(0)
            content = self.lock_file.read()

            if not content:
                logger.warning("锁文件为空，返回默认数据")
                return self._get_default_lock_data()

            data = json.loads(content)

            # 验证数据结构
            if not self._validate_lock_data(data):
                logger.warning("锁文件数据结构异常，返回默认数据")
                return self._get_default_lock_data()

            return data

        except json.JSONDecodeError as e:
            logger.error(f"锁文件JSON解析失败: {e}，返回默认数据")
            return self._get_default_lock_data()
        except Exception as e:
            logger.error(f"读取锁文件异常: {e}，返回默认数据")
            return self._get_default_lock_data()

    def _write_lock_data(self, data: dict) -> bool:
        """写入锁文件数据（带容错）"""
        try:
            if not self.lock_file:
                logger.error("文件锁未持有，无法写入数据")
                return False

            data['last_updated'] = time.time()
            self.lock_file.seek(0)
            self.lock_file.truncate()
            json.dump(data, self.lock_file, indent=2)
            self.lock_file.flush()

            # 强制同步到磁盘（Linux）
            if hasattr(self.lock_file, 'fileno') and sys.platform != 'win32':
                try:
                    os.fsync(self.lock_file.fileno())
                except Exception as e:
                    logger.debug(f"文件同步失败: {e}")

            return True
        except Exception as e:
            logger.error(f"写入锁文件失败: {e}")
            return False

    def _get_default_lock_data(self) -> dict:
        """获取默认锁文件数据"""
        return {
            'current_concurrent': 0,
            'max_concurrent': self.max_total_concurrent,
            'last_updated': time.time(),
            'active_instances': {}
        }

    def _validate_lock_data(self, data: dict) -> bool:
        """验证锁文件数据结构"""
        required_keys = ['current_concurrent', 'max_concurrent', 'active_instances']
        return all(key in data for key in required_keys)

    def _cleanup_stale_instances(self, lock_data: dict, timeout: Optional[int] = None) -> dict:
        """
        清理过期的实例记录（改进版）

        Args:
            lock_data: 锁数据
            timeout: 实例超时时间（秒）

        Returns:
            dict: 清理后的锁数据
        """
        timeout = timeout or self.stale_instance_timeout
        current_time = time.time()
        active_instances = lock_data.get('active_instances', {})

        cleaned_instances = {}
        total_cleaned = 0
        cleaned_ids = []

        for instance_id, instance_info in active_instances.items():
            last_heartbeat = instance_info.get('last_heartbeat', 0)
            age = current_time - last_heartbeat

            if age < timeout:
                cleaned_instances[instance_id] = instance_info
            else:
                concurrent_count = instance_info.get('concurrent_count', 0)
                total_cleaned += concurrent_count
                cleaned_ids.append(instance_id)
                logger.warning(
                    f"清理过期实例 - id: {instance_id}, 年龄: {age:.1f}s, "
                    f"释放槽位: {concurrent_count}"
                )

        if total_cleaned > 0:
            old_concurrent = lock_data.get('current_concurrent', 0)
            lock_data['current_concurrent'] = max(0, old_concurrent - total_cleaned)
            lock_data['active_instances'] = cleaned_instances
            logger.info(
                f"清理完成 - 清理实例数: {len(cleaned_ids)}, "
                f"释放槽位数: {total_cleaned}, "
                f"当前并发数: {lock_data['current_concurrent']}"
            )

        return lock_data

    async def acquire_slot(
            self,
            instance_id: str,
            timeout: Optional[float] = None,
            owner_id: str = None,
            request_path: str = None
    ) -> bool:
        """
        获取一个并发槽位（改进版）

        Args:
            instance_id: 实例标识
            timeout: 等待槽位的超时时间（秒）
            owner_id: 请求所有者ID
            request_path: 请求路径

        Returns:
            bool: 是否成功获取槽位
        """
        timeout = timeout or self.acquire_timeout
        start_time = time.time()
        hostname = socket.gethostname()
        pid = os.getpid()
        current_time = time.time()

        while time.time() - start_time < timeout:
            if not self._acquire_file_lock():
                await asyncio.sleep(0.1)
                continue

            try:
                # 读取当前状态
                lock_data = self._read_lock_data()

                # 清理过期实例
                lock_data = self._cleanup_stale_instances(lock_data)

                current_concurrent = lock_data.get('current_concurrent', 0)
                max_concurrent = lock_data.get('max_concurrent', self.max_total_concurrent)

                # 检查是否有可用槽位
                if current_concurrent < max_concurrent:
                    # 获取槽位
                    lock_data['current_concurrent'] = current_concurrent + 1

                    # 初始化或更新实例信息
                    if instance_id not in lock_data.get('active_instances', {}):
                        lock_data.setdefault('active_instances', {})[instance_id] = {
                            'concurrent_count': 1,
                            'last_heartbeat': current_time,
                            'meta': {
                                'hostname': hostname,
                                'pid': pid,
                                'owner_id': owner_id or 'unknown',
                                'request_path': request_path or 'unknown',
                                'acquire_time': current_time,
                                'instance_id': instance_id
                            }
                        }
                    else:
                        # 增量更新现有实例
                        instance = lock_data['active_instances'][instance_id]
                        instance['concurrent_count'] += 1
                        instance['last_heartbeat'] = current_time
                        instance['meta']['last_acquire_time'] = current_time

                    # 保存数据
                    if self._write_lock_data(lock_data):
                        logger.info(
                            f"获取并发槽位成功 - instance_id: {instance_id}, "
                            f"当前: {lock_data['current_concurrent']}/{max_concurrent}, "
                            f"owner: {owner_id}"
                        )
                        return True
                    else:
                        logger.error(f"保存锁文件失败 - instance_id: {instance_id}")
                        continue
                else:
                    # 无可用槽位，记录占用情况
                    active_instances = lock_data.get('active_instances', {})
                    logger.warning(
                        f"无可用槽位 - 当前: {current_concurrent}/{max_concurrent}, "
                        f"活跃实例数: {len(active_instances)}"
                    )

            except Exception as e:
                logger.error(f"获取并发槽位异常: {e}", exc_info=True)
            finally:
                self._release_file_lock()

            # 等待后重试
            await asyncio.sleep(0.5)

        logger.warning(
            f"获取并发槽位超时 - instance_id: {instance_id}, "
            f"超时时间: {timeout}秒"
        )
        return False

    async def release_slot(self, instance_id: str) -> bool:
        """
        释放一个并发槽位

        ⭐ 修复：更好的容错性和日志记录

        Args:
            instance_id: 实例标识

        Returns:
            bool: 是否成功释放槽位
        """
        if not self._acquire_file_lock(timeout=3.0):
            logger.error(f"释放槽位时无法获取文件锁 - instance_id: {instance_id}")
            return False

        try:
            lock_data = self._read_lock_data()
            current_concurrent = lock_data.get('current_concurrent', 0)
            active_instances = lock_data.get('active_instances', {})

            # ⭐ 改进1：检查实例是否存在
            if instance_id not in active_instances:
                # 实例不存在可能是因为：
                # 1. 已被清理（超时）
                # 2. 重复释放
                # 3. 实例ID不匹配
                # 这在分布式环境中是可以接受的
                logger.debug(
                    f"实例未找到（可能已被清理） - instance_id: {instance_id}, "
                    f"当前活跃实例数: {len(active_instances)}"
                )
                return False

            instance_info = active_instances[instance_id]
            instance_concurrent = instance_info.get('concurrent_count', 0)

            # ⭐ 改进2：检查槽位计数是否有效
            if instance_concurrent <= 0:
                logger.warning(
                    f"实例槽位计数异常 - instance_id: {instance_id}, "
                    f"concurrent_count: {instance_concurrent}，清理该实例"
                )
                # 清理这个异常的实例
                del active_instances[instance_id]
                self._write_lock_data(lock_data)
                return False

            # ⭐ 改进3：原子性更新数据
            lock_data['current_concurrent'] = max(0, current_concurrent - 1)
            instance_info['concurrent_count'] = instance_concurrent - 1
            instance_info['last_heartbeat'] = time.time()

            # 如果实例没有活跃槽位，清理实例记录
            if instance_info['concurrent_count'] == 0:
                del active_instances[instance_id]
                logger.debug(f"实例记录已清理 - instance_id: {instance_id}")

            # 保存数据
            if self._write_lock_data(lock_data):
                logger.info(
                    f"✅ 释放并发槽位成功 - instance_id: {instance_id}, "
                    f"当前: {lock_data['current_concurrent']}/{lock_data.get('max_concurrent')}"
                )
                return True
            else:
                logger.error(
                    f"保存锁文件失败（槽位释放可能不完整） - instance_id: {instance_id}"
                )
                return False

        except Exception as e:
            logger.error(
                f"释放并发槽位异常 - instance_id: {instance_id}, 错误: {e}",
                exc_info=True
            )
            return False
        finally:
            self._release_file_lock()

    async def heartbeat(self, instance_id: str) -> bool:
        """
        发送心跳，保持实例活跃状态

        ⭐ 改进：确保时间戳正确更新，防止实例被清理

        Args:
            instance_id: 实例标识

        Returns:
            bool: 心跳是否成功
        """
        if not self._acquire_file_lock(timeout=2.0):
            logger.warning(f"无法获取文件锁，心跳失败 - instance_id: {instance_id}")
            return False

        try:
            lock_data = self._read_lock_data()
            active_instances = lock_data.get('active_instances', {})

            if instance_id in active_instances:
                # ⭐ 更新最后心跳时间
                active_instances[instance_id]['last_heartbeat'] = time.time()

                # 保存数据
                if self._write_lock_data(lock_data):
                    logger.debug(f"💓 心跳更新成功 - instance_id: {instance_id}")
                    return True
                else:
                    logger.error(f"保存心跳数据失败 - instance_id: {instance_id}")
                    return False
            else:
                logger.debug(f"实例未找到（心跳失败） - instance_id: {instance_id}")
                return False

        except Exception as e:
            logger.error(f"发送心跳异常 - instance_id: {instance_id}, 错误: {e}")
            return False
        finally:
            self._release_file_lock()

    async def get_current_status(self, include_details: bool = False) -> dict:
        """
        获取当前并发状态（改进版）

        Args:
            include_details: 是否包含详细的实例信息

        Returns:
            dict: 当前并发状态信息
        """
        if not self._acquire_file_lock(timeout=2.0):
            logger.error("无法获取文件锁，返回空状态")
            return {}

        try:
            lock_data = self._read_lock_data()

            # 清理过期实例
            lock_data = self._cleanup_stale_instances(lock_data)

            current = lock_data.get('current_concurrent', 0)
            max_conc = lock_data.get('max_concurrent', self.max_total_concurrent)
            available = max(0, max_conc - current)

            status = {
                'current_concurrent': current,
                'max_concurrent': max_conc,
                'available_slots': available,
                'utilization': f"{(current / max_conc * 100):.1f}%" if max_conc > 0 else "0%",
                'active_instances': len(lock_data.get('active_instances', {})),
                'last_updated': lock_data.get('last_updated', 0)
            }

            if include_details:
                # 包含详细的实例信息
                active_instances_info = {}
                for instance_id, instance_data in lock_data.get('active_instances', {}).items():
                    meta = instance_data.get('meta', {})
                    active_instances_info[instance_id] = {
                        'concurrent_count': instance_data.get('concurrent_count', 0),
                        'last_heartbeat': instance_data.get('last_heartbeat', 0),
                        'meta': {
                            'hostname': meta.get('hostname', 'unknown'),
                            'pid': meta.get('pid', 'unknown'),
                            'owner_id': meta.get('owner_id', 'unknown'),
                            'request_path': meta.get('request_path', 'unknown'),
                            'acquire_time': meta.get('acquire_time', 0),
                            'instance_id': meta.get('instance_id', 'unknown')
                        }
                    }
                status['active_instances_details'] = active_instances_info

            return status

        except Exception as e:
            logger.error(f"获取并发状态异常: {e}")
            return {}
        finally:
            self._release_file_lock()


class DistributedConcurrencyContext:
    """分布式并发控制上下文管理器"""

    def __init__(
            self,
            concurrency_control: DistributedConcurrencyControl,
            instance_id: str,
            owner_id: str = None,
            request_path: str = None
    ):
        self.concurrency_control = concurrency_control
        self.instance_id = instance_id
        self.owner_id = owner_id
        self.request_path = request_path
        self.acquired = False

    async def __aenter__(self):
        """进入上下文，获取并发槽位"""
        self.acquired = await self.concurrency_control.acquire_slot(
            self.instance_id,
            owner_id=self.owner_id,
            request_path=self.request_path
        )
        return self.acquired

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出上下文，释放并发槽位"""
        if self.acquired:
            await self.concurrency_control.release_slot(self.instance_id)
        return False
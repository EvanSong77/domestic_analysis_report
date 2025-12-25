# -*- coding: utf-8 -*-
# @Time    : 2025/12/23
# @Author  : EvanSong

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional

from src.utils import log_utils

logger = log_utils.logger


@dataclass
class TaskInfo:
    """任务信息"""
    task: asyncio.Task
    instance_id: str
    req_id: str
    start_time: float
    status: str = "running"  # running, cancelling, cancelled, completed, failed
    cancellation_requested: bool = False


class TaskManager:
    """
    任务管理器 - 支持任务取消和状态跟踪（改进版）

    ⭐ 改进：
    1. 允许重新提交已完成的任务（使用同一 req_id）
    2. 自动清理已完成的任务
    3. 更灵活的任务注册逻辑
    """

    def __init__(self, cleanup_delay: float = 60.0):
        self.tasks: Dict[str, TaskInfo] = {}  # req_id -> TaskInfo
        self._lock = asyncio.Lock()
        self.cleanup_delay = cleanup_delay  # 清理延迟（秒）

    async def register_task(self, req_id: str, instance_id: str, task: asyncio.Task) -> bool:
        """
        注册一个新任务

        ⭐ 改进：
        - 如果旧任务已完成/取消/失败，允许覆盖注册
        - 支持重新提交同一 req_id 的任务

        Args:
            req_id: 请求ID（作为任务ID）
            instance_id: 实例ID（用于并发控制）
            task: asyncio任务对象

        Returns:
            bool: 注册是否成功
        """
        async with self._lock:
            if req_id in self.tasks:
                old_task = self.tasks[req_id]

                # ⭐ 改进：如果旧任务已完成/取消/失败，允许覆盖
                if old_task.status in ["completed", "cancelled", "failed"]:
                    logger.info(
                        f"旧任务已结束（{old_task.status}），允许重新注册 - req_id: {req_id}"
                    )
                    del self.tasks[req_id]
                elif old_task.task.done():
                    # ⭐ 如果任务对象本身已完成（即使状态字段还没更新），也允许覆盖
                    logger.info(
                        f"旧任务已完成，允许重新注册 - req_id: {req_id}, 状态: {old_task.status}"
                    )
                    del self.tasks[req_id]
                else:
                    logger.warning(
                        f"任务仍在运行中，跳过注册 - req_id: {req_id}, 状态: {old_task.status}"
                    )
                    return False

            task_info = TaskInfo(
                task=task,
                instance_id=instance_id,
                req_id=req_id,
                start_time=time.time(),
                status="running"
            )
            self.tasks[req_id] = task_info

            logger.info(f"任务已注册 - req_id: {req_id}, instance_id: {instance_id}")

        # ⭐ 关键：在释放锁之后再添加回调，避免在持有锁时调用回调
        task.add_done_callback(self._make_completion_callback(req_id))
        return True

    def _make_completion_callback(self, req_id: str):
        """
        创建任务完成回调（工厂函数）

        ⭐ 改进：
        - 取消的任务立即清理
        - 完成的任务延迟清理（保留查询状态）
        - 失败的任务立即清理
        """

        def callback(task: asyncio.Task):
            try:
                if task.cancelled():
                    logger.info(f"✅ 任务已被取消（完成） - req_id: {req_id}")
                    # 立即清理取消的任务
                    asyncio.create_task(self._cleanup_task_immediate(req_id))
                elif task.exception():
                    logger.error(f"❌ 任务执行失败 - req_id: {req_id}, 异常: {task.exception()}")
                    # 失败的任务立即清理
                    asyncio.create_task(self._cleanup_task_immediate(req_id))
                else:
                    logger.info(f"✅ 任务执行完成 - req_id: {req_id}")
                    # 完成的任务延迟清理
                    asyncio.create_task(self._cleanup_task_after_delay(req_id, delay=self.cleanup_delay))
            except Exception as e:
                logger.error(f"处理任务完成回调异常 - req_id: {req_id}, 错误: {e}")

        return callback

    async def _cleanup_task_immediate(self, req_id: str):
        """
        立即清理任务（用于取消或失败的任务）

        Args:
            req_id: 请求ID
        """
        async with self._lock:
            if req_id in self.tasks:
                task_info = self.tasks[req_id]

                # 更新状态
                if task_info.task.cancelled():
                    task_info.status = "cancelled"
                elif task_info.task.exception():
                    task_info.status = "failed"

                # 立即删除，允许重新提交
                del self.tasks[req_id]
                logger.info(f"任务已立即清理 - req_id: {req_id}, 状态: {task_info.status}")

    async def _cleanup_task_after_delay(self, req_id: str, delay: float = 60.0):
        """
        延迟清理已完成的任务

        ⭐ 改进：延迟时间可配置（默认 60 秒）

        Args:
            req_id: 请求ID
            delay: 延迟清理时间（秒）
        """
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return

        async with self._lock:
            if req_id in self.tasks:
                task_info = self.tasks[req_id]
                # 只清理已完成的任务
                if task_info.status == "completed":
                    del self.tasks[req_id]
                    logger.debug(f"任务已延迟清理 - req_id: {req_id}")

    async def cancel_task(self, req_id: str) -> bool:
        """
        取消单个任务

        Args:
            req_id: 请求ID

        Returns:
            bool: 取消是否成功
        """
        async with self._lock:
            task_info = self.tasks.get(req_id)
            if not task_info:
                logger.warning(f"任务不存在，无法取消 - req_id: {req_id}")
                return False

            if task_info.status != "running":
                logger.warning(f"任务不在运行状态，无法取消 - req_id: {req_id}, 状态: {task_info.status}")
                return False

            # 标记取消请求
            task_info.cancellation_requested = True
            task_info.status = "cancelling"

            # 取消任务
            task_info.task.cancel()

            logger.info(f"任务取消请求已发送 - req_id: {req_id}")
            return True

    async def cancel_all_tasks(self) -> Dict[str, bool]:
        """
        取消所有运行中的任务

        Returns:
            Dict[str, bool]: 每个任务的取消结果
        """
        async with self._lock:
            # 获取所有运行中的任务ID
            running_task_ids = [
                req_id for req_id, info in self.tasks.items()
                if info.status == "running"
            ]

        # 在锁外执行取消操作，避免死锁
        results = {}
        for req_id in running_task_ids:
            result = await self.cancel_task(req_id)
            results[req_id] = result

        logger.info(f"已尝试取消所有任务 - 总数: {len(results)}")
        return results

    async def get_task_status(self, req_id: str) -> Optional[Dict]:
        """
        获取任务状态

        Args:
            req_id: 请求ID

        Returns:
            Optional[Dict]: 任务状态信息，如果任务不存在返回None
        """
        async with self._lock:
            task_info = self.tasks.get(req_id)
            if not task_info:
                return None

            # 计算运行时间
            running_time = time.time() - task_info.start_time

            # 检查任务是否已完成
            if task_info.task.done():
                if task_info.status == "running":
                    if task_info.task.cancelled():
                        task_info.status = "cancelled"
                    elif task_info.task.exception():
                        task_info.status = "failed"
                    else:
                        task_info.status = "completed"

                elif task_info.status == "cancelling":
                    if task_info.task.cancelled():
                        logger.info(f"取消操作已完成 - req_id: {req_id}, 更新状态为 cancelled")
                        task_info.status = "cancelled"
                    elif task_info.task.exception():
                        logger.warning(f"任务在取消过程中失败 - req_id: {req_id}, 更新状态为 failed")
                        task_info.status = "failed"
                    else:
                        logger.info(f"任务在 cancelling 状态下完成 - req_id: {req_id}, 更新状态为 completed")
                        task_info.status = "completed"

            return {
                "req_id": req_id,
                "instance_id": task_info.instance_id,
                "status": task_info.status,
                "running_time": round(running_time, 2),
                "cancellation_requested": task_info.cancellation_requested,
                "start_time": task_info.start_time,
                "task_done": task_info.task.done()
            }

    async def get_all_tasks_status(self) -> Dict[str, Dict]:
        """
        获取所有任务状态

        Returns:
            Dict[str, Dict]: 所有任务的状态信息
        """
        async with self._lock:
            req_ids = list(self.tasks.keys())

        result = {}
        for req_id in req_ids:
            status = await self.get_task_status(req_id)
            if status:
                result[req_id] = status

        return result

    async def is_task_cancelled(self, req_id: str) -> bool:
        """
        检查任务是否被请求取消

        Args:
            req_id: 请求ID

        Returns:
            bool: 是否被请求取消
        """
        async with self._lock:
            task_info = self.tasks.get(req_id)
            if not task_info:
                return False
            return task_info.cancellation_requested

    async def cleanup_all_completed(self):
        """
        清理所有已完成的任务
        """
        async with self._lock:
            to_remove = []
            for req_id, task_info in self.tasks.items():
                if task_info.status in ["completed", "failed", "cancelled"]:
                    to_remove.append(req_id)

            for req_id in to_remove:
                del self.tasks[req_id]

            if to_remove:
                logger.info(f"已清理已完成任务 - 数量: {len(to_remove)}")


# 全局任务管理器实例
task_manager = TaskManager(cleanup_delay=60.0)  # 已完成任务 60 秒后清理
# src/services/task_manager.py - 改进部分

from dataclasses import dataclass, field
import asyncio
import time
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
    completed_time: Optional[float] = None  # ⭐ 完成时间
    is_soft_deleted: bool = False  # ⭐ 软删除标记
    cancel_time: Optional[float] = None  # ⭐ 取消时间


class TaskManager:
    """改进版任务管理器"""

    def __init__(self, cleanup_delay: float = 60.0, query_grace_period: float = 300.0):
        self.tasks: Dict[str, TaskInfo] = {}
        self._lock = asyncio.Lock()
        self.cleanup_delay = cleanup_delay
        self.query_grace_period = query_grace_period  # ⭐ 查询宽限期（5分钟）

    async def register_task(self, req_id: str, instance_id: str, task: asyncio.Task) -> bool:
        """注册任务"""
        async with self._lock:
            if req_id in self.tasks:
                old_task = self.tasks[req_id]
                # 允许重新注册已完成的任务
                if old_task.status in ["completed", "cancelled", "failed"]:
                    logger.info(f"旧任务已结束，允许重新注册 - req_id: {req_id}")
                    del self.tasks[req_id]
                elif old_task.task.done():
                    logger.info(f"旧任务已完成，允许重新注册 - req_id: {req_id}")
                    del self.tasks[req_id]
                else:
                    logger.warning(f"任务仍在运行中，跳过注册 - req_id: {req_id}")
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

        # 添加完成回调（在锁外）
        task.add_done_callback(self._make_completion_callback(req_id))
        return True

    def _make_completion_callback(self, req_id: str):
        """创建任务完成回调"""

        def callback(task: asyncio.Task):
            try:
                if task.cancelled():
                    logger.info(f"✅ 任务已被取消（完成） - req_id: {req_id}")
                    # 取消的任务立即清理
                    asyncio.create_task(self._cleanup_task_immediate(req_id))
                elif task.exception():
                    logger.error(f"❌ 任务执行失败 - req_id: {req_id}, 异常: {task.exception()}")
                    asyncio.create_task(self._cleanup_task_immediate(req_id))
                else:
                    logger.info(f"✅ 任务执行完成 - req_id: {req_id}")
                    # ⭐ 改进：完成的任务延迟清理（软删除）
                    asyncio.create_task(self._cleanup_task_after_delay(req_id, delay=self.cleanup_delay))
            except Exception as e:
                logger.error(f"处理任务完成回调异常 - req_id: {req_id}, 错误: {e}")

        return callback

    async def _cleanup_task_immediate(self, req_id: str):
        """立即清理任务（用于取消或失败的任务）"""
        async with self._lock:
            if req_id in self.tasks:
                task_info = self.tasks[req_id]
                if task_info.task.cancelled():
                    task_info.status = "cancelled"
                elif task_info.task.exception():
                    task_info.status = "failed"

                # ⭐ 改为软删除，而非硬删除
                task_info.is_soft_deleted = True
                task_info.completed_time = time.time()
                logger.info(f"任务已软删除（立即） - req_id: {req_id}, 状态: {task_info.status}")

    async def _cleanup_task_after_delay(self, req_id: str, delay: float = 60.0):
        """延迟清理已完成的任务（软删除）"""
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return

        async with self._lock:
            if req_id in self.tasks:
                task_info = self.tasks[req_id]
                if task_info.status in ["completed", "failed", "cancelled"]:
                    # ⭐ 软删除：保留任务信息但标记为已删除
                    task_info.is_soft_deleted = True
                    task_info.completed_time = time.time()
                    logger.debug(f"任务已软删除（延迟） - req_id: {req_id}")

    async def cancel_task(self, req_id: str) -> bool:
        """
        改进版取消任务

        ⭐ 关键改进：
        1. 原子性更新状态
        2. 支持已取消的任务重新取消（幂等性）
        3. 不删除任务信息（便于审计）
        4. 返回明确的取消结果
        """
        async with self._lock:
            task_info = self.tasks.get(req_id)

            if not task_info:
                logger.warning(f"任务不存在，无法取消 - req_id: {req_id}")
                return False

            # ⭐ 改进：支持多次取消请求（幂等性）
            if task_info.status in ["cancelling", "cancelled"]:
                logger.info(f"任务已在取消中或已取消 - req_id: {req_id}, 当前状态: {task_info.status}")
                return True  # 返回 True 表示取消请求已处理

            if task_info.status in ["completed", "failed"]:
                logger.warning(f"任务已完成，无法取消 - req_id: {req_id}, 状态: {task_info.status}")
                return False

            if task_info.status != "running":
                logger.warning(f"任务状态异常，无法取消 - req_id: {req_id}, 状态: {task_info.status}")
                return False

            # ⭐ 原子性更新状态并标记取消时间
            task_info.status = "cancelling"
            task_info.cancellation_requested = True
            task_info.cancel_time = time.time()

            # 取消任务
            task_info.task.cancel()

            logger.info(f"任务取消请求已发送 - req_id: {req_id}, instance_id: {task_info.instance_id}")
            return True

    async def cancel_all_tasks(self) -> Dict[str, bool]:
        """改进版取消所有运行中的任务"""
        async with self._lock:
            running_task_ids = [
                req_id for req_id, info in self.tasks.items()
                if info.status == "running"
            ]

        logger.info(f"准备取消 {len(running_task_ids)} 个运行中的任务")

        results = {}
        for req_id in running_task_ids:
            result = await self.cancel_task(req_id)
            results[req_id] = result

        return results

    async def get_task_status(self, req_id: str) -> Optional[Dict]:
        """改进版获取任务状态"""
        async with self._lock:
            task_info = self.tasks.get(req_id)

            if not task_info:
                return None

            # ⭐ 软删除的任务，在宽限期内仍可查询
            if task_info.is_soft_deleted:
                age = time.time() - task_info.completed_time
                if age > self.query_grace_period:
                    # 宽限期已过，才真正删除
                    del self.tasks[req_id]
                    return None
                # 宽限期内，继续返回状态
                logger.debug(f"返回软删除任务状态 - req_id: {req_id}, 年龄: {age:.1f}s")

            # ⭐ 检查取消状态
            if task_info.status == "cancelling" and task_info.task.done():
                # 任务确实已取消
                if task_info.task.cancelled():
                    task_info.status = "cancelled"
                    task_info.completed_time = time.time()
                    logger.debug(f"更新状态为已取消 - req_id: {req_id}")

            running_time = time.time() - task_info.start_time

            response = {
                "req_id": req_id,
                "instance_id": task_info.instance_id,
                "status": task_info.status,
                "running_time": round(running_time, 2),
                "cancellation_requested": task_info.cancellation_requested,
                "start_time": task_info.start_time,
                "task_done": task_info.task.done(),
                "is_soft_deleted": task_info.is_soft_deleted
            }

            # ⭐ 新增：取消相关信息
            if task_info.cancel_time:
                response["cancel_time"] = task_info.cancel_time
                response["cancel_elapsed"] = round(time.time() - task_info.cancel_time, 2)

            if task_info.completed_time:
                response["completed_time"] = task_info.completed_time
                response["completed_elapsed"] = round(time.time() - task_info.completed_time, 2)

            return response

    async def get_all_tasks_status(self) -> Dict[str, Dict]:
        """获取所有任务状态"""
        async with self._lock:
            req_ids = list(self.tasks.keys())

        result = {}
        for req_id in req_ids:
            status = await self.get_task_status(req_id)
            if status:
                result[req_id] = status

        return result

    async def is_task_cancelled(self, req_id: str) -> bool:
        """检查任务是否被请求取消"""
        async with self._lock:
            task_info = self.tasks.get(req_id)
            if not task_info:
                return False
            return task_info.cancellation_requested

    async def cleanup_all_completed(self):
        """清理所有已过期的软删除任务"""
        async with self._lock:
            to_remove = []
            current_time = time.time()

            for req_id, task_info in self.tasks.items():
                if task_info.is_soft_deleted and task_info.completed_time:
                    age = current_time - task_info.completed_time
                    if age > self.query_grace_period:
                        to_remove.append(req_id)

            for req_id in to_remove:
                del self.tasks[req_id]

            if to_remove:
                logger.info(f"已清理过期任务 - 数量: {len(to_remove)}, 宽限期: {self.query_grace_period}秒")


# 全局任务管理器实例
task_manager = TaskManager(cleanup_delay=60.0, query_grace_period=300.0)
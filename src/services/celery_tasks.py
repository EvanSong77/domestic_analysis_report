# -*- coding: utf-8 -*-
# @Time    : 2025/01/14
# @Author  : EvanSong

import asyncio
import time
import json
from typing import Dict, Any
from celery import Task
from redis import Redis
from src.config.celery_config import celery_app
from src.config.config import get_settings
from src.services.ai_model_service import AIModelService
from src.services.async_data_query_service import AsyncDataQueryService
from src.services.async_result_service import AsyncResultService
from src.services.callback_service import CallbackService
from src.utils import log_utils

logger = log_utils.logger

settings = get_settings()
redis_config = settings.get_redis_config()

REDIS_URL = redis_config.get_status_url()

def get_redis_client():
    """获取 Redis 客户端"""
    return Redis.from_url(REDIS_URL, decode_responses=True)

def get_active_task_count() -> int:
    """
    统计当前正在执行的任务数量（状态为 processing 和 pending）

    Returns:
        int: 当前活跃任务数量
    """
    try:
        redis_client = get_redis_client()
        pattern = "task_status:*"
        keys = redis_client.keys(pattern)

        active_count = 0
        active_tasks = []

        for key in keys:
            try:
                data = redis_client.get(key)
                if data:
                    status_data = json.loads(data)
                    status = status_data.get('status')
                    if status in ['processing', 'pending']:
                        active_count += 1
                        active_tasks.append({
                            'req_id': status_data.get('req_id'),
                            'status': status,
                            'stage': status_data.get('stage')
                        })
            except Exception as e:
                logger.warning(f"解析任务状态失败 - key: {key}, 错误: {e}")
                continue

        logger.debug(f"当前活跃任务数量: {active_count}, 任务列表: {active_tasks}")
        return active_count
    except Exception as e:
        logger.error(f"统计活跃任务数量失败 - 错误: {e}")
        return 0

def can_submit_task() -> Dict[str, Any]:
    """
    检查是否可以提交新任务（基于并发限制）

    Returns:
        dict: 包含 can_submit (bool) 和 message (str) 的字典
    """
    try:
        max_concurrent = settings.app.system_concurrent
        active_count = get_active_task_count()

        if active_count >= max_concurrent:
            return {
                'can_submit': False,
                'current_count': active_count,
                'max_concurrent': max_concurrent,
                'message': f'当前有 {active_count} 个任务正在执行，已达到最大并发数 {max_concurrent}，请稍后提交'
            }

        return {
            'can_submit': True,
            'current_count': active_count,
            'max_concurrent': max_concurrent,
            'message': f'当前有 {active_count} 个任务正在执行，可以提交新任务'
        }
    except Exception as e:
        logger.error(f"检查任务提交权限失败 - 错误: {e}")
        return {
            'can_submit': False,
            'message': f'检查任务提交权限失败: {str(e)}'
        }


def update_task_status(req_id: str, task_id: str, status: str, **kwargs):
    """
    更新任务状态到 Redis

    Args:
        req_id: 请求ID
        task_id: Celery 任务ID
        status: 任务状态
        **kwargs: 额外的状态信息
    """
    try:
        redis_client = get_redis_client()
        key = f"task_status:{req_id}"

        status_data = {
            'req_id': req_id,
            'task_id': task_id,
            'status': status,
            'updated_at': time.time()
        }
        status_data.update(kwargs)

        expire_time = redis_config.task_status_expire
        redis_client.setex(key, expire_time, json.dumps(status_data))
        logger.debug(f"任务状态已更新到 Redis - req_id: {req_id}, status: {status}, 过期时间: {expire_time}秒")
    except Exception as e:
        logger.error(f"更新任务状态到 Redis 失败 - req_id: {req_id}, 错误: {e}")

def get_task_status_from_redis(req_id: str) -> Dict[str, Any]:
    """
    从 Redis 获取任务状态

    Args:
        req_id: 请求ID

    Returns:
        dict: 任务状态信息
    """
    try:
        redis_client = get_redis_client()
        key = f"task_status:{req_id}"
        data = redis_client.get(key)

        if data:
            return json.loads(data)
        else:
            return None
    except Exception as e:
        logger.error(f"从 Redis 获取任务状态失败 - req_id: {req_id}, 错误: {e}")
        return None


def is_task_cancelled(req_id: str) -> bool:
    """
    检查任务是否被取消

    Args:
        req_id: 请求ID

    Returns:
        bool: 任务是否被取消
    """
    try:
        redis_client = get_redis_client()
        key = f"task_cancel:{req_id}"
        cancelled = redis_client.get(key)
        return cancelled is not None
    except Exception as e:
        logger.error(f"检查任务取消状态失败 - req_id: {req_id}, 错误: {e}")
        return False


def mark_task_cancelled(req_id: str):
    """
    标记任务为已取消

    Args:
        req_id: 请求ID
    """
    try:
        redis_client = get_redis_client()
        key = f"task_cancel:{req_id}"

        expire_time = redis_config.task_cancel_expire
        redis_client.setex(key, expire_time, "1")
        logger.debug(f"任务已标记为取消 - req_id: {req_id}, 过期时间: {expire_time}秒")
    except Exception as e:
        logger.error(f"标记任务取消失败 - req_id: {req_id}, 错误: {e}")


def unmark_task_cancelled(req_id: str):
    """
    取消任务的取消标记

    Args:
        req_id: 请求ID
    """
    try:
        redis_client = get_redis_client()
        key = f"task_cancel:{req_id}"
        redis_client.delete(key)
        logger.debug(f"任务取消标记已清除 - req_id: {req_id}")
    except Exception as e:
        logger.error(f"清除任务取消标记失败 - req_id: {req_id}, 错误: {e}")


@celery_app.task(bind=True, name='process_diagnosis_report')
def process_diagnosis_report(self, req_id: str, request_data: Dict[str, Any], return_data_format: Dict[str, Any]):
    """
    Celery 任务：处理完整的诊断报告生成流程

    流程：
    1. 查询数据
    2. 生成报告
    3. 保存结果
    4. 发送回调

    Args:
        req_id: 请求ID
        request_data: 请求数据
        return_data_format: 返回数据格式

    Returns:
        dict: 任务执行结果
    """
    task_id = self.request.id
    loop = None
    logger.info(f"开始处理诊断报告 - 请求ID: {req_id}, 任务ID: {task_id}")

    redis_client = get_redis_client()
    lock_key = f"task_lock:{req_id}"
    lock_acquired = False
    lock_refresh_interval = 600  # 锁刷新间隔：10分钟
    initial_lock_ttl = 1800  # 初始锁过期时间：30分钟
    last_refresh_time = time.time()

    try:
        # 尝试获取分布式锁，防止同一个任务被多个worker重复处理
        lock_acquired = redis_client.setnx(lock_key, task_id)
        redis_client.expire(lock_key, initial_lock_ttl)

        if not lock_acquired:
            # 检查锁对应的任务ID是否存在且正在执行
            existing_task_id = redis_client.get(lock_key)
            if existing_task_id:
                logger.warning(f"任务 {req_id} 已被其他worker ({existing_task_id}) 处理，当前worker ({task_id}) 退出")
                return {
                    'status': 'duplicate',
                    'req_id': req_id,
                    'message': f'任务已被其他worker ({existing_task_id}) 处理'
                }
            else:
                # 锁已过期但未释放，尝试重新获取
                lock_acquired = redis_client.setnx(lock_key, task_id)
                redis_client.expire(lock_key, initial_lock_ttl)
                if not lock_acquired:
                    logger.warning(f"任务 {req_id} 锁竞争失败，当前worker ({task_id}) 退出")
                    return {
                        'status': 'duplicate',
                        'req_id': req_id,
                        'message': '任务锁竞争失败'
                    }

        update_task_status(req_id, task_id, 'processing', stage='started', message='任务已开始执行')

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # 定义锁刷新函数
        def refresh_lock():
            nonlocal last_refresh_time
            current_time = time.time()
            if current_time - last_refresh_time > lock_refresh_interval:
                # 只有当锁属于当前任务时才刷新
                if redis_client.get(lock_key) == task_id:
                    redis_client.expire(lock_key, initial_lock_ttl)
                    last_refresh_time = current_time
                    logger.debug(f"任务 {req_id} 的锁已刷新，新的过期时间: {initial_lock_ttl}秒")

        async def _handle_error_result(error_message: str, error_type: str = "unknown"):
            """
            处理错误结果：保存错误信息并发送回调
            """
            try:
                logger.info(f"开始处理错误结果 - 请求ID: {req_id}, 错误类型: {error_type}")

                update_task_status(req_id, task_id, 'failed', stage='error', error_type=error_type, error_message=error_message)

                async_result_service = AsyncResultService()

                error_data = {
                    'status': 'error',
                    'error_type': error_type,
                    'error_message': error_message,
                    'report_content': f"【ERROR】：{error_message}"
                }

                save_result = await async_result_service.save_diagnosis_result(
                    req_id,
                    return_data_format,
                    error_data,
                    timeout=30.0
                )

                if save_result['status'] != 'success':
                    logger.error(f"错误结果保存失败 - 请求ID: {req_id}, 错误: {save_result['message']}")
                else:
                    logger.info(f"错误结果保存成功 - 请求ID: {req_id}")

                await async_result_service.close()

                callback_service = CallbackService()
                callback_result = await callback_service.send_callback(
                    req_id,
                    return_data_format,
                    error_data
                )

                if callback_result['status'] != 'success':
                    logger.warning(f"错误回调失败 - 请求ID: {req_id}, 错误: {callback_result['message']}")
                else:
                    logger.info(f"错误回调成功 - 请求ID: {req_id}")

            except Exception as e:
                logger.error(f"处理错误结果异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)

        async def _process():
            from src.models.diagnosis_request import DiagnosisRequest

            request = DiagnosisRequest(**request_data)
            
            # 刷新锁
            refresh_lock()

            logger.info(f"[1/4] 开始异步查询数据 - 请求ID: {req_id}")
            update_task_status(req_id, task_id, 'processing', stage='querying_data', message='正在查询数据')

            async_data_service = AsyncDataQueryService()
            data_result = await async_data_service.get_diagnosis_data(request, timeout=1800.0)

            if data_result['status'] != 'success':
                logger.error(f"数据查询失败 - 请求ID: {req_id}, 错误: {data_result['message']}")
                await async_data_service.close()
                await _handle_error_result(data_result['message'], "data_query_error")
                return {'status': 'error', 'message': data_result['message']}

            logger.info(f"[1/4] 异步数据查询成功 - 请求ID: {req_id}, 记录数: {data_result.get('record_count', 0)}")
            update_task_status(req_id, task_id, 'processing', stage='data_queried', record_count=data_result.get('record_count', 0), message='数据查询成功')
            await async_data_service.close()

            if is_task_cancelled(req_id):
                logger.info(f"任务已被取消 - 请求ID: {req_id}")
                update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已被取消')
                return {'status': 'cancelled', 'req_id': req_id, 'message': '任务已被取消'}

            # 刷新锁
            refresh_lock()

            logger.info(f"[2/4] 开始生成报告 - 请求ID: {req_id}")
            update_task_status(req_id, task_id, 'processing', stage='generating_report', message='正在生成报告')

            ai_service = AIModelService()
            report_result = await ai_service.generate_diagnosis_report(data_result['data'])

            if report_result['status'] != 'success':
                logger.error(f"报告生成失败 - 请求ID: {req_id}, 错误: {report_result['message']}")
                await _handle_error_result(report_result['message'], "report_generation_error")
                return {'status': 'error', 'message': report_result['message']}

            logger.info(f"[2/4] 报告生成成功 - 请求ID: {req_id}")
            update_task_status(req_id, task_id, 'processing', stage='report_generated', message='报告生成成功')

            if is_task_cancelled(req_id):
                logger.info(f"任务已被取消 - 请求ID: {req_id}")
                update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已被取消')
                return {'status': 'cancelled', 'req_id': req_id, 'message': '任务已被取消'}

            # 刷新锁
            refresh_lock()

            logger.info(f"[3/4] 开始异步保存结果 - 请求ID: {req_id}")
            update_task_status(req_id, task_id, 'processing', stage='saving_result', message='正在保存结果')

            async_result_service = AsyncResultService()

            if 'report_contents' in report_result['data']:
                save_data = {
                    'report_contents': report_result['data']['report_contents'],
                    'report_results': report_result['data']['report_results'],
                    'actual_params': report_result['data']['actual_params'],
                    'usage': report_result['data']['usage'],
                    'prompt_tokens': report_result['data']['prompt_tokens'],
                    'completion_tokens': report_result['data']['completion_tokens'],
                    'report_count': len(report_result['data']['report_contents'])
                }
            else:
                save_data = report_result['data']

            save_result = await async_result_service.save_diagnosis_result(
                req_id,
                return_data_format,
                save_data,
                timeout=30.0
            )

            if save_result['status'] != 'success':
                logger.error(f"结果保存失败 - 请求ID: {req_id}, 错误: {save_result['message']}")
                await async_result_service.close()
                await _handle_error_result(f"结果保存失败: {save_result['message']}", "result_save_error")
                return {'status': 'error', 'message': save_result['message']}

            logger.info(f"[3/4] 异步结果保存成功 - 请求ID: {req_id}")
            update_task_status(req_id, task_id, 'processing', stage='result_saved', message='结果保存成功')
            await async_result_service.close()

            if is_task_cancelled(req_id):
                logger.info(f"任务已被取消 - 请求ID: {req_id}")
                update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已被取消')
                return {'status': 'cancelled', 'req_id': req_id, 'message': '任务已被取消'}

            # 刷新锁
            refresh_lock()

            logger.info(f"[4/4] 开始发送回调 - 请求ID: {req_id}")
            update_task_status(req_id, task_id, 'processing', stage='sending_callback', message='正在发送回调')

            if is_task_cancelled(req_id):
                logger.info(f"任务已被取消 - 请求ID: {req_id}")
                update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已被取消')
                return {'status': 'cancelled', 'req_id': req_id, 'message': '任务已被取消'}

            callback_service = CallbackService()

            if 'report_contents' in report_result['data']:
                callback_result = await callback_service.send_callback(
                    req_id,
                    return_data_format,
                    {
                        'report_contents': report_result['data']['report_contents'],
                        'report_results': report_result['data']['report_results'],
                        'actual_params': report_result['data']['actual_params']
                    }
                )
            else:
                callback_result = await callback_service.send_callback(
                    req_id,
                    return_data_format,
                    report_result['data']
                )

            if callback_result['status'] != 'success':
                logger.warning(f"回调失败 - 请求ID: {req_id}, 错误: {callback_result['message']}")
            else:
                logger.info(f"[4/4] 回调成功 - 请求ID: {req_id}")

            logger.info(f"✅ 诊断请求处理完成 - 请求ID: {req_id}")
            unmark_task_cancelled(req_id)
            update_task_status(req_id, task_id, 'completed', stage='finished', message='任务完成')

            return {
                'status': 'success',
                'req_id': req_id,
                'message': '诊断报告生成完成'
            }

        result = loop.run_until_complete(_process())

        return result

    except asyncio.CancelledError:
        logger.info(f"任务被取消 - 请求ID: {req_id}, 任务ID: {task_id}")
        update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已被取消')
        return {
            'status': 'cancelled',
            'req_id': req_id,
            'message': '任务已被取消'
        }
    except Exception as e:
        logger.error(f"任务执行异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)
        update_task_status(req_id, task_id, 'failed', stage='exception', error_message=str(e))
        return {
            'status': 'error',
            'req_id': req_id,
            'message': f'任务执行异常: {str(e)}'
        }
    finally:
        if loop:
            loop.close()

        if is_task_cancelled(req_id):
            logger.info(f"任务被取消 - 请求ID: {req_id}, 任务ID: {task_id}")
            update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已被取消')

        # 释放分布式锁
        if lock_acquired:
            current_task_id = redis_client.get(lock_key)
            if current_task_id == task_id:
                redis_client.delete(lock_key)
                logger.debug(f"任务 {req_id} 的锁已释放，任务ID: {task_id}")
            else:
                logger.debug(f"任务 {req_id} 的锁不属于当前worker，跳过释放，当前锁: {current_task_id}, 任务ID: {task_id}")


def cancel_all_tasks():
    """
    取消所有正在执行的诊断任务（同步函数）

    ⭐ 注意：这不是 Celery task，而是同步函数
    Returns:
        dict: 取消结果
    """
    logger.info("开始取消所有任务")

    try:
        from celery import current_app

        inspect = current_app.control.inspect()
        active_tasks = inspect.active()

        cancelled_count = 0
        cancelled_tasks = []

        if active_tasks:
            for worker_name, tasks in active_tasks.items():
                for task in tasks:
                    task_id = task['id']
                    task_name = task['name']

                    if task_name == 'process_diagnosis_report':
                        try:
                            req_id = None
                            
                            args = task.get('args', [])
                            kwargs = task.get('kwargs', {})
                            
                            if args and len(args) > 0:
                                req_id = args[0]
                            elif 'req_id' in kwargs:
                                req_id = kwargs['req_id']
                            
                            if req_id:
                                mark_task_cancelled(req_id)
                                update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已取消')
                                logger.info(f"已标记任务为取消 - req_id: {req_id}, task_id: {task_id}")
                            else:
                                logger.warning(f"无法获取 req_id - task_id: {task_id}, args: {args}, kwargs: {kwargs}")
                            
                            current_app.control.revoke(task_id, terminate=True, signal='SIGTERM')
                            
                            cancelled_count += 1
                            cancelled_tasks.append({
                                'task_id': task_id,
                                'worker': worker_name,
                                'task_name': task_name,
                                'req_id': req_id
                            })
                            logger.info(f"已取消任务 - 任务ID: {task_id}, Worker: {worker_name}")
                        except Exception as e:
                            logger.error(f"取消任务失败 - 任务ID: {task_id}, 错误: {e}")

        logger.info(f"取消所有任务完成 - 已取消: {cancelled_count}")

        return {
            'status': 'success',
            'cancelled_count': cancelled_count,
            'cancelled_tasks': cancelled_tasks,
            'message': f'已成功取消 {cancelled_count} 个任务'
        }

    except Exception as e:
        logger.error(f"取消所有任务异常: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'message': f'取消所有任务失败: {str(e)}'
        }


def cancel_task(task_id: str, req_id: str = None) -> Dict[str, Any]:
    """
    取消指定的 Celery 任务

    Args:
        task_id: Celery 任务ID
        req_id: 请求ID（可选，用于更新 Redis 状态）

    Returns:
        dict: 取消结果
    """
    try:
        result = celery_app.AsyncResult(task_id)

        if result.ready():
            logger.warning(f"任务已完成，无法取消 - 任务ID: {task_id}")
            return {
                'task_id': task_id,
                'status': 'error',
                'message': '任务已完成，无法取消'
            }

        celery_app.control.revoke(task_id, terminate=True, signal='SIGTERM')

        if req_id:
            mark_task_cancelled(req_id)
            update_task_status(req_id, task_id, 'cancelled', stage='cancelled', message='任务已取消')

        logger.info(f"任务取消请求已发送 - 任务ID: {task_id}")

        return {
            'task_id': task_id,
            'status': 'success',
            'message': '任务取消请求已发送'
        }

    except Exception as e:
        logger.error(f"取消任务异常 - 任务ID: {task_id}, 错误: {str(e)}", exc_info=True)
        return {
            'task_id': task_id,
            'status': 'error',
            'message': f'取消任务失败: {str(e)}'
        }

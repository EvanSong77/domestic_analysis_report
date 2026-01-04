# -*- coding: utf-8 -*-
# @Time    : 2025/11/18 13:48
# @Author  : EvanSong

import asyncio
import time
import uuid

from fastapi import APIRouter, Depends, BackgroundTasks

from src.config.config import get_settings
from src.models.diagnosis_request import DiagnosisRequest
from src.models.diagnosis_response import StandardResponse
from src.services.ai_model_service import AIModelService
from src.services.async_data_query_service import AsyncDataQueryService
from src.services.async_result_service import AsyncResultService
from src.services.callback_service import CallbackService
from src.services.distributed_concurrency_control import DistributedConcurrencyControl
from src.services.task_manager import task_manager
from src.utils import log_utils
from src.utils.bearer import verify_token

logger = log_utils.logger
Report_router = APIRouter()
settings = get_settings()

# 初始化分布式并发控制
try:
    concurrency_control = DistributedConcurrencyControl(config=settings.concurrency)
    logger.info("分布式并发控制初始化成功")
except Exception as e:
    logger.error(f"初始化并发控制失败: {e}")
    concurrency_control = None


async def _send_heartbeat(instance_id: str, stop_event: asyncio.Event):
    """
    心跳保活任务（独立函数）

    ⭐ 改进：
    - 首次延迟缩短为 0.5 秒（确保及时更新）
    - 之后每 2 秒一次（减少文件 I/O 压力）
    - 使用 stop_event 支持优雅停止
    """
    first_beat = True
    try:
        while not stop_event.is_set():
            # ⭐ 首次心跳快速发送（0.5秒）
            if first_beat:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=0.5)
                    break  # stop_event 被设置
                except asyncio.TimeoutError:
                    pass  # 超时，继续发送心跳
                first_beat = False
            else:
                # 之后每 2 秒发送一次
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=2.0)
                    break  # stop_event 被设置，退出
                except asyncio.TimeoutError:
                    pass  # 超时，继续发送心跳

            if concurrency_control:
                try:
                    success = await concurrency_control.heartbeat(instance_id)
                    if not success:
                        logger.warning(f"⚠️  心跳发送失败 - instance_id: {instance_id}")
                    else:
                        logger.debug(f"💓 心跳已发送 - instance_id: {instance_id}")
                except Exception as e:
                    logger.error(f"心跳发送异常 - instance_id: {instance_id}, 错误: {e}")

    except asyncio.CancelledError:
        logger.debug(f"心跳任务被取消 - instance_id: {instance_id}")
    except Exception as e:
        logger.error(f"心跳任务异常 - instance_id: {instance_id}, 错误: {e}")


async def _process_full_diagnosis_report(
        req_id: str,
        instance_id: str,
        request: DiagnosisRequest,
        return_data_format: dict,
        heartbeat_stop_event: asyncio.Event
):
    """
    后台异步处理：完整流程，包括数据查询、报告生成、保存、回调

    ⭐ 改进：
    - 心跳已在外部启动，这里只需关心业务逻辑
    - 执行完成后通知心跳线程停止
    """
    try:
        logger.info(f"开始处理诊断请求 - 请求ID: {req_id}, 实例ID: {instance_id}")

        # ========== 任务执行开始 ==========

        # 1. 查询数据（使用异步服务）
        try:
            logger.info(f"[1/4] 开始异步查询数据 - 请求ID: {req_id}")

            # 检查是否被取消
            if await task_manager.is_task_cancelled(req_id):
                logger.info(f"任务被取消，停止数据查询 - 请求ID: {req_id}")
                return

            # 使用异步数据查询服务，设置超时时间
            async_data_service = AsyncDataQueryService()
            data_result = await async_data_service.get_diagnosis_data(request, timeout=1800.0)

            # 在查询过程中再次检查是否被取消
            if await task_manager.is_task_cancelled(req_id):
                logger.info(f"任务在数据查询过程中被取消 - 请求ID: {req_id}")
                await async_data_service.close()
                return

            if data_result['status'] != 'success':
                logger.error(f"数据查询失败 - 请求ID: {req_id}, 错误: {data_result['message']}")
                await async_data_service.close()
                return

            logger.info(f"[1/4] 异步数据查询成功 - 请求ID: {req_id}, 记录数: {data_result.get('record_count', 0)}")

            # 查询完成后关闭连接
            await async_data_service.close()

        except asyncio.TimeoutError:
            logger.error(f"数据查询超时 - 请求ID: {req_id}")
            return
        except Exception as e:
            logger.error(f"数据查询异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)
            return

        # 2. 生成报告
        try:
            logger.info(f"[2/4] 开始生成报告 - 请求ID: {req_id}")

            # 检查是否被取消
            if await task_manager.is_task_cancelled(req_id):
                logger.info(f"任务被取消，停止报告生成 - 请求ID: {req_id}")
                return

            ai_service = AIModelService()
            report_result = await ai_service.generate_diagnosis_report(data_result['data'])

            if report_result['status'] != 'success':
                logger.error(f"报告生成失败 - 请求ID: {req_id}, 错误: {report_result['message']}")
                return

            logger.info(f"[2/4] 报告生成成功 - 请求ID: {req_id}")

        except Exception as e:
            logger.error(f"报告生成异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)
            return

        # 3. 保存结果（使用异步服务）
        try:
            logger.info(f"[3/4] 开始异步保存结果 - 请求ID: {req_id}")

            # 检查是否被取消
            if await task_manager.is_task_cancelled(req_id):
                logger.info(f"任务被取消，停止结果保存 - 请求ID: {req_id}")
                return

            async_result_service = AsyncResultService()

            # 准备保存数据
            if 'report_contents' in report_result['data']:
                # 多个报告的情况
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
                # 单个报告的情况
                save_data = report_result['data']

            # 异步保存结果，设置超时时间
            save_result = await async_result_service.save_diagnosis_result(
                req_id,
                return_data_format,
                save_data,
                timeout=30.0
            )

            # 在保存过程中再次检查是否被取消
            if await task_manager.is_task_cancelled(req_id):
                logger.info(f"任务在结果保存过程中被取消 - 请求ID: {req_id}")
                await async_result_service.close()
                return

            if save_result['status'] != 'success':
                logger.error(f"结果保存失败 - 请求ID: {req_id}, 错误: {save_result['message']}")
                await async_result_service.close()
                return

            logger.info(f"[3/4] 异步结果保存成功 - 请求ID: {req_id}")

            # 保存完成后关闭连接
            await async_result_service.close()

        except asyncio.TimeoutError:
            logger.error(f"结果保存超时 - 请求ID: {req_id}")
            return
        except Exception as e:
            logger.error(f"结果保存异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)
            return

        # 4. 发送回调
        try:
            logger.info(f"[4/4] 开始发送回调 - 请求ID: {req_id}")

            # 检查是否被取消
            if await task_manager.is_task_cancelled(req_id):
                logger.info(f"任务被取消，停止回调发送 - 请求ID: {req_id}")
                return

            callback_service = CallbackService()

            if 'report_contents' in report_result['data']:
                # 多个报告的情况
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
                # 单个报告的情况
                callback_result = await callback_service.send_callback(
                    req_id,
                    return_data_format,
                    report_result['data']
                )

            if callback_result['status'] != 'success':
                logger.warning(f"回调失败 - 请求ID: {req_id}, 错误: {callback_result['message']}")
            else:
                logger.info(f"[4/4] 回调成功 - 请求ID: {req_id}")

        except Exception as e:
            logger.error(f"回调发送异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)

        # ========== 任务执行完成 ==========

        logger.info(f"✅ 诊断请求处理完成 - 请求ID: {req_id}")

    except asyncio.CancelledError:
        logger.info(f"任务被异步取消 - 请求ID: {req_id}")
        raise
    except Exception as e:
        logger.error(f"后台处理异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)

    finally:
        # ⭐ 关键：通知心跳停止
        heartbeat_stop_event.set()
        logger.debug(f"已通知心跳停止 - 请求ID: {req_id}, 实例ID: {instance_id}")


@Report_router.post("/diagnosis", dependencies=[Depends(verify_token)])
async def generate_diagnosis_report(
        request: DiagnosisRequest,
        background_tasks: BackgroundTasks
):
    """
    经营异常诊断结果计算 - 改进版本（快速响应 + 快速心跳）

    ⭐ 核心改进：
    1. 立刻获取槽位后立即返回（不阻塞）
    2. 心跳在外部独立启动，不依赖业务流程
    3. 状态查询和取消操作可立即进行

    流程：
    1. 获取槽位 → 立刻返回 202
    2. 启动心跳任务 → 0.5秒后第一次发送
    3. 启动业务处理 → 后台异步执行
    4. 用户可以立即查询状态或取消任务

    Args:
        request: 诊断请求
        background_tasks: FastAPI后台任务队列

    Returns:
        StandardResponse: 成功或拒绝的响应
    """
    instance_id = f"instance_{uuid.uuid4()}"

    try:
        logger.info(f"收到诊断报告请求 - 请求ID: {request.reqId}, 实例ID: {instance_id}")

        # ========== 步骤 1：尝试获取并发槽位 ==========

        if not concurrency_control:
            logger.error("并发控制未启用，无法处理请求")
            return StandardResponse.error(503, "系统配置错误")

        # 尝试获取槽位
        acquired = await concurrency_control.acquire_slot(
            instance_id,
            timeout=settings.concurrency.acquire_timeout if hasattr(settings.concurrency, 'acquire_timeout') else 30,
            owner_id=request.reqId,
            request_path="POST /diagnosis"
        )

        if not acquired:
            # ❌ 获取槽位失败：系统已满，拒绝请求
            logger.warning(f"❌ 请求被拒绝（系统已满） - 请求ID: {request.reqId}")

            try:
                status = await concurrency_control.get_current_status(include_details=True)
                logger.warning(
                    f"并发状态 - 当前: {status.get('current_concurrent')}/{status.get('max_concurrent')}, "
                    f"活跃实例: {status.get('active_instances')}"
                )
            except Exception as e:
                logger.error(f"获取并发状态失败: {e}")

            return {
                "code": 429,
                "msg": "系统繁忙，请稍后重试",
                "data": {
                    'retryAfter': 30,
                    'message': '所有处理槽位都已被占用'
                }
            }

        # ✅ 成功获取槽位
        logger.info(f"✅ 成功获取槽位 - 请求ID: {request.reqId}, 实例ID: {instance_id}")

        # ========== 步骤 2：准备返回数据格式 ==========

        return_data_format = {
            "period": request.period,
            "diagnosisType": request.diagnosisType,
            "provinceName": request.provinceName,
            "officeLv2Name": request.officeLv2Name,
            "distributionType": request.distributionType,
            "itIncludeType": request.itIncludeType,
            "currentPage": request.currentPage,
            "pageSize": request.pageSize
        }

        # ========== 步骤 3：创建心跳停止事件 ==========

        # ⭐ 改进：使用 asyncio.Event 而不是在业务流程中管理心跳
        heartbeat_stop_event = asyncio.Event()

        # ========== 步骤 4：立即启动心跳任务（关键！） ==========

        heartbeat_task = asyncio.create_task(
            _send_heartbeat(instance_id, heartbeat_stop_event)
        )
        logger.info(
            f"💓 心跳任务已启动 - instance_id: {instance_id}, "
            f"首次延迟 0.5秒"
        )

        # ========== 步骤 5：创建业务处理包装函数 ==========

        async def managed_task_wrapper():
            """
            业务处理包装函数：
            1. 向任务管理器注册
            2. 执行业务流程
            3. 通知心跳停止并等待其完成
            4. 释放槽位
            """
            current_task = asyncio.current_task()
            slot_released = False

            try:
                # ⭐ 步骤1：向任务管理器注册
                registered = await task_manager.register_task(
                    req_id=request.reqId,
                    instance_id=instance_id,
                    task=current_task
                )

                if not registered:
                    logger.error(f"❌ 任务注册失败 - 请求ID: {request.reqId}")
                    heartbeat_stop_event.set()  # 停止心跳
                    try:
                        # 等待心跳任务完成（最多 2 秒）
                        await asyncio.wait_for(heartbeat_task, timeout=2.0)
                    except asyncio.TimeoutError:
                        heartbeat_task.cancel()
                        logger.warning(f"心跳任务未在规定时间内停止，强制取消 - instance_id: {instance_id}")
                    except Exception as e:
                        logger.debug(f"等待心跳任务异常: {e}")

                    if concurrency_control:
                        try:
                            released = await concurrency_control.release_slot(instance_id)
                            if released:
                                slot_released = True
                                logger.info(f"由于注册失败，槽位已释放 - instance_id: {instance_id}")
                        except Exception as e:
                            logger.error(f"释放槽位异常 - instance_id: {instance_id}, 错误: {e}")
                    return

                logger.info(f"✅ 任务已成功注册 - 请求ID: {request.reqId}, 实例ID: {instance_id}")

                # ⭐ 步骤2：执行业务流程
                await _process_full_diagnosis_report(
                    req_id=request.reqId,
                    instance_id=instance_id,
                    request=request,
                    return_data_format=return_data_format,
                    heartbeat_stop_event=heartbeat_stop_event
                )

                logger.info(f"✅ 业务处理流程完成 - 请求ID: {request.reqId}")

            except asyncio.CancelledError:
                logger.info(f"🛑 业务任务被取消 - 请求ID: {request.reqId}, 实例ID: {instance_id}")
                heartbeat_stop_event.set()  # 停止心跳
                raise

            except Exception as e:
                logger.error(f"❌ 业务任务异常 - 请求ID: {request.reqId}, 错误: {str(e)}", exc_info=True)
                heartbeat_stop_event.set()  # 停止心跳
                raise

            finally:
                # ⭐ 关键：等待心跳任务优雅停止
                try:
                    await asyncio.wait_for(heartbeat_task, timeout=2.0)
                    logger.debug(f"心跳任务已优雅停止 - instance_id: {instance_id}")
                except asyncio.TimeoutError:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        logger.warning(f"心跳任务未在规定时间内停止，已强制取消 - instance_id: {instance_id}")
                except Exception as e:
                    logger.debug(f"等待心跳任务异常: {e}")

                # ⭐ 关键：释放槽位
                if not slot_released and concurrency_control:
                    try:
                        logger.debug(f"开始释放槽位 - 请求ID: {request.reqId}, 实例ID: {instance_id}")
                        released = await concurrency_control.release_slot(instance_id)

                        if released:
                            slot_released = True
                            logger.info(f"✅ 槽位已释放（业务完成后） - 请求ID: {request.reqId}, 实例ID: {instance_id}")
                        else:
                            logger.debug(f"槽位释放返回 False - 实例可能已被清理 - instance_id: {instance_id}")

                    except Exception as e:
                        logger.error(f"释放槽位异常 - 请求ID: {request.reqId}, 错误: {e}")

        # ========== 步骤 6：将业务处理任务添加到后台任务队列 ==========

        background_tasks.add_task(managed_task_wrapper)

        logger.info(f"📋 后台业务处理任务已添加到队列 - 请求ID: {request.reqId}, 实例ID: {instance_id}")

        # ========== 步骤 7：立即响应客户端 ==========

        return StandardResponse.success({
            'reqId': request.reqId,
            'instanceId': instance_id,
            'message': '诊断请求已接受，正在处理中',
            'status': 'processing',
            'note': '系统采用严格顺序执行，支持任务取消',
            'cancelEndpoint': f"/fin-report/diagnosis/cancel/{request.reqId}",
            'statusEndpoint': f"/fin-report/diagnosis/task/status/{request.reqId}",
            'concurrencyStatusEndpoint': "/fin-report/diagnosis/concurrency/status"
        })

    except Exception as e:
        logger.error(f"处理诊断报告请求异常 - 请求ID: {request.reqId}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"系统异常: {str(e)}")

@Report_router.post("/gp/cancel/{req_id}", dependencies=[Depends(verify_token)])
async def cancel_diagnosis_task(req_id: str):
    """
    改进版：取消正在执行的诊断任务

    ⭐ 关键改进：
    1. 先从任务管理器取消（主路径）
    2. 再从并发控制清理槽位（副路径）
    3. 支持重复取消（幂等性）
    4. 清晰的状态反馈
    """
    try:
        logger.info(f"收到任务取消请求 - req_id: {req_id}")

        cancel_success = False
        instance_id = None
        current_status = None

        # ========== 步骤1：从任务管理器取消任务（主路径） ==========
        cancelled = await task_manager.cancel_task(req_id)

        if cancelled:
            # 取消成功，获取任务详细信息
            task_status = await task_manager.get_task_status(req_id)
            if task_status:
                instance_id = task_status.get('instance_id')
                current_status = task_status.get('status')
                cancel_success = True
                logger.info(f"✅ 任务取消请求已发送（通过任务管理器） - req_id: {req_id}, instance_id: {instance_id}")
        else:
            # 任务可能不在任务管理器中，尝试从并发控制查找（副路径）
            logger.info(f"任务不在任务管理器中或无法取消，尝试从并发控制中查找 - req_id: {req_id}")

            if concurrency_control:
                status = await concurrency_control.get_current_status(include_details=True)
                active_instances = status.get('active_instances_details', {})

                # 查找是否有匹配的实例
                for inst_id, instance_data in active_instances.items():
                    meta = instance_data.get('meta', {})
                    if meta.get('owner_id') == req_id:
                        found_instance = True
                        instance_id = inst_id

                        # 尝试释放槽位来间接停止任务
                        try:
                            released = await concurrency_control.release_slot(instance_id)
                            if released:
                                cancel_success = True
                                current_status = "cancelling"
                                logger.info(
                                    f"✅ 并发槽位已释放（间接取消） - req_id: {req_id}, "
                                    f"instance_id: {instance_id}"
                                )
                            else:
                                logger.warning(
                                    f"⚠️  释放并发槽位失败 - req_id: {req_id}, "
                                    f"instance_id: {instance_id}"
                                )
                        except Exception as e:
                            logger.error(f"释放槽位异常 - req_id: {req_id}, 错误: {e}")

                        break

                if not cancel_success and instance_id is None:
                    logger.warning(f"任务既不在任务管理器也不在并发控制中 - req_id: {req_id}")

        # ========== 步骤2：构建响应 ==========
        if cancel_success:
            return StandardResponse.success({
                'req_id': req_id,
                'cancelled': True,
                'instance_id': instance_id,
                'current_status': current_status,
                'message': f'任务 {req_id} 取消请求已发送',
                'note': '任务可能需要几秒钟才能真正停止',
                'status_endpoint': f"/fin-report/diagnosis/task/status/{req_id}"
            })
        else:
            # ⭐ 改进：更详细的失败原因
            return StandardResponse.error(
                400,
                f"任务 {req_id} 不存在、已完成或无法取消",
                {
                    'req_id': req_id,
                    'possible_reasons': [
                        '任务不存在',
                        '任务已完成或已取消',
                        '任务信息已过期被清理（查询宽限期已过）'
                    ],
                    'suggestion': '请检查请求ID是否正确，或查询任务状态'
                }
            )

    except Exception as e:
        logger.error(f"取消任务异常 - req_id: {req_id}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"取消任务失败: {str(e)}")


@Report_router.post("/gp/cancel-all", dependencies=[Depends(verify_token)])
async def cancel_all_diagnosis_tasks():
    """
    改进版：取消所有正在执行的诊断任务

    ⭐ 关键改进：
    1. 区分已注册和未注册的任务
    2. 更详细的取消结果统计
    3. 处理并发错误
    """
    try:
        logger.info("收到取消所有任务的请求")

        registered_results = {}
        unregistered_results = []

        # ========== 步骤1：从任务管理器取消所有已注册的任务 ==========
        registered_results = await task_manager.cancel_all_tasks()
        registered_cancelled_count = sum(1 for result in registered_results.values() if result)

        logger.info(
            f"已从任务管理器取消任务 - 总数: {len(registered_results)}, "
            f"成功: {registered_cancelled_count}"
        )

        # ========== 步骤2：从并发控制中查找并取消未注册的任务 ==========
        if concurrency_control:
            status = await concurrency_control.get_current_status(include_details=True)
            active_instances = status.get('active_instances_details', {})

            # 获取已注册任务的instance_id集合
            registered_instance_ids = set()
            for req_id, cancelled in registered_results.items():
                if cancelled:
                    task_status = await task_manager.get_task_status(req_id)
                    if task_status and 'instance_id' in task_status:
                        registered_instance_ids.add(task_status['instance_id'])

            # 处理未注册的任务
            for instance_id, instance_data in active_instances.items():
                if instance_id in registered_instance_ids:
                    continue  # 已处理过

                meta = instance_data.get('meta', {})
                req_id = meta.get('owner_id', 'unknown')

                try:
                    released = await concurrency_control.release_slot(instance_id)
                    if released:
                        unregistered_results.append({
                            'req_id': req_id,
                            'instance_id': instance_id,
                            'cancelled': True,
                            'registered': False,
                            'slot_released': True
                        })
                        logger.info(
                            f"✅ 未注册任务已取消 - req_id: {req_id}, "
                            f"instance_id: {instance_id}"
                        )
                    else:
                        unregistered_results.append({
                            'req_id': req_id,
                            'instance_id': instance_id,
                            'cancelled': False,
                            'registered': False,
                            'slot_released': False,
                            'error': '无法释放并发槽位'
                        })
                except Exception as e:
                    unregistered_results.append({
                        'req_id': req_id,
                        'instance_id': instance_id,
                        'cancelled': False,
                        'registered': False,
                        'slot_released': False,
                        'error': str(e)
                    })
                    logger.error(f"取消未注册任务异常 - req_id: {req_id}, 错误: {e}")

        # ========== 步骤3：统计和响应 ==========
        total_tasks = len(registered_results) + len(unregistered_results)
        cancelled_count = (
                sum(1 for result in registered_results.values() if result) +
                sum(1 for result in unregistered_results if result.get('cancelled'))
        )

        if total_tasks == 0:
            return StandardResponse.success({
                'cancelled': False,
                'message': '没有运行中的任务可取消',
                'total_tasks': 0,
                'cancelled_tasks': 0,
                'registered_tasks': 0,
                'unregistered_tasks': 0
            })

        logger.info(
            f"批量取消任务完成 - 总数: {total_tasks}, 已取消: {cancelled_count}, "
            f"已注册: {len(registered_results)}, 未注册: {len(unregistered_results)}"
        )

        return StandardResponse.success({
            'cancelled': cancelled_count > 0,
            'message': f'已成功取消 {cancelled_count}/{total_tasks} 个任务',
            'total_tasks': total_tasks,
            'cancelled_tasks': cancelled_count,
            'registered_tasks': len(registered_results),
            'registered_results': registered_results,
            'unregistered_tasks': len(unregistered_results),
            'unregistered_results': unregistered_results,
            'note': '部分任务可能需要几秒钟才能真正停止'
        })

    except Exception as e:
        logger.error(f"取消所有任务异常: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"取消所有任务失败: {str(e)}")


@Report_router.get("/gp/status/{req_id}", dependencies=[Depends(verify_token)])
async def get_task_status_by_req_id(req_id: str):
    """
    改进版：根据req_id获取任务状态

    ⭐ 关键改进：
    1. 支持查询软删除的任务（宽限期内）
    2. 显示取消进度
    3. 显示完成进度
    """
    try:
        logger.debug(f"查询任务状态 - req_id: {req_id}")

        # 1. 从任务管理器获取状态
        task_status = await task_manager.get_task_status(req_id)

        if task_status is None:
            # 任务可能已清理或不存在，检查并发控制
            if concurrency_control:
                status = await concurrency_control.get_current_status(include_details=True)
                active_instances = status.get('active_instances_details', {})

                for instance_id, instance_data in active_instances.items():
                    meta = instance_data.get('meta', {})
                    if meta.get('owner_id') == req_id:
                        acquire_time = meta.get('acquire_time', 0)
                        running_time = int(time.time() - acquire_time) if acquire_time else 0

                        logger.warning(
                            f"⚠️  任务在并发控制中但不在任务管理器 - "
                            f"req_id: {req_id}, instance_id: {instance_id}"
                        )

                        return StandardResponse.success({
                            'req_id': req_id,
                            'instance_id': instance_id,
                            'status': 'processing',
                            'message': '任务正在处理中（正在启动中...）',
                            'running_time': running_time,
                            'registered': False,
                            'last_heartbeat': instance_data.get('last_heartbeat', 0),
                            'note': '任务已获得槽位，心跳即将启动'
                        })

            logger.warning(f"任务不存在 - req_id: {req_id}")
            return StandardResponse.error(404, f"任务不存在或已过期: {req_id}")

        return StandardResponse.success(task_status)

    except Exception as e:
        logger.error(f"查询任务状态异常 - req_id: {req_id}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"查询任务状态失败: {str(e)}")

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


async def _process_full_diagnosis_report(
        req_id: str,
        instance_id: str,
        request: DiagnosisRequest,
        return_data_format: dict
):
    """
    后台异步处理：完整流程，包括数据查询、报告生成、保存、回调

    关键特性：
    - 任务执行完全后才释放槽位（不是立即释放）
    - 执行过程中维持心跳，防止被误认为崩溃
    - 所有处理都在后台进行
    - 最后确保释放槽位
    - 支持任务取消检查

    ⭐ 改进：
    - 移除 finally 块中的槽位释放逻辑
    - 让 wrapper 函数统一管理槽位释放
    - 避免双重释放问题
    - 改进心跳逻辑，确保实例不被清理
    """
    heartbeat_task = None

    try:
        logger.info(f"开始处理诊断请求 - 请求ID: {req_id}, 实例ID: {instance_id}")

        # 启动心跳保活任务（⭐ 关键：必须立即启动）
        if concurrency_control:
            async def send_heartbeat():
                """
                每3秒发送一次心跳，保持槽位活跃

                ⭐ 改进：
                - 首次延迟缩短为 1 秒（确保及时更新）
                - 之后每 3 秒一次（不太频繁）
                - 添加详细的日志记录
                """
                first_beat = True
                try:
                    while True:
                        # ⭐ 首次心跳快速发送，之后才延迟
                        if first_beat:
                            await asyncio.sleep(1)  # 1秒后发送第一次心跳
                            first_beat = False
                        else:
                            await asyncio.sleep(3)  # 之后每3秒一次

                        success = await concurrency_control.heartbeat(instance_id)
                        if not success:
                            logger.warning(f"❌ 心跳发送失败 - instance_id: {instance_id}")
                        else:
                            logger.debug(f"💓 心跳已发送 - instance_id: {instance_id}")

                except asyncio.CancelledError:
                    logger.debug(f"心跳任务被取消 - instance_id: {instance_id}")
                except Exception as e:
                    logger.error(f"心跳发送异常 - instance_id: {instance_id}, 错误: {e}")

            heartbeat_task = asyncio.create_task(send_heartbeat())
            logger.info(f"💓 心跳保活已启动 - instance_id: {instance_id}, 首次延迟 1秒")

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
            data_result = await async_data_service.get_diagnosis_data(request, timeout=180.0)

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
        raise  # 重新抛出，让上层处理
    except Exception as e:
        logger.error(f"后台处理异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)

    finally:
        # ⭐ 关键：只取消心跳任务，不释放槽位
        # 槽位释放由 managed_task_wrapper 负责

        logger.debug(f"执行清理逻辑 - 请求ID: {req_id}, 实例ID: {instance_id}")

        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                logger.debug(f"💓 心跳保活已取消 - instance_id: {instance_id}")
            except Exception as e:
                logger.error(f"取消心跳任务异常 - instance_id: {instance_id}, 错误: {e}")


@Report_router.post("/diagnosis", dependencies=[Depends(verify_token)])
async def generate_diagnosis_report(
        request: DiagnosisRequest,
        background_tasks: BackgroundTasks
):
    """
    经营异常诊断结果计算 - 严格顺序执行版本

    核心特性：
    ✅ 获得槽位后，必须等任务完全执行完才释放
    ✅ 中间来的所有请求都被拒绝（返回 429）
    ✅ 最多同时处理 N 个请求（由 max_total_concurrent 控制）
    ✅ 按队列顺序处理，先到先得
    ✅ 心跳保活防止被误清理
    ✅ 任务正确注册到任务管理器，支持取消

    流程：
    1. 尝试获取槽位
       - 成功：继续处理
       - 失败：立即返回 429（系统繁忙）

    2. 立即返回响应给客户端（异步处理）

    3. 后台完整执行：
       查询数据 → 生成报告 → 保存结果 → 发送回调 → 释放槽位

    4. 客户端可通过回调或轮询查询结果

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

            # 获取当前并发状态用于日志
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
                    'retryAfter': 30,  # 建议 30 秒后重试
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
            "distribution_type": request.distribution_type,
            "IT_include_type": request.IT_include_type,
            "currentPage": request.currentPage,
            "pageSize": request.pageSize
        }

        # ========== 步骤 3：创建并管理后台任务 ==========

        # ⭐ 关键改进：使用 wrapper 函数确保任务正确注册
        async def managed_task_wrapper():
            """
            任务包装函数：
            1. 获取当前任务对象
            2. 向任务管理器注册
            3. 执行实际的诊断处理流程
            4. 确保在异常或取消时正确清理资源

            ⭐ 改进：
            - 更智能的槽位释放逻辑
            - 区分"实例不存在"和"释放失败"
            - 改进日志输出的准确性
            """
            current_task = asyncio.current_task()

            # 标志：槽位是否已释放（防止双重释放）
            slot_released = False
            # 标志：是否需要释放槽位
            should_release_slot = False

            try:
                # ⭐ 步骤1：向任务管理器注册任务
                registered = await task_manager.register_task(
                    req_id=request.reqId,
                    instance_id=instance_id,
                    task=current_task
                )

                if not registered:
                    logger.error(f"❌ 任务注册失败 - 请求ID: {request.reqId}")
                    if concurrency_control:
                        try:
                            released = await concurrency_control.release_slot(instance_id)
                            if released:
                                slot_released = True
                                logger.info(f"由于注册失败，槽位已释放 - instance_id: {instance_id}")
                            else:
                                logger.error(f"释放槽位失败 - instance_id: {instance_id}")
                        except Exception as e:
                            logger.error(f"释放槽位异常 - instance_id: {instance_id}, 错误: {e}")
                    return

                logger.info(f"✅ 任务已成功注册 - 请求ID: {request.reqId}, 实例ID: {instance_id}")

                # 标记需要释放槽位
                should_release_slot = True

                # ⭐ 步骤2：执行实际的诊断处理流程
                await _process_full_diagnosis_report(
                    req_id=request.reqId,
                    instance_id=instance_id,
                    request=request,
                    return_data_format=return_data_format
                )

                logger.info(f"✅ 诊断处理流程完成 - 请求ID: {request.reqId}")

            except asyncio.CancelledError:
                # 任务被取消
                logger.info(f"🛑 任务被取消 - 请求ID: {request.reqId}, 实例ID: {instance_id}")
                raise  # 重新抛出异常

            except Exception as e:
                # 任务执行异常
                logger.error(f"❌ 任务执行异常 - 请求ID: {request.reqId}, 错误: {str(e)}", exc_info=True)
                raise  # 重新抛出异常

            finally:
                # ⭐ 关键：统一在这里释放槽位
                if should_release_slot and not slot_released and concurrency_control:
                    try:
                        logger.debug(f"开始释放槽位 - 请求ID: {request.reqId}, 实例ID: {instance_id}")
                        released = await concurrency_control.release_slot(instance_id)

                        if released:
                            slot_released = True
                            logger.info(f"✅ 槽位已释放（任务完成后） - 请求ID: {request.reqId}, 实例ID: {instance_id}")
                        else:
                            # 实例可能已被清理或不存在，这不算失败
                            logger.debug(f"槽位释放返回 False - 实例可能已被清理 - instance_id: {instance_id}")

                    except Exception as e:
                        logger.error(f"释放槽位异常 - 请求ID: {request.reqId}, 错误: {e}")

        # ========== 步骤 4：将包装函数添加到后台任务队列 ==========

        background_tasks.add_task(managed_task_wrapper)

        logger.info(f"📋 后台任务已添加到队列 - 请求ID: {request.reqId}, 实例ID: {instance_id}")

        # ========== 步骤 5：立即响应客户端 ==========

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


@Report_router.get("/gp/status/{req_id}", dependencies=[Depends(verify_token)])
async def get_task_status_by_req_id(req_id: str):
    """
    根据req_id获取任务状态（替代原有的instance_id查询）

    功能：
    - 查询任务当前状态
    - 获取任务运行时间
    - 检查是否被请求取消

    Args:
        req_id: 请求ID（任务ID）

    Returns:
        StandardResponse: 任务状态信息
    """
    try:
        logger.info(f"查询任务状态 - req_id: {req_id}")

        # 1. 从任务管理器获取状态
        task_status = await task_manager.get_task_status(req_id)

        if task_status is None:
            # 任务不存在于任务管理器，检查是否在并发控制中
            if concurrency_control:
                status = await concurrency_control.get_current_status(include_details=True)
                active_instances = status.get('active_instances_details', {})

                # 查找是否有匹配的实例
                for instance_id, instance_data in active_instances.items():
                    meta = instance_data.get('meta', {})
                    if meta.get('owner_id') == req_id:
                        # 找到匹配的实例，但任务未注册
                        acquire_time = meta.get('acquire_time', 0)
                        running_time = int(time.time() - acquire_time) if acquire_time else 0

                        return StandardResponse.success({
                            'req_id': req_id,
                            'instance_id': instance_id,
                            'status': 'processing',
                            'message': '任务正在处理中（未注册到任务管理器）',
                            'running_time': running_time,
                            'registered': False,
                            'note': '任务可能已启动但未正确注册'
                        })

            return StandardResponse.error(404, f"任务不存在: {req_id}")

        # 2. 补充并发控制信息
        concurrent_status = None
        if concurrency_control and 'instance_id' in task_status:
            instance_id = task_status['instance_id']
            status = await concurrency_control.get_current_status(include_details=True)
            active_instances = status.get('active_instances_details', {})

            if instance_id in active_instances:
                concurrent_status = {
                    'slot_acquired': True,
                    'concurrent_count': active_instances[instance_id].get('concurrent_count', 0)
                }
            else:
                concurrent_status = {
                    'slot_acquired': False,
                    'note': '并发槽位可能已释放'
                }

        response_data = {
            'req_id': task_status['req_id'],
            'instance_id': task_status.get('instance_id'),
            'status': task_status['status'],
            'running_time': task_status['running_time'],
            'cancellation_requested': task_status['cancellation_requested'],
            'start_time': task_status['start_time'],
            'task_done': task_status['task_done'],
            'registered': True
        }

        if concurrent_status:
            response_data['concurrent_status'] = concurrent_status

        return StandardResponse.success(response_data)

    except Exception as e:
        logger.error(f"查询任务状态异常 - req_id: {req_id}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"查询任务状态失败: {str(e)}")


# 优先级高：/cancel 的具体路由
@Report_router.post("/gp/cancel-all", dependencies=[Depends(verify_token)])
async def cancel_all_diagnosis_tasks():
    """
    取消所有正在执行的诊断任务

    功能：
    - 取消所有运行中的任务（包括未注册到任务管理器的任务）
    - 释放所有并发槽位
    - 批量清理任务资源

    Returns:
        StandardResponse: 批量取消结果
    """
    try:
        logger.info("收到取消所有任务的请求")

        all_results = {}
        unregistered_tasks = []

        # 1. 从任务管理器取消所有已注册的任务
        registered_results = await task_manager.cancel_all_tasks()
        all_results.update(registered_results)

        # 2. 从并发控制中查找并取消未注册的任务
        if concurrency_control:
            status = await concurrency_control.get_current_status(include_details=True)
            active_instances = status.get('active_instances_details', {})

            # 获取已注册任务的instance_id列表
            registered_instance_ids = set()
            for req_id, cancelled in registered_results.items():
                if cancelled:
                    task_status = await task_manager.get_task_status(req_id)
                    if task_status and 'instance_id' in task_status:
                        registered_instance_ids.add(task_status['instance_id'])

            # 处理未注册的任务
            for instance_id, instance_data in active_instances.items():
                if instance_id in registered_instance_ids:
                    continue  # 已通过任务管理器处理

                meta = instance_data.get('meta', {})
                req_id = meta.get('owner_id')

                if req_id:
                    # 尝试通过释放并发槽位来取消未注册的任务
                    try:
                        released = await concurrency_control.release_slot(instance_id)
                        if released:
                            all_results[req_id] = True
                            unregistered_tasks.append({
                                'req_id': req_id,
                                'instance_id': instance_id,
                                'cancelled': True,
                                'registered': False,
                                'slot_released': True
                            })
                            logger.info(f"未注册任务已取消 - req_id: {req_id}, instance_id: {instance_id}")
                        else:
                            all_results[req_id] = False
                            unregistered_tasks.append({
                                'req_id': req_id,
                                'instance_id': instance_id,
                                'cancelled': False,
                                'registered': False,
                                'slot_released': False,
                                'error': '无法释放并发槽位'
                            })
                            logger.warning(f"未注册任务取消失败 - req_id: {req_id}, instance_id: {instance_id}")
                    except Exception as e:
                        all_results[req_id] = False
                        unregistered_tasks.append({
                            'req_id': req_id,
                            'instance_id': instance_id,
                            'cancelled': False,
                            'registered': False,
                            'slot_released': False,
                            'error': str(e)
                        })
                        logger.error(f"取消未注册任务异常 - req_id: {req_id}, 错误: {e}")
                else:
                    # 没有req_id的实例，直接释放槽位
                    try:
                        released = await concurrency_control.release_slot(instance_id)
                        if released:
                            logger.info(f"匿名实例槽位已释放 - instance_id: {instance_id}")
                        else:
                            logger.warning(f"释放匿名实例槽位失败 - instance_id: {instance_id}")
                    except Exception as e:
                        logger.error(f"释放匿名实例槽位异常 - instance_id: {instance_id}, 错误: {e}")

        # 3. 释放所有已注册任务的并发槽位
        release_results = {}
        for req_id, cancelled in registered_results.items():
            if cancelled:
                task_status = await task_manager.get_task_status(req_id)
                if task_status and 'instance_id' in task_status:
                    instance_id = task_status['instance_id']

                    if concurrency_control:
                        try:
                            released = await concurrency_control.release_slot(instance_id)
                            release_results[req_id] = {
                                'cancelled': True,
                                'slot_released': released,
                                'instance_id': instance_id,
                                'registered': True
                            }
                        except Exception as e:
                            release_results[req_id] = {
                                'cancelled': True,
                                'slot_released': False,
                                'error': str(e),
                                'instance_id': instance_id,
                                'registered': True
                            }

        # 合并结果
        final_results = {**release_results}
        for task in unregistered_tasks:
            if task['req_id']:
                final_results[task['req_id']] = {
                    'cancelled': task['cancelled'],
                    'slot_released': task['slot_released'],
                    'instance_id': task['instance_id'],
                    'registered': False,
                    'error': task.get('error')
                }

        cancelled_count = sum(1 for result in all_results.values() if result)
        total_tasks = len(all_results)

        if total_tasks == 0:
            return StandardResponse.success({
                'cancelled': False,
                'message': '没有运行中的任务可取消',
                'results': {},
                'note': '系统当前没有运行中的任务'
            })

        return StandardResponse.success({
            'cancelled': True if cancelled_count > 0 else False,
            'message': f'已成功取消 {cancelled_count}/{total_tasks} 个任务',
            'total_tasks': total_tasks,
            'cancelled_tasks': cancelled_count,
            'results': final_results,
        })

    except Exception as e:
        logger.error(f"取消所有任务异常: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"取消所有任务失败: {str(e)}")


@Report_router.post("/gp/cancel/{req_id}", dependencies=[Depends(verify_token)])
async def cancel_diagnosis_task(req_id: str):
    """
    取消正在执行的诊断任务

    功能：
    - 取消指定req_id的任务
    - 支持取消未注册到任务管理器的任务
    - 释放对应的并发槽位
    - 清理任务资源

    Args:
        req_id: 请求ID（任务ID）

    Returns:
        StandardResponse: 取消结果
    """
    try:
        logger.info(f"收到任务取消请求 - req_id: {req_id}")

        instance_id = None

        # 1. 首先尝试从任务管理器取消任务
        cancelled = await task_manager.cancel_task(req_id)

        if cancelled:
            # 任务在任务管理器中且取消成功
            logger.info(f"任务取消请求已处理（通过任务管理器） - req_id: {req_id}")

            # 获取任务信息以释放并发槽位
            task_status = await task_manager.get_task_status(req_id)
            if task_status and 'instance_id' in task_status:
                instance_id = task_status['instance_id']

        else:
            # 任务不在任务管理器或无法取消，尝试从并发控制中查找
            logger.info(f"任务不在任务管理器中，尝试从并发控制中查找 - req_id: {req_id}")

            if concurrency_control:
                # 从并发控制中查找匹配的实例
                status = await concurrency_control.get_current_status(include_details=True)
                active_instances = status.get('active_instances_details', {})

                # 查找是否有匹配的实例
                found_instance = None
                for inst_id, instance_data in active_instances.items():
                    meta = instance_data.get('meta', {})
                    if meta.get('owner_id') == req_id:
                        found_instance = inst_id
                        instance_id = inst_id
                        break

                if found_instance:
                    # 找到匹配的实例，通过释放并发槽位来间接取消任务
                    logger.info(f"在并发控制中找到匹配的实例 - req_id: {req_id}, instance_id: {instance_id}")

                    try:
                        released = await concurrency_control.release_slot(instance_id)
                        if released:
                            logger.info(f"并发槽位已释放（间接取消任务） - req_id: {req_id}, instance_id: {instance_id}")
                            cancelled = True
                        else:
                            logger.warning(f"释放并发槽位失败 - req_id: {req_id}, instance_id: {instance_id}")
                            return StandardResponse.error(500, f"取消任务失败: 无法释放并发槽位")
                    except Exception as e:
                        logger.error(f"释放并发槽位异常 - req_id: {req_id}, 错误: {e}")
                        return StandardResponse.error(500, f"取消任务失败: {str(e)}")
                else:
                    # 在并发控制中也找不到任务
                    logger.warning(f"任务不存在于任务管理器或并发控制中 - req_id: {req_id}")
                    return StandardResponse.error(404, f"任务不存在: {req_id}")
            else:
                # 并发控制未启用
                logger.warning(f"任务不存在且并发控制未启用 - req_id: {req_id}")
                return StandardResponse.error(404, f"任务不存在: {req_id}")

        # 2. 如果找到了instance_id，确保并发槽位已释放
        if instance_id and concurrency_control:
            try:
                # 再次确认槽位已释放（如果之前没有释放）
                released = await concurrency_control.release_slot(instance_id)
                if released:
                    logger.info(f"并发槽位已确认释放 - req_id: {req_id}, instance_id: {instance_id}")
            except Exception as e:
                logger.error(f"确认释放并发槽位异常 - req_id: {req_id}, 错误: {e}")
                # 不返回错误，因为任务可能已经成功取消

        return StandardResponse.success({
            'req_id': req_id,
            'cancelled': cancelled,
            'instance_id': instance_id,
            'message': f'任务 {req_id} 已取消' if cancelled else f'任务 {req_id} 不存在或无法取消',
            'registered': cancelled and instance_id is not None,
        })

    except Exception as e:
        logger.error(f"取消任务异常 - req_id: {req_id}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"取消任务失败: {str(e)}")

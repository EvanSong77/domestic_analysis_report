# -*- coding: utf-8 -*-
# @Time    : 2025/11/18 13:48
# @Author  : EvanSong

import asyncio
import uuid

from fastapi import APIRouter, Depends, BackgroundTasks

from src.config.config import get_settings
from src.models.diagnosis_request import DiagnosisRequest
from src.models.diagnosis_response import StandardResponse
from src.services import ResultService
from src.services.ai_model_service import AIModelService
from src.services.callback_service import CallbackService
from src.services.data_query_service import DataQueryService
from src.services.distributed_concurrency_control import DistributedConcurrencyControl
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

    Args:
        req_id: 请求ID
        instance_id: 实例ID
        request: 诊断请求对象
        return_data_format: 返回数据格式
    """
    heartbeat_task = None

    try:
        logger.info(f"开始处理诊断请求 - 请求ID: {req_id}, 实例ID: {instance_id}")

        # 启动心跳保活任务
        if concurrency_control:
            async def send_heartbeat():
                """每5秒发送一次心跳，保持槽位活跃"""
                while True:
                    try:
                        await asyncio.sleep(5)
                        success = await concurrency_control.heartbeat(instance_id)
                        if not success:
                            logger.warning(f"心跳发送失败 - instance_id: {instance_id}")
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.error(f"心跳发送异常 - instance_id: {instance_id}, 错误: {e}")

            heartbeat_task = asyncio.create_task(send_heartbeat())
            logger.info(f"心跳保活已启动 - instance_id: {instance_id}")

        # ========== 任务执行开始 ==========

        # 1. 查询数据
        try:
            logger.info(f"[1/4] 开始查询数据 - 请求ID: {req_id}")
            data_service = DataQueryService()
            data_result = data_service.get_diagnosis_data(request)

            if data_result['status'] != 'success':
                logger.error(f"数据查询失败 - 请求ID: {req_id}, 错误: {data_result['message']}")
                return

            logger.info(f"[1/4] 数据查询成功 - 请求ID: {req_id}")

        except Exception as e:
            logger.error(f"数据查询异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)
            return

        # 2. 生成报告
        try:
            logger.info(f"[2/4] 开始生成报告 - 请求ID: {req_id}")
            ai_service = AIModelService()
            report_result = await ai_service.generate_diagnosis_report(data_result['data'])

            if report_result['status'] != 'success':
                logger.error(f"报告生成失败 - 请求ID: {req_id}, 错误: {report_result['message']}")
                return

            logger.info(f"[2/4] 报告生成成功 - 请求ID: {req_id}")

        except Exception as e:
            logger.error(f"报告生成异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)
            return

        # 3. 保存结果
        try:
            logger.info(f"[3/4] 开始保存结果 - 请求ID: {req_id}")
            result_service = ResultService()

            if 'report_contents' in report_result['data']:
                # 多个报告的情况
                save_result = result_service.save_diagnosis_result(
                    req_id,
                    return_data_format,
                    {
                        'report_contents': report_result['data']['report_contents'],
                        'report_results': report_result['data']['report_results'],
                        'actual_params': report_result['data']['actual_params'],
                        'usage': report_result['data']['usage'],
                        'prompt_tokens': report_result['data']['prompt_tokens'],
                        'completion_tokens': report_result['data']['completion_tokens'],
                        'report_count': len(report_result['data']['report_contents'])
                    }
                )
            else:
                # 单个报告的情况
                save_result = result_service.save_diagnosis_result(
                    req_id,
                    return_data_format,
                    report_result['data']
                )

            if save_result['status'] != 'success':
                logger.error(f"结果保存失败 - 请求ID: {req_id}, 错误: {save_result['message']}")
                return

            logger.info(f"[3/4] 结果保存成功 - 请求ID: {req_id}")

        except Exception as e:
            logger.error(f"结果保存异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)
            return

        # 4. 发送回调
        try:
            logger.info(f"[4/4] 开始发送回调 - 请求ID: {req_id}")
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

    except Exception as e:
        logger.error(f"后台处理异常 - 请求ID: {req_id}, 错误: {str(e)}", exc_info=True)

    finally:
        # 取消心跳任务
        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                logger.debug(f"心跳保活已取消 - instance_id: {instance_id}")
            except Exception as e:
                logger.error(f"取消心跳任务异常 - instance_id: {instance_id}, 错误: {e}")

        # ⭐ 关键：在任务完全执行完毕后才释放槽位
        if concurrency_control:
            try:
                released = await concurrency_control.release_slot(instance_id)
                if released:
                    logger.info(f"✅ 槽位已释放（任务完成后） - 请求ID: {req_id}, 实例ID: {instance_id}")
                else:
                    logger.warning(f"❌ 释放槽位失败 - 请求ID: {req_id}, 实例ID: {instance_id}")
            except Exception as e:
                logger.error(f"释放槽位异常 - 请求ID: {req_id}, 错误: {e}")


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

        # ========== 步骤 2：立即返回响应 ==========

        return_data_format = {
            "period": request.period,
            "diagnosisType": request.diagnosisType,
            "provinceName": request.provinceName,
            "officeLv2Name": request.officeLv2Name,
            "currentPage": request.currentPage,
            "pageSize": request.pageSize
        }

        # 将完整处理流程添加到后台任务
        background_tasks.add_task(
            _process_full_diagnosis_report,
            req_id=request.reqId,
            instance_id=instance_id,
            request=request,
            return_data_format=return_data_format
        )

        logger.info(f"后台任务已启动 - 请求ID: {request.reqId}, 实例ID: {instance_id}")

        # ========== 步骤 3：立即响应客户端 ==========

        return StandardResponse.success({
            'reqId': request.reqId,
            'instanceId': instance_id,
            'message': '诊断请求已接受，正在处理中',
            'status': 'processing',
            'note': '系统采用严格顺序执行，请勿重复提交'
        })

    except Exception as e:
        logger.error(f"处理诊断报告请求异常 - 请求ID: {request.reqId}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"系统异常: {str(e)}")


@Report_router.get("/diagnosis/status/{instance_id}", dependencies=[Depends(verify_token)])
async def get_diagnosis_status(instance_id: str):
    """
    获取诊断任务状态

    用途：
    - 客户端查询任务处理进度
    - 了解当前执行状态

    返回：
    - processing: 任务正在处理中
    - completed_or_not_found: 任务已完成或未找到

    Args:
        instance_id: 实例ID

    Returns:
        dict: 任务状态信息
    """
    try:
        if not concurrency_control:
            return StandardResponse.error(503, "并发控制未启用")

        status = await concurrency_control.get_current_status(include_details=True)
        active_instances = status.get('active_instances_details', {})

        if instance_id in active_instances:
            instance_info = active_instances[instance_id]
            meta = instance_info.get('meta', {})

            # 计算已运行时间
            import time
            acquire_time = meta.get('acquire_time', 0)
            running_time = int(time.time() - acquire_time) if acquire_time else 0

            return StandardResponse.success({
                'instanceId': instance_id,
                'status': 'processing',
                'message': '任务正在处理中，请勿关闭',
                'runningTime': f"{running_time}s",
                'meta': {
                    'owner': meta.get('owner_id'),
                    'startTime': meta.get('acquire_time'),
                    'requestPath': meta.get('request_path')
                }
            })
        else:
            return StandardResponse.success({
                'instanceId': instance_id,
                'status': 'completed',
                'message': '任务已完成或未找到',
                'note': '请检查回调通知或数据库结果'
            })

    except Exception as e:
        logger.error(f"获取诊断状态异常 - instance_id: {instance_id}, 错误: {str(e)}")
        return StandardResponse.error(500, f"系统异常: {str(e)}")


@Report_router.get("/diagnosis/concurrency/status", dependencies=[Depends(verify_token)])
async def get_concurrency_status():
    """
    获取并发控制状态

    用途：
    - 查看有多少请求正在处理
    - 查看还有多少空余槽位
    - 监控系统压力

    Returns:
        dict: 并发系统状态和活跃任务列表
    """
    try:
        if not concurrency_control:
            return StandardResponse.error(503, "并发控制未启用")

        status = await concurrency_control.get_current_status(include_details=True)

        # 提取活跃任务的关键信息
        active_tasks = {}
        for instance_id, instance_data in status.get('active_instances_details', {}).items():
            meta = instance_data.get('meta', {})
            import time
            acquire_time = meta.get('acquire_time', 0)
            running_time = int(time.time() - acquire_time) if acquire_time else 0

            active_tasks[instance_id] = {
                'owner': meta.get('owner_id'),
                'runningTime': f"{running_time}s",
                'acquireTime': acquire_time
            }

        return StandardResponse.success({
            'concurrency': {
                'current': status.get('current_concurrent', 0),
                'max': status.get('max_concurrent', 0),
                'available': status.get('available_slots', 0),
                'utilization': status.get('utilization', '0%')
            },
            'activeTasks': active_tasks,
            'message': f"当前{status.get('current_concurrent', 0)}/{status.get('max_concurrent', 0)}槽位已占用"
        })

    except Exception as e:
        logger.error(f"获取并发状态异常: {str(e)}")
        return StandardResponse.error(500, f"系统异常: {str(e)}")

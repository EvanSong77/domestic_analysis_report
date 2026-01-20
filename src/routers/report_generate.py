# -*- coding: utf-8 -*-
# @Time    : 2025/11/18 13:48
# @Author  : EvanSong

from fastapi import APIRouter, Depends

from src.models.diagnosis_request import DiagnosisRequest
from src.models.diagnosis_response import StandardResponse
from src.services.celery_tasks import process_diagnosis_report, get_task_status_from_redis, cancel_task, cancel_all_tasks, can_submit_task
from src.utils import log_utils
from src.utils.bearer import verify_token

logger = log_utils.logger
Report_router = APIRouter()


@Report_router.post("/diagnosis", dependencies=[Depends(verify_token)])
async def generate_diagnosis_report(request: DiagnosisRequest):
    """
    经营异常诊断结果计算 - Celery 版本

    ⭐ 核心改进：
    1. 使用 Celery + Redis 实现分布式任务队列
    2. 支持任务取消和状态查询
    3. 支持并发控制（通过 Celery Worker 配置）
    4. 任务持久化，支持重启恢复
    5. 实时状态同步到 Redis，无卡顿
    6. 并发限制：达到最大并发数时拒绝新任务

    流程：
    1. 检查并发限制
    2. 提交 Celery 任务
    3. 立即返回任务ID
    4. 后台异步执行（数据查询、报告生成、保存、回调）
    5. 用户可以查询状态或取消任务

    Args:
        request: 诊断请求

    Returns:
        StandardResponse: 成功或拒绝的响应
    """
    try:
        logger.info(f"收到诊断报告请求 - 请求ID: {request.reqId}")

        submit_check = can_submit_task()
        if not submit_check['can_submit']:
            logger.warning(f"任务提交被拒绝 - 请求ID: {request.reqId}, 原因: {submit_check['message']}")
            return StandardResponse.error(
                429,
                submit_check['message'],
                {
                    'current_count': submit_check.get('current_count'),
                    'max_concurrent': submit_check.get('max_concurrent')
                }
            )

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

        request_data = request.model_dump()

        task = process_diagnosis_report.delay(
            req_id=request.reqId,
            request_data=request_data,
            return_data_format=return_data_format
        )

        from src.services.celery_tasks import update_task_status
        update_task_status(request.reqId, task.id, 'pending', stage='submitted', message='任务已提交，等待执行')

        logger.info(f"✅ Celery 任务已提交 - 请求ID: {request.reqId}, 任务ID: {task.id}")

        return StandardResponse.success({
            'reqId': request.reqId,
            'taskId': task.id,
            'message': '诊断请求已接受，正在处理中',
            'status': 'pending',
            'note': '使用 Celery + Redis 实现分布式任务队列，支持任务取消',
            'cancelEndpoint': f"/fin-report/diagnosis/cancel/{request.reqId}",
            'statusEndpoint': f"/fin-report/diagnosis/task/status/{request.reqId}",
            'cancelAllEndpoint': "/fin-report/diagnosis/cancel-all"
        })

    except Exception as e:
        logger.error(f"处理诊断报告请求异常 - 请求ID: {request.reqId}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"系统异常: {str(e)}")


@Report_router.post("/gp/cancel/{req_id}", dependencies=[Depends(verify_token)])
async def cancel_diagnosis_task(req_id: str):
    """
    取消正在执行的诊断任务

    ⭐ 关键改进：
    1. 从 Redis 获取任务信息
    2. 支持任务取消
    3. 清晰的状态反馈

    Args:
        req_id: 请求ID

    Returns:
        StandardResponse: 取消结果
    """
    try:
        logger.info(f"收到任务取消请求 - req_id: {req_id}")

        from src.services.celery_tasks import get_task_status_from_redis

        status = get_task_status_from_redis(req_id)

        if not status:
            logger.warning(f"未找到对应的任务 - req_id: {req_id}")
            return StandardResponse.error(404, f"任务不存在或已完成: {req_id}")

        task_id = status.get('task_id')
        current_status = status.get('status')

        if current_status in ['completed', 'failed', 'cancelled']:
            logger.warning(f"任务已结束，无法取消 - req_id: {req_id}, 状态: {current_status}")
            return StandardResponse.error(400, f"任务已{current_status}，无法取消: {req_id}")

        result = cancel_task(task_id, req_id)

        if result['status'] == 'success':
            return StandardResponse.success({
                'req_id': req_id,
                'task_id': task_id,
                'cancelled': True,
                'message': f'任务 {req_id} 取消请求已发送',
                'note': '任务可能需要几秒钟才能真正停止',
                'status_endpoint': f"/fin-report/diagnosis/task/status/{req_id}"
            })
        else:
            return StandardResponse.error(400, result['message'])

    except Exception as e:
        logger.error(f"取消任务异常 - req_id: {req_id}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"取消任务失败: {str(e)}")


@Report_router.post("/gp/cancel-all", dependencies=[Depends(verify_token)])
async def cancel_all_diagnosis_tasks():
    """
    取消所有正在执行的诊断任务

    ⭐ 关键改进：
    1. 取消所有正在运行的诊断任务
    2. 详细的取消结果统计

    Returns:
        StandardResponse: 取消结果
    """
    try:
        logger.info("收到取消所有任务的请求")

        result = cancel_all_tasks()

        if result['status'] == 'success':
            return StandardResponse.success({
                'cancelled': result['cancelled_count'] > 0,
                'message': result['message'],
                'total_cancelled': result['cancelled_count'],
                'cancelled_tasks': result['cancelled_tasks'],
                'note': '部分任务可能需要几秒钟才能真正停止'
            })
        else:
            return StandardResponse.error(500, result['message'])

    except Exception as e:
        logger.error(f"取消所有任务异常: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"取消所有任务失败: {str(e)}")


@Report_router.get("/gp/status/{req_id}", dependencies=[Depends(verify_token)])
async def get_task_status_by_req_id(req_id: str):
    """
    根据 req_id 获取任务状态

    ⭐ 关键改进：
    1. 从 Redis 直接读取任务状态（实时，无卡顿）
    2. 显示任务状态和进度
    3. 显示任务结果（如果已完成）

    Args:
        req_id: 请求ID

    Returns:
        StandardResponse: 任务状态
    """
    try:
        logger.debug(f"查询任务状态 - req_id: {req_id}")

        status = get_task_status_from_redis(req_id)

        if not status:
            logger.warning(f"未找到对应的任务 - req_id: {req_id}")
            return StandardResponse.error(404, f"任务不存在或已过期: {req_id}")

        response = {
            'req_id': req_id,
            'task_id': status.get('task_id'),
            'status': status.get('status'),
            'stage': status.get('stage'),
            'message': status.get('message'),
            'updated_at': status.get('updated_at')
        }

        if 'record_count' in status:
            response['record_count'] = status['record_count']

        if 'error_type' in status:
            response['error_type'] = status['error_type']

        if 'error_message' in status:
            response['error'] = status['error_message']

        return StandardResponse.success(response)

    except Exception as e:
        logger.error(f"查询任务状态异常 - req_id: {req_id}, 错误: {str(e)}", exc_info=True)
        return StandardResponse.error(500, f"查询任务状态失败: {str(e)}")

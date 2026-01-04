# -*- coding: utf-8 -*-
# @Time    : 2025/10/20
# @Author  : EvanSong

import json
from typing import Dict

import httpx

from src.config.config import get_settings
from src.utils import log_utils

logger = log_utils.logger


class CallbackService:
    """回调服务 - 将结果回调给外部系统"""

    def __init__(self):
        self.settings = get_settings()
        self.callback_config = self.settings.get_callback_config()
        self.callback_url = self.callback_config.get_url(self.settings.environment)
        self.auth_token = self.callback_config.get_bearer_token(self.settings.environment)
        self.timeout = self.callback_config.timeout

    async def send_callback(self, req_id: str, request_data: Dict, report_result: Dict) -> Dict:
        """发送回调请求
        
        Args:
            req_id: 请求ID
            request_data: 原始请求数据
            report_result: 报告生成结果
            
        Returns:
            回调结果
        """
        # 检查回调配置是否有效
        if not self.callback_url:
            logger.warning(f"回调URL未配置，跳过回调 - 请求ID: {req_id}")
            return {
                'status': 'skip',
                'message': '回调URL未配置，跳过回调'
            }

        try:
            # 构建回调数据 - 支持单个和多个报告结果
            if 'report_results' in report_result:
                # 多个报告的情况 - 使用实际的参数值
                resp_results = []
                for i, report_result_item in enumerate(report_result['report_results']):
                    # 获取实际的参数值
                    actual_params = report_result.get('actual_params', [{}] * len(report_result['report_results']))
                    params = actual_params[i] if i < len(actual_params) else {}

                    # 为每个报告构建结果对象
                    result_item = {
                        "period": params.get('period', request_data.get("period", "")),
                        "diagnosisType": params.get('diagnosisType', request_data.get("diagnosisType", "")),
                        "provinceName": params.get('provinceName', request_data.get("provinceName", "")),
                        "officeLv2Name": params.get('officeLv2Name', request_data.get("officeLv2Name", "")),
                        "distributionType": params.get('distributionType', request_data.get("distributionType", "")),
                        "itIncludeType": params.get('itIncludeType', request_data.get("itIncludeType", "")),
                        "diagnosisResult": report_result_item.get('report_content', '')
                    }

                    resp_results.append(result_item)

                callback_data = {
                    "reqId": req_id,
                    "respResult": json.dumps(resp_results, ensure_ascii=False)
                }
            else:
                # 单个报告的情况
                callback_data = {
                    "reqId": req_id,
                    "respResult": json.dumps([{
                        "period": request_data.get("period", ""),
                        "diagnosisType": request_data.get("diagnosisType", ""),
                        "provinceName": request_data.get("provinceName", ""),
                        "officeLv2Name": request_data.get("officeLv2Name", ""),
                        "distributionType": request_data.get("distributionType", ""),
                        "itIncludeType": request_data.get("itIncludeType", ""),
                        "diagnosisResult": report_result.get("report_content", "")
                    }], ensure_ascii=False)
                }

            if self.auth_token:
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.auth_token}"
                }
            else:
                headers = {
                    "Content-Type": "application/json"
                }

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.callback_url,
                    headers=headers,
                    json=callback_data
                )

                if response.status_code == 200:
                    logger.info(f"回调成功 - 请求ID: {req_id}")
                    return {
                        'status': 'success',
                        'message': '回调成功',
                        'response_data': response.json()
                    }
                else:
                    logger.error(f"回调失败 - 请求ID: {req_id}, 状态码: {response.status_code}, 响应: {response.text}")
                    return {
                        'status': 'error',
                        'message': f"回调失败: {response.status_code} - {response.text}"
                    }

        except httpx.TimeoutException:
            logger.error(f"回调超时 - 请求ID: {req_id}")
            return {
                'status': 'error',
                'message': '回调请求超时'
            }
        except Exception as e:
            logger.error(f"回调异常 - 请求ID: {req_id}, 错误: {str(e)}")
            return {
                'status': 'error',
                'message': f"回调异常: {str(e)}"
            }

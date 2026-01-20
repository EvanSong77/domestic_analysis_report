# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 15:21
# @Author  : EvanSong
from typing import Dict, Optional


class StandardResponse:
    """改进版标准响应格式"""

    @staticmethod
    def success(data: Dict = None, msg: str = "操作成功") -> Dict:
        """
        成功响应

        Args:
            data: 响应数据
            msg: 响应消息

        Returns:
            标准响应字典
        """
        return {
            "code": 200,
            "msg": msg,
            "data": data or {}
        }

    @staticmethod
    def error(code: int = 500, msg: str = "操作失败", data: Optional[Dict] = None) -> Dict:
        """
        错误响应

        Args:
            code: 错误码
            msg: 错误消息
            data: 错误详细信息（可选）

        Returns:
            标准响应字典
        """
        return {
            "code": code,
            "msg": msg,
            "data": data or {}
        }

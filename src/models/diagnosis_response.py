# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 15:21
# @Author  : EvanSong
from typing import Dict


class StandardResponse:
    """标准响应格式"""

    @staticmethod
    def success(data: Dict = None, msg: str = "操作成功") -> Dict:
        """成功响应"""
        return {
            "code": 200,
            "msg": msg,
            "data": data or {}
        }

    @staticmethod
    def error(code: int = 500, msg: str = "操作失败") -> Dict:
        """错误响应"""
        return {
            "code": code,
            "msg": msg,
            "data": {}
        }

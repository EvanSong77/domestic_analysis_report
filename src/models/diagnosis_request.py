# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 14:30
# @Author  : EvanSong
from typing import Optional

from pydantic import BaseModel, Field


class DiagnosisRequest(BaseModel):
    """诊断分析请求模型"""

    # 可选参数带默认值
    reqId: str = Field("", description="请求唯一标识(时间戳)")
    period: str = Field("", description="期间")
    diagnosisType: str = Field("", description="分析类型")
    provinceName: str = Field("", description="省份")
    officeLv2Name: str = Field("", description="二级办")
    distributionType: str = Field("", description="分销类型")
    itIncludeType: str = Field("", description="IT包含类型")

    currentPage: Optional[int] = Field(None, description="当前页码")
    pageSize: Optional[int] = Field(None, description="每页条数")

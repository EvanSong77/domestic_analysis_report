# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 9:42
# @Author  : EvanSong
from fastapi import APIRouter

from src.routers.report_generate import Report_router
from src.routers.root import root_router

v1_router = APIRouter(prefix="/fin-report", tags=["generate"])
v1_router.include_router(Report_router, tags=["生成报告"])

__all__ = ["v1_router", "root_router"]

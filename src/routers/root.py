# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 13:37
# @Author  : EvanSong
from fastapi import APIRouter

root_router = APIRouter(tags=["root"])


@root_router.get("/", tags=["root"])
async def root():
    return {"message": "Welcome to Domestic Analysis Report Server!"}

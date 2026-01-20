# -*- coding: utf-8 -*-
# @Time    : 2024/10/30 17:13
# @Author  : EvanSong
from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer

from src.config.config import get_settings

settings = get_settings()

# 用于从请求中获取 Bearer Token
oauth2_scheme: OAuth2PasswordBearer = OAuth2PasswordBearer(tokenUrl="token")


async def verify_token(token: str = Depends(oauth2_scheme)):
    """使用配置中的 token 进行校验"""
    if token == settings.security.bearer_token:
        return True
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

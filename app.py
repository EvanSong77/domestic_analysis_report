# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 9:42
# @Author  : EvanSong

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config.config import get_settings
from src.routers import v1_router, root_router
from src.utils import log_utils
from src.utils.time_middleware import ProcessTimeMiddleware

logger = log_utils.logger
settings = get_settings()


async def lifespan(app: FastAPI):
    logger.info(f"当前环境使用的是:{settings.environment.upper()}")
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(ProcessTimeMiddleware)

app.include_router(v1_router)
app.include_router(root_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["GET", "HEAD", "POST", "OPTIONS", "PUT", "PATCH", "DELETE"],
    allow_headers=["*"],
)

if __name__ == '__main__':
    logger.info("*****AI Search server start*****")
    uvicorn.run(
        "app:app",
        host=settings.app.host,
        port=settings.app.port,
        # log_level=settings.app.log_level.lower()
    )

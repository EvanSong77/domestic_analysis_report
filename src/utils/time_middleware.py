# -*- coding: utf-8 -*-
# @Time    : 2025/5/13 19:39
# @Author  : EvanSong
import time

from starlette.middleware.base import BaseHTTPMiddleware

from src.utils import log_utils

logger = log_utils.logger


class ProcessTimeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = f"{process_time:.4f} seconds"
        logger.info(
            f"[{request.method}] {request.url.path} - Status: {response.status_code} - Time: {process_time:.4f}s"
        )
        return response

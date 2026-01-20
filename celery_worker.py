# -*- coding: utf-8 -*-
# @Time    : 2025/01/14
# @Author  : EvanSong

"""
Celery Worker 启动脚本

用于启动 Celery Worker 进程，处理异步任务
"""

from src.config.celery_config import celery_app
from src.utils import log_utils

logger = log_utils.logger

if __name__ == '__main__':
    logger.info("启动 Celery Worker...")
    celery_app.start()

# -*- coding: utf-8 -*-
# @Time    : 2024/10/30 17:17
# @Author  : EvanSong
import logging
import os
from logging.handlers import RotatingFileHandler

import coloredlogs

from src.config.config import get_settings

# 获取配置
settings = get_settings()

# 创建日志目录
log_dir = os.path.dirname(settings.app.log_file) if settings.app.log_file else './logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 创建 logger 对象
logger = logging.getLogger("app")
logger.setLevel(getattr(logging, settings.app.log_level.upper()))
logger.propagate = False  # 关闭日志传播

# 避免重复处理器
if not logger.handlers:
    # 创建文件处理器
    log_file = settings.app.log_file or 'logs/app.log'
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, settings.app.log_level.upper()))
    file_formatter = logging.Formatter(
        '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)

    # 创建彩色控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, settings.app.log_level.upper()))
    console_formatter = coloredlogs.ColoredFormatter(
        '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s',
        level_styles={
            'debug': {'color': 'cyan'},
            'info': {'color': 'green'},
            'warning': {'color': 'yellow'},
            'error': {'color': 'red', 'bold': True},
            'critical': {'color': 'magenta', 'bold': True}
        },
        field_styles={
            'asctime': {'color': 'blue'},
            'levelname': {'color': 'black', 'bold': True},
            'filename': {'color': 'blue'},
            'lineno': {'color': 'blue'}
        }
    )
    console_handler.setFormatter(console_formatter)

    # 仅将处理器添加到 app logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

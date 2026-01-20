# -*- coding: utf-8 -*-
# @Time    : 2025/7/10 10:04
# @Author  : EvanSong
import time

from openai import AsyncOpenAI

from src.utils import log_utils
from src.config.config import get_settings

logger = log_utils.logger
settings = get_settings()


async def request_model_async(messages, temperature=0.4, top_p=0.95, enable_thinking=False, model_name="qwen3-32b"):
    """使用 AsyncOpenAI 发送异步请求"""
    s_t = time.time()
    
    # 获取模型配置
    model_config = settings.get_model_config(model_name)
    
    client = AsyncOpenAI(
        base_url=model_config.base_url,
        api_key=model_config.api_key
    )
    
    if '235b' in model_config.model_name:
        response = await client.chat.completions.create(
            model=model_config.model_name,
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            seed=42,
        )
    else:
        response = await client.chat.completions.create(
            model=model_config.model_name,
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            seed=42,
            extra_body={"chat_template_kwargs": {"enable_thinking": enable_thinking}}
        )
    logger.info(f"{model_config.model_name}响应耗时: {time.time() - s_t:.2f}s")
    return response.choices[0].message.content


async def request_235b_model_async(messages, temperature=0.6, top_p=0.95):
    """使用 AsyncOpenAI 发送异步请求（235b） - 向后兼容函数"""
    return await request_model_async(messages, temperature, top_p, model_name="qwen3-235b")

# -*- coding: utf-8 -*-
# @Time    : 2025/12/23
# @Author  : EvanSong

import asyncio
import json
import os
from typing import Any, Callable, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from src.utils import log_utils

logger = log_utils.logger


class AsyncExecutor:
    """异步执行器 - 将阻塞操作转换为异步操作"""
    
    _thread_pool = None
    _process_pool = None
    
    @classmethod
    def get_thread_pool(cls, max_workers: int = 10):
        """获取线程池（单例）"""
        if cls._thread_pool is None:
            cls._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
            logger.info(f"线程池已初始化，最大工作线程数: {max_workers}")
        return cls._thread_pool
    
    @classmethod
    def get_process_pool(cls, max_workers: int = 4):
        """获取进程池（单例）"""
        if cls._process_pool is None:
            cls._process_pool = ProcessPoolExecutor(max_workers=max_workers)
            logger.info(f"进程池已初始化，最大工作进程数: {max_workers}")
        return cls._process_pool
    
    @classmethod
    async def run_in_thread(cls, func: Callable, *args, timeout: Optional[float] = None, **kwargs) -> Any:
        """在线程池中运行阻塞函数
        
        Args:
            func: 要执行的阻塞函数
            *args: 函数参数
            timeout: 超时时间（秒）
            **kwargs: 函数关键字参数
            
        Returns:
            函数执行结果
        """
        loop = asyncio.get_event_loop()
        pool = cls.get_thread_pool()
        
        try:
            if timeout:
                # 带超时的执行
                return await asyncio.wait_for(
                    loop.run_in_executor(pool, lambda: func(*args, **kwargs)),
                    timeout=timeout
                )
            else:
                # 不带超时的执行
                return await loop.run_in_executor(pool, lambda: func(*args, **kwargs))
                
        except asyncio.TimeoutError:
            logger.warning(f"线程池执行超时 - 函数: {func.__name__}, 超时时间: {timeout}秒")
            raise Exception(f"线程池执行超时 - 函数: {func.__name__}, 超时时间: {timeout}秒")
        except Exception as e:
            logger.error(f"线程池执行异常 - 函数: {func.__name__}, 错误: {e}")
            raise Exception(f"线程池执行异常 - 函数: {func.__name__}, 错误: {e}")
    
    @classmethod
    async def run_in_process(cls, func: Callable, *args, timeout: Optional[float] = None, **kwargs) -> Any:
        """在进程池中运行CPU密集型函数
        
        Args:
            func: 要执行的CPU密集型函数
            *args: 函数参数
            timeout: 超时时间（秒）
            **kwargs: 函数关键字参数
            
        Returns:
            函数执行结果
        """
        loop = asyncio.get_event_loop()
        pool = cls.get_process_pool()
        
        try:
            if timeout:
                # 带超时的执行
                return await asyncio.wait_for(
                    loop.run_in_executor(pool, lambda: func(*args, **kwargs)),
                    timeout=timeout
                )
            else:
                # 不带超时的执行
                return await loop.run_in_executor(pool, lambda: func(*args, **kwargs))
                
        except asyncio.TimeoutError:
            logger.warning(f"进程池执行超时 - 函数: {func.__name__}, 超时时间: {timeout}秒")
            raise Exception(f"进程池执行超时 - 函数: {func.__name__}, 超时时间: {timeout}秒")
        except Exception as e:
            logger.error(f"进程池执行异常 - 函数: {func.__name__}, 错误: {e}")
            raise Exception(f"进程池执行异常 - 函数: {func.__name__}, 错误: {e}")
    
    @classmethod
    async def read_file_async(cls, file_path: str, encoding: str = 'utf-8', timeout: float = 10.0) -> str:
        """异步读取文件内容
        
        Args:
            file_path: 文件路径
            encoding: 文件编码
            timeout: 读取超时时间
            
        Returns:
            文件内容
        """
        def read_file_sync():
            with open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        
        return await cls.run_in_thread(read_file_sync, timeout=timeout)
    
    @classmethod
    async def write_file_async(cls, file_path: str, content: str, encoding: str = 'utf-8', timeout: float = 10.0) -> bool:
        """异步写入文件内容
        
        Args:
            file_path: 文件路径
            content: 要写入的内容
            encoding: 文件编码
            timeout: 写入超时时间
            
        Returns:
            是否写入成功
        """
        def write_file_sync():
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            return True
        
        try:
            await cls.run_in_thread(write_file_sync, timeout=timeout)
            return True
        except Exception as e:
            logger.error(f"异步写入文件失败 - 路径: {file_path}, 错误: {e}")
            return False
    
    @classmethod
    async def json_loads_async(cls, json_str: str, timeout: float = 5.0) -> Any:
        """异步解析JSON字符串
        
        Args:
            json_str: JSON字符串
            timeout: 解析超时时间
            
        Returns:
            解析后的Python对象
        """
        return await cls.run_in_thread(json.loads, json_str, timeout=timeout)
    
    @classmethod
    async def json_dumps_async(cls, obj: Any, ensure_ascii: bool = False, indent: Optional[int] = None,
                              timeout: float = 5.0) -> str:
        """异步序列化为JSON字符串
        
        Args:
            obj: 要序列化的Python对象
            ensure_ascii: 是否确保ASCII编码
            indent: 缩进空格数
            timeout: 序列化超时时间
            
        Returns:
            JSON字符串
        """
        return await cls.run_in_thread(json.dumps, obj, ensure_ascii=ensure_ascii, indent=indent, timeout=timeout)
    
    @classmethod
    async def process_list_async(cls, items: List, process_func: Callable, batch_size: int = 100,
                                timeout_per_item: float = 1.0) -> List:
        """异步批量处理列表
        
        Args:
            items: 要处理的列表
            process_func: 处理函数
            batch_size: 批处理大小
            timeout_per_item: 每个项目的超时时间
            
        Returns:
            处理后的列表
        """
        results = []
        
        # 分批处理
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_tasks = []
            
            # 为每个项目创建异步任务
            for item in batch:
                task = cls.run_in_thread(process_func, item, timeout=timeout_per_item)
                batch_tasks.append(task)
            
            # 并发执行批处理任务
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 处理结果
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.warning(f"批处理项目失败: {result}")
                else:
                    results.append(result)
        
        return results
    
    @classmethod
    def shutdown(cls):
        """关闭线程池和进程池"""
        if cls._thread_pool:
            cls._thread_pool.shutdown(wait=True)
            cls._thread_pool = None
            logger.info("线程池已关闭")
        
        if cls._process_pool:
            cls._process_pool.shutdown(wait=True)
            cls._process_pool = None
            logger.info("进程池已关闭")


# 全局异步执行器实例
async_executor = AsyncExecutor()


async def async_file_exists(file_path: str, timeout: float = 5.0) -> bool:
    """异步检查文件是否存在"""
    def check_file_sync():
        return os.path.exists(file_path)
    
    try:
        return await async_executor.run_in_thread(check_file_sync, timeout=timeout)
    except Exception:
        return False


async def async_list_files(directory: str, pattern: str = "*", timeout: float = 10.0) -> List[str]:
    """异步列出目录中的文件"""
    import glob
    
    def list_files_sync():
        return glob.glob(os.path.join(directory, pattern))
    
    try:
        return await async_executor.run_in_thread(list_files_sync, timeout=timeout)
    except Exception as e:
        logger.error(f"异步列出文件失败 - 目录: {directory}, 错误: {e}")
        return []
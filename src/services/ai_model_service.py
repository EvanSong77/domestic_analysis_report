# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 14:35 (modified)
# @Author  : EvanSong (modified by assistant)

import asyncio
import json
import os
import time
from asyncio import Semaphore
from typing import Dict, List, Optional

import httpx

from src.config.config import get_settings
from src.config.constants import MODULE_DIMENSIONS, CONTENT_TYPE_CONFIG, USER_PROMPT_DICTS, SPECIAL_PROVINCES_SQL
from src.utils import log_utils
from src.utils.db_utils import MySQLDataConnector
from src.utils.async_processor import DataTemplateProcessor
from src.utils.tag_repair import XMLTagValidator

logger = log_utils.logger


class ProgressCounter:
    """进度计数器 - 用于在异步任务间共享进度状态"""

    def __init__(self):
        self.value = 0


class AIModelService:
    """
    AI模型服务 - 调用大模型生成诊断分析报告（改进版）

    关键变化：
      - 使用 origin_index 作为分组 key（防止相同 params 合并）
      - 为无数据的板块创建占位任务（默认 preserve_no_data=True）
      - 对 no_data 任务短路，不调用模型并返回 status='no_data'
      - 可配置 progress_interval（默认 2 秒）
    """

    def __init__(self,
                 max_concurrent: int = None,
                 preserve_no_data: bool = True,
                 progress_interval: int = 2):
        self.settings = get_settings()
        self.model_config = self.settings.get_model_config()
        logger.info(f"使用模型：{self.model_config.model_name}")

        self.client: Optional[httpx.AsyncClient] = None
        self.prompts_dir = "data/prompts"
        # 优先使用传入的参数，否则使用配置文件中的值
        self.max_concurrent = max_concurrent if max_concurrent is not None else getattr(self.settings.app, "max_concurrent", 8)
        self.db_connector = MySQLDataConnector(db_type="data_db")
        self.preserve_no_data = preserve_no_data
        self.progress_interval = max(1, int(progress_interval))

        # 连接数据库
        try:
            connection_result = self.db_connector.connect_database()
            if connection_result.get('status') != 'success':
                logger.error(f"数据库连接失败: {connection_result.get('message')}")
        except Exception as e:
            logger.exception(f"数据库连接异常: {e}")

        self.db_connector.execute_query(SPECIAL_PROVINCES_SQL)
        self.special_provinces = [d['PROVINCE_NAME'] for d in self.db_connector.query_results]

        self.validator = XMLTagValidator(self.settings.environment)

    def _load_prompts(self) -> Dict[str, object]:
        """加载 system 和 user 提示词，以及模板 JSON"""
        prompts: Dict[str, object] = {}
        # system
        system_path = os.path.join(self.prompts_dir, "system.txt")
        if os.path.exists(system_path):
            with open(system_path, 'r', encoding='utf-8') as f:
                prompts['system'] = f.read().strip()

        # user template
        user_path = os.path.join(self.prompts_dir, "user.txt")
        if os.path.exists(user_path):
            with open(user_path, 'r', encoding='utf-8') as f:
                prompts['user_template'] = f.read().strip()

        # template json (model templates)
        template_path = getattr(self.settings.app, "template_path", None)
        if template_path and os.path.exists(template_path):
            try:
                with open(template_path, 'r', encoding='utf-8') as f:
                    prompts['template_data'] = json.load(f)
            except Exception as e:
                logger.exception(f"加载 template_data 失败: {e}")

        return prompts

    @staticmethod
    def _get_data_template_config() -> Dict:
        data_template_path = "data/data_template.json"
        if os.path.exists(data_template_path):
            try:
                with open(data_template_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.exception(f"读取 data_template.json 失败: {e}")
        return {}

    def __del__(self):
        try:
            if hasattr(self, 'db_connector') and self.db_connector:
                self.db_connector.close_connection()
        except Exception:
            pass

    def _get_template_by_dimension(self, template_data: Dict, params: Dict, content_type: str) -> str:
        if not params.get('provinceName') and not params.get('officeLv2Name'):
            level = "TOTAL"
        elif params.get('provinceName') and not params.get('officeLv2Name'):
            level = "PROVINCE"
        else:
            level = "OFFICE"

        diagnosis_type = params.get('diagnosisType') or "ORG"
        # 处理特殊省份(如：北京、上海、天津)
        if (diagnosis_type == 'ORG' or diagnosis_type == 'CHAN') and level == "PROVINCE" and params.get('provinceName') in self.special_provinces:
            logger.info(f"特殊省份：{params.get('provinceName')}")
            return template_data.get("OFFICE", {}).get(diagnosis_type, {}).get(content_type, "")
        else:
            return template_data.get(level, {}).get(diagnosis_type, {}).get(content_type, "")

    @staticmethod
    def _fill_user_prompt(user_template: str, data_content: str, task_info: dict) -> str:
        # 使用最简单的 {} 占位替换（与原逻辑兼容）
        return user_template.format(data=data_content, reminder=USER_PROMPT_DICTS.get(task_info['original_params']['diagnosisType']).get(task_info['content_type']))

    async def generate_diagnosis_report(self, query_results: List[Dict]) -> Dict:
        """
        主入口：根据 query_results 生成报告

        ⭐ 改进：processor.process() 在线程池中异步执行
        """
        try:
            # 1) 扩展任务（为每条输入创建对应维度的内容类型任务，占位可配置）
            expanded_tasks = []

            for origin_index, query_result in enumerate(query_results):
                data_template = self._get_data_template_config()
                processor = DataTemplateProcessor(data_template, self.special_provinces, self.db_connector)

                # ⭐ 改进：在线程池中执行 process()，不阻塞事件循环
                from src.utils.async_utils import async_executor

                processed_results = await async_executor.run_in_thread(
                    lambda qr=query_result, p=processor: p.process(qr),
                    timeout=None
                )

                # 获取当前诊断类型（维度）
                diagnosis_type = query_result.get('diagnosisType', '') or "ORG"

                # 获取当前维度对应的内容类型
                dimension_content_types = list(CONTENT_TYPE_CONFIG.get(diagnosis_type, {}).keys())

                for content_type in dimension_content_types:
                    content_list = processed_results.get(content_type)
                    if content_list and len(content_list) > 0:
                        expanded_tasks.append({
                            'origin_index': origin_index,
                            'original_params': query_result,
                            'content_type': content_type,
                            'data': content_list,
                            'no_data': False
                        })
                    else:
                        # 是否保留占位由 preserve_no_data 控制
                        if self.preserve_no_data:
                            expanded_tasks.append({
                                'origin_index': origin_index,
                                'original_params': query_result,
                                'content_type': content_type,
                                'data': [],
                                'no_data': True
                            })
                        else:
                            logger.debug(f"跳过无数据板块: origin_index={origin_index}, content_type={content_type}")

            if not expanded_tasks:
                return {'status': 'error', 'message': '没有找到有效的报告生成任务'}

            # 2) 并发生成
            return await self._generate_concurrent_reports_by_content_type(expanded_tasks, len(query_results))

        except Exception as e:
            logger.exception("generate_diagnosis_report 失败")
            return {'status': 'error', 'message': f"报告生成失败: {str(e)}"}

    async def _generate_concurrent_reports_by_content_type(self, expanded_tasks: List[Dict], input_count: int) -> Dict:
        """
        并发生成：expanded_tasks 中每项为单个板块任务（包含 origin_index）
        返回时按 origin_index 聚合回每条输入对应的完整报告（CURRENT + CUMULATIVE）
        """
        # 加载 prompts
        prompts = self._load_prompts()
        if 'user_template' not in prompts:
            return {'status': 'error', 'message': 'user提示词模板不存在'}

        semaphore = Semaphore(self.max_concurrent)

        # 创建进度计数器（替代原来的 completed_ref）
        progress_counter = ProgressCounter()

        # 创建任务 coroutine（注意：不立即 await）
        coros = []
        for i, task_info in enumerate(expanded_tasks):
            coros.append(
                self._process_single_content_type_with_semaphore(
                    semaphore, i, task_info, prompts, len(expanded_tasks), progress_counter
                )
            )

        logger.debug(f"开始并发生成 {len(coros)} 个板块（来自 {input_count} 条输入），最大并发数: {self.max_concurrent}")
        total_start_time = time.time()

        total = len(coros)

        async def track_progress(progress_counter, total_count, start_time_ref):
            """
            进度跟踪 - 原地更新进度条（不重复打印新行）
            """
            import sys

            while progress_counter.value < total_count:
                await asyncio.sleep(self.progress_interval)
                current_completed = progress_counter.value
                elapsed_time = time.time() - start_time_ref
                progress = (current_completed / total_count) * 100 if total_count > 0 else 100

                bar_length = 30
                filled_length = int(bar_length * current_completed // total_count) if total_count > 0 else bar_length
                bar = '█' * filled_length + '░' * (bar_length - filled_length)

                # 构建进度信息
                if current_completed > 0:
                    avg_time_per_task = elapsed_time / current_completed
                    remaining_tasks = total_count - current_completed
                    estimated_remaining_time = avg_time_per_task * remaining_tasks
                    if estimated_remaining_time < 60:
                        time_str = f"{estimated_remaining_time:.0f}秒"
                    elif estimated_remaining_time < 3600:
                        time_str = f"{estimated_remaining_time / 60:.1f}分钟"
                    else:
                        time_str = f"{estimated_remaining_time / 3600:.1f}小时"

                    progress_line = (
                        f"\r进度: [{bar}] {progress:.1f}% | "
                        f"已完成: {current_completed}/{total_count} | "
                        f"耗时: {elapsed_time:.1f}秒 | "
                        f"平均耗时: {avg_time_per_task:.1f}秒/个 | "
                        f"剩余时间: {time_str}\n"
                    )
                else:
                    progress_line = f"\r进度: [{bar}] {progress:.1f}% | 已完成: {current_completed}/{total_count}\n"

                # 原地更新（使用 \r 回到行首，flush=True 立即输出）
                sys.stdout.write(progress_line)
                sys.stdout.flush()

            # 任务完成，打印最终状态
            sys.stdout.write("\n")
            sys.stdout.flush()
            logger.info("所有任务已完成！")

        progress_task = asyncio.create_task(track_progress(progress_counter, total, time.time()))

        try:
            results = await asyncio.gather(*coros, return_exceptions=True)
            progress_counter.value = total  # 确保进度条达到 100%
            total_elapsed_time = time.time() - total_start_time
            logger.debug(f"并发生成板块总耗时：{total_elapsed_time:.2f}秒，平均每个板块：{(total_elapsed_time / total) if total > 0 else 0:.2f}秒")

            # 聚合：使用 origin_index 作为 key，确保相同 params 的不同输入独立
            grouped_results: Dict[int, Dict] = {}
            total_usage = {'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}

            for i, raw_res in enumerate(results):
                task_info = expanded_tasks[i]
                origin_index = int(task_info['origin_index'])
                if origin_index not in grouped_results:
                    grouped_results[origin_index] = {
                        'params': task_info['original_params'],
                        'results': {},
                        'status': 'success'
                    }

                # 如果为占位 no_data，则短路并写入占位结果（不调用模型）
                if task_info.get('no_data'):
                    grouped_results[origin_index]['results'][task_info['content_type']] = {
                        'status': 'no_data',
                        'message': '该板块无数据，已跳过生成',
                        'report_content': ''
                    }
                    continue

                # 处理异常或成功返回
                if isinstance(raw_res, Exception):
                    grouped_results[origin_index]['status'] = 'error'
                    grouped_results[origin_index]['results'][task_info['content_type']] = {
                        'status': 'error',
                        'message': f"报告生成异常: {str(raw_res)}",
                        'report_content': ""
                    }
                elif isinstance(raw_res, dict) and raw_res.get('status') == 'success':
                    content = raw_res.get('content', '')
                    usage = raw_res.get('usage', {}) or {}
                    prompt_tokens = int(usage.get('prompt_tokens', raw_res.get('prompt_tokens', 0) or 0))
                    completion_tokens = int(usage.get('completion_tokens', raw_res.get('completion_tokens', 0) or 0))

                    grouped_results[origin_index]['results'][task_info['content_type']] = {
                        'status': 'success',
                        'message': '报告生成成功',
                        'report_content': content
                    }

                    total_usage['prompt_tokens'] += prompt_tokens
                    total_usage['completion_tokens'] += completion_tokens
                    total_usage['total_tokens'] += (prompt_tokens + completion_tokens)
                else:
                    # 其它错误结构
                    msg = raw_res.get('message') if isinstance(raw_res, dict) else str(raw_res)
                    grouped_results[origin_index]['status'] = 'error'
                    grouped_results[origin_index]['results'][task_info['content_type']] = {
                        'status': 'error',
                        'message': msg or '报告生成失败',
                        'report_content': ""
                    }

            # 1. 预处理所有 origin_index 的数据，准备并发修复
            origin_data_list = []
            for origin_index in range(0, input_count):
                group = grouped_results.get(origin_index, {'params': None, 'results': {}, 'status': 'success'})

                combined_content = ""
                cur_content, cum_content = "", ""
                has_success = False
                status_info = {}

                # 获取当前 origin_index 对应的所有维度（诊断类型）
                origin_diagnosis_types = set()
                for task in expanded_tasks:
                    if task['origin_index'] == origin_index:
                        diagnosis_type = task['original_params'].get('diagnosisType', '') or "ORG"
                        origin_diagnosis_types.add(diagnosis_type)

                # 确保按 "ORG"、"CHAN"、"IND"、"PROD" 的顺序处理
                ordered_dimensions = [dim for dim in MODULE_DIMENSIONS if dim in origin_diagnosis_types]

                # 遍历所有内容类型，按维度顺序动态拼接内容
                for dimension in ordered_dimensions:
                    for content_type in CONTENT_TYPE_CONFIG.get(dimension, {}).keys():
                        content_result = group['results'].get(content_type, {})
                        status_info[content_type] = content_result.get('status', 'missing')

                        # 只有 status == success 的板块才拼接内容
                        if content_result.get('status') == 'success':
                            combined_content += f"{content_result.get('report_content', '')}\n"
                            if "CURRENT" in content_type:
                                cur_content += f"{content_result.get('report_content', '')}\n"
                            else:
                                cum_content += f"{content_result.get('report_content', '')}\n"
                            has_success = True

                # 移除末尾多余的换行
                combined_content = combined_content.rstrip('\n')

                # 准备修复模板
                cur_template, cum_template = "", ""
                for dim in group['results'].keys():
                    if "CURRENT" in dim:
                        cur_template += self._get_template_by_dimension(prompts['template_data'], group['params'], dim)
                    else:
                        cum_template += self._get_template_by_dimension(prompts['template_data'], group['params'], dim)

                origin_data_list.append({
                    'origin_index': origin_index,
                    'group': group,
                    'combined_content': combined_content,
                    'cur_content': cur_content,
                    'cum_content': cum_content,
                    'has_success': has_success,
                    'status_info': status_info,
                    'cur_template': cur_template,
                    'cum_template': cum_template
                })

            # 2. 并发修复标签
            async def process_origin_data(origin_data):
                """
                处理单个 origin_index 的标签修复
                """
                origin_index = origin_data['origin_index']
                group = origin_data['group']
                combined_content = origin_data['combined_content']
                cur_content = origin_data['cur_content']
                cum_content = origin_data['cum_content']
                has_success = origin_data['has_success']
                status_info = origin_data['status_info']
                cur_template = origin_data['cur_template']
                cum_template = origin_data['cum_template']

                fixed_combined_content = combined_content
                if self.settings.use_fix_tag:
                    # 获取诊断类型
                    diagnosis_type = group['params'].get('diagnosisType', 'ORG') if group['params'] else 'ORG'
                    
                    # 调用异步修复方法
                    fixed_cur_content, fixed_cum_content, cur_validate_result, cum_validate_result = await self._async_fix_tags(
                        cur_content, cum_content, cur_template, cum_template, diagnosis_type
                    )

                    if not cur_validate_result['is_valid'] or not cum_validate_result['is_valid']:
                        fixed_combined_content = fixed_cur_content + fixed_cum_content

                # 构建状态信息字符串
                status_str = ", ".join([f"{ct}: {status}" for ct, status in status_info.items()])

                # 生成最终报告
                if not has_success:
                    return {
                        'origin_index': origin_index,
                        'report': {
                            'status': 'error',
                            'message': f'所有板块生成均失败或无有效内容（{status_str}）',
                            'report_content': fixed_combined_content,
                            'params': group.get('params')
                        }
                    }
                else:
                    return {
                        'origin_index': origin_index,
                        'report': {
                            'status': 'success',
                            'message': f"报告生成完成（{status_str}）",
                            'report_content': fixed_combined_content,
                            'params': group.get('params')
                        }
                    }

            # 并发处理所有 origin_index 的修复任务
            logger.debug(f"开始并发修复 {len(origin_data_list)} 个 origin_index 的标签")
            repair_start_time = time.time()
            repair_results = await asyncio.gather(*[process_origin_data(data) for data in origin_data_list])
            repair_end_time = time.time()
            logger.debug(f"并发修复完成，耗时：{repair_end_time - repair_start_time:.2f}秒")

            # 3. 收集修复结果，按 origin_index 顺序生成最终报告
            final_reports = [None] * input_count
            for result in repair_results:
                origin_index = result['origin_index']
                final_reports[origin_index] = result['report']

            return {
                'status': 'success',
                'message': f'已生成 {len([r for r in final_reports if r["status"] == "success"])} 个完整报告（输入共 {input_count} 条）',
                'data': {
                    'report_contents': [r['report_content'] for r in final_reports],
                    'report_results': final_reports,
                    'actual_params': [r['params'] for r in final_reports],
                    'usage': total_usage,
                    'prompt_tokens': total_usage['prompt_tokens'],
                    'completion_tokens': total_usage['completion_tokens']
                }
            }

        finally:
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

    async def _process_single_content_type_with_semaphore(self, semaphore: Semaphore,
                                                          index: int,
                                                          task_info: Dict,
                                                          prompts: Dict,
                                                          total_count: int,
                                                          progress_counter: ProgressCounter) -> Dict:
        """
        带并发控制的单个板块处理。
        新增参数：progress_counter - 用于更新全局进度
        """
        async with semaphore:
            data_content = "\n".join(task_info.get('data') or [])
            logger.debug(f"{task_info['original_params'].get('diagnosisType', '')} - {task_info['content_type']} 给模型前（预处理后数据）:\n{data_content}")

            # 填充 user prompt
            user_prompt = self._fill_user_prompt(prompts['user_template'], data_content, task_info)

            messages = []
            # system prompt with template injection
            if 'system' in prompts:
                system_prompt = prompts['system']
                if 'template_data' in prompts:
                    template = self._get_template_by_dimension(prompts['template_data'],
                                                               task_info['original_params'],
                                                               task_info['content_type'])
                    if template:
                        system_prompt = system_prompt.replace("{}", template)
                messages.append({"role": "system", "content": system_prompt})

            messages.append({"role": "user", "content": user_prompt})

            logger.debug(f"开始生成第 {index + 1}/{total_count} 个板块 ({task_info['content_type']})")
            start_time = time.time()

            # 如果标记为 no_data，我们不会调用模型（短路）：返回一个标记字典（聚合端会识别）
            if task_info.get('no_data'):
                elapsed_time = time.time() - start_time
                logger.debug(f"第 {index + 1}/{total_count} 个板块 ({task_info['content_type']}) 无数据，短路跳过，耗时: {elapsed_time:.2f}秒")
                # 更新进度计数
                progress_counter.value += 1
                return {
                    'status': 'no_data',
                    'content': '',
                    'usage': {},
                    'prompt_tokens': 0,
                    'completion_tokens': 0,
                    'response_time': 0.0
                }

            # 否则调用模型
            result = await self._call_model_with_index(index, messages)
            elapsed_time = time.time() - start_time
            logger.debug(f"第 {index + 1}/{total_count} 个板块 ({task_info['content_type']}) 生成完成，耗时: {elapsed_time:.2f}秒")

            # 更新进度计数
            progress_counter.value += 1

            return result

    async def _call_model_with_index(self, index: int, messages: list) -> Dict:
        try:
            return await self._call_model(messages)
        except Exception as e:
            logger.exception(f"第{index + 1}个板块生成失败: {e}")
            return {'status': 'error', 'message': f"第{index + 1}个板块生成失败: {e}"}

    def _save_failed_case(self, source_text: str, diagnosis_type: str, time_type: str):
        """
        保存修复失败的案例到fix_data.json文件
        参数：
            source_text: 待修复的文本内容
            diagnosis_type: 诊断类型，如"ORG"、"CHAN"、"IND"、"PROD"
            time_type: 时间类型，如"CURRENT"或"CUMULATIVE"
        """
        import json
        import os
        
        # 定义fix_data.json文件路径
        fix_data_path = "tests/fix_data.json"
        
        # 确保目录存在
        os.makedirs(os.path.dirname(fix_data_path), exist_ok=True)
        
        # 读取现有数据
        existing_data = []
        if os.path.exists(fix_data_path):
            try:
                with open(fix_data_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except Exception as e:
                logger.error(f"读取fix_data.json失败: {e}")
                existing_data = []
        
        # 构建新的失败案例
        new_case = {
            "data_type": "OFFICE",
            "diagnosis_type": diagnosis_type,
            "time_type": time_type,
            "source_text": source_text
        }
        
        # 检查是否已存在相同案例（基于source_text的哈希值）
        import hashlib
        new_case_hash = hashlib.md5(source_text.encode('utf-8')).hexdigest()
        existing_hashes = [hashlib.md5(case['source_text'].encode('utf-8')).hexdigest() for case in existing_data]
        
        if new_case_hash not in existing_hashes:
            # 添加新案例
            existing_data.append(new_case)
            
            # 保存回文件
            try:
                with open(fix_data_path, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, ensure_ascii=False, indent=2)
                logger.info(f"已保存修复失败案例到{fix_data_path}")
            except Exception as e:
                logger.error(f"保存fix_data.json失败: {e}")
        else:
            logger.debug("该失败案例已存在，跳过保存")

    async def _async_fix_tags(self, cur_content: str, cum_content: str, cur_template: str, cum_template: str, diagnosis_type: str = "ORG") -> tuple:
        """
        异步修复标签的独立方法
        参数：
            cur_content: 当前内容
            cum_content: 累计内容
            cur_template: 当前模板
            cum_template: 累计模板
            diagnosis_type: 诊断类型，用于保存失败案例
        返回：
            tuple: (修复后的cur_content, 修复后的cum_content, cur_validate_result, cum_validate_result)
        """
        # 1. 验证标签完整性
        cur_validate_result = self.validator.validate(cur_content)
        cum_validate_result = self.validator.validate(cum_content)

        # 2. 并发修复标签
        async def fix_content(content, template, validate_result, time_type):
            if not validate_result['is_valid']:
                self.validator.print_error(validate_result)
                fix_result = await self.validator.model_fix_tag(content, template, validate_result)
                if fix_result.get('status') == 'success':
                    fix_contents = fix_result.get('content', '')
                    if fix_contents and self.validator.validate(fix_contents)['is_valid']:
                        return fix_contents
                    else:
                        logger.error("标签修复失败，已回退！")
                        # 保存修复失败案例
                        self._save_failed_case(content, diagnosis_type, time_type)
                else:
                    logger.error(f"模型修复失败: {fix_result.get('message', '未知错误')}")
                    # 保存修复失败案例
                    self._save_failed_case(content, diagnosis_type, time_type)
            return content

        # 并发执行修复任务 - asyncio.gather保证返回顺序与输入顺序一致
        cur_content, cum_content = await asyncio.gather(
            fix_content(cur_content, cur_template, cur_validate_result, "CURRENT"),
            fix_content(cum_content, cum_template, cum_validate_result, "CUMULATIVE")
        )

        return cur_content, cum_content, cur_validate_result, cum_validate_result

    async def _call_model(self, messages: list) -> Dict:
        """调用大模型的 HTTP API（使用 httpx.AsyncClient）"""
        try:
            timeout_val = getattr(self.model_config, 'timeout', 60)
            async with httpx.AsyncClient(timeout=timeout_val) as client:
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {getattr(self.model_config, 'api_key', '')}"
                }

                payload = {
                    "model": getattr(self.model_config, 'model_name', ''),
                    "messages": messages,
                    "temperature": getattr(self.model_config, 'temperature', 0.0),
                    "top_p": getattr(self.model_config, 'top_p', 1.0),
                    "stream": False,
                    "seed": 42,
                    "chat_template_kwargs": {"enable_thinking": False},
                    "reasoning_effort": "low"
                }

                start_time = time.time()
                response = await client.post(f"{getattr(self.model_config, 'base_url', '')}/chat/completions",
                                             headers=headers, json=payload)
                response_time = time.time() - start_time

                if response.status_code != 200:
                    text = response.text if hasattr(response, 'text') else str(response.content)
                    logger.error(f"模型API调用失败: {response.status_code} - {text}")
                    return {'status': 'error', 'message': f"模型API调用失败: {response.status_code} - {text}"}

                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    content = result['choices'][0].get('message', {}).get('content', '')
                    usage = result.get('usage', {}) or {}
                    prompt_tokens = usage.get('prompt_tokens', 0)
                    completion_tokens = usage.get('completion_tokens', 0)
                    return {
                        'status': 'success',
                        'content': content,
                        'usage': usage,
                        'prompt_tokens': int(prompt_tokens or 0),
                        'completion_tokens': int(completion_tokens or 0),
                        'response_time': response_time
                    }
                else:
                    logger.error(f"模型响应格式异常: {result}")
                    return {'status': 'error', 'message': f"模型响应格式异常: {result}"}

        except httpx.TimeoutException:
            logger.exception("模型调用超时")
            return {'status': 'error', 'message': f"模型调用超时（{getattr(self.model_config, 'timeout', 60)}秒）"}
        except Exception as e:
            logger.exception(f"模型调用异常: {e}")
            return {'status': 'error', 'message': f"模型调用异常: {str(e)}"}

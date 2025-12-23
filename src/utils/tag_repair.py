# -*- coding: utf-8 -*-
# @Time    : 2025/12/10 13:57
# @Author  : EvanSong
import asyncio
import re
import time
from typing import List, Dict

import httpx

from src.utils import log_utils

logger = log_utils.logger


class XMLTagValidator:
    """XML标签完整性检测工具"""

    def __init__(self, environment):
        self.tag_pattern = re.compile(r'</?([a-zA-Z_][a-zA-Z0-9_]*)>')
        self.model_name = "Deepseek-V3"
        self.api_key = "pk-99d173c0-6c1b-476a-85a5-2d478a2436f6"

        # 新增：定义只能出现一对的标签
        self.single_pair_tags = {'current', 'accumulate'}

        if environment == "test":
            self.base_url = "http://10.1.110.2:8888/deepseek-v3/v1"
        elif environment == "uat":
            self.base_url = "http://10.1.110.2:31010/deepseek-v3/v1"
        else:
            self.base_url = "http://10.1.110.2:11010/deepseek-v3/v1"

    def extract_tags(self, text: str) -> List[Dict]:
        """提取文本中的所有标签"""
        tags = []
        for match in self.tag_pattern.finditer(text):
            full_tag = match.group(0)
            tag_name = match.group(1)
            is_closing = full_tag.startswith('</')

            # 跳过 </br> 标签
            if tag_name == 'br' and is_closing:
                continue

            # 计算行号
            line_number = text[:match.start()].count('\n') + 1

            tags.append({
                'name': tag_name,
                'is_closing': is_closing,
                'position': match.start(),
                'full_tag': full_tag,
                'line_number': line_number
            })

        return tags

    def validate(self, text: str) -> Dict:
        """
        验证标签完整性
        - 普通标签：宽松匹配（反向查找）
        - 特殊标签（current/accumulate）：只允许出现一对
        - 检测标签交叉
        - 优化错误优先级展示
        """
        tags = self.extract_tags(text)

        stack = []
        errors = []
        unmatched_open_tags = []
        unmatched_close_tags = []

        # 统计特殊标签的出现次数
        single_pair_tag_count = {tag_name: {'open': 0, 'close': 0}
                                 for tag_name in self.single_pair_tags}

        for tag in tags:
            # 统计特殊标签
            if tag['name'] in self.single_pair_tags:
                if tag['is_closing']:
                    single_pair_tag_count[tag['name']]['close'] += 1
                else:
                    single_pair_tag_count[tag['name']]['open'] += 1

            if not tag['is_closing']:
                # 开始标签，压入栈
                stack.append(tag)
            else:
                # 结束标签，寻找匹配的开始标签
                found = False
                matched_index = -1

                for i in range(len(stack) - 1, -1, -1):
                    if stack[i]['name'] == tag['name']:
                        matched_index = i
                        found = True
                        break

                if not found:
                    unmatched_close_tags.append(tag)
                    errors.append({
                        'type': 'missing_open',
                        'tag': tag['name'],
                        'position': tag['position'],
                        'line_number': tag['line_number'],
                        'message': f"缺少开始标签 <{tag['name']}>"
                    })
                else:
                    # 检测交叉标签
                    if matched_index != len(stack) - 1:
                        crossed_tags = [stack[i]['name'] for i in range(matched_index + 1, len(stack))]
                        errors.append({
                            'type': 'tag_crossing',
                            'tag': tag['name'],
                            'position': tag['position'],
                            'line_number': tag['line_number'],
                            'message': f"标签交叉：</{tag['name']}> 跨越了未闭合的标签 {crossed_tags}",
                            'crossed_tags': crossed_tags
                        })

                    # 无论是否交叉，都执行匹配
                    stack.pop(matched_index)

        # 栈中剩余的都是未匹配的开始标签
        for tag in stack:
            unmatched_open_tags.append(tag)
            errors.append({
                'type': 'missing_close',
                'tag': tag['name'],
                'position': tag['position'],
                'line_number': tag['line_number'],
                'message': f"缺少结束标签 </{tag['name']}>"
            })

        # 检查特殊标签是否超过一对
        multiple_tags = set()  # 记录哪些标签有重复问题

        for tag_name, counts in single_pair_tag_count.items():
            if counts['open'] > 1:
                multiple_tags.add(tag_name)
                errors.append({
                    'type': 'multiple_open',
                    'tag': tag_name,
                    'position': None,
                    'line_number': None,
                    'message': f"<{tag_name}> 开始标签出现了 {counts['open']} 次，只允许1次"
                })

            if counts['close'] > 1:
                multiple_tags.add(tag_name)
                errors.append({
                    'type': 'multiple_close',
                    'tag': tag_name,
                    'position': None,
                    'line_number': None,
                    'message': f"</{tag_name}> 结束标签出现了 {counts['close']} 次，只允许1次"
                })

        # ========== 新增：过滤衍生错误 ==========
        # 如果某个标签有重复问题，移除它的 missing_open/missing_close 错误
        filtered_errors = []
        for error in errors:
            error_tag = error.get('tag')
            error_type = error.get('type')

            # 如果是重复标签导致的 missing_open/missing_close，跳过
            if error_tag in multiple_tags and error_type in ('missing_open', 'missing_close'):
                continue

            filtered_errors.append(error)

        # 同步更新 unmatched 列表
        unmatched_open_tags = [tag for tag in unmatched_open_tags if tag['name'] not in multiple_tags]
        unmatched_close_tags = [tag for tag in unmatched_close_tags if tag['name'] not in multiple_tags]
        # ==========================================

        return {
            'is_valid': len(filtered_errors) == 0,
            'total_tags': len(tags),
            'matched_pairs': (len(tags) - len(filtered_errors)) // 2,
            'errors': filtered_errors,  # 使用过滤后的错误列表
            'unmatched_open_tags': unmatched_open_tags,
            'unmatched_close_tags': unmatched_close_tags
        }

    def _fix_special_tags(self, content: str) -> str:
        """
        强制修正 <current> 和 <accumulate> 标签：
        1. 只保留一对
        2. 开始标签必须在最开头
        3. 结束标签必须在最末尾
        4. 删除中间多余的标签
        """
        for tag_name in self.single_pair_tags:
            # 查找所有该标签的开始和结束标签
            open_pattern = f'<{tag_name}>'
            close_pattern = f'</{tag_name}>'

            # 统计数量
            open_count = content.count(open_pattern)
            close_count = content.count(close_pattern)

            # 如果这个标签不存在，跳过
            if open_count == 0 and close_count == 0:
                continue

            # 如果只有一对且位置正确，跳过
            if open_count == 1 and close_count == 1:
                if content.startswith(f'<{tag_name}>') and content.endswith(f'</{tag_name}>'):
                    continue

            if tag_name == 'current':
                if content.endswith(f'</{tag_name}>'):
                    continue
                content_without_tags = content.replace(close_pattern, '')
                # 在开头和结尾重新添加一对标签
                content = f'{content_without_tags}</{tag_name}>'

                logger.info(f"已修正 {tag_name} 标签，确保只有一对且位置正确")
                break
            else:
                # 移除所有该标签
                content_without_tags = content.replace(open_pattern, '').replace(close_pattern, '')

                # 在开头和结尾重新添加一对标签
                content = f'<{tag_name}>{content_without_tags}</{tag_name}>'

                logger.info(f"已修正 {tag_name} 标签，确保只有一对且位置正确")
                break

        return content

    def generate_prompt(self, text: str, template: str = None, validation_result: Dict = None) -> List[Dict]:
        """
        生成修复提示词（分为 system 和 user）

        参数:
            text: 待修复的文本
            template: 参考模板（可选）
            validation_result: 验证结果

        返回:
            [{'role': 'system', 'content': str}, {'role': 'user', 'content': str}]
        """
        if validation_result is None:
            validation_result = self.validate(text)

        unmatched_open = validation_result['unmatched_open_tags']
        unmatched_close = validation_result['unmatched_close_tags']
        crossing_errors = [e for e in validation_result['errors'] if e['type'] == 'tag_crossing']
        multiple_errors = [e for e in validation_result['errors'] if e['type'] in ('multiple_open', 'multiple_close')]

        # System Prompt
        system_prompt = """你是一个XML标签修复专家。你的任务是修复XML文本中缺失或多余的标签。

## 核心原则：
1. **只修复标签**，绝对不要修改任何文本内容
2. **不要添加XML声明**（如 <?xml version="1.0"?>）
3. **不要添加根标签**（如 <root>）
4. **不要添加任何注释**
5. **保持原文本的所有空格、换行、缩进完全不变**
6. 注意：</br> 是自闭合标签，不需要配对

## 特殊标签规则（非常重要）：
<accumulate>、<current> 是顶层容器标签：
- 这两个标签在整个文本中**只能出现一对**
- <accumulate> 必须是文本的**第一个标签**（最外层），但是<current> 不是文本的**第一个标签**（最外层）
- </accumulate>、</current> 必须是文本的**最后一个标签**（最外层）
- 它们包裹整个文档的所有内容
- 其他所有标签都必须在它们内部

错误示例：
```
<accumulate>
  ...
</accumulate>  ← 错误：这里就应该结束了
<other>...</other>  ← 错误：不应该有标签在 </accumulate> 之后
```

正确示例：
```
<accumulate>
  <gpm_baseline>...</gpm_baseline>
  <customer_abnormal>...</customer_abnormal>  ← 所有内容都在内部
</accumulate>  ← 最后一个标签
```

    ## 修复规则：
    - 对于缺少结束标签的情况，在合适的位置添加结束标签
    - 对于缺少开始标签的情况，在对应的结束标签前添加开始标签
    - 对于多余的标签，直接删除
    - 对于标签交叉的情况，调整标签位置使其正确嵌套

    ## 输出要求：
    直接输出修复后的完整文本，不要任何额外的内容、解释、或包装。"""

        if template:
            system_prompt += f"\n\n## 参考模板结构：\n```\n{template}\n```\n\n请参考模板的标签结构和嵌套关系进行修复。"

        # User Prompt
        user_prompt = "## 检测到的问题：\n\n"

        if unmatched_open:
            user_prompt += "### 缺少结束标签：\n"
            for tag in unmatched_open:
                user_prompt += f"- <{tag['name']}> (第 {tag['line_number']} 行)\n"
            user_prompt += "\n"

        if unmatched_close:
            user_prompt += "### 缺少开始标签或多余的结束标签：\n"
            for tag in unmatched_close:
                user_prompt += f"- </{tag['name']}> (第 {tag['line_number']} 行)\n"
            user_prompt += "\n"

        if crossing_errors:
            user_prompt += "### 标签交叉问题：\n"
            for error in crossing_errors:
                crossed_tags = error.get('crossed_tags', [])
                user_prompt += f"- </{error['tag']}> (第 {error['line_number']} 行) 跨越了 {crossed_tags}\n"
            user_prompt += "\n"

        if multiple_errors:
            user_prompt += "### 标签重复问题：\n"
            for error in multiple_errors:
                user_prompt += f"- {error['message']}\n"
            user_prompt += "\n"

        user_prompt += f"""## 待修复的文本：
    ```
    {text}
    ```

    请直接输出修复后的完整文本，不要添加XML声明、根标签、注释或任何解释。保持原文本的内容和格式完全不变，只添加或删除缺失或多余的标签。
    <accumulate>、<current>只能出现其中一对，不能既出现<current></current>又出现<accumulate></accumulate>"""

        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

    def is_fix_head_tag(self, text, validation_result):
        """用来修复accumulate或者current顶层标签"""
        new_errors = []
        new_text = text
        for error in validation_result['errors']:
            if error['type'] == 'multiple_close' and error['tag'] in ['accumulate', 'current']:
                new_text = new_text.replace(f"</{error['tag']}>", "") + f"</{error['tag']}>"
                if new_text and self.validate(new_text)['is_valid']:
                    continue
            else:
                new_errors.append(error)
        validation_result['errors'] = new_errors
        return new_text, validation_result

    async def model_fix_tag(self, text: str, template: str = None, validation_result: Dict = None) -> Dict:
        """调用大模型来修复标签 HTTP API（使用 httpx.AsyncClient）"""
        text, validation_result = self.is_fix_head_tag(text, validation_result)
        if not validation_result['errors']:
            return {
                'status': 'success',
                'content': text
            }
        messages = self.generate_prompt(text, template, validation_result)
        timeout_val = 600
        try:
            async with httpx.AsyncClient(timeout=timeout_val) as client:
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                }

                payload = {
                    "model": self.model_name,
                    "messages": messages,
                    "temperature": 0.2,
                    "top_p": 0.95,
                    "stream": False,
                    "seed": 42,
                    "chat_template_kwargs": {"enable_thinking": False}
                }

                start_time = time.time()
                response = await client.post(f"{self.base_url}/chat/completions",
                                             headers=headers, json=payload)
                response_time = time.time() - start_time

                if response.status_code != 200:
                    text = response.text if hasattr(response, 'text') else str(response.content)
                    logger.error(f"模型API调用失败: {response.status_code} - {text}")
                    return {'status': 'error', 'message': f"模型API调用失败: {response.status_code} - {text}"}

                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    content = result['choices'][0].get('message', {}).get('content', '')

                    # 后处理
                    content = self._fix_special_tags(content)

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
            return {'status': 'error', 'message': f"模型调用超时（{timeout_val}秒）"}
        except Exception as e:
            logger.exception(f"模型调用异常: {e}")
            return {'status': 'error', 'message': f"模型调用异常: {str(e)}"}

    @staticmethod
    def print_error(validation_result: Dict):
        """打印验证报告（简洁一行）"""
        errors = []

        if validation_result['unmatched_open_tags']:
            tags = ', '.join([f"<{tag['name']}>(L{tag['line_number']})"
                              for tag in validation_result['unmatched_open_tags']])
            errors.append(f"缺少结束: {tags}")

        if validation_result['unmatched_close_tags']:
            tags = ', '.join([f"</{tag['name']}>(L{tag['line_number']})"
                              for tag in validation_result['unmatched_close_tags']])
            errors.append(f"缺少开始: {tags}")

        # 特殊标签重复错误
        multiple_errors = [e for e in validation_result['errors']
                           if e['type'] in ('multiple_open', 'multiple_close')]
        if multiple_errors:
            multi_msgs = [e['message'] for e in multiple_errors]
            errors.append(f"重复标签: {'; '.join(multi_msgs)}")

        # 新增：交叉标签错误
        crossing_errors = [e for e in validation_result['errors']
                           if e['type'] == 'tag_crossing']
        if crossing_errors:
            cross_msgs = [f"</{e['tag']}>(L{e['line_number']})跨越{e['crossed_tags']}"
                          for e in crossing_errors]
            errors.append(f"交叉标签: {'; '.join(cross_msgs)}")

        logger.warning(
            f"⚠️  标签不完整 | 总标签: {validation_result['total_tags']} | "
            f"匹配对: {validation_result['matched_pairs']} | "
            f"错误: {len(validation_result['errors'])} | {' | '.join(errors)}"
        )


def main():
    """主函数 - 使用示例"""

    # 示例模板
    total_org_template = """<accumulate><summary>累计：</summary></br><gpm_yoy>累计毛利同比下降>1%业务员：<underline><salesman>张三(工号)</salesman>(-9.6%)</underline>、<underline><salesman>李四(工号)</salesman>(-4.1%)</underline>。</br> </gpm_yoy><gpm_mom>累计毛利环比下降>1%业务员：<underline><salesman>李四(工号)</salesman>(-4.1%)</underline>、<underline><salesman>张三(工号)</salesman>(-0.4%)</underline>。</br> </gpm_mom><gpm_decline>累计毛利连续2月下降业务员：<underline><salesman>张三(工号)</salesman>(37.32%/30.54%/27.44%)</underline>。</br> </gpm_decline><special_rate>网线&硬盘累计收入占比>20%业务员：<underline><salesman>李四(工号)</salesman>(-4.1%)</underline>、<underline><salesman>张三(工号)</salesman>(-0.4%)</underline>。</br></special_rate>
<project_abnormal><summary>工程异常项目：</summary></br> 工程项目低毛利<prj_list>TOP</prj_list>，以下列示TOP5：其中业务员<prj_list_sale>张三(工号)</prj_list_sale>，客户<prj_list_cust>1-1234567(客户编码)</prj_list_cust>，对整体影响较大；</br> <prj_detail><prj_detail_sale>张三</prj_detail_sale>-金华市公安局交通警察支队市本级雪亮工程项目(<contract>1-20394603274</contract>)</prj_detail>：累计毛利率-287.16%；</br> <prj_detail><prj_detail_sale>李四</prj_detail_sale>-绍兴市xxx(<contract>合同编号</contract>)</prj_detail>：累计毛利率-287.16%；</br>
<prj_detail><prj_detail_sale>王五</prj_detail_sale>-杭州市xxx(<contract>合同编号</contract>)</prj_detail>：累计毛利率-287.16%；</br>
<prj_detail><prj_detail_sale>赵云</prj_detail_sale>-长沙市xxx(<contract>合同编号</contract>)</prj_detail>：累计毛利率-287.16%；</br>
<prj_detail><prj_detail_sale>张飞</prj_detail_sale>-广州市xxx(<contract>合同编号</contract>)</prj_detail>：毛利率-287.16%；</br></project_abnormal>
<product_abnormal><summary>产品异常合同：</summary></br>产品合同低毛利<prd_list>TOP</prd_list>，以下列示TOP5：其中业务员<prd_list_sale>张三(工号)</prd_list_sale>，客户<prd_list_cust>1-1234567(客户编码)</prd_list_cust>，对整体影响较大；</br> <prd_detail><prd_detail_sale>张三</prd_detail_sale>-金华市公安局交通警察支队市本级雪亮工程项目(<contract>1-20394603274</contract>)</prd_detail>：累计毛利率-287.16%；</br><prd_detail><prd_detail_sale>李四</prd_detail_sale>-绍兴市xxx(<contract>合同编号</contract>)</prd_detail>：累计毛利率-287.16%；</br>
<prd_detail><prd_detail_sale>王五</prd_detail_sale>-杭州市xxx(<contract>合同编号</contract>)</prd_detail>：累计毛利率-287.16%；</br>
<prd_detail><prd_detail_sale>赵云</prd_detail_sale>-长沙市xxx(<contract>合同编号</contract>)</prd_detail>：累计毛利率-287.16%；</br>
<prd_detail><prd_detail_sale>张飞</prd_detail_sale>-广州市xxx(<contract>合同编号</contract>)</prd_detail>：累计毛利率-287.16%；</br></product_abnormal></accumulate>"""

    # 示例待检测文本（缺少 </product_abnormal> 和 </gpm_yoy>）
    test_text = """<accumulate><summary>累计：</summary></br><gpm_yoy>累计毛利同比下降>1%业务员：<underline><salesman>刘鑫洲(118320)</salesman>(-163.3%)</underline>、<underline><salesman>吴学渊(16133)</salesman>(-163.1%)</underline>、<underline><salesman>胥才伟(476677)</salesman>(-20.5%)</underline>、<underline><salesman>李韩超(327120)</salesman>(-14.7%)</underline>、<underline><salesman>贾荣胜(339745)</salesman>(-6.9%)</underline>、<underline><salesman>郑俊(347412)</salesman>(-1.0%)</underline>。</br> </gpm_yoy><gpm_mom>累计毛利环比下降>1%业务员：无。</br> </gpm_mom><gpm_decline>累计毛利连续2月下降业务员：无。</br> </gpm_decline><special_rate>网线&硬盘累计收入占比>20%业务员：无。</br></special_rate></accumulate>
<project_abnormal><summary>工程异常项目：</summary></br> 工程项目低毛利<prj_list>TOP</prj_list>，以下列示TOP5：其中业务员<prj_list_sale>徐涛(24417)</prj_list_sale>，客户<prj_list_cust>乐山市翰程办公设备有限公司(1@19986105917)</prj_list_cust>，对整体影响较大；</br> <prj_detail><prj_detail_sale>徐涛</prj_detail_sale>-乐山市徐家扁安全监控改造一期建设项目（大屏部分）(<contract>S21424110320</contract>)</prj_detail>：累计毛利率-100.0%；</br> <prj_detail><prj_detail_sale>徐涛</prj_detail_sale>-大屏零星设备采购(<contract>S21424110383</contract>)</prj_detail>：累计毛利率-100.0%；</br> <prj_detail><prj_detail_sale>贾荣胜</prj_detail_sale>-四川海创尚纬新能源科技有限公司年产 10万吨动力储能电池负极材料一体化项目(一期)大屏项目(<contract>S21425040237</contract>)</prj_detail>：累计毛利率13.9%；</br></project_abnormal>
<product_abnormal><summary>产品异常合同：</summary></br>产品合同低毛利<prd_list>TOP</prd_list>，以下列示TOP5：其中业务员<prd_list_sale>徐涛(24417)</prd_list_sale>，客户<prd_list_cust>乐山顺捷网络科技有限公司(1@11397947672)</prd_list_cust>，对整体影响较大；</br> <prd_detail><prd_detail_sale>徐涛</prd_detail_sale>-犍为县平安城市智能视频监控升级改造及智能交通一体化系统项目--软硬件建设、维护服务(<contract>20724013395080060</contract>)</prd_detail>：累计毛利率-100.0%；</br><prd_detail><prd_detail_sale>徐涛</prd_detail_sale>-五通桥区高清晰智能化天网二期及智能交通管控建设(<contract>20723103395080018</contract>)</prd_detail>：累计毛利率-100.0%；</br>
<prd_detail><prd_detail_sale>贾荣胜</prd_detail_sale>-四川省乐山市第一职业高级中学新校园数字广播、监控与安防、一卡通系统、网络设备与中心机房采购项目(<contract>20724063395080061</contract>)</prd_detail>：累计毛利率-100.0%；</br>
<prd_detail><prd_detail_sale>贾荣胜</prd_detail_sale>-峨嵋悦居项目智能化系统安装工程(<contract>26024093286610162</contract>)</prd_detail>：累计毛利率-100.0%；</br>
<prd_detail><prd_detail_sale>刘鑫洲</prd_detail_sale>-国能大渡河枕头坝水电站门禁系统改造(<contract>20723113233850032</contract>)</prd_detail>：累计毛利率-100.0%；</br></product_abnormal></accumulate>"""

    # 创建验证器
    validator = XMLTagValidator("test")

    # 1. 验证标签完整性
    result = validator.validate(test_text)

    # 2. 打印报告
    validator.print_error(result)

    fix_result = asyncio.run(validator.model_fix_tag(test_text, total_org_template, result))

    print("修复结果:", fix_result)


if __name__ == "__main__":
    main()

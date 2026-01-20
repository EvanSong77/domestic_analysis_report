# -*- coding: utf-8 -*-
# @Time    : 2026/1/8 19:42
# @Author  : EvanSong
# -*- coding: utf-8 -*-
# @Time    : 2026/1/8 19:42
# @Author  : EvanSong
import asyncio
import json

from src.utils.tag_repair import XMLTagValidator


async def process_single_data(validator, data, templates):
    """处理单条数据的异步函数"""
    data_type = data["data_type"]
    diagnosis_type = data["diagnosis_type"]
    time_type = data["time_type"]
    source_text = data["source_text"]

    dia_templates = templates[data_type][diagnosis_type]
    total_templates = ""
    for k, v in dia_templates.items():
        if time_type in v:
            total_templates += v[time_type]

    # 1. 验证标签完整性
    result = validator.validate(source_text)

    # 2. 打印报告
    validator.print_error(result)

    # 3. 模型修复标签
    fix_result = await validator.model_fix_tag(source_text, total_templates, result)

    # 4. 检查修复结果
    success = False
    if fix_result.get('status') == 'success':
        print(f"原始内容: {source_text}")
        fix_contents = fix_result.get('content', '')
        print(f"修复结果: {fix_contents}")
        if fix_contents and validator.validate(fix_contents)['is_valid']:
            success = True
            print("标签修复成功！")
        else:
            print("标签修复失败！")

    return success


async def batch_test(data_path, template_path, max_concurrent=5):
    """批处理测试，支持并发"""
    # 创建验证器
    validator = XMLTagValidator("test")

    # 加载数据和模板
    with open(data_path, "r", encoding="utf-8") as fp:
        datas = json.load(fp)

    with open(template_path, "r", encoding="utf-8") as fp:
        templates = json.load(fp)

    total_count = len(datas)

    # 使用信号量限制并发数
    semaphore = asyncio.Semaphore(max_concurrent)

    async def bounded_process(data):
        async with semaphore:
            return await process_single_data(validator, data, templates)

    # 并发处理所有数据
    results = await asyncio.gather(*[bounded_process(data) for data in datas])

    # 统计结果
    true_count = sum(results)
    print(f"\n准确率: {true_count / total_count:.2%}")
    print(f"成功数: {true_count}/{total_count}")


if __name__ == '__main__':
    # 运行异步函数，max_concurrent 可根据需要调整
    asyncio.run(batch_test("./fix_data.json", "../data/model_template.json", max_concurrent=5))

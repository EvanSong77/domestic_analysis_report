# -*- coding: utf-8 -*-
# streamlit_test.py
import streamlit as st
import asyncio

from src.utils.tag_repair import XMLTagValidator

# 页面配置
st.set_page_config(
    page_title="XML标签验证器",
    page_icon="🔍",
    layout="wide"
)

# 初始化 session_state
if 'validator' not in st.session_state:
    st.session_state.validator = XMLTagValidator("test")

if 'validation_result' not in st.session_state:
    st.session_state.validation_result = None

if 'fix_result' not in st.session_state:
    st.session_state.fix_result = None

# 新增：保存输入文本和模板
if 'input_text' not in st.session_state:
    st.session_state.input_text = ""

if 'template_text' not in st.session_state:
    st.session_state.template_text = ""

# 标题
st.title("🔍 XML标签完整性验证工具")
st.markdown("---")

# 侧边栏配置
with st.sidebar:
    st.header("⚙️ 配置")
    environment = st.selectbox(
        "环境选择",
        ["test", "uat", "prod"],
        index=0
    )

    if st.button("🔄 重新初始化验证器"):
        st.session_state.validator = XMLTagValidator(environment)
        st.success(f"已切换到 {environment} 环境")

    st.markdown("---")
    st.markdown("### 📋 功能说明")
    st.markdown("""
    1. **验证标签**：检测XML标签完整性
    2. **查看错误**：显示详细错误信息
    3. **AI修复**：使用大模型自动修复
    4. **特殊规则**：
       - `<current>` 只能出现1对
       - `<accumulate>` 只能出现1对
       - 检测标签交叉问题
    """)

# 主界面分为两列
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📝 待验证的XML文本")
    test_text = st.text_area(
        "输入XML文本",
        value=st.session_state.input_text,
        height=400,
        placeholder="请输入XML文本...",
        key="test_text_input"
    )

    # 更新 session_state
    if test_text != st.session_state.input_text:
        st.session_state.input_text = test_text

    # 按钮行
    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        validate_btn = st.button("🔍 验证标签", type="primary", use_container_width=True)
    with btn_col2:
        clear_text_btn = st.button("🗑️ 清除文本", use_container_width=True)

    if clear_text_btn:
        st.session_state.input_text = ""
        st.session_state.validation_result = None
        st.session_state.fix_result = None
        st.rerun()

with col2:
    st.subheader("📄 参考模板（可选）")
    template = st.text_area(
        "输入参考模板",
        value=st.session_state.template_text,
        height=400,
        placeholder="可选：输入参考模板用于AI修复...",
        key="template_input"
    )

    # 更新 session_state
    if template != st.session_state.template_text:
        st.session_state.template_text = template

    # 按钮行
    clear_template_btn = st.button("🗑️ 清除模板", use_container_width=True)

    if clear_template_btn:
        st.session_state.template_text = ""
        st.rerun()

# 验证按钮逻辑
if validate_btn:
    if st.session_state.input_text.strip():
        with st.spinner("正在验证..."):
            result = st.session_state.validator.validate(st.session_state.input_text)
            st.session_state.validation_result = result
            st.session_state.fix_result = None  # 清除之前的修复结果
    else:
        st.warning("请先输入XML文本")

# 验证结果展示
if st.session_state.validation_result:
    st.markdown("---")
    result = st.session_state.validation_result

    # 总览
    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)

    with col_stat1:
        st.metric("总标签数", result['total_tags'])

    with col_stat2:
        st.metric("匹配对数", result['matched_pairs'])

    with col_stat3:
        st.metric("错误数", len(result['errors']))

    with col_stat4:
        if result['is_valid']:
            st.success("✅ 标签完整")
        else:
            st.error("❌ 标签不完整")

    # 详细错误信息
    if not result['is_valid']:
        st.subheader("🚨 错误详情")

        # 错误分类
        missing_open = [e for e in result['errors'] if e['type'] == 'missing_open']
        missing_close = [e for e in result['errors'] if e['type'] == 'missing_close']
        crossing = [e for e in result['errors'] if e['type'] == 'tag_crossing']
        multiple = [e for e in result['errors'] if e['type'] in ('multiple_open', 'multiple_close')]

        # 使用标签页展示不同类型的错误
        tabs = st.tabs([
            f"❌ 缺少开始标签 ({len(missing_open)})",
            f"❌ 缺少结束标签 ({len(missing_close)})",
            f"⚠️ 标签交叉 ({len(crossing)})",
            f"🔁 标签重复 ({len(multiple)})"
        ])

        with tabs[0]:  # 缺少开始标签
            if missing_open:
                for error in missing_open:
                    st.error(f"**第 {error['line_number']} 行**: {error['message']}")
            else:
                st.info("无此类错误")

        with tabs[1]:  # 缺少结束标签
            if missing_close:
                for error in missing_close:
                    st.error(f"**第 {error['line_number']} 行**: {error['message']}")
            else:
                st.info("无此类错误")

        with tabs[2]:  # 标签交叉
            if crossing:
                for error in crossing:
                    crossed_tags = error.get('crossed_tags', [])
                    st.warning(f"**第 {error['line_number']} 行**: {error['message']}")
                    if crossed_tags:
                        st.code(f"跨越的标签: {', '.join(crossed_tags)}")
            else:
                st.info("无此类错误")

        with tabs[3]:  # 标签重复
            if multiple:
                for error in multiple:
                    st.warning(f"**{error['message']}**")
            else:
                st.info("无此类错误")

        # AI修复按钮
        st.markdown("---")
        col_fix1, col_fix2, col_fix3 = st.columns([1, 1, 2])

        with col_fix1:
            if st.button("🤖 使用AI修复", type="secondary", use_container_width=True):
                with st.spinner("AI正在修复标签..."):
                    # 异步调用修复
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    fix_result = loop.run_until_complete(
                        st.session_state.validator.model_fix_tag(
                            st.session_state.input_text,
                            st.session_state.template_text if st.session_state.template_text.strip() else None,
                            result
                        )
                    )
                    loop.close()
                    st.session_state.fix_result = fix_result

        with col_fix2:
            if st.button("🔄 重新验证", use_container_width=True):
                with st.spinner("正在验证..."):
                    result = st.session_state.validator.validate(st.session_state.input_text)
                    st.session_state.validation_result = result
                    st.rerun()

# AI修复结果展示
if st.session_state.fix_result:
    st.markdown("---")
    st.subheader("🤖 AI修复结果")

    fix_result = st.session_state.fix_result

    if fix_result['status'] == 'success':
        # 显示修复后的内容
        st.success("✅ 修复成功！")

        col_res1, col_res2, col_res3 = st.columns(3)
        with col_res1:
            st.metric("响应时间", f"{fix_result['response_time']:.2f}s")
        with col_res2:
            st.metric("输入Token", fix_result['prompt_tokens'])
        with col_res3:
            st.metric("输出Token", fix_result['completion_tokens'])

        st.markdown("### 修复后的文本")
        fixed_content = fix_result['content']
        st.code(fixed_content, language="xml", line_numbers=True)

        # 操作按钮
        fix_btn_col1, fix_btn_col2, fix_btn_col3 = st.columns(3)

        with fix_btn_col1:
            st.download_button(
                label="📥 下载修复后的文本",
                data=fixed_content,
                file_name="fixed_xml.txt",
                mime="text/plain",
                use_container_width=True
            )

        with fix_btn_col2:
            if st.button("🔍 验证修复后的内容", use_container_width=True):
                with st.spinner("正在验证修复结果..."):
                    verify_result = st.session_state.validator.validate(fixed_content)
                    if verify_result['is_valid']:
                        st.success("✅ 修复后的内容标签完整！")
                    else:
                        st.warning(f"⚠️ 修复后仍有 {len(verify_result['errors'])} 个错误")
                        with st.expander("查看剩余错误"):
                            for error in verify_result['errors']:
                                st.text(error['message'])

        with fix_btn_col3:
            if st.button("✏️ 应用修复结果", use_container_width=True):
                st.session_state.input_text = fixed_content
                st.session_state.validation_result = None
                st.session_state.fix_result = None
                st.success("已将修复结果应用到输入框")
                st.rerun()
    else:
        st.error(f"❌ 修复失败: {fix_result['message']}")

# 示例数据
st.markdown("---")
with st.expander("📚 查看示例数据"):
    example_col1, example_col2, example_col3 = st.columns(3)

    with example_col1:
        st.markdown("**示例1：缺少结束标签**")
        example1 = """<summary>异常趋势</summary>
<current>
  <gpm_task>内容</gpm_task>
  <gpm_yoy>内容
</current>"""
        st.code(example1, language="xml")

        if st.button("📋 加载示例1", use_container_width=True):
            st.session_state.input_text = example1
            st.session_state.validation_result = None
            st.session_state.fix_result = None
            st.rerun()

    with example_col2:
        st.markdown("**示例2：标签交叉**")
        example2 = """<accumulate>
  <summary>累计</summary>
  <gpm_baseline>内容
  </accumulate>
</gpm_baseline>"""
        st.code(example2, language="xml")

        if st.button("📋 加载示例2", use_container_width=True):
            st.session_state.input_text = example2
            st.session_state.validation_result = None
            st.session_state.fix_result = None
            st.rerun()

    with example_col3:
        st.markdown("**示例3：标签重复**")
        example3 = """<accumulate>
  <current>内容</current>
</accumulate>
</accumulate>"""
        st.code(example3, language="xml")

        if st.button("📋 加载示例3", use_container_width=True):
            st.session_state.input_text = example3
            st.session_state.validation_result = None
            st.session_state.fix_result = None
            st.rerun()

# 页脚
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: gray;'>XML标签验证工具 v1.0 | Powered by Deepseek-V3</div>",
    unsafe_allow_html=True
)
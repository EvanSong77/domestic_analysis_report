import streamlit as st
import json
import yaml
import os
from pathlib import Path
from datetime import datetime
import zipfile
import io

st.set_page_config(page_title="配置文件管理系统", layout="wide", initial_sidebar_state="expanded")

# 页面样式
st.markdown("""
    <style>
        .main {
            background-color: #f5f5f5;
        }
        .stMetric {
            background-color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
    </style>
""", unsafe_allow_html=True)

# 初始化状态
if "refresh" not in st.session_state:
    st.session_state.refresh = False

# 配置文件存储路径
CONFIG_DIR = "./"
if not os.path.exists(CONFIG_DIR):
    os.makedirs(CONFIG_DIR)


def get_config_files():
    """获取所有配置文件"""
    files = {}
    for root, dirs, filenames in os.walk(CONFIG_DIR):
        for filename in filenames:
            if filename.endswith(('.json', '.yaml', '.yml')):
                rel_path = os.path.relpath(os.path.join(root, filename), CONFIG_DIR)
                files[rel_path] = os.path.join(root, filename)
    return files


def read_config_file(filepath):
    """读取配置文件"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            if filepath.endswith('.json'):
                return json.load(f), 'json'
            elif filepath.endswith(('.yaml', '.yml')):
                return yaml.safe_load(f), 'yaml'
            else:
                return f.read(), 'text'
    except Exception as e:
        st.error(f"读取文件失败: {e}")
        return None, None


def save_config_file(filepath, content, file_type):
    """保存配置文件"""
    try:
        os.makedirs(os.path.dirname(filepath) or CONFIG_DIR, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            if file_type == 'json':
                json.dump(content, f, ensure_ascii=False, indent=2)
            elif file_type == 'yaml':
                yaml.dump(content, f, allow_unicode=True, default_flow_style=False)
            else:
                f.write(content)
        return True
    except Exception as e:
        st.error(f"保存文件失败: {e}")
        return False


# 侧边栏
st.sidebar.title("🎛️ 配置管理系统")
st.sidebar.markdown("---")

config_files = get_config_files()
col1, col2 = st.sidebar.columns(2)
with col1:
    st.metric("📁 文件总数", len(config_files))
with col2:
    st.metric("📊 文件夹数", len(set(os.path.dirname(f) for f in config_files.keys())))

st.sidebar.markdown("---")

# 导航菜单 - 检查是否有编辑任务
default_nav = 0
if "edit_file_path" in st.session_state and st.session_state.edit_file_path:
    default_nav = 2  # 跳转到编辑配置

nav_option = st.sidebar.radio(
    "选择功能",
    ["📋 文件列表", "➕ 创建新配置", "✏️ 编辑配置", "📊 对比配置", "⚙️ 批量操作"],
    index=default_nav
)

st.sidebar.markdown("---")
st.sidebar.info(f"📂 配置路径: `{os.path.abspath(CONFIG_DIR)}`")

# ==================== 文件列表 ====================
if nav_option == "📋 文件列表":
    st.title("📋 配置文件列表")

    if config_files:
        search_term = st.text_input("🔍 搜索文件", "")

        filtered_files = {k: v for k, v in config_files.items() if search_term.lower() in k.lower()}

        if filtered_files:
            st.subheader(f"找到 {len(filtered_files)} 个文件")

            for rel_path, full_path in sorted(filtered_files.items()):
                col1, col2, col3, col4 = st.columns([2, 1, 1, 1.5])

                file_size = os.path.getsize(full_path) / 1024
                file_type = full_path.split('.')[-1].upper()
                file_time = datetime.fromtimestamp(os.path.getmtime(full_path)).strftime("%Y-%m-%d %H:%M:%S")

                with col1:
                    st.caption(f"📄 **{rel_path}**")
                    st.caption(f"_修改时间: {file_time}_")

                with col2:
                    st.caption(f"`{file_type}`")
                    st.caption(f"_{file_size:.1f}KB_")

                with col3:
                    if st.button("👁️ 查看", key=f"view_{rel_path}"):
                        st.session_state.view_file = rel_path

                with col4:
                    col_edit, col_copy, col_del = st.columns(3, gap="small")
                    with col_edit:
                        if st.button("✏️", key=f"edit_{rel_path}", help="编辑"):
                            st.session_state.edit_file_path = rel_path
                            st.rerun()
                    with col_copy:
                        if st.button("📋", key=f"copy_{rel_path}", help="复制"):
                            content, ftype = read_config_file(full_path)
                            if ftype == 'json':
                                st.session_state.clipboard = json.dumps(content, ensure_ascii=False, indent=2)
                            elif ftype == 'yaml':
                                st.session_state.clipboard = yaml.dump(content, allow_unicode=True)
                            st.success("已复制到剪贴板")
                    with col_del:
                        if st.button("🗑️", key=f"del_{rel_path}", help="删除"):
                            os.remove(full_path)
                            st.success(f"✅ 已删除: {rel_path}")
                            st.rerun()

                st.divider()

            # 查看文件详情
            if "view_file" in st.session_state:
                rel_path = st.session_state.view_file
                if rel_path in config_files:
                    full_path = config_files[rel_path]
                    content, file_type = read_config_file(full_path)

                    st.subheader(f"📄 查看: {rel_path}")

                    tab1, tab2 = st.tabs(["格式化", "原始文本"])

                    with tab1:
                        if file_type == 'json' or file_type == 'yaml':
                            st.json(content)
                        else:
                            st.code(content, language="text")

                    with tab2:
                        if file_type == 'json':
                            st.code(json.dumps(content, ensure_ascii=False, indent=2), language="json")
                        elif file_type == 'yaml':
                            st.code(yaml.dump(content, allow_unicode=True), language="yaml")

                    # 下载按钮
                    col1, col2 = st.columns(2)
                    with col1:
                        if file_type == 'json':
                            st.download_button(
                                "⬇️ 下载 JSON",
                                json.dumps(content, ensure_ascii=False, indent=2),
                                file_name=os.path.basename(full_path),
                                mime="application/json"
                            )
                    with col2:
                        if file_type == 'yaml':
                            st.download_button(
                                "⬇️ 下载 YAML",
                                yaml.dump(content, allow_unicode=True),
                                file_name=os.path.basename(full_path),
                                mime="application/yaml"
                            )
        else:
            st.warning("🔍 未找到匹配的文件")
    else:
        st.info("📭 暂无配置文件，请创建一个新配置")

# ==================== 创建新配置 ====================
elif nav_option == "➕ 创建新配置":
    st.title("➕ 创建新配置文件")

    col1, col2, col3 = st.columns(3)

    with col1:
        file_name = st.text_input("📄 文件名", placeholder="config", value="config")

    with col2:
        file_type = st.selectbox("📋 文件类型", ["JSON", "YAML"])

    with col3:
        folder_path = st.text_input("📁 文件夹", value=".", placeholder=".")

    st.divider()

    # 模板选择
    template = st.selectbox("📌 选择模板", [
        "空白",
        "基础配置",
        "数据模板",
        "模型配置",
        "自定义"
    ])

    # 内容编辑
    if file_type == "JSON":
        if template == "空白":
            default_content = "{}"
        elif template == "基础配置":
            default_content = json.dumps({
                "version": "1.0.0",
                "environment": "test",
                "debug": False,
                "language": "zh"
            }, ensure_ascii=False, indent=2)
        elif template == "数据模板":
            default_content = json.dumps({
                "TOTAL": {"ORG": {}, "CHAN": {}},
                "PROVINCE": {"ORG": {}}
            }, ensure_ascii=False, indent=2)
        elif template == "模型配置":
            default_content = json.dumps({
                "models": {
                    "name": "qwen3-32b",
                    "max_tokens": 10240,
                    "temperature": 0,
                    "top_p": 0.95
                }
            }, ensure_ascii=False, indent=2)
        else:
            default_content = "{}"

        content = st.text_area("编辑内容 (JSON)", value=default_content, height=300)

        if st.button("💾 创建配置", use_container_width=True):
            try:
                parsed = json.loads(content)
                full_path = os.path.join(CONFIG_DIR, folder_path, f"{file_name}.json") if folder_path != "." else os.path.join(CONFIG_DIR, f"{file_name}.json")
                if save_config_file(full_path, parsed, 'json'):
                    st.success(f"✅ 配置已创建: {os.path.relpath(full_path, CONFIG_DIR)}")
                    st.rerun()
            except json.JSONDecodeError as e:
                st.error(f"❌ JSON格式错误: {e}")

    else:  # YAML
        if template == "空白":
            default_content = ""
        elif template == "基础配置":
            default_content = """version: "1.0.0"
environment: "test"
debug: false
language: "zh"
"""
        elif template == "数据模板":
            default_content = """TOTAL:
  ORG: {}
  CHAN: {}
PROVINCE:
  ORG: {}
"""
        elif template == "模型配置":
            default_content = """models:
  name: "qwen3-32b"
  max_tokens: 10240
  temperature: 0
  top_p: 0.95
"""
        else:
            default_content = ""

        content = st.text_area("编辑内容 (YAML)", value=default_content, height=300)

        if st.button("💾 创建配置", use_container_width=True):
            try:
                parsed = yaml.safe_load(content) or {}
                full_path = os.path.join(CONFIG_DIR, folder_path, f"{file_name}.yaml") if folder_path != "." else os.path.join(CONFIG_DIR, f"{file_name}.yaml")
                if save_config_file(full_path, parsed, 'yaml'):
                    st.success(f"✅ 配置已创建: {os.path.relpath(full_path, CONFIG_DIR)}")
                    st.rerun()
            except yaml.YAMLError as e:
                st.error(f"❌ YAML格式错误: {e}")

# ==================== 编辑配置 ====================
elif nav_option == "✏️ 编辑配置":
    st.title("✏️ 编辑配置文件")

    if config_files:
        # 检查是否从文件列表页面跳转过来
        default_file = None
        if "edit_file_path" in st.session_state:
            default_file = st.session_state.edit_file_path
            # 清除session_state中的标记，防止重复使用
            del st.session_state.edit_file_path

        file_list = list(config_files.keys())

        # 找到默认选中的文件索引
        default_index = 0
        if default_file and default_file in file_list:
            default_index = file_list.index(default_file)

        selected_file = st.selectbox("选择要编辑的文件", file_list, index=default_index, key="edit_file_selector")

        if selected_file and selected_file in config_files:
            full_path = config_files[selected_file]
            content, file_type = read_config_file(full_path)

            if content is not None:
                st.subheader(f"正在编辑: {selected_file}")
                st.info(f"📝 类型: `{file_type.upper()}` | 📁 路径: `{full_path}`")

                # 初始化编辑器内容
                editor_key = f"editor_{selected_file}"
                if editor_key not in st.session_state:
                    if file_type == 'json':
                        st.session_state[editor_key] = json.dumps(content, ensure_ascii=False, indent=2)
                    elif file_type == 'yaml':
                        st.session_state[editor_key] = yaml.dump(content, allow_unicode=True, default_flow_style=False)
                    else:
                        st.session_state[editor_key] = content

                if file_type == 'json':
                    edited_content_str = st.text_area(
                        "编辑JSON内容",
                        value=st.session_state[editor_key],
                        height=400,
                        key=editor_key
                    )

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("💾 保存修改", use_container_width=True, key=f"save_{selected_file}"):
                            try:
                                edited_content = json.loads(edited_content_str)
                                if save_config_file(full_path, edited_content, 'json'):
                                    st.success("✅ 配置已保存")
                                    # 更新session_state
                                    st.session_state[editor_key] = edited_content_str
                            except json.JSONDecodeError as e:
                                st.error(f"❌ JSON格式错误: {str(e)}")

                    with col2:
                        st.download_button(
                            "⬇️ 下载",
                            edited_content_str,
                            file_name=os.path.basename(full_path),
                            mime="application/json",
                            use_container_width=True,
                            key=f"download_{selected_file}"
                        )

                    with col3:
                        if st.button("↻ 重置", use_container_width=True, key=f"reset_{selected_file}"):
                            st.session_state[editor_key] = json.dumps(content, ensure_ascii=False, indent=2)
                            st.rerun()

                elif file_type == 'yaml':
                    edited_content_str = st.text_area(
                        "编辑YAML内容",
                        value=st.session_state[editor_key],
                        height=400,
                        key=editor_key
                    )

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("💾 保存修改", use_container_width=True, key=f"save_{selected_file}"):
                            try:
                                edited_content = yaml.safe_load(edited_content_str)
                                if save_config_file(full_path, edited_content, 'yaml'):
                                    st.success("✅ 配置已保存")
                                    # 更新session_state
                                    st.session_state[editor_key] = edited_content_str
                            except yaml.YAMLError as e:
                                st.error(f"❌ YAML格式错误: {str(e)}")

                    with col2:
                        st.download_button(
                            "⬇️ 下载",
                            edited_content_str,
                            file_name=os.path.basename(full_path),
                            mime="application/yaml",
                            use_container_width=True,
                            key=f"download_{selected_file}"
                        )

                    with col3:
                        if st.button("↻ 重置", use_container_width=True, key=f"reset_{selected_file}"):
                            st.session_state[editor_key] = yaml.dump(content, allow_unicode=True, default_flow_style=False)
                            st.rerun()

                else:
                    edited_content_str = st.text_area(
                        "编辑文本内容",
                        value=st.session_state[editor_key],
                        height=400,
                        key=editor_key
                    )

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("💾 保存修改", use_container_width=True, key=f"save_{selected_file}"):
                            if save_config_file(full_path, edited_content_str, 'text'):
                                st.success("✅ 文本已保存")
                                st.session_state[editor_key] = edited_content_str

                    with col2:
                        st.download_button(
                            "⬇️ 下载",
                            edited_content_str,
                            file_name=os.path.basename(full_path),
                            use_container_width=True,
                            key=f"download_{selected_file}"
                        )

                    with col3:
                        if st.button("↻ 重置", use_container_width=True, key=f"reset_{selected_file}"):
                            st.session_state[editor_key] = content
                            st.rerun()
            else:
                st.error("❌ 无法读取文件内容")
        else:
            st.warning("⚠️ 请选择一个有效的文件")
    else:
        st.info("📭 暂无配置文件")

# ==================== 配置对比 ====================
elif nav_option == "📊 对比配置":
    st.title("📊 配置文件对比")

    if len(config_files) >= 2:
        col1, col2 = st.columns(2)

        with col1:
            file1 = st.selectbox("文件1️⃣", list(config_files.keys()), key="file1")

        with col2:
            file2 = st.selectbox("文件2️⃣", list(config_files.keys()), key="file2")

        if file1 and file2 and file1 != file2:
            if st.button("🔍 开始对比", use_container_width=True):
                content1, type1 = read_config_file(config_files[file1])
                content2, type2 = read_config_file(config_files[file2])

                col1, col2 = st.columns(2)

                with col1:
                    st.subheader(f"📄 {file1}")
                    if type1 == 'json' or type1 == 'yaml':
                        st.json(content1)
                    else:
                        st.code(content1)

                with col2:
                    st.subheader(f"📄 {file2}")
                    if type2 == 'json' or type2 == 'yaml':
                        st.json(content2)
                    else:
                        st.code(content2)

                # 差异分析
                if type1 == type2 and type1 in ['json', 'yaml']:
                    st.divider()
                    st.subheader("📝 差异分析")
                    if content1 == content2:
                        st.success("✅ 两个文件内容完全相同")
                    else:
                        st.warning("⚠️ 文件存在差异")
    else:
        st.info("📭 需要至少2个文件来进行对比")

# ==================== 批量操作 ====================
elif nav_option == "⚙️ 批量操作":
    st.title("⚙️ 批量操作")

    operation = st.selectbox("选择操作", ["上传文件", "导出全部", "批量删除", "文件夹管理"])

    if operation == "上传文件":
        st.subheader("📤 上传配置文件")
        uploaded_files = st.file_uploader("选择文件", accept_multiple_files=True, type=['json', 'yaml', 'yml'])

        target_folder = st.text_input("目标文件夹", value="", placeholder="子文件夹名(可选)")

        if uploaded_files and st.button("上传", use_container_width=True):
            for uploaded_file in uploaded_files:
                if target_folder:
                    file_path = os.path.join(CONFIG_DIR, target_folder, uploaded_file.name)
                else:
                    file_path = os.path.join(CONFIG_DIR, uploaded_file.name)

                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())

                st.success(f"✅ {uploaded_file.name}")
            st.rerun()

    elif operation == "导出全部":
        st.subheader("📥 导出所有配置")

        if st.button("打包导出", use_container_width=True):
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zipf:
                for rel_path, full_path in config_files.items():
                    zipf.write(full_path, arcname=rel_path)

            zip_buffer.seek(0)
            st.download_button(
                "⬇️ 下载压缩包",
                zip_buffer.getvalue(),
                file_name="configs_export.zip",
                mime="application/zip",
                use_container_width=True
            )

    elif operation == "批量删除":
        st.subheader("🗑️ 批量删除")

        files_to_delete = st.multiselect("选择要删除的文件", list(config_files.keys()))

        if files_to_delete:
            if st.button("⚠️ 确认删除", use_container_width=True, type="secondary"):
                for file in files_to_delete:
                    os.remove(config_files[file])
                    st.success(f"✅ {file}")
                st.rerun()

    elif operation == "文件夹管理":
        st.subheader("📁 文件夹管理")

        # 显示文件夹结构
        st.write("**当前文件夹结构:**")
        for root, dirs, files in os.walk(CONFIG_DIR):
            level = root.replace(CONFIG_DIR, '').count(os.sep)
            indent = '   ' * level
            folder_name = os.path.basename(root)
            st.text(f"{indent}📁 {folder_name or 'configs'}/")

            subindent = '   ' * (level + 1)
            for file in files:
                if file.endswith(('.json', '.yaml', '.yml')):
                    st.text(f"{subindent}📄 {file}")

        # 新建文件夹
        st.divider()
        new_folder = st.text_input("新建文件夹", placeholder="my_configs")

        if st.button("✅ 创建", use_container_width=True):
            folder_path = os.path.join(CONFIG_DIR, new_folder)
            os.makedirs(folder_path, exist_ok=True)
            st.success(f"✅ 文件夹已创建: {new_folder}")
            st.rerun()
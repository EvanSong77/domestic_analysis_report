import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
import yaml

# 数据库连接器 - 根据你的实际路径调整
try:
    # 假设这里的路径是正确的，如果实际运行时报错，需要用户检查路径
    from src.utils.db_utils import MySQLDataConnector
except ImportError:
    st.error("❌ 无法导入数据库连接器,请检查路径")
    MySQLDataConnector = None

# 设置页面配置
st.set_page_config(
    page_title="📊 国内分析报告生成系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 加载配置
@st.cache_data
def load_config():
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {}


config = load_config()

# API基础URL
API_BASE_URL = f"http://0.0.0.0:{config.get('app', {}).get('port', 12576)}"
SECURITY_TOKEN = config.get('security', {}).get('bearer_token', '')

# 页面样式 (保留并微调)
st.markdown("""
    <style>
        .main {
            background-color: #f5f7fa;
            padding-top: 2rem;
        }
        .stMetric {
            background-color: white;
            padding: 1rem;
            border-radius: 0.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            text-align: center;
        }
        .stButton > button {
            border-radius: 0.5rem;
            font-weight: bold;
            height: 3rem; /* 增加按钮高度 */
        }
        .report-card {
            background: white;
            padding: 1.5rem;
            border-radius: 0.75rem; /* 略微增加圆角 */
            box-shadow: 0 4px 12px rgba(0,0,0,0.08); /* 增加阴影深度 */
            margin-bottom: 2rem;
        }
        .status-processing {
            color: #f97316;
            font-weight: bold;
        }
        .status-completed {
            color: #10b981;
            font-weight: bold;
        }
        .status-error {
            color: #ef4444;
            font-weight: bold;
        }
        .progress-container {
            background: #e0f2fe; /* 进度区域背景色 */
            padding: 2rem;
            border-radius: 0.75rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            margin: 1.5rem 0;
            border-left: 5px solid #0369a1;
        }
    </style>
""", unsafe_allow_html=True)


# 查询报告结果的函数
def query_report_result(req_id):
    """从数据库查询报告生成结果"""
    if not MySQLDataConnector:
        return None

    try:
        query_sql = f"""SELECT * FROM tb_bij_operation_diagnosis_req_result WHERE REQ_ID = '{req_id}' ORDER BY CREATE_TIME DESC LIMIT 1"""

        db_connector = MySQLDataConnector(db_type="intermediate_db")
        db_connector.connect_database()

        # 假设执行查询后返回的是包含结果的列表
        db_connector.execute_query(query_sql)
        data_result = db_connector.query_results
        db_connector.close_connection()

        if data_result and len(data_result) > 0:
            # 假设 REPORT_RESULT_JSON 字段在第一个结果的第一个元素中
            data_str = data_result[0]['resp_result']

            if isinstance(data_str, str):
                try:
                    return json.loads(data_str)
                except json.JSONDecodeError:
                    st.error("❌ 数据库查询结果非有效JSON格式")
                    return None
            return data_str

        return None
    except Exception as e:
        st.error(f"❌ 查询数据库失败: {str(e)}")
        return None


# 检查任务状态的函数
def check_task_status(instance_id):
    """检查任务处理状态"""
    try:
        headers = {"Authorization": f"Bearer {SECURITY_TOKEN}"}
        response = requests.get(
            f"{API_BASE_URL}/fin-report/diagnosis/status/{instance_id}",
            headers=headers,
            timeout=20
        )

        if response.status_code == 200:
            result = response.json()
            return result.get("data", {})
        return None
    except Exception as e:
        # 如果是连接超时，不显示红色错误，改为黄色警告
        if "timeout" in str(e).lower():
            st.warning(f"⚠️ 检查状态连接超时: {str(e)}")
        else:
            st.error(f"❌ 检查状态失败: {str(e)}")
        return None


# 侧边栏
st.sidebar.title("🚀 国内分析报告系统")
st.sidebar.markdown("---")

# 侧边栏状态显示
if "req_id" in st.session_state and st.session_state.req_id:
    st.sidebar.markdown(f"**📋 请求ID**")
    st.sidebar.code(st.session_state.req_id[:20] + "...")
    status_emoji = {
        "idle": "⚪",
        "processing": "🔄",
        "completed": "✅",
        "error": "❌"
    }
    st.sidebar.markdown(f"**{status_emoji.get(st.session_state.status, '⚪')} 当前状态:** `{st.session_state.status.upper()}`")

st.sidebar.markdown("---")
st.sidebar.header("ℹ️ 系统信息")
st.sidebar.info(f"⚙️ 环境: **{config.get('environment', 'test')}**")
st.sidebar.info(f"🔢 版本: **{config.get('version', '1.0.0')}**")

# 主页面标题
st.title("📈 运营诊断报告生成平台")
st.markdown("高效、智能地生成国内运营分析报告。")
st.markdown("---")

# 初始化会话状态
if "req_id" not in st.session_state:
    st.session_state.req_id = ""
if "instance_id" not in st.session_state:
    st.session_state.instance_id = ""
if "status" not in st.session_state:
    st.session_state.status = "idle"
if "report_result" not in st.session_state:
    st.session_state.report_result = None
if "start_time" not in st.session_state:
    st.session_state.start_time = None
if "polling_active" not in st.session_state:
    st.session_state.polling_active = False
if "request_params" not in st.session_state:
    st.session_state.request_params = {}

# 报告生成表单
with st.form("report_generation_form"):
    st.subheader("📝 报告生成参数")
    st.markdown("请填写报告生成所需的关键信息。")

    col1, col2, col3 = st.columns(3)

    with col1:
        period = st.text_input("📅 期间", placeholder="如: 202506", value="202506", help="必填：指定报告分析的时间段。")

    with col2:
        diagnosis_type = st.selectbox(
            "🔍 分析类型",
            ["", "ORG", "CHAN", "IND", "PROD"],
            help="报告关注的主题，可留空进行通用分析。ORG: 组织, CHAN: 渠道, IND: 指标, PROD: 产品"
        )

    with col3:
        province_name = st.text_input("🏙️ 省份", placeholder="如: 浙江省", value="", help="留空则默认为全国维度。")

    col4, col5 = st.columns([1, 2])
    with col4:
        office_lv2_name = st.text_input("🏢 二级办", placeholder="如: 杭州办", value="", help="仅在指定省份时有效，留空则为该省全部二级办。")

    # --- 表单操作区域 ---
    st.markdown("---")

    # 在 Form 内，仅使用 st.form_submit_button
    col_submit, _, _ = st.columns([2, 1.5, 1])

    with col_submit:
        submit_button = st.form_submit_button(
            "🚀 提交生成请求",
            type="primary",
            disabled=st.session_state.status == "processing"
        )
# --- ⬆️ st.form 结束 ⬆️ ---


# 提交后的逻辑处理 (放在 Form 块之后)
if submit_button:
    if not period:
        st.error("❌ 期间为必填项，请检查输入。")
    else:
        req_id = f"{datetime.now().strftime('%Y%m%d%H%M%f')[:16]}"

        request_data = {
            "reqId": req_id,
            "period": period,
            "diagnosisType": diagnosis_type,
            "provinceName": province_name,
            "officeLv2Name": office_lv2_name,
            "currentPage": 1,
            "pageSize": 10
        }

        headers = {
            "Authorization": f"Bearer {SECURITY_TOKEN}",
            "Content-Type": "application/json"
        }

        try:
            with st.spinner("📤 正在提交请求到后端服务..."):
                response = requests.post(
                    f"{API_BASE_URL}/fin-report/diagnosis",
                    json=request_data,
                    headers=headers,
                    timeout=10
                )

            if response.status_code == 200:
                result = response.json()
                st.session_state.req_id = req_id
                st.session_state.instance_id = result.get("data", {}).get("instanceId", "")
                st.session_state.status = "processing"
                st.session_state.start_time = time.time()
                st.session_state.report_result = None
                st.session_state.polling_active = True
                st.session_state.request_params = request_data
                st.success(f"✅ 报告生成请求已提交! 实例ID: `{st.session_state.instance_id[:10]}...`")
                st.balloons()
                st.rerun()
            elif response.status_code == 429:
                st.error("❌ 系统繁忙,当前并发过高，请稍后重试。")
            else:
                error_detail = response.json().get("message", "未知错误") if response.content else "无详细信息"
                st.error(f"❌ 请求失败: {response.status_code}. 详情: {error_detail}")
        except Exception as e:
            st.error(f"❌ 请求异常: {str(e)}")

# 放置在表单外的操作按钮 (修复 NameError 的关键)
st.markdown("---")
st.subheader("🛠️ 任务管理")

# 手动查询区域
with st.expander("🔍 手动查询报告结果", expanded=False):
    st.markdown("输入请求ID来查询已生成的报告结果")

    col_input, col_button = st.columns([3, 1])

    with col_input:
        manual_req_id = st.text_input(
            "请求ID",
            value=st.session_state.req_id if st.session_state.req_id else "",
            placeholder="输入完整的请求ID，如: 2025121512345678",
            help="输入要查询的请求ID，留空则使用当前会话的请求ID",
            key="manual_req_id_input"
        )

    with col_button:
        st.markdown("<br>", unsafe_allow_html=True)  # 添加垂直间距对齐
        query_button = st.button("🔄 查询", use_container_width=True, type="primary")

    if query_button:
        # 确定要查询的 req_id
        req_id_to_query = manual_req_id.strip() if manual_req_id.strip() else st.session_state.req_id

        if not req_id_to_query:
            st.warning("⚠️ 请输入请求ID或先提交报告生成请求。")
        else:
            with st.spinner(f"🔍 正在查询请求ID: {req_id_to_query}..."):
                result = query_report_result(req_id_to_query)

                if result:
                    # 更新会话状态
                    st.session_state.req_id = req_id_to_query
                    st.session_state.report_result = result
                    st.session_state.status = "completed"
                    st.session_state.polling_active = False

                    st.success(f"✅ 报告结果查询成功! 请求ID: `{req_id_to_query}`")
                    st.balloons()
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error(f"❌ 未找到请求ID `{req_id_to_query}` 的报告结果。\n\n可能原因：\n- 任务仍在处理中\n- 请求ID不存在\n- 任务已失败")

# 重置按钮单独放置
col_reset, _ = st.columns([1, 4])
with col_reset:
    if st.button("🗑️ 重置会话", use_container_width=True):
        # 重置所有状态
        keys_to_reset = ["req_id", "instance_id", "status", "report_result", "start_time", "polling_active", "request_params"]
        for key in keys_to_reset:
            if key in st.session_state:
                del st.session_state[key]
        st.info("✅ 会话状态已重置。")
        time.sleep(0.5)
        st.rerun()
# 进度显示区域
if st.session_state.status == "processing":
    st.markdown("---")
    st.markdown("<div class='progress-container'>", unsafe_allow_html=True)
    st.subheader("⏳ 任务处理中...")

    # 显示进度信息
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📝 请求ID", st.session_state.req_id[:15] + "...")
    with col2:
        elapsed = int(time.time() - st.session_state.start_time) if st.session_state.start_time else 0
        st.metric("⏱️ 已用时间", f"**{elapsed}** 秒")
    with col3:
        st.metric("🆔 实例ID", st.session_state.instance_id[:10] + "...")

    # 进度条
    progress_bar = st.progress(0)
    status_text = st.empty()

    # 轮询检查状态的逻辑
    max_wait_time = 600
    poll_interval = 3

    if st.session_state.polling_active:

        elapsed_time = int(time.time() - st.session_state.start_time)

        # 实时更新进度条
        progress = min(elapsed_time / max_wait_time, 0.95)
        progress_bar.progress(progress)

        status_text.info(f"🔍 正在检查任务状态... (已等待 {elapsed_time}/{max_wait_time} 秒)")

        if elapsed_time < max_wait_time:
            task_status = check_task_status(st.session_state.instance_id)

            if task_status and task_status.get("status") == "completed":
                status_text.success("✅ 任务已完成，正在从数据库获取报告结果...")
                progress_bar.progress(1.0)

                # 查询结果
                time.sleep(1)
                result = query_report_result(st.session_state.req_id)

                if result:
                    st.session_state.report_result = result
                    st.session_state.status = "completed"
                    st.session_state.polling_active = False
                    st.success("🎉 报告生成与获取成功!")
                    st.toast("报告已生成！", icon="🎉")
                    time.sleep(1)
                    st.rerun()
                else:
                    status_text.warning("⚠️ 任务完成，但结果尚未写入数据库，继续等待...")
                    time.sleep(poll_interval)
                    st.rerun()

            elif task_status and task_status.get("status") == "error":
                st.session_state.status = "error"
                st.session_state.polling_active = False
                error_message = task_status.get("message", "任务处理失败，无详细信息")
                status_text.error(f"❌ 任务处理失败: {error_message}")

            else:
                # 任务仍在处理中，等待后重新运行
                time.sleep(poll_interval)
                st.rerun()
        else:
            # 超时处理
            st.session_state.status = "error"  # 标记为错误状态，停止轮询
            st.session_state.polling_active = False
            st.warning("⚠️ 任务处理时间超过最大等待时间，请稍后手动点击 '手动查询结果' 按钮。")

    st.markdown("</div>", unsafe_allow_html=True)

# 报告结果显示区域
if st.session_state.status == "completed" and st.session_state.report_result:
    st.markdown("---")
    st.header("✅ 最终报告结果")

    result_data = st.session_state.report_result

    st.subheader("📑 报告内容详情")

    if isinstance(result_data, list):
        for idx, item in enumerate(result_data):  # 改名为 item 更清晰
            with st.container():
                st.markdown("<div class='report-card'>", unsafe_allow_html=True)

                # 报告标题
                st.title(f"报告 #{idx + 1}")

                # 参数信息
                col_info1, col_info2, col_info3, col_info4 = st.columns(4)
                with col_info1:
                    st.markdown(f"**📅 期间:** `{item.get('period', 'N/A')}`")
                with col_info2:
                    st.markdown(f"**🔍 类型:** `{item.get('diagnosisType', 'N/A')}`")
                with col_info3:
                    st.markdown(f"**🏙️ 省份:** `{item.get('provinceName', '全国')}`")
                with col_info4:
                    st.markdown(f"**🏢 二级办:** `{item.get('officeLv2Name', '全部')}`")

                # 报告内容 - 需要从字典中提取实际内容
                st.divider()

                # 假设报告内容在 'content' 或 'report' 字段中
                # 你需要根据实际的数据结构调整这个字段名
                report_content = item.get('diagnosisResult') or item.get('report') or item.get('reportContent') or str(item)

                st.markdown(report_content)

                # 下载按钮
                st.markdown("---")
                col_dl1, col_dl2, _ = st.columns([1, 1, 3])
                with col_dl1:
                    st.download_button(
                        "📥 下载Markdown",
                        report_content if isinstance(report_content, str) else json.dumps(report_content, ensure_ascii=False, indent=2),
                        file_name=f"report_{idx + 1}_{st.session_state.req_id[:10]}.md",
                        mime="text/markdown",
                        key=f"download_md_{idx}"
                    )
                with col_dl2:
                    st.download_button(
                        "📥 下载JSON (完整数据)",
                        json.dumps(item, ensure_ascii=False, indent=2),  # 改为 item，只下载当前报告的数据
                        file_name=f"report_{idx + 1}_{st.session_state.req_id[:10]}.json",
                        mime="application/json",
                        key=f"download_json_{idx}"
                    )

                st.markdown("</div>", unsafe_allow_html=True)
    elif isinstance(result_data, str):
        # 单个报告
        st.markdown("<div class='report-card'>", unsafe_allow_html=True)
        st.markdown(result_data)
        st.markdown("</div>", unsafe_allow_html=True)

# 并发状态监控
st.markdown("---")
with st.expander("⚙️ 系统监控 (点击展开)"):
    col_status_button, _ = st.columns([1, 4])
    with col_status_button:
        if st.button("📊 刷新并发状态", key="refresh_concurrency"):
            try:
                headers = {"Authorization": f"Bearer {SECURITY_TOKEN}"}
                response = requests.get(
                    f"{API_BASE_URL}/fin-report/diagnosis/concurrency/status",
                    headers=headers,
                    timeout=20
                )

                if response.status_code == 200:
                    result = response.json()
                    concurrency_data = result.get("data", {})

                    st.markdown("#### 当前并发槽位状态")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("🔄 当前并发", concurrency_data.get("concurrency", {}).get("current", 0))
                    with col2:
                        st.metric("📊 最大并发", concurrency_data.get("concurrency", {}).get("max", 0))
                    with col3:
                        st.metric("✅ 可用槽位", concurrency_data.get("concurrency", {}).get("available", 0))
                    with col4:
                        utilization_str = concurrency_data.get("concurrency", {}).get("utilization")
                        if isinstance(utilization_str, (int, float)):
                            utilization_str = f"{utilization_str * 100:.1f}%"
                        elif not isinstance(utilization_str, str):
                            utilization_str = "0%"

                        st.metric("📈 利用率", utilization_str)

                    # 活跃任务
                    active_tasks = concurrency_data.get("activeTasks", {})
                    st.markdown("---")
                    st.subheader("📋 活跃任务列表")
                    if active_tasks:
                        tasks_data = []
                        for inst_id, task_info in active_tasks.items():
                            tasks_data.append({
                                "实例ID": inst_id[:12] + "...",
                                "所有者": task_info.get("owner", "未知")[:15] + "...",
                                "请求ID": task_info.get("reqId", "N/A")[:15] + "...",
                                "运行时间": task_info.get("runningTime", "0s")
                            })

                        st.dataframe(pd.DataFrame(tasks_data), use_container_width=True, height=200)
                    else:
                        st.success("✅ 当前没有正在运行的活跃任务。")
                else:
                    st.error(f"❌ 获取状态失败: {response.status_code}")
            except Exception as e:
                st.error(f"❌ 获取状态异常: {str(e)}")

# 页脚
st.markdown("---")
st.markdown("📊 **国内分析报告生成系统** | V1.2 | Powered by AI | © 2025 All Rights Reserved")

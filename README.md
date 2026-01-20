# 📊 国内经营异常诊断分析系统

基于AI的国内经营异常诊断分析系统，提供智能化的数据分析和报告生成服务，助力企业精准识别经营异常，优化业务决策。

---

## 📋 项目概述

### 系统定位
本系统是一套面向企业内部的智能化经营异常诊断平台，依托先进的AI模型和多维度数据分析能力，实现对省区、二级办等层级的经营状况自动诊断、分析和报告生成，为管理层提供数据驱动的决策支持。

### 核心价值
- **智能化诊断**：减少人工分析成本，提高诊断准确性和效率
- **多维度洞察**：从不同层级和角度分析经营数据，发现潜在问题
- **自动化报告**：实时生成标准化分析报告，提升报告质量和时效性
- **可扩展架构**：支持多环境部署和灵活扩展，适应业务发展需求
- **高可用性设计**：Celery + Redis分布式任务队列，确保系统稳定运行

---

## ✨ 核心特性

### 1. 智能诊断引擎
- 基于Qwen3-32b/Qwen3-235b大模型的经营异常智能诊断
- 支持多维度指标体系，覆盖财务、运营、市场等核心维度
- 支持分销类型（distribution_type）和IT包含类型（IT_include_type）参数化分析
- 自适应学习能力，持续优化诊断模型
- 支持定制化诊断规则和模型参数

### 2. 多维度数据分析
- 支持省区、二级办等多级组织架构分析
- 支持按期间（period）、诊断类型（diagnosisType）等多维度筛选
- 对接Doris大数据平台，支持海量数据查询和分析
- 提供异步数据查询服务，支持超时控制和连接管理

### 3. 自动化报告生成
- 实时生成结构化诊断分析报告
- 支持单个或多个报告批量生成
- 报告内容涵盖异常描述、根因分析、改进建议等
- 支持结果回调通知机制，可配置回调URL和认证信息

### 4. 任务管理功能
- 支持任务注册、状态查询和取消功能
- 提供按req_id查询任务状态接口
- 支持批量取消所有任务
- 任务执行过程支持取消检查，确保资源正确释放
- 任务管理器统一管理所有运行中的任务
- **Celery + Redis分布式任务队列**：支持真正的分布式任务处理和任务持久化
- **长任务支持**：支持超过3小时的长任务执行（task_time_limit=14400秒，task_soft_time_limit=12600秒）
- **实时状态同步**：任务状态实时同步到Redis，查询无卡顿
- **详细进度跟踪**：支持查看任务执行的各个阶段（数据查询、报告生成、结果保存、回调发送）

### 6. 安全可靠的系统设计
- Bearer Token认证机制，确保API安全访问
- 完善的权限控制体系，实现数据访问精细化管理
- 数据加密传输，保护敏感信息安全
- 详细的日志记录，便于问题追溯和审计
- 支持请求速率限制（rate_limit）

### 7. 多环境部署支持
- 支持test、uat、prod多环境隔离部署
- 容器化架构，确保环境一致性和部署效率
- 自动化部署脚本，简化部署流程
- 配置文件集中管理，支持动态切换环境
- 支持环境变量覆盖配置（APP_ENVIRONMENT）

---

## 🚀 快速开始

### 环境要求

| 依赖项 | 版本要求 | 用途 |
| ------ | -------- | ---- |
| Python | 3.10+ | 运行环境 |
| Docker | 20.10+ | 容器化部署 |
| Docker Compose | 1.29+ | 多容器编排 |
| MySQL | 8.0+ | 中间结果存储 |
| Doris | 最新 | 业务数据存储 |

### 本地开发

#### 1. 安装依赖
```bash
pip install -r script/requirements.txt
```

#### 2. 配置数据库
- 修改`config.yaml`中的数据库连接信息
- 确保MySQL中间表和Doris数据表可访问
- 配置文件支持多环境隔离，可通过`APP_ENVIRONMENT`环境变量切换

#### 3. 启动服务
```bash
python app.py
```

#### 4. 服务验证
- **服务地址**: `http://localhost:12576`
- **API文档**: `http://localhost:12576/docs` (Swagger UI)
- **健康检查**: `http://localhost:12576/health`

### 容器化部署

#### 1. 构建镜像
```bash
# Linux/macOS
sh script/uat/image_builder.sh
sh script/prod/image_builder.sh
```

#### 2. 部署到指定环境
```bash
# 测试环境
docker-compose -f script/uat/docker-compose.yml up -d
docker-compose -f script/prod/docker-compose.yml up -d
```

---

## 📄 API接口文档

### 1. 经营异常诊断结果计算

#### 请求信息

**请求URL**：`POST /fin-report/diagnosis`
**请求方法**：POST
**认证方式**：Bearer Token

**请求头（Headers）**

| 参数名称 | 类型 | 是否必须 | 示例 | 备注 |
|---------|------|----------|------|------|
| Content-Type | string | 是 | application/json | 请求体类型 |
| Authorization | string | 是 | Bearer yLZJJb8-EBsdUf2IimbGFNkaONMwbZy2WNh5luqpkWk | 鉴权Token |

**请求体（Body）**

```json
{
  "reqId": "202510151603000",
  "period": "202510",
  "diagnosisType": "1",
  "provinceName": "广东省",
  "officeLv2Name": "广州办",
  "distribution_type": "分销类型",
  "IT_include_type": "IT包含类型",
  "currentPage": 1,
  "pageSize": 10
}
```

| 参数名称 | 类型 | 是否必须 | 默认值 | 备注 |
|---------|------|----------|--------|------|
| reqId | string | 是 | "" | 请求唯一标识(时间戳) |
| period | string | 是 | "" | 分析期间，格式：YYYYMM |
| diagnosisType | string | 是 | "" | 分析类型 |
| provinceName | string | 是 | "" | 省份 |
| officeLv2Name | string | 是 | "" | 二级办 |
| distribution_type | string | 是 | "" | 分销类型 |
| IT_include_type | string | 是 | "" | IT包含类型 |
| currentPage | integer | 否 | 1 | 当前页码 |
| pageSize | integer | 否 | 10 | 每页条数 |

#### 响应数据

**成功响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": {
    "reqId": "202510151603000",
    "instanceId": "instance_12345678-1234-5678-1234-567812345678",
    "message": "诊断请求已接受，正在处理中",
    "status": "processing",
    "note": "使用 Celery + Redis 实现分布式任务队列，支持任务取消",
    "cancelEndpoint": "/fin-report/diagnosis/cancel/202510151603000",
    "statusEndpoint": "/fin-report/diagnosis/task/status/202510151603000",
    "cancelAllEndpoint": "/fin-report/diagnosis/cancel-all"
  }
}
```

### 2. 获取诊断任务状态

#### 请求信息

**请求URL**：`GET /fin-report/diagnosis/task/status/{req_id}`
**请求方法**：GET
**认证方式**：Bearer Token

**路径参数**

| 参数名称 | 类型 | 是否必须 | 示例 | 备注 |
|---------|------|----------|------|------|
| req_id | string | 是 | 202510151603000 | 请求ID |

#### 响应数据

**处理中响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": {
    "req_id": "202510151603000",
    "instance_id": "instance_12345678-1234-5678-1234-567812345678",
    "status": "processing",
    "running_time": 15,
    "cancellation_requested": false,
    "start_time": 1734326400,
    "task_done": false,
    "registered": true,
    "concurrent_status": {
      "slot_acquired": true,
      "concurrent_count": 1
    }
  }
}
```

**任务不存在响应**：
```json
{
  "code": 404,
  "msg": "任务不存在: 202510151603000",
  "data": null
}
```

### 3. 取消诊断任务

#### 请求信息

**请求URL**：`POST /fin-report/diagnosis/cancel/{req_id}`
**请求方法**：POST
**认证方式**：Bearer Token

**路径参数**

| 参数名称 | 类型 | 是否必须 | 示例 | 备注 |
|---------|------|----------|------|------|
| req_id | string | 是 | 202510151603000 | 请求ID |

#### 响应数据

**取消成功响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": {
    "cancelled": true,
    "message": "任务已成功取消",
    "req_id": "202510151603000"
  }
}
```

**任务不存在响应**：
```json
{
  "code": 404,
  "msg": "任务不存在: 202510151603000",
  "data": null
}
```

### 4. 批量取消所有任务

#### 请求信息

**请求URL**：`POST /fin-report/diagnosis/cancel-all`
**请求方法**：POST
**认证方式**：Bearer Token

#### 响应数据

**批量取消成功响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": {
    "cancelled": true,
    "message": "已成功取消 3/5 个任务",
    "total_tasks": 5,
    "cancelled_tasks": 3,
    "results": {
      "202510151603000": {
        "cancelled": true,
        "slot_released": true,
        "instance_id": "instance_12345678-1234-5678-1234-567812345678",
        "registered": true
      }
    }
  }
}
```

**没有任务可取消响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": {
    "cancelled": false,
    "message": "没有运行中的任务可取消",
    "results": {},
    "note": "系统当前没有运行中的任务"
  }
}
```

---

## 🔄 核心业务流程

### 诊断报告生成流程

```
┌─────────────────────────────────────────────────────────────────┐
│                     客户端请求                                    │
└─────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                     API接口层                                 │
│ 接收诊断请求 → 验证Token → 生成实例ID → 尝试获取并发槽位            │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                     并发控制层                               │
│ 槽位已满？ → 是 → 返回429错误                               │
│        ↓ 否                                                 │
│ 获取槽位 → 启动心跳保活 → 返回响应给客户端                   │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                     后台任务处理                             │
│ 1. 查询数据 → 2. 生成报告 → 3. 保存结果 → 4. 发送回调         │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                     资源释放                                 │
│ 取消心跳任务 → 释放并发槽位 → 记录处理完成                   │
└─────────────────────────────────────────────────────────────────┘
```

### 数据处理流程

1. **数据查询**：从Doris大数据平台查询经营数据
2. **数据处理**：对查询结果进行清洗、转换和特征提取
3. **AI诊断**：调用大模型进行经营异常诊断分析
4. **报告生成**：根据诊断结果生成结构化报告
5. **结果保存**：将结果保存到MySQL中间表
6. **回调通知**：通过HTTP回调将结果通知给客户端

---

## 📁 项目架构与结构

### 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     客户端层 (Client Layer)                     │
├─────────────────┬─────────────────┬───────────────────────────┤
│ 企业管理系统    │ 数据分析平台    │ 其他集成系统             │
└─────────────────┴─────────────────┴───────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                     API网关层 (API Gateway)                  │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                   应用服务层 (Application Layer)              │
├─────────────────┬─────────────────┬───────────────────────────┤
│ 认证授权服务    │ 诊断分析服务    │ 报告生成服务             │
└─────────────────┴─────────────────┴───────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                   数据服务层 (Data Layer)                    │
├─────────────────┬─────────────────┬───────────────────────────┤
│ 数据查询服务    │ AI模型服务      │ 结果处理服务             │
└─────────────────┴─────────────────┴───────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                   数据存储层 (Storage Layer)                 │
├─────────────────┬─────────────────┬───────────────────────────┤
│ MySQL中间表     │ Doris大数据平台 │ 大模型服务               │
└─────────────────┴─────────────────┴───────────────────────────┘
```

### 项目目录结构

```
domestic_analysis_report/
├── config.yaml          # 多环境配置文件
├── app.py               # 应用入口
├── requirements.txt     # Python依赖清单
├── docker-compose.yml   # Docker Compose配置
├── Dockerfile          # Docker镜像构建配置
├── deploy.sh           # Linux/macOS部署脚本
├── deploy.bat          # Windows部署脚本
├── tag_web.py          # 标签修复Web界面（Streamlit应用）
├── src/                # 源代码目录
│   ├── config/           # 配置管理模块
│   │   └── __init__.py   # 配置加载和管理
│   ├── models/           # 数据模型定义
│   │   ├── diagnosis_request.py   # 诊断请求模型
│   │   └── diagnosis_response.py  # 诊断响应模型
│   ├── routers/          # API路由定义
│   │   ├── report_generate.py  # 报告生成路由
│   │   ├── root.py             # 根路由
│   │   └── __init__.py         # 路由注册
│   ├── services/         # 业务逻辑层
│   │   ├── ai_model_service.py           # AI模型服务
│   │   ├── async_data_query_service.py   # 异步数据查询服务
│   │   ├── async_result_service.py       # 异步结果服务
│   │   ├── callback_service.py           # 回调服务
│   │   ├── celery_tasks.py              # Celery任务定义
│   │   └── result_service.py           # 结果服务
│   └── utils/            # 工具类库
│       ├── bearer.py             # Bearer认证工具
│       ├── db_utils.py           # 数据库工具
│       ├── log_utils.py          # 日志工具
│       ├── tag_repair.py         # XML标签验证和修复工具
│       └── time_middleware.py    # 时间中间件
├── data/               # 数据文件目录
│   └── prompts/        # 提示词模板
├── logs/               # 日志文件目录
└── tests/              # 测试文件目录
```

### 核心模块说明

| 模块 | 主要职责 | 文件位置 |
|------|----------|----------|
| 配置管理 | 加载和管理系统配置 | src/config/ |
| 数据模型 | 定义请求和响应数据结构 | src/models/ |
| API路由 | 定义和处理HTTP请求 | src/routers/ |
| 业务服务 | 实现核心业务逻辑 | src/services/ |
| 工具类库 | 提供通用工具函数 | src/utils/ |
| XML标签验证器 | XML标签验证、修复和AI模型调用 | src/utils/tag_repair.py |
| Web界面工具 | 交互式标签验证和修复界面 | tag_web.py |
| 数据文件 | 存储提示词模板和配置文件 | data/ |
| 日志文件 | 存储系统日志 | logs/ |

---

## ⚙️ 配置管理

### 多环境配置

项目采用YAML格式的配置文件，支持test、uat、prod三个环境的配置隔离。核心配置项包括：

- **基本配置**：版本、环境、调试模式、语言、标签修复开关等
- **数据库配置**：MySQL中间表和Doris数据表连接信息，支持SQL语句配置
- **AI模型配置**：Qwen3-32b/Qwen3-235b模型参数，支持多环境URL配置
- **应用配置**：服务地址、端口、日志级别、并发数、模板路径等
- **安全配置**：Bearer Token、速率限制等
- **回调配置**：回调URL和认证信息，支持多环境配置
- **并发控制**：最大并发数、心跳间隔、锁文件路径、超时时间等

### 配置详细说明

#### 1. 基本配置
```yaml
# 基本配置
version: "1.3.5"
environment: "test"
debug: false
language: "zh"
use_fix_tag: true  # 是否使用标签修复功能
```

#### 2. 数据库配置
- **中间表数据库**：用于存储中间结果，支持test/uat/prod环境
- **数据表数据库**：用于存储原始业务数据，支持test/uat/prod环境
- **连接参数**：主机、端口、数据库、用户名、密码、超时时间、最大连接数
- **SQL语句配置**：插入和查询SQL语句可配置

#### 3. AI模型配置
- **Qwen3-32b模型**：支持test/uat/prod环境的base_url配置
- **Qwen3-235b模型**：支持test/uat/prod环境的base_url配置
- **模型参数**：api_key、model_name、max_tokens、temperature、top_p、timeout、max_retries

#### 4. 应用配置
- **服务配置**：host、port、workers、reload
- **路径配置**：data_path、log_file、template_path
- **日志配置**：log_level
- **并发配置**：max_concurrent

#### 5. 安全配置
- **认证配置**：bearer_token
- **速率限制**：requests_per_minute、burst_limit

#### 6. 回调配置
- **环境配置**：test/uat/prod环境的回调URL、bearer_token、timeout

#### 8. Celery配置
- **任务配置**：task_serializer、accept_content、result_serializer
- **时区配置**：timezone、enable_utc
- **超时配置**：task_time_limit（硬超时）、task_soft_time_limit（软超时）
- **Worker配置**：worker_prefetch_multiplier、worker_max_tasks_per_child
- **任务确认**：task_acks_late、task_reject_on_worker_lost
- **结果配置**：result_expires、task_send_sent_event、task_send_event

### 配置示例

```yaml
# 基本配置
version: "1.3.5"
environment: "test"
debug: false
language: "zh"
use_fix_tag: true

# 数据库配置
intermediate_db:
  test:
    host: "10.1.253.215"
    port: 3306
    database: "finoa"
    username: "eleinvoice_user"
    password: "Dwjks_2490sHW"
    table_name: "tb_bij_operation_diagnosis_req_result"
    timeout: 30
    max_connections: 10
    # SQL语句配置
    insert_sql: "INSERT INTO tb_bij_operation_diagnosis_req_result (req_id, req_param, resp_result, create_time, update_time) VALUES (%(req_id)s, %(req_param)s, %(resp_result)s, NOW(), NOW())"
    select_sql: "SELECT id, req_id, req_param, resp_result, create_time, update_time FROM tb_bij_operation_diagnosis_req_result WHERE req_id = %(req_id)s ORDER BY create_time DESC LIMIT 1"

data_db:
  test:
    host: "10.1.104.242"
    port: 9030
    database: "finbi_dm"
    username: "FINBI_UAT"
    password: "FIUi9#3698677O"
    table_name: "DM_F_AI_GROSS_ANALYZE_DORIS"
    # 基础查询SQL，会动态添加WHERE条件
    query_sql: "SELECT * FROM DM_F_AI_GROSS_ANALYZE_DORIS"
    timeout: 30
    max_connections: 10

# AI模型配置
models:
  qwen3-32b:
    base_url:
      test: "https://aimarket.dahuatech.com/it/qwen-fast/v1"
      uat: "https://aimarket.dahuatech.com/it/qwen-fast/v1"
      prod: "https://aimarket.dahuatech.com/it/qwen-fast/v1"
    api_key: "k0FG94TBecKxBgip4BQR2jyBO6x8pb2s"
    model_name: "qwen3-32b"
    max_tokens: 40960
    temperature: 0
    top_p: 0.95
    timeout: 600
    max_retries: 3

# 应用配置
app:
  host: "0.0.0.0"
  port: 12576
  workers: 1
  reload: true
  data_path: "data"
  log_level: "INFO"
  log_file: "logs/app.log"
  template_path: "data/model_template.json"
  max_concurrent: 8

# 安全配置
security:
  bearer_token: "yLZJJb8-EBsdUf2IimbGFNkaONMwbZy2WNh5luqpkWk"
  rate_limit:
    requests_per_minute: 60
    burst_limit: 100

# 回调配置
callback:
  test:
    url: "https://finba.dah-demo.com/bij/reqresult/callback"
    bearer_token: ""
    timeout: 30

# Redis配置
redis:
  test:
    host: "10.1.1.80"
    port: 12505
    password: "dahua123"
    db_broker: 1  # Celery broker使用的数据库编号
    db_backend: 2  # Celery result backend使用的数据库编号
    db_status: 3  # 任务状态存储使用的数据库编号
    max_connections: 10
    socket_timeout: 5
    socket_connect_timeout: 5
  uat:
    host: "10.1.1.80"
    port: 12505
    password: "dahua123"
    db_broker: 1
    db_backend: 2
    db_status: 3
    max_connections: 10
    socket_timeout: 5
    socket_connect_timeout: 5
  prod:
    host: "10.1.1.80"
    port: 12505
    password: "dahua123"
    db_broker: 1
    db_backend: 2
    db_status: 3
    max_connections: 20
    socket_timeout: 5
    socket_connect_timeout: 5

# Celery配置
celery:
  task_serializer: "json"
  accept_content: ["json"]
  result_serializer: "json"
  timezone: "Asia/Shanghai"
  enable_utc: true
  task_track_started: true
  task_time_limit: 14400  # 任务硬超时时间(秒) - 4小时
  task_soft_time_limit: 12600  # 任务软超时时间(秒) - 3.5小时
  worker_prefetch_multiplier: 1
  worker_max_tasks_per_child: 100
  task_acks_late: true
  task_reject_on_worker_lost: true
  result_expires: 3600
  task_send_sent_event: true
  task_send_event: true
```

### 环境变量覆盖

可通过环境变量动态覆盖配置文件中的设置：

```bash
# 切换环境
APP_ENVIRONMENT=uat python app.py

# Docker部署时覆盖环境
APP_ENVIRONMENT=prod docker-compose up -d
```

### 配置加载优先级

1. **环境变量**：最高优先级，通过`APP_ENVIRONMENT`指定环境
2. **配置文件**：从`config.yaml`加载对应环境的配置
3. **默认值**：代码中定义的默认值，最低优先级

---

## 🛠️ 技术栈

| 类别 | 技术 | 版本 | 用途 |
|------|------|------|------|
| 后端框架 | FastAPI | 0.104+ | 构建高性能API服务 |
| 编程语言 | Python | 3.10+ | 开发语言 |
| AI模型 | Qwen3-32b/Qwen3-235b | 最新 | 经营异常诊断 |
| 关系型数据库 | MySQL | 8.0+ | 中间结果存储 |
| 大数据平台 | Doris | 最新 | 业务数据存储和查询 |
| 数据库驱动 | PyMySQL | 1.1.0+ | MySQL连接驱动 |
| 配置管理 | Pydantic + PyYAML | 2.5+ | 数据验证和配置管理 |
| 容器化 | Docker + Docker Compose | 20.10+ | 应用容器化和编排 |
| 认证授权 | FastAPI Security | 最新 | API认证和授权 |
| 日志管理 | loguru | 0.7.0+ | 结构化日志记录 |

---

## 📝 开发指南

### 代码规范

- 遵循PEP 8代码规范
- 使用类型注解，提高代码可读性和类型安全性
- 编写详细的日志记录，便于问题追溯
- 使用Git进行版本控制，遵循Git Flow工作流
- 每个函数和方法添加详细的文档字符串

### 开发流程

1. **需求分析**：明确功能需求和技术实现方案
2. **设计阶段**：设计API接口、数据模型和业务逻辑
3. **编码实现**：按照代码规范编写代码
4. **单元测试**：编写并执行单元测试
5. **集成测试**：进行模块间的集成测试
6. **代码评审**：提交代码评审，确保代码质量
7. **部署测试**：在测试环境验证功能

### 添加新功能

1. **添加API接口**：
   - 在`src/routers/`目录下创建或修改路由文件
   - 定义路由路径、请求方法和处理函数
   - 使用Pydantic模型定义请求和响应数据结构
   - 在`src/routers/__init__.py`中注册路由

2. **添加业务逻辑**：
   - 在`src/services/`目录下创建或修改服务文件
   - 实现核心业务逻辑
   - 调用相关工具类和第三方服务

3. **添加数据模型**：
   - 在`src/models/`目录下创建Pydantic模型
   - 定义数据结构和验证规则

4. **更新配置**：
   - 在`config.yaml`中添加必要的配置项
   - 支持多环境配置

5. **更新文档**：
   - 更新API接口文档
   - 更新README.md相关内容

---

## 🔧 后处理的标签修复功能

### 功能概述

系统集成了智能XML标签修复功能，通过AI模型自动检测和修复报告生成过程中可能出现的XML标签不完整问题。该功能确保生成的经营异常诊断报告具有完整的XML标签结构，便于后续的数据解析和处理。

### 核心特性

#### 1. 智能标签验证
- **完整性检测**：自动检测XML标签的匹配情况，包括开始标签、结束标签的对应关系
- **特殊标签规则**：支持对`<current>`、`<accumulate>`等特殊标签的规则验证（只能出现一对）
- **标签交叉检测**：识别并报告标签嵌套交叉的问题
- **错误优先级优化**：智能过滤因重复标签导致的衍生错误

#### 2. AI驱动的标签修复
- **大模型修复**：基于DeepSeek-V3.2大模型，智能修复缺失或多余的标签
- **模板指导修复**：支持参考模板结构进行更准确的修复
- **并发修复能力**：支持对当前报告和累计报告进行并发修复
- **修复结果验证**：修复后自动验证标签完整性，确保修复质量

#### 3. 灵活的配置选项
- **开关控制**：通过`use_fix_tag`配置项控制是否启用标签修复功能
- **环境适配**：支持test/uat/prod多环境配置
- **模型参数可调**：可配置AI模型的超时时间、温度参数等

### 工作流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    报告内容生成                                  │
│  AI模型生成报告 → 提取当前/累计报告内容 → 合并报告内容           │
└─────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                    标签完整性验证                             │
│ 1. 提取所有XML标签 → 2. 验证标签匹配 → 3. 检测特殊标签规则    │
│ 4. 识别标签交叉 → 5. 生成错误报告                             │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                    AI模型修复（如需要）                       │
│ 1. 构建修复提示词 → 2. 调用大模型API → 3. 获取修复结果        │
│ 4. 特殊标签后处理 → 5. 验证修复结果                           │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│                    修复结果整合                               │
│ 合并修复后的当前/累计报告 → 生成最终报告                      │
└─────────────────────────────────────────────────────────────────┘
```

### 配置说明

#### 1. 基本配置
在`config.yaml`中启用标签修复功能：
```yaml
# 基本配置
version: "1.3.5"
environment: "test"
debug: false
language: "zh"
use_fix_tag: true  # 启用标签修复功能
```

#### 2. AI模型配置
标签修复使用独立的AI模型配置：
```yaml
# 标签修复模型配置（在tag_repair.py中硬编码）
model_name: "DeepSeek-V3.2"
api_key: "k0FG94TBecKxBgip4BQR2jyBO6x8pb2s"
base_url: "https://aimarket.dahuatech.com/it/dpsk-v32/v1"
```

#### 3. 修复规则配置
特殊标签规则在代码中定义：
```python
# 只能出现一对的标签
single_pair_tags = {'current', 'accumulate'}

# 修复规则
# 1. <current>和<accumulate>标签只能出现一对
# 2. 开始标签必须在最开头，结束标签必须在最末尾
# 3. 删除中间多余的标签
```

### 核心模块说明

| 模块 | 文件位置 | 主要职责 |
|------|----------|----------|
| XML标签验证器 | `src/utils/tag_repair.py` | 标签提取、验证、错误检测、AI修复 |
| AI模型服务集成 | `src/services/ai_model_service.py` | 在报告生成后调用标签修复 |
| 配置管理 | `src/config/config.py` | 管理`use_fix_tag`配置项 |
| Web界面工具 | `tag_web.py` | 提供交互式的标签验证和修复界面 |

### 使用示例

#### 1. 代码调用示例
```python
from src.utils.tag_repair import XMLTagValidator

# 创建验证器
validator = XMLTagValidator("test")

# 验证标签完整性
text = "<accumulate><summary>累计报告</summary></accumulate>"
result = validator.validate(text)

# 打印错误报告
validator.print_error(result)

# 调用AI模型修复
if not result['is_valid']:
    fix_result = await validator.model_fix_tag(text, template=None, validation_result=result)
    if fix_result['status'] == 'success':
        fixed_text = fix_result['content']
```

#### 2. Web界面使用
系统提供了独立的Web界面用于手动验证和修复标签：
```bash
# 启动标签修复Web界面
streamlit run tag_web.py
```

访问 `http://localhost:8501` 可使用以下功能：
- **文本输入**：粘贴待验证的XML文本
- **模板参考**：提供参考模板结构
- **验证功能**：检测标签完整性并显示错误详情
- **AI修复**：调用大模型自动修复标签问题
- **结果下载**：下载修复后的文本

### 错误类型说明

| 错误类型 | 描述 | 修复策略 |
|----------|------|----------|
| `missing_open` | 缺少开始标签 | 在对应的结束标签前添加开始标签 |
| `missing_close` | 缺少结束标签 | 在合适的位置添加结束标签 |
| `multiple_open` | 开始标签重复 | 删除多余的开始标签，只保留一对 |
| `multiple_close` | 结束标签重复 | 删除多余的结束标签，只保留一对 |
| `tag_crossing` | 标签交叉嵌套 | 调整标签位置使其正确嵌套 |

### 性能考虑

- **并发修复**：当前报告和累计报告可并行修复，提高效率
- **超时控制**：模型调用设置600秒超时，防止长时间阻塞
- **结果缓存**：修复结果可缓存，避免重复修复相同内容
- **资源监控**：记录Token使用情况和响应时间，便于成本控制

### 注意事项

1. **内容保护**：修复过程只修改标签，绝对不修改文本内容
2. **格式保持**：保持原文本的所有空格、换行、缩进完全不变
3. **特殊标签**：`<current>`和`<accumulate>`有特殊规则，修复时需特别注意
4. **自闭合标签**：`</br>`是自闭合标签，不需要配对修复
5. **修复回退**：如果修复失败或修复后仍无效，系统会回退到原始内容

---

## 📊 监控与日志

### 日志配置

系统采用结构化日志记录，支持不同级别日志输出：

- **DEBUG**：详细的调试信息，开发环境使用
- **INFO**：一般信息，生产环境默认级别
- **WARNING**：警告信息，需要关注但不影响系统运行
- **ERROR**：错误信息，影响功能正常运行
- **CRITICAL**：严重错误，导致系统崩溃

日志文件存储在`logs/`目录下，按日期和环境划分，便于日志管理和分析。

### 监控指标

系统提供关键监控指标，包括：
- API请求量和响应时间
- 诊断分析任务执行时间
- AI模型调用次数和性能
- Celery Worker状态和任务队列
- Redis连接状态和任务状态

### 常见问题排查

1. **API返回401错误**：
   - 检查Authorization头是否正确
   - 验证Token是否有效

2. **任务一直处于pending状态**：
   - 检查Celery Worker是否正常运行
   - 使用`celery inspect active`查看活跃任务
   - 检查Redis连接是否正常

3. **任务执行失败**：
   - 查看Worker日志，检查错误信息
   - 检查AI模型调用是否超时
   - 验证数据库连接是否正常

4. **任务处理超时**：
   - 检查数据查询是否耗时过长
   - 验证AI模型调用是否超时
   - 检查网络连接是否正常

---

## 📖 相关文档

| 文档名称 | 路径 | 说明 |
|----------|------|------|
| API文档 | http://localhost:12576/docs | 在线Swagger API文档 |
| 配置说明 | config.yaml | 完整的配置参数说明 |
| 部署脚本 | deploy.sh/deploy.bat | 自动化部署脚本 |

---

## 📞 联系方式

- **项目负责人**：EvanSong
- **技术支持**：[技术支持邮箱]
- **问题反馈**：[问题反馈渠道]

---

*最后更新: 2025-12-24*
# -*- coding: utf-8 -*-
# @Time    : 2025/10/16 11:11
# @Author  : EvanSong
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    from pydantic import BaseSettings

    SettingsConfigDict = dict


class DatabaseConfig(BaseModel):
    """数据库配置"""
    host: str = Field(description="数据库主机地址")
    port: int = Field(ge=1, le=65535, description="数据库端口")
    database: str = Field(description="数据库名称")
    username: str = Field(description="用户名")
    password: str = Field(description="密码")
    table_name: str = Field(description="表名")
    query_sql: str = Field(default="", description="查询SQL语句")
    insert_sql: str = Field(default="", description="插入SQL语句")
    select_sql: str = Field(default="", description="查询SQL语句")
    timeout: int = Field(default=30, gt=0, description="连接超时时间(秒)")
    max_connections: int = Field(default=10, ge=1, description="最大连接数")


class ModelConfig(BaseModel):
    """AI模型配置"""

    # 配置模型设置，解决命名空间冲突警告
    model_config = {"protected_namespaces": ()}

    base_url: str | dict[str, str] = Field(description="API基础URL，可以是字符串或按环境区分的字典")
    api_key: str = Field(description="API密钥")
    model_name: str = Field(description="模型名称")
    max_tokens: int = Field(default=4096, description="最大token数")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="温度参数")
    top_p: float = Field(default=0.95, ge=0.0, le=1.0, description="Top-p采样参数")
    timeout: int = Field(default=120, gt=0, description="请求超时时间(秒)")
    max_retries: int = Field(default=3, ge=0, description="最大重试次数")

    def get_base_url(self, env: str = None) -> str:
        """根据环境获取基础URL

        Args:
            env: 环境名称 (test, uat, prod)，默认使用配置中的当前环境

        Returns:
            对应环境的基础URL
        """
        if isinstance(self.base_url, str):
            return self.base_url.rstrip('/')
        elif isinstance(self.base_url, dict):
            # 如果没有指定环境，返回第一个可用的URL
            if not env:
                for env_key in ['test', 'uat', 'prod']:
                    if env_key in self.base_url and self.base_url[env_key]:
                        return self.base_url[env_key].rstrip('/')
                raise ValueError("没有找到可用的基础URL配置")

            # 根据指定环境返回URL
            if env in self.base_url and self.base_url[env]:
                return self.base_url[env].rstrip('/')
            else:
                raise ValueError(f"环境 '{env}' 的基础URL配置不存在")
        else:
            raise ValueError("base_url配置格式错误")

    @field_validator('base_url')
    @classmethod
    def validate_base_url(cls, v):
        """验证基础URL格式"""
        if isinstance(v, str):
            if not v.startswith(('http://', 'https://')):
                raise ValueError('基础URL必须以http://或https://开头')
            return v.rstrip('/')
        elif isinstance(v, dict):
            # 验证字典中的每个URL
            for env, url in v.items():
                if url and not url.startswith(('http://', 'https://')):
                    raise ValueError(f"环境 '{env}' 的基础URL必须以http://或https://开头")
                if url:
                    v[env] = url.rstrip('/')
            return v
        else:
            raise ValueError('base_url必须是字符串或字典')


class AppConfig(BaseModel):
    """应用配置"""
    host: str = Field(default="127.0.0.1", description="服务器主机")
    port: int = Field(default=8000, ge=1, le=65535, description="服务器端口")
    workers: int = Field(default=1, ge=1, description="工作进程数")
    reload: bool = Field(default=False, description="开发模式自动重载")
    data_path: str = Field(default="data", description="数据文件路径")
    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    log_file: str = Field(default="logs/app.log", description="日志文件路径")
    template_path: str = Field(default="data/template.json", description="底表和明细的提示词")
    max_concurrent: int = Field(default=8, ge=1, description="模型最大并发数")
    system_concurrent: int = Field(default=8, ge=1, description="系统最大并发任务数")


class SecurityConfig(BaseModel):
    """安全配置"""
    bearer_token: str = Field(default="", description="Bearer Token")
    rate_limit: Dict[str, int] = Field(default_factory=dict, description="限流配置")


class CallbackConfig(BaseModel):
    """回调配置"""
    url: str = Field(default="", description="回调URL")
    bearer_token: str = Field(default="", description="Bearer Token")
    timeout: int = Field(default=30, gt=0, description="回调超时时间(秒)")

    def get_url(self, env: str = None) -> str:
        """根据环境获取回调URL

        Args:
            env: 环境名称 (test, uat, prod)，默认使用配置中的当前环境

        Returns:
            对应环境的回调URL
        """
        if isinstance(self.url, str):
            return self.url
        elif isinstance(self.url, dict):
            # 如果没有指定环境，返回第一个可用的URL
            if not env:
                for env_key in ['test', 'uat', 'prod']:
                    if env_key in self.url and self.url[env_key]:
                        return self.url[env_key]
                raise ValueError("没有找到可用的回调URL配置")

            # 根据指定环境返回URL
            if env in self.url and self.url[env]:
                return self.url[env]
            else:
                raise ValueError(f"环境 '{env}' 的回调URL配置不存在")
        else:
            raise ValueError("url配置格式错误")

    def get_bearer_token(self, env: str = None) -> str:
        """根据环境获取Bearer Token

        Args:
            env: 环境名称 (test, uat, prod)，默认使用配置中的当前环境

        Returns:
            对应环境的Bearer Token
        """
        if isinstance(self.bearer_token, str):
            return self.bearer_token
        elif isinstance(self.bearer_token, dict):
            # 如果没有指定环境，返回第一个可用的Token
            if not env:
                for env_key in ['test', 'uat', 'prod']:
                    if env_key in self.bearer_token and self.bearer_token[env_key]:
                        return self.bearer_token[env_key]
                raise ValueError("没有找到可用的Bearer Token配置")

            # 根据指定环境返回Token
            if env in self.bearer_token and self.bearer_token[env]:
                return self.bearer_token[env]
            else:
                raise ValueError(f"环境 '{env}' 的Bearer Token配置不存在")
        else:
            raise ValueError("bearer_token配置格式错误")


class ReportsConfig(BaseModel):
    """报告生成配置"""
    max_file_size_mb: int = Field(default=100, gt=0, description="最大文件大小(MB)")
    preview_rows: int = Field(default=20, ge=1, description="数据预览行数")
    max_preview_chars: int = Field(default=10000, ge=100, description="文本预览字符数")
    supported_formats: list[str] = Field(default_factory=list, description="支持的文件格式")
    default_export_format: str = Field(default="xlsx", description="默认导出格式")
    cache_ttl_minutes: int = Field(default=60, gt=0, description="缓存过期时间(分钟)")


class RedisConfig(BaseModel):
    """Redis配置"""
    host: str = Field(description="Redis主机地址")
    port: int = Field(ge=1, le=65535, description="Redis端口")
    password: str = Field(default="", description="Redis密码")
    db_broker: int = Field(default=1, ge=0, le=15, description="Celery broker使用的数据库编号")
    db_backend: int = Field(default=2, ge=0, le=15, description="Celery result backend使用的数据库编号")
    db_status: int = Field(default=3, ge=0, le=15, description="任务状态存储使用的数据库编号")
    max_connections: int = Field(default=10, ge=1, description="最大连接数")
    socket_timeout: int = Field(default=5, gt=0, description="Socket超时时间(秒)")
    socket_connect_timeout: int = Field(default=5, gt=0, description="Socket连接超时时间(秒)")
    task_status_expire: int = Field(default=1800, gt=0, description="任务状态过期时间(秒)")
    task_cancel_expire: int = Field(default=1800, gt=0, description="任务取消标记过期时间(秒)")

    def get_broker_url(self) -> str:
        """获取Celery broker URL

        Returns:
            Redis broker URL
        """
        if self.password:
            return f'redis://:{self.password}@{self.host}:{self.port}/{self.db_broker}'
        else:
            return f'redis://{self.host}:{self.port}/{self.db_broker}'

    def get_backend_url(self) -> str:
        """获取Celery result backend URL

        Returns:
            Redis backend URL
        """
        if self.password:
            return f'redis://:{self.password}@{self.host}:{self.port}/{self.db_backend}'
        else:
            return f'redis://{self.host}:{self.port}/{self.db_backend}'

    def get_status_url(self) -> str:
        """获取任务状态存储URL

        Returns:
            Redis status URL
        """
        if self.password:
            return f'redis://:{self.password}@{self.host}:{self.port}/{self.db_status}'
        else:
            return f'redis://{self.host}:{self.port}/{self.db_status}'


class CeleryConfig(BaseModel):
    """Celery配置"""
    task_serializer: str = Field(default="json", description="任务序列化格式")
    accept_content: list[str] = Field(default_factory=lambda: ["json"], description="接受的内容类型")
    result_serializer: str = Field(default="json", description="结果序列化格式")
    timezone: str = Field(default="Asia/Shanghai", description="时区")
    enable_utc: bool = Field(default=True, description="启用UTC")
    task_track_started: bool = Field(default=True, description="跟踪任务开始时间")
    task_time_limit: int = Field(default=14400, gt=0, description="任务硬超时时间(秒)")
    task_soft_time_limit: int = Field(default=12600, gt=0, description="任务软超时时间(秒)")
    worker_prefetch_multiplier: int = Field(default=1, ge=1, description="Worker预取倍数")
    worker_max_tasks_per_child: int = Field(default=100, ge=1, description="每个Worker子进程最大任务数")
    task_acks_late: bool = Field(default=True, description="延迟确认任务")
    task_reject_on_worker_lost: bool = Field(default=True, description="Worker丢失时拒绝任务")
    result_expires: int = Field(default=1800, gt=0, description="结果过期时间(秒)")
    task_send_sent_event: bool = Field(default=True, description="发送任务发送事件")
    task_send_event: bool = Field(default=True, description="发送任务事件")


class Settings(BaseSettings):
    """国内分析报告系统配置 - 重构版本"""

    # 配置模型设置
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        env_nested_delimiter = '__'
        case_sensitive = False

    # 基本信息
    version: str = Field(default="1.0.0", description="应用版本")
    environment: str = Field(default="test", description="运行环境")
    debug: bool = Field(default=False, description="调试模式")
    language: str = Field(default="zh", pattern="^(zh|en)$", description="界面语言")
    use_fix_tag: bool = Field(default=False, description="是否使用固定标签")

    # 数据库配置
    intermediate_db: Dict[str, DatabaseConfig] = Field(default_factory=dict, description="中间表数据库配置")
    data_db: Dict[str, DatabaseConfig] = Field(default_factory=dict, description="数据表数据库配置")

    # AI模型配置
    models: Dict[str, ModelConfig] = Field(default_factory=dict, description="模型配置")

    # 应用配置
    app: AppConfig = Field(default_factory=AppConfig, description="应用配置")

    # 安全配置
    security: SecurityConfig = Field(default_factory=SecurityConfig, description="安全配置")

    # 回调配置
    callback: CallbackConfig = Field(default_factory=CallbackConfig, description="回调配置")

    # Redis配置
    redis: Dict[str, RedisConfig] = Field(default_factory=dict, description="Redis配置")

    # Celery配置
    celery: CeleryConfig = Field(default_factory=CeleryConfig, description="Celery配置")

    @classmethod
    def load_from_file(cls, config_path: Path) -> Settings:
        """从YAML文件加载配置

        Args:
            config_path: 配置文件路径

        Returns:
            Settings实例

        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError: 配置文件格式错误
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f) or {}

            # 解析中间表数据库配置
            if 'intermediate_db' in config_data:
                config_data['intermediate_db'] = {
                    env: DatabaseConfig(**db_config)
                    for env, db_config in config_data['intermediate_db'].items()
                }

            # 解析数据表数据库配置
            if 'data_db' in config_data:
                config_data['data_db'] = {
                    env: DatabaseConfig(**db_config)
                    for env, db_config in config_data['data_db'].items()
                }

            # 解析模型配置
            if 'models' in config_data:
                config_data['models'] = {
                    model: ModelConfig(**model_config)
                    for model, model_config in config_data['models'].items()
                }

            # 解析应用配置
            if 'app' in config_data:
                config_data['app'] = AppConfig(**config_data['app'])

            # 解析安全配置
            if 'security' in config_data:
                config_data['security'] = SecurityConfig(**config_data['security'])

            # 解析回调配置
            if 'callback' in config_data:
                # 如果callback配置是字典且有环境键，则使用当前环境的配置
                if isinstance(config_data['callback'], dict) and any(
                        env in config_data['callback'] for env in ['test', 'uat', 'prod']):
                    current_env = config_data.get('environment', 'test')
                    if current_env in config_data['callback']:
                        config_data['callback'] = CallbackConfig(**config_data['callback'][current_env])
                    else:
                        # 如果当前环境不存在，使用第一个可用的环境配置
                        for env in ['test', 'uat', 'prod']:
                            if env in config_data['callback'] and config_data['callback'][env]:
                                config_data['callback'] = CallbackConfig(**config_data['callback'][env])
                                break
                        else:
                            # 如果没有找到任何环境配置，创建默认配置
                            config_data['callback'] = CallbackConfig()
                else:
                    # 如果是旧的单环境配置格式，直接解析
                    config_data['callback'] = CallbackConfig(**config_data['callback'])

            # 解析Redis配置
            if 'redis' in config_data:
                config_data['redis'] = {
                    env: RedisConfig(**redis_config)
                    for env, redis_config in config_data['redis'].items()
                }

            # 解析Celery配置
            if 'celery' in config_data:
                config_data['celery'] = CeleryConfig(**config_data['celery'])

            return cls(**config_data)
        except yaml.YAMLError as e:
            raise ValueError(f"配置文件格式错误: {e}")
        except Exception as e:
            raise ValueError(f"加载配置失败: {e}")

    def save_to_file(self, config_path: Optional[Path] = None) -> None:
        """保存配置到文件

        Args:
            config_path: 配置文件路径，默认使用config.yaml
        """
        path = config_path or Path("config.yaml")
        path.parent.mkdir(parents=True, exist_ok=True)

        # 转换为字典
        config_dict = self.model_dump(mode='json')

        # 将嵌套的模型实例转换为字典
        def convert_to_dict(obj):
            if isinstance(obj, BaseModel):
                return obj.model_dump(mode='json')
            elif isinstance(obj, dict):
                return {k: convert_to_dict(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_dict(item) for item in obj]
            else:
                return obj

        config_dict = convert_to_dict(config_dict)

        with open(path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(
                config_dict,
                f,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False
            )

    def get_database_config(self, db_type: str = "data_db", env: str = None) -> DatabaseConfig:
        """获取数据库配置

        Args:
            db_type: 数据库类型 ("intermediate_db" 或 "data_db"), 默认使用数据表数据库
            env: 环境名称 (test, uat, prod)，默认使用当前环境

        Returns:
            DatabaseConfig实例

        Raises:
            ValueError: 数据库类型或环境配置不存在
        """
        env = env or self.environment

        if db_type == "intermediate_db":
            if env not in self.intermediate_db:
                raise ValueError(f"中间表数据库配置不存在: {env}")
            return self.intermediate_db[env]
        elif db_type == "data_db":
            if env not in self.data_db:
                raise ValueError(f"数据表数据库配置不存在: {env}")
            return self.data_db[env]
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")

    def get_model_config(self, model_name: str = None, env: str = None) -> ModelConfig:
        """获取模型配置

        Args:
            model_name: 模型名称
            env: 环境名称 (test, uat, prod)，默认使用当前环境

        Returns:
            ModelConfig实例

        Raises:
            ValueError: 模型配置不存在
        """
        # 优先从新的环境变量获取模型名称
        env_model_name = os.getenv('APP_MODEL')
        
        # 确定使用的模型名称：环境变量 > 传入参数 > 默认使用第一个配置的模型
        if env_model_name and env_model_name in self.models:
            model_name = env_model_name
        elif not model_name:
            # 如果没有传入模型名称且环境变量没有指定，使用第一个配置的模型
            model_name = next(iter(self.models.keys())) if self.models else "qwen3-32b"
        
        if model_name not in self.models:
            raise ValueError(f"模型配置不存在: {model_name}")

        env = env or self.environment
        model_config = self.models[model_name]

        # 确保返回的配置包含当前环境的正确base_url
        if hasattr(model_config, 'get_base_url'):
            # 创建一个新的配置实例，确保base_url是正确的环境URL
            config_dict = model_config.model_dump()
            # 将base_url替换为对应环境的URL
            if isinstance(config_dict.get('base_url'), dict):
                config_dict['base_url'] = model_config.get_base_url(env)
            return ModelConfig(**config_dict)

        return model_config

    def get_callback_config(self) -> CallbackConfig:
        """获取回调配置
        
        Returns:
            CallbackConfig实例
        """
        return self.callback

    def get_redis_config(self, env: str = None) -> RedisConfig:
        """获取Redis配置

        Args:
            env: 环境名称 (test, uat, prod)，默认使用当前环境

        Returns:
            RedisConfig实例

        Raises:
            ValueError: Redis配置不存在
        """
        env = env or self.environment

        if env not in self.redis:
            raise ValueError(f"Redis配置不存在: {env}")
        return self.redis[env]


@lru_cache()
def get_settings() -> Settings:
    """获取全局配置实例（单例模式）

    Returns:
        Settings实例
    """
    # 使用绝对路径，确保在不同目录下都能找到配置文件
    current_dir = Path(__file__).parent.parent.parent  # 项目根目录
    config_path = current_dir / "config.yaml"

    try:
        if os.path.exists(config_path):
            settings = Settings.load_from_file(config_path)

            # 环境变量覆盖：支持通过环境变量动态设置环境
            env_from_env = os.getenv('APP_ENVIRONMENT')
            if env_from_env and env_from_env in ['test', 'uat', 'prod']:
                settings.environment = env_from_env
                print(f"使用环境变量设置环境: {env_from_env}")

            # 环境变量覆盖：支持通过环境变量动态设置log_level
            log_level_from_env = os.getenv('APP_LOG_LEVEL')
            valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if log_level_from_env and log_level_from_env.upper() in valid_log_levels:
                settings.app.log_level = log_level_from_env.upper()
                print(f"使用环境变量设置日志级别: {log_level_from_env.upper()}")

            # 环境变量覆盖：支持通过环境变量动态设置模型最大并发数
            max_concurrent_from_env = os.getenv('APP_MODEL_CONCURRENT')
            if max_concurrent_from_env:
                try:
                    max_concurrent_value = int(max_concurrent_from_env)
                    if max_concurrent_value >= 1:
                        settings.app.max_concurrent = max_concurrent_value
                        print(f"使用环境变量设置模型最大并发数: {max_concurrent_value}")
                except ValueError:
                    print(f"环境变量APP_MODEL_CONCURRENT值无效: {max_concurrent_from_env}，必须是大于等于1的整数")

            # 环境变量覆盖：支持通过环境变量动态设置系统最大并发任务数
            max_system_concurrent_from_env = os.getenv('MAX_SYSTEM_CONCURRENT')
            if max_system_concurrent_from_env:
                try:
                    max_system_concurrent_value = int(max_system_concurrent_from_env)
                    if max_system_concurrent_value >= 1:
                        settings.app.system_concurrent = max_system_concurrent_value
                        print(f"使用环境变量设置系统最大并发任务数: {max_system_concurrent_value}")
                except ValueError:
                    print(f"环境变量MAX_SYSTEM_CONCURRENT值无效: {max_system_concurrent_from_env}，必须是大于等于1的整数")

            # 环境变量覆盖：支持通过环境变量动态设置Redis配置
            redis_host_from_env = os.getenv('REDIS_HOST')
            redis_port_from_env = os.getenv('REDIS_PORT')
            redis_password_from_env = os.getenv('REDIS_PASSWORD')

            if redis_host_from_env or redis_port_from_env or redis_password_from_env:
                current_env = settings.environment
                if current_env in settings.redis:
                    redis_config = settings.redis[current_env]
                    if redis_host_from_env:
                        redis_config.host = redis_host_from_env
                        print(f"使用环境变量设置Redis主机: {redis_host_from_env}")
                    if redis_port_from_env:
                        try:
                            redis_port_value = int(redis_port_from_env)
                            if 1 <= redis_port_value <= 65535:
                                redis_config.port = redis_port_value
                                print(f"使用环境变量设置Redis端口: {redis_port_value}")
                        except ValueError:
                            print(f"环境变量REDIS_PORT值无效: {redis_port_from_env}，必须是1-65535之间的整数")
                    if redis_password_from_env:
                        redis_config.password = redis_password_from_env
                        print(f"使用环境变量设置Redis密码: ***")

            return settings
        else:
            # 创建默认配置
            settings = Settings()
            settings.save_to_file(config_path)
            return settings
    except Exception as e:
        print(f"加载配置失败: {e}")
        # 返回默认配置
        return Settings()


def reload_settings() -> Settings:
    """重新加载配置

    Returns:
        新的Settings实例
    """
    get_settings.cache_clear()
    return get_settings()

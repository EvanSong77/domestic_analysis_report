# -*- coding: utf-8 -*-
# @Time    : 2025/01/14
# @Author  : EvanSong

from celery import Celery
from src.config.config import get_settings

settings = get_settings()

redis_config = settings.get_redis_config()
celery_config = settings.celery

broker_url = redis_config.get_broker_url()
result_backend = redis_config.get_backend_url()

celery_app = Celery(
    'diagnosis_tasks',
    broker=broker_url,
    backend=result_backend,
    include=['src.services.celery_tasks']
)

celery_app.conf.update(
    task_serializer=celery_config.task_serializer,
    accept_content=celery_config.accept_content,
    result_serializer=celery_config.result_serializer,
    timezone=celery_config.timezone,
    enable_utc=celery_config.enable_utc,
    task_track_started=celery_config.task_track_started,
    task_time_limit=celery_config.task_time_limit,
    task_soft_time_limit=celery_config.task_soft_time_limit,
    worker_prefetch_multiplier=celery_config.worker_prefetch_multiplier,
    worker_max_tasks_per_child=celery_config.worker_max_tasks_per_child,
    task_acks_late=celery_config.task_acks_late,
    task_reject_on_worker_lost=celery_config.task_reject_on_worker_lost,
    result_expires=celery_config.result_expires,
    task_send_sent_event=celery_config.task_send_sent_event,
    task_send_event=celery_config.task_send_event,
)

"""
Модуль для подключения к Redis.
Используется:
- В FastAPI (async) — для WebSocket + Pub/Sub подписок.
- В воркерах (sync) — для публикации сообщений о прогрессе.
"""

import os

# Асинхронный клиент (для FastAPI)
import redis.asyncio as aioredis

# Синхронный клиент (для воркеров)
import redis


# Хост Redis берём из переменной окружения (в docker-compose задан как "redis").
# Если запуск локальный — используем localhost.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = 6379


# ===========================================================
# 1. АСИНХРОННЫЙ КЛИЕНТ (для FastAPI)
# ===========================================================
async def get_redis():
    """
    Возвращает асинхронный клиент Redis.
    Используется в WebSocket-эндпоинте для подписки на каналы.

    decode_responses=True — автоматически декодирует bytes в строки,
    чтобы не делать .decode('utf-8') вручную.
    """
    client = aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        decode_responses=True
    )
    return client


# ===========================================================
# 2. СИНХРОННЫЙ КЛИЕНТ (для воркеров)
# ===========================================================
def get_redis_sync():
    """
    Возвращает синхронный клиент Redis.
    Используется в воркерах (worker_transcribe, worker_summarize)
    для публикации сообщений о статусе обработки.
    """
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )
"""
FastAPI-приложение Meeting Assistant.
Содержит:
- Инициализацию БД при старте
- Producer для Kafka
- Эндпоинт загрузки аудио
- Эндпоинт получения статуса
- WebSocket для real-time уведомлений
"""

import os
import json
import uuid
import asyncio
from fastapi import FastAPI, UploadFile, File, WebSocket, WebSocketDisconnect, HTTPException
from confluent_kafka import Producer

from app.database import init_db, create_meeting, get_meeting
from app.redis_client import get_redis


# ===========================================================
# 1. СОЗДАНИЕ ПРИЛОЖЕНИЯ И KAFKA PRODUCER
# ===========================================================
app = FastAPI(title="Meeting Assistant API", version="1.0")

# Настройка Producer (отправителя сообщений в Kafka).
# bootstrap.servers — адрес брокера. Внутри Docker это "kafka:29092".
conf = {'bootstrap.servers': os.getenv('KAFKA_BROKER', 'kafka:29092')}
producer = Producer(conf)


# ===========================================================
# 2. СОБЫТИЕ СТАРТА — создаём таблицы в БД
# ===========================================================
@app.on_event("startup")
async def startup_event():
    """
    Выполняется один раз при старте контейнера.
    Создаёт таблицу meetings, если её ещё нет.
    """
    print("[STARTUP] Инициализация базы данных...")
    init_db()
    print("[STARTUP] БД готова.")


# ===========================================================
# 3. ЭНДПОИНТ ЗАГРУЗКИ АУДИО
# ===========================================================
@app.post("/upload/audio")
async def upload_audio(file: UploadFile = File(...)):
    """
    Принимает аудиофайл от клиента.

    Шаги:
    1. Генерируем уникальный meeting_id.
    2. Сохраняем файл в папку uploads/.
    3. Создаём запись в БД со статусом "pending".
    4. Отправляем сообщение в Kafka (топик transcription.requests).
    5. Возвращаем клиенту meeting_id.
    """
    # 1. Генерируем уникальный ID встречи
    meeting_id = str(uuid.uuid4())

    # 2. Сохраняем файл
    file_path = f"uploads/{meeting_id}_{file.filename}"
    try:
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения файла: {e}")

    # 3. Создаём запись в БД
    create_meeting(meeting_id, file_path)

    # 4. Отправляем сообщение в Kafka (Producer)
    message = json.dumps({
        "meeting_id": meeting_id,
        "file_path": file_path
    })
    try:
        producer.produce("transcription.requests", value=message)
        producer.flush()  # Заставляем сообщение уйти в Kafka немедленно
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка отправки в Kafka: {e}")

    # 5. Возвращаем ответ
    return {
        "meeting_id": meeting_id,
        "status": "pending",
        "message": "Аудио принято в обработку. Подключитесь к /ws/{meeting_id} для получения результата."
    }


# ===========================================================
# 4. ЭНДПОИНТ ПОЛУЧЕНИЯ СТАТУСА ВСТРЕЧИ
# ===========================================================
@app.get("/meetings/{meeting_id}")
async def get_meeting_status(meeting_id: str):
    """
    Возвращает текущий статус и данные встречи.
    Если встреча не найдена — возвращает 404.
    """
    meeting = get_meeting(meeting_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Встреча не найдена")
    return meeting


# ===========================================================
# 5. КОРНЕВОЙ ЭНДПОИНТ (проверка, что API живой)
# ===========================================================
@app.get("/")
def root():
    return {"status": "ok", "service": "Meeting Assistant"}


# ===========================================================
# 6. WEBSOCKET — REAL-TIME УВЕДОМЛЕНИЯ
# ===========================================================
@app.websocket("/ws/{meeting_id}")
async def websocket_endpoint(websocket: WebSocket, meeting_id: str):
    """
    WebSocket-эндпоинт. Клиент подключается и получает сообщения
    о прогрессе обработки встречи в реальном времени.

    Работает так:
    1. Подключаемся к Redis.
    2. Подписываемся на канал "meeting:{meeting_id}".
    3. Слушаем сообщения. Всё, что публикуют воркеры, отправляем клиенту.
    4. Когда приходит сообщение со status=done или status=error — закрываем соединение.
    """
    await websocket.accept()
    print(f"[WS] Клиент подключился к встрече {meeting_id}")

    redis = await get_redis()
    pubsub = redis.pubsub()
    channel = f"meeting:{meeting_id}"
    await pubsub.subscribe(channel)

    try:
        while True:
            # Получаем сообщение с таймаутом 30 секунд (чтобы не висеть вечно)
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=30.0)
            if message:
                data = message["data"]
                await websocket.send_text(data)

                # Проверяем, не финальное ли это сообщение
                try:
                    parsed = json.loads(data)
                    if parsed.get("status") in ("done", "error"):
                        print(f"[WS] Встреча {meeting_id} завершена, закрываем соединение.")
                        break
                except json.JSONDecodeError:
                    pass

            await asyncio.sleep(0.1)  # Небольшая пауза, чтобы не грузить CPU

    except WebSocketDisconnect:
        print(f"[WS] Клиент отключился от встречи {meeting_id}")
    except Exception as e:
        print(f"[WS] Ошибка в WebSocket: {e}")
    finally:
        # Всегда чистим ресурсы
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis.close()
        try:
            await websocket.close()
        except Exception:
            pass
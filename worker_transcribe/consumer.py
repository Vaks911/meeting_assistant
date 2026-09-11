"""
Worker-1: Транскрипция аудио через Whisper.
Слушает Kafka-топик transcription.requests, транскрибирует аудио
и публикует результат в summarization.requests для второго воркера.
"""

import os
import json
import time
import redis
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faster_whisper import WhisperModel

from app.database import update_meeting


# ===========================================================
# 1. ЗАГРУЗКА МОДЕЛИ WHISPER
# ===========================================================
# "base" — компромисс между скоростью и качеством (около 140 МБ).
# При первом запуске модель скачается с Hugging Face и закэшируется.
# compute_type="int8" — квантование для работы на CPU (быстро, мало памяти).
print("[WHISPER] Загружаю модель (может занять 1-2 минуты при первом запуске)...")
model = WhisperModel("tiny", device="cpu", compute_type="int8")
print("[WHISPER] Модель загружена.")


# ===========================================================
# 2. НАСТРОЙКА KAFKA
# ===========================================================
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BROKER', 'kafka:29092'),
    'group.id': 'transcribe_workers',     # Группа потребителей (уникальная для этого воркера)
    'auto.offset.reset': 'earliest'        # Читаем с самого начала, если нет сохранённого offset
}


# ===========================================================
# 3. СОЗДАНИЕ ТОПИКОВ (если их ещё нет)
# ===========================================================
admin = AdminClient({'bootstrap.servers': conf['bootstrap.servers']})
topics = [
    NewTopic("transcription.requests", num_partitions=1, replication_factor=1),
    NewTopic("summarization.requests", num_partitions=1, replication_factor=1),
]
futures = admin.create_topics(topics)
for topic, f in futures.items():
    try:
        f.result()
        print(f"[KAFKA] Топик '{topic}' готов.")
    except Exception as e:
        # Если топик уже существует — не ошибка
        print(f"[KAFKA] Топик '{topic}': {e}")


# ===========================================================
# 4. ИНИЦИАЛИЗАЦИЯ CONSUMER, PRODUCER И REDIS
# ===========================================================
consumer = Consumer(conf)
consumer.subscribe(["transcription.requests"])

producer = Producer({'bootstrap.servers': conf['bootstrap.servers']})

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    decode_responses=True
)

print("[WORKER] Worker-transcribe запущен, слушаю Kafka...")

# ===========================================================
# 5. ГЛАВНЫЙ ЦИКЛ ОБРАБОТКИ
# ===========================================================
while True:
    msg = consumer.poll(1.0)  # Ждём сообщение до 1 секунды

    # Пустой poll — нормально, продолжаем
    if msg is None:
        continue

    # Ошибка Kafka
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        print(f"[KAFKA] Ошибка: {msg.error()}")
        continue

    # Разбираем сообщение
    try:
        data = json.loads(msg.value())
        meeting_id = data["meeting_id"]
        file_path = data["file_path"]
    except Exception as e:
        print(f"[KAFKA] Ошибка парсинга сообщения: {e}")
        continue

    print(f"[WORKER] Начинаю транскрипцию: {meeting_id}")

    # Обновляем статус в БД и уведомляем клиента
    update_meeting(meeting_id, status="transcribing")
    r.publish(f"meeting:{meeting_id}", json.dumps({
        "status": "transcribing",
        "progress": 0
    }))

    # ===========================================================
    # ТРАНСКРИПЦИЯ
    # ===========================================================
    try:
        segments, info = model.transcribe(file_path, beam_size=5)
        transcription = " ".join(segment.text for segment in segments)
        print(f"[WHISPER] Транскрипция готова ({len(transcription)} символов)")
    except Exception as e:
        print(f"[WHISPER] Ошибка: {e}")
        update_meeting(meeting_id, status="error")
        r.publish(f"meeting:{meeting_id}", json.dumps({
            "status": "error",
            "message": f"Ошибка транскрипции: {e}"
        }))
        continue

    # ===========================================================
    # СОХРАНЯЕМ РЕЗУЛЬТАТ И ОТПРАВЛЯЕМ ДАЛЬШЕ
    # ===========================================================
    update_meeting(meeting_id, transcription=transcription, status="summarizing")
    r.publish(f"meeting:{meeting_id}", json.dumps({
        "status": "summarizing",
        "progress": 50
    }))

    # Отправляем в Kafka для второго воркера
    next_msg = json.dumps({
        "meeting_id": meeting_id,
        "transcription": transcription
    })
    try:
        producer.produce("summarization.requests", value=next_msg)
        producer.flush()
        print(f"[KAFKA] Отправил {meeting_id} в summarization.requests")
    except Exception as e:
        print(f"[KAFKA] Ошибка отправки: {e}")
        update_meeting(meeting_id, status="error")
        r.publish(f"meeting:{meeting_id}", json.dumps({
            "status": "error",
            "message": f"Ошибка отправки в Kafka: {e}"
        }))
"""
Worker-2: Суммаризация транскрипции через GigaChat.
Слушает Kafka, анализирует текст встречи через LLM, извлекает саммари и action items.
"""

import os
import json
import time
import redis
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import ValidationError

from app.database import update_meeting
from app.models import MeetingSummary
from worker_summarize import tools as agent_tools


# ===========================================================
# 1. НАСТРОЙКА KAFKA + СОЗДАНИЕ ТОПИКОВ
# ===========================================================
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BROKER', 'kafka:29092'),
    'group.id': 'summarize_workers',
    'auto.offset.reset': 'earliest'
}

admin = AdminClient({'bootstrap.servers': conf['bootstrap.servers']})
topics = [
    NewTopic("transcription.requests", num_partitions=1, replication_factor=1),
    NewTopic("summarization.requests", num_partitions=1, replication_factor=1),
]
futures = admin.create_topics(topics)
for topic, f in futures.items():
    try:
        f.result()
        print(f"[KAFKA] Topic '{topic}' ready.")
    except Exception as e:
        print(f"[KAFKA] Topic '{topic}': {e}")

consumer = Consumer(conf)
consumer.subscribe(["summarization.requests"])

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    decode_responses=True
)

GIGACHAT_CREDENTIALS = os.getenv("GIGACHAT_CREDENTIALS", "").strip()


# ===========================================================
# 2. INIT GIGACHAT
# ===========================================================
giga_client = None
if GIGACHAT_CREDENTIALS:
    try:
        from gigachat import GigaChat
        giga_client = GigaChat(
            credentials=GIGACHAT_CREDENTIALS,
            verify_ssl_certs=False,
            scope="GIGACHAT_API_PERS",
        )
        print("[LLM] GigaChat initialized.")
    except Exception as e:
        print(f"[LLM] GigaChat init error: {e}")
        giga_client = None
else:
    print("[LLM] GIGACHAT_CREDENTIALS not set - using stub mode.")


# ===========================================================
# 3. LLM CALL
# ===========================================================
def call_llm(transcription: str) -> MeetingSummary:
    """
    Запускает агентный цикл с function calling:

    1. Отправляем промпт + список доступных функций.
    2. Если модель запросила вызов функции — выполняем инструмент,
       возвращаем результат модели и повторяем шаг 1.
    3. Когда модель дала финальный ответ без вызова функции — парсим JSON.
    """
    if giga_client is None:
        print("[LLM] GigaChat unavailable, using stub.")
        return MeetingSummary(
            summary=f"[STUB] Transcription length: {len(transcription)} chars.",
            key_points=["GigaChat is not connected."],
            action_items=[]
        )

    system_prompt = (
        "You are an assistant for analyzing business meetings in Russian. "
        "The transcription may contain speech-recognition errors and noise; "
        "infer the meaning where possible instead of complaining about quality. "
        "Answer STRICTLY in Russian. "
        "You have access to tools (functions) and may call them when helpful: "
        "for example, create calendar events for action items with deadlines, "
        "send a Telegram alert about the meeting, or fetch meeting statistics. "
        "After using any tools, ALWAYS finish by returning the final structured JSON. "
        "Return STRICTLY JSON without markdown, "
        "without comments, without text before or after JSON. "
        "Response format:\n"
        "{\n"
        '  "summary": "brief meeting summary (2-3 sentences)",\n'
        '  "key_points": ["key point 1", "key point 2"],\n'
        '  "action_items": [\n'
        '    {"task": "what to do", "assignee": "who", "deadline": "when"}\n'
        "  ]\n"
        "}\n"
        "If assignee or deadline is missing, set value to 'not specified'."
    )

    user_prompt = f"Transcription of the meeting:\n\n{transcription}\n\nReturn JSON."

    from gigachat.models import Chat, Messages, MessagesRole

    functions = agent_tools.get_function_schemas()
    messages = [
        Messages(role=MessagesRole.SYSTEM, content=system_prompt),
        Messages(role=MessagesRole.USER, content=user_prompt),
    ]

    # Агентный цикл с защитой от бесконечного вызова функций
    MAX_TOOL_CALLS = 5
    raw = None
    for _ in range(MAX_TOOL_CALLS + 1):
        try:
            response = giga_client.chat(Chat(
                model=os.getenv("GIGACHAT_MODEL", "GigaChat-3-Ultra"),
                messages=messages,
                functions=functions,
                function_call="auto",
                temperature=0.3,
                max_tokens=2000,
            ))
        except Exception as e:
            print(f"[LLM] GigaChat request error: {e}")
            raise

        msg = response.choices[0].message

        # Модель решила вызвать функцию
        if getattr(msg, "function_call", None):
            name = msg.function_call.name
            args = agent_tools.parse_arguments(getattr(msg.function_call, "arguments", None))
            print(f"[TOOL] GigaChat вызывает: {name}({args})")

            result = agent_tools.execute_tool(name, args)
            print(f"[TOOL] Результат: {result}")

            # Возвращаем модели контекст: сообщение ассистента + результат функции
            messages.append(Messages(
                role=MessagesRole.ASSISTANT,
                content=msg.content,
                function_call=msg.function_call,
            ))
            messages.append(Messages(
                role=MessagesRole.FUNCTION,
                name=name,
                content=json.dumps(result, ensure_ascii=False),
            ))
            continue

        # Финальный ответ без вызова функции
        raw = (msg.content or "").strip()
        break

    if raw is None:
        raise RuntimeError("GigaChat did not produce a final answer.")

    print(f"[LLM] GigaChat response: {raw[:300]}...")

    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[LLM] JSON parse error: {e}\nRaw: {raw}")
        raise

    return MeetingSummary.model_validate(data)


print("[WORKER] Worker-summarize started, listening Kafka...")


# ===========================================================
# 4. MAIN LOOP
# ===========================================================
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        print(f"[KAFKA] Error: {msg.error()}")
        continue

    try:
        data = json.loads(msg.value())
        meeting_id = data["meeting_id"]
        transcription = data["transcription"]
    except Exception as e:
        print(f"[KAFKA] Parse error: {e}")
        continue

    print(f"[WORKER] Summarizing: {meeting_id}")

    r.publish(f"meeting:{meeting_id}", json.dumps({
        "status": "summarizing",
        "progress": 70
    }))

    try:
        result = call_llm(transcription)
    except Exception as e:
        print(f"[LLM] Fatal error: {e}")
        update_meeting(meeting_id, status="error")
        r.publish(f"meeting:{meeting_id}", json.dumps({
            "status": "error",
            "message": f"LLM error: {str(e)}"
        }))
        continue

    action_items_json = json.dumps(
        [item.model_dump() for item in result.action_items],
        ensure_ascii=False
    )

    update_meeting(
        meeting_id,
        status="done",
        summary=result.summary,
        action_items=action_items_json
    )

    final_message = {
        "status": "done",
        "summary": result.summary,
        "key_points": result.key_points,
        "action_items": [item.model_dump() for item in result.action_items]
    }
    r.publish(f"meeting:{meeting_id}", json.dumps(final_message, ensure_ascii=False))

    print(f"[WORKER] Meeting {meeting_id} done. Action items: {len(result.action_items)}")